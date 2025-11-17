import asyncio
import logging
from typing import Any, Dict, Optional
from pydantic import ValidationError
from copy import deepcopy
from pebble import ProcessPool
from concurrent.futures import TimeoutError as FuturesTimeoutError

from rf_shared.nats_client import NatsProducer
from rf_shared.models import MetadataRecord, Envelope
from zmsclient.zmc.v1.models import MonitorStatus

from rf_survey.models import ReceiverConfig, SweepConfig, ApplicationInfo, ProcessingJob
from rf_survey.receiver import Receiver
from rf_survey.validators import ZmsReconfigurationParams
from rf_survey.watchdog import ApplicationWatchdog
from rf_survey.interfaces import IZmsMonitor, IMetrics
from rf_survey.blocking_tasks import process_capture_job_blocking

logger = logging.getLogger(__name__)


class SurveyApp:
    """
    Encapsulates the state and logic for the RF Survey application.
    """

    def __init__(
        self,
        app_info: ApplicationInfo,
        sweep_config: SweepConfig,
        receiver: Receiver,
        producer: NatsProducer,
        watchdog: ApplicationWatchdog,
        zms_monitor: IZmsMonitor,
        metrics: IMetrics,
    ):
        self.app_info = app_info

        self.sweep_config = sweep_config
        self.receiver = receiver
        self.producer = producer
        self.watchdog = watchdog

        self.zms_monitor = zms_monitor
        self._running_event = asyncio.Event()
        self._reconfigure_event = asyncio.Event()
        self._active_sweep_task: Optional[asyncio.Task] = None

        self.metrics = metrics

        self._processing_queue = asyncio.Queue(maxsize=8)
        self.process_pool = ProcessPool(max_workers=2)

    async def start_survey(self):
        """Signals the survey runner to start and resumes the watchdog."""
        logger.info("Survey is being started/resumed.")
        await self.watchdog.start()
        self._running_event.set()

    async def pause_survey(self):
        """Signals the survey runner to pause and pauses the watchdog."""
        logger.warning("Survey is being paused.")
        await self.watchdog.pause()
        self._running_event.clear()

    async def run(self):
        """
        Initializes resources, runs the main application loop, and cleans up.
        """
        try:
            self.receiver.initialize()
            # Store for metadata creation
            self.serial = self.receiver.serial
            await self.producer.connect()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._survey_runner())
                tg.create_task(self._processing_worker())
                tg.create_task(self.zms_monitor.run())
                tg.create_task(self.watchdog.run())
                tg.create_task(self._health_monitor())
                tg.create_task(self.metrics.run())

        except asyncio.CancelledError:
            logger.info("Main application task cancelled. Shutting down gracefully.")

        except Exception as e:
            logger.error(f"Critical error in run loop: {e}", exc_info=True)

        finally:
            logger.info("Cleaning up resources...")
            self.process_pool.close()
            self.process_pool.join()
            await self.producer.close()
            logger.info("Shutdown complete.")

    async def _survey_runner(self):
        """
        A supervisor loop that manages the lifecycle of the sweep task.

        It waits for the application to be in a "running" state, then starts
        a sweep as a cancellable sub-task. If a pause or reconfiguration
        command is received, the `apply_zms_reconfiguration` method will cancel
        the active sweep task, and this loop will gracefully handle the
        cancellation and then re-evaluate the application's state (e.g.,
        it will pause if the running event has been cleared).
        """

        logger.info("Survey runner supervisor started.")
        cycles_run = 0

        try:
            while True:
                target_cycles = self.sweep_config.cycles
                if target_cycles > 0 and cycles_run >= target_cycles:
                    logger.info(
                        f"Completed {target_cycles} configured cycles. Finishing."
                    )
                    break

                # Primary pausing mechanisim
                await self._running_event.wait()
                # Reset reconfigure event
                self._reconfigure_event.clear()

                logger.debug("Starting a new sweep task.")
                sweep_config_snapshot = deepcopy(self.sweep_config)

                self._active_sweep_task = asyncio.create_task(
                    self._perform_sweep(sweep_config_snapshot)
                )

                try:
                    await self._active_sweep_task

                except RuntimeError as e:
                    logger.error(f"Sweep task failed with a hardware error: {e}")
                    logger.warning(
                        "Attempting to recover by re-initializing the receiver."
                    )

                    try:
                        await self.receiver.reconfigure(self.receiver.config)
                        logger.info("Receiver re-initialized successfully.")
                        logger.info("Cooling down for 3 seconds...")
                        await asyncio.sleep(3.0)
                    except Exception as recovery_e:
                        logger.critical(
                            f"FATAL: Failed to recover the receiver: {recovery_e}. Triggering application shutdown.",
                            exc_info=True,
                        )
                        raise

                else:
                    cycles_run += 1
                    logger.debug("Sweep task completed successfully.")

        except asyncio.CancelledError:
            logger.info("Survey runner supervisor task was cancelled. Shutting down.")

        except Exception as e:
            logger.critical(
                f"Critical error in survey runner supervisor: {e}", exc_info=True
            )

        finally:
            if self._active_sweep_task and not self._active_sweep_task.done():
                self._active_sweep_task.cancel()
            logger.info("Survey runner supervisor has shut down.")

    async def _perform_sweep(self, sweep_config: SweepConfig):
        """
        Performs a single sweep across the specified frequency range.
        """
        center_hz = sweep_config.start_hz
        end_hz = sweep_config.end_hz
        step_hz = sweep_config.step_hz

        while center_hz <= end_hz:
            for _ in range(sweep_config.records_per_step):
                if self._reconfigure_event.is_set():
                    logger.info(
                        "Reconfigure detected pre-capture. Gracefully exiting sweep."
                    )
                    return

                wait_duration = sweep_config.next_collection_wait_duration()
                await self._wait_until_next_collection(wait_duration)

                if self._reconfigure_event.is_set():
                    logger.info(
                        "Reconfigure detected post-wait. Gracefully exiting sweep."
                    )
                    return

                # Get the samples from receiver
                # The config is guaranteed to be what ever the capture was configured with
                # due to internal locking
                capture_result = await self.receiver.receive_samples(center_hz)

                # Create a processing job
                job = ProcessingJob(
                    raw_capture=capture_result.raw_capture,
                    receiver_config_snapshot=capture_result.receiver_config,
                    sweep_config_snapshot=sweep_config,
                )

                await self.watchdog.pet("sdr_data_loop")

                try:
                    # Send job to processing task
                    await asyncio.wait_for(self._processing_queue.put(job), timeout=1.0)
                    logger.debug("Successfully queued capture job for processing.")
                except asyncio.TimeoutError:
                    logger.error(
                        "Processing queue is full! The system is backlogged. Dropping capture."
                    )
                    continue

            center_hz += step_hz

    async def _processing_worker(self):
        """
        A consumer task that pulls capture jobs from a queue and
        processes them.
        """
        logger.info("Processing worker started.")

        try:
            while True:
                try:
                    # Get job from the queue
                    job = await asyncio.wait_for(
                        self._processing_queue.get(), timeout=1.0
                    )
                    # Process the job
                    await self._process_single_job(job)
                    await self.watchdog.pet("app_worker_loop")

                except asyncio.TimeoutError:
                    await self.watchdog.pet("app_worker_loop")
                    continue

        except asyncio.CancelledError:
            logger.info("Processing worker task cancelled.")

        finally:
            remaining_jobs = self._processing_queue.qsize()
            if remaining_jobs > 0:
                logger.warning(
                    f"Shutting down with {remaining_jobs} unprocessed jobs in the queue. These will be dropped."
                )

    async def _process_single_job(self, job: ProcessingJob):
        """
        Helper function to process one job.
        """
        try:
            logger.debug(
                f"Processing job for capture at {job.raw_capture.center_freq_hz} Hz..."
            )

            metadata_record = await self._process_capture_job(job)
            await self.publish_metadata(metadata_record)

            logger.debug("Processing job finished successfully.")

        except Exception as e:
            logger.error(f"Failed to process capture job: {e}", exc_info=True)

    async def _process_capture_job(self, job: ProcessingJob) -> MetadataRecord:
        """
        Submits the blocking I/O job to the process pool and awaits its result.
        """
        loop = asyncio.get_running_loop()
        processing_timeout_sec = 15.0

        def schedule_and_get_result():
            future = self.process_pool.schedule(
                function=process_capture_job_blocking,
                args=[job, self.serial, self.app_info],
                timeout=processing_timeout_sec,
            )

            result = future.result()
            return result

        try:
            metadata_record = await loop.run_in_executor(
                None,
                schedule_and_get_result,
            )
            return metadata_record

        except FuturesTimeoutError:
            logger.error(
                f"Processing job timed out after {processing_timeout_sec}s and was terminated. "
            )
            raise RuntimeError("Blocking I/O operation timed out") from None

    async def publish_metadata(self, record: MetadataRecord) -> None:
        logger.info(f"Publishing metadata: {record}")
        envelope = Envelope.from_metadata(record)
        payload = envelope.model_dump_json().encode()

        await self.producer.publish(payload)

    async def _wait_until_next_collection(self, wait_duration: float) -> None:
        logger.info(
            f"Waiting for {wait_duration:.4f} seconds before next collection..."
        )
        await asyncio.sleep(wait_duration)

    async def apply_zms_reconfiguration(
        self, status: MonitorStatus, params: Optional[Dict[str, Any]]
    ) -> None:
        """
        Validates the raw ZMS parameters, then dispatches the configs
        to the sub-components. This is a callback pased to ZMS Monitor task.
        Raises ValueError on validation failure.
        """
        logger.info(f"Validating and applying ZMS reconfiguration: {params}")

        # Pause active surveys until we reconfigure
        await self.pause_survey()

        logger.warning("Signaling active sweep to stop for reconfiguration.")
        self._reconfigure_event.set()

        if self._active_sweep_task and not self._active_sweep_task.done():
            await self._active_sweep_task

        if params:
            try:
                validated_params = ZmsReconfigurationParams(**params)

            except ValidationError as e:
                error_details = e.errors()
                logger.error(f"ZMS parameter validation failed: {error_details}")
                raise ValueError(f"Invalid parameters from ZMS: {error_details}") from e

            new_receiver_config = ReceiverConfig(
                gain_db=validated_params.gain_db,
                duration_sec=validated_params.duration_sec,
                bandwidth_hz=validated_params.bandwidth_hz,
            )

            new_sweep_config = SweepConfig(
                start_hz=validated_params.start_freq_hz,
                end_hz=validated_params.end_freq_hz,
                step_hz=validated_params.bandwidth_hz,
                interval_sec=validated_params.sample_interval,
                # Carry over values that are not set by ZMS
                cycles=self.sweep_config.cycles,
                records_per_step=self.sweep_config.records_per_step,
                max_jitter_sec=self.sweep_config.max_jitter_sec,
            )

            await self.receiver.reconfigure(new_receiver_config)
            self.sweep_config = new_sweep_config

            self.metrics.update_receiver_config(new_receiver_config)
            self.metrics.update_sweep_config(new_sweep_config)

        # Restart surveys if we were not told to pause
        if status != MonitorStatus.PAUSED:
            await self.start_survey()

    async def _health_monitor(self):
        """
        Periodically polls for state that changes continuously and updates metrics.
        """
        logger.info("Health monitor (for polling metrics) started.")
        try:
            while True:
                await asyncio.sleep(30)

                # Poll for temperature
                temp = await self.receiver.get_temperature()
                if temp is not None:
                    self.metrics.update_temperature(temp)

                queue_size = self._processing_queue.qsize()
                self.metrics.update_queue_size(queue_size)

                logger.debug("Polled metrics updated (temp, queue).")

        except asyncio.CancelledError:
            logger.info("Health monitor was cancelled.")
