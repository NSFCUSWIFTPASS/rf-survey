import asyncio
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from websockets.asyncio.client import ClientConnection

from zmsclient.zmc.client_asyncio import ZmsZmcClientAsyncio
from zmsclient.zmc.v1.models import (
    Subscription,
    EventFilter,
    MonitorState,
    MonitorStatus,
    MonitorOpStatus,
    MonitorPending,
    UpdateMonitorStateOpStatus,
    Error,
    Event,
    AnyObject,
)

from rf_survey.commands import (
    MonitorCommand,
    ProcessReconfigurationCommand,
)
from rf_survey.zms_event_subscriber import ZmsEventSubscriber
from rf_survey.types import ReconfigurationCallback

EVENT_SOURCETYPE_ZMC = 2
EVENT_CODE_MONITOR = 2005
EVENT_CODE_MONITOR_STATE = 2009
EVENT_CODE_MONITOR_PENDING = 2010
EVENT_CODE_MONITOR_ACTION = 2011
EVENT_CODE_MONITOR_TASK = 2012

MONITOR_EVENT_CODES = {
    EVENT_CODE_MONITOR_PENDING,
}

logger = logging.getLogger(__name__)


class ZmsMonitor:
    def __init__(
        self,
        monitor_id: str,
        element_id: str,
        user_id: str,
        zmc_client: ZmsZmcClientAsyncio,
        reconfiguration_callback: ReconfigurationCallback,
    ):
        self._status_queue = asyncio.Queue()
        self._command_queue = asyncio.Queue()
        self.zmc_client = zmc_client
        self.reconfiguration_callback = reconfiguration_callback

        self.monitor_id = monitor_id
        self.user_id = user_id
        self.element_id = element_id

        # State managed by the loop
        self._op_status: MonitorOpStatus = MonitorOpStatus.ACTIVE
        self._current_parameters: Optional[AnyObject] = None
        self._status_ack_by: Optional[datetime] = None
        self._last_pending_id_to_ack: Optional[str] = None
        self._last_pending_outcome: Optional[int] = None
        self._last_pending_message: Optional[str] = None

    async def run(self):
        logger.info("ZmsMonitor task starting...")

        try:
            # Check Zms for our monitor state and send initial heartbeat
            if not await self._initialize_state():
                logger.error("Failed to initialize monitor state. Shutting down.")
                return

            async with asyncio.TaskGroup() as monitor_tg:
                monitor_tg.create_task(self._state_machine_loop())
                monitor_tg.create_task(self._event_listener_loop())

        except asyncio.CancelledError:
            logger.info("Monitor task cancelled.")

        except Exception as e:
            logger.critical(
                f"A critical error occurred in ZmsMonitor: {e}", exc_info=True
            )
            raise

        finally:
            logger.info("ZmsMonitor has shut down.")

    async def _initialize_state(self) -> bool:
        """
        Fetches the initial Monitor object from OpenZMS, determines the target
        config via (pending or state), applies that configuration, and
        sends the first heartbeat.
        """
        logger.info(f"Fetching initial state for monitor {self.monitor_id}...")
        try:
            response = await self.zmc_client.get_monitor(
                monitor_id=self.monitor_id, elaborate=True
            )
            monitor = response.parsed
            if isinstance(monitor, Error):
                logger.error(f"Failed to get monitor object: {monitor.error}")
                return False

            target_config = None
            pending_id_to_ack = None

            if monitor.pending and monitor.pending.id != monitor.state.last_pending_id:
                logger.info(
                    f"Found unacknowledged pending config (ID: {monitor.pending.id}). "
                    "Using it as the target config for initialization."
                )
                target_config = monitor.pending
                pending_id_to_ack = monitor.pending.id
            else:
                logger.info(
                    "Using current monitor state as the target config for initialization."
                )
                target_config = monitor.state

            # Determine target status and parameters
            target_status = target_config.status
            target_parameters = getattr(target_config, "parameters", None)
            params_dict = target_parameters.to_dict() if target_parameters else None

            logger.info(
                f"Applying initial configuration. Target status: '{target_status.value}'"
            )

            # Set operational status based on the target
            self._update_op_status_from_target(target_status)

            await self.reconfiguration_callback(target_status, params_dict)

            # Apply parameters from the target
            if target_parameters:
                logger.info(f"Applied new parameters: {target_parameters.to_dict()}")
                self._current_parameters = target_parameters

            # Setup the acks for the pending event
            if pending_id_to_ack:
                self._prepare_for_ack(
                    pending_id_to_ack, 0, "Configuration applied successfully."
                )

            logger.info(
                f"Sending initial heartbeat with op_status '{self._op_status.value}'."
            )
            await self._send_heartbeat()

            return True

        except Exception as e:
            logger.error(f"Failed during state initialization: {e}", exc_info=True)
            return False

    async def _state_machine_loop(self):
        """The main loop that waits for commands or heartbeat timeouts."""
        try:
            while True:
                try:
                    await self._run_next_cycle()

                except Exception as e:
                    logger.error(
                        f"Error in state machine loop iteration: {e}. Retrying in 10s.",
                    )
                    await asyncio.sleep(10)

        except asyncio.CancelledError:
            logger.info("State machine loop was cancelled.")

        finally:
            logger.info("State machine loop has shut down.")

    async def _run_next_cycle(self):
        time_until_ack_by = 0

        # Determine the deadline for the next heartbeat
        if self._status_ack_by:
            now = datetime.now(timezone.utc)
            diff = (self._status_ack_by - now).total_seconds()
            time_until_ack_by = max(0.0, diff)

        try:
            command = await asyncio.wait_for(
                self._command_queue.get(), timeout=time_until_ack_by
            )

            # If we get here, a command was received before the timeout.
            await self._process_command(command)

        except asyncio.TimeoutError:
            # This is the expected outcome when the heartbeat timer expires.
            logger.debug("Heartbeat interval expired. Sending heartbeat.")
            await self._send_heartbeat()

    async def _event_listener_loop(self):
        logger.info("Starting WebSocket listener...")

        try:
            filter = EventFilter(element_ids=[self.element_id], user_ids=[self.user_id])
            subscription_config = Subscription(id=str(uuid.uuid4()), filters=[filter])

            adapter = ZmsEventAdapter(
                self.zmc_client,
                self._command_queue,
                self.monitor_id,
                subscription=subscription_config,
                reconnect_on_error=True,
            )

            await adapter.run_async()

        except asyncio.CancelledError:
            logger.info("Event listener loop was cancelled.")

        except Exception as e:
            logger.critical(f"WebSocket listener failed critically: {e}", exc_info=True)

    async def _process_command(self, command: MonitorCommand):
        logger.debug(f"Received COMMAND: {command}")

        match command:
            case ProcessReconfigurationCommand(pending=pending_config):
                target_status = getattr(pending_config, "status", None)
                if not target_status:
                    logger.error(
                        f"Received invalid MonitorPending object with no status. Ignoring: {pending_config}"
                    )
                    return

                pending_id = getattr(pending_config, "id", None)
                if not pending_id:
                    logger.error(
                        f"Received invalid MonitorPending object with no id. Ignoring: {pending_config}"
                    )
                    return

                target_parameters = getattr(pending_config, "parameters", None)
                params_dict = target_parameters.to_dict() if target_parameters else None

                self._update_op_status_from_target(target_status)

                logger.info(
                    f"Processing reconfiguration for MonitorPending ID {pending_id}. "
                    f"New target status: {target_status}"
                )

                try:
                    # If we get here, the configuration was successful.
                    await self.reconfiguration_callback(target_status, params_dict)

                    if target_parameters:
                        logger.info(
                            f"Applied new parameters: {target_parameters.to_dict()}"
                        )
                        self._current_parameters = target_parameters

                    self._prepare_for_ack(
                        pending_id, 0, "Configuration applied successfully"
                    )

                except Exception as e:
                    # The configuration logic failed.
                    logger.error(
                        f"Failed to apply new configuration: {e}", exc_info=True
                    )
                    self._prepare_for_ack(
                        pending_id, 1, f"Failed to apply configuration: {e}"
                    )

                await self._send_heartbeat()

            case _:
                logger.warning(f"Received an unhandled command type: {type(command)}")

    async def _send_heartbeat(self):
        """Constructs and sends a heartbeat PUT request to the ZMS API."""
        body = UpdateMonitorStateOpStatus(
            op_status=self._op_status, parameters=self._current_parameters
        )

        if self._last_pending_id_to_ack:
            body.last_pending_id = self._last_pending_id_to_ack
            body.last_pending_outcome = self._last_pending_outcome
            body.last_pending_message = self._last_pending_message

        logger.debug(f"Sending heartbeat: {body.to_dict()}")
        response = await self.zmc_client.update_monitor_state_op_status(
            monitor_id=self.monitor_id, body=body
        )
        state = response.parsed
        if isinstance(state, MonitorState):
            # Successfully sent, update the next deadline
            if state.status_ack_by:
                self._status_ack_by = state.status_ack_by
                logger.debug(
                    f"Next heartbeat due by: {self._status_ack_by.isoformat()}"
                )
            else:
                logger.debug("No next heartbeat required by ZMS for now.")

            self._clear_ack_state()

        else:
            # If there is some error set an ackby to 30s in future
            self._status_ack_by = datetime.now(timezone.utc) + timedelta(seconds=30)
            logger.error(
                f"Heartbeat failed. Server response: {state.error if isinstance(state, Error) else 'Unknown'}"
            )

    def _update_op_status_from_target(self, target_status: MonitorStatus) -> None:
        if target_status == MonitorStatus.PAUSED:
            self._op_status = MonitorOpStatus.PAUSED
        else:
            # For any other target status (ACTIVE, DEGRADED, DOWN),
            # if the monitor is running, its operational status is ACTIVE.
            self._op_status = MonitorOpStatus.ACTIVE

    def _prepare_for_ack(self, pending_id: str, outcome: int, message: str) -> None:
        self._last_pending_id_to_ack = pending_id
        self._last_pending_outcome = outcome
        self._last_pending_message = message

    def _clear_ack_state(self) -> None:
        self._last_pending_id_to_ack = None
        self._last_pending_outcome = None
        self._last_pending_message = None


class ZmsEventAdapter(ZmsEventSubscriber):
    def __init__(
        self,
        zmc_client: ZmsZmcClientAsyncio,
        command_queue: asyncio.Queue,
        monitor_id: str,
        **kwargs,
    ):
        super().__init__(zmsclient=zmc_client, **kwargs)
        self._command_queue = command_queue
        self.monitor_id = monitor_id

    async def on_event(self, ws: ClientConnection, evt: Event, message: bytes | str):
        if evt.header.source_type != EVENT_SOURCETYPE_ZMC:
            logger.error(
                "on_event: unexpected source type: %r (%r)",
                evt.header.source_type,
                message,
            )
            return

        if evt.header.code not in MONITOR_EVENT_CODES:
            return

        event_monitor_id = None

        if evt.header.code == EVENT_CODE_MONITOR:
            event_monitor_id = getattr(evt.object_, "id", None)
        else:
            event_monitor_id = getattr(evt.object_, "monitor_id", None)

        if event_monitor_id is None:
            logger.warning(
                f"Event with code {evt.header.code} "
                "was missing the required monitor ID attribute. Ignoring."
            )
            return

        if event_monitor_id != self.monitor_id:
            # An event for not our monitor
            return

        match evt.header.code:
            case EVENT_CODE_MONITOR_PENDING:
                if isinstance(evt.object_, MonitorPending):
                    pending_config = evt.object_
                    logger.info(
                        f"Received and queueing reconfiguration command for pending ID: {pending_config.id}"
                    )

                    await self._command_queue.put(
                        ProcessReconfigurationCommand(pending=pending_config)
                    )
                else:
                    logger.warning(
                        "Received a MONITOR_PENDING event but its payload was not a valid "
                        f"MonitorPending object. Type was: {type(evt.object_)}"
                    )


class NullZmsMonitor:
    async def run(self):
        logger.warning("Using NullZmsMonitor. ZMS is disabled.")
        pass
