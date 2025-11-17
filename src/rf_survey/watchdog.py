import sys
import os
import asyncio
import logging
import time
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class WatchdogTimeoutError(Exception):
    """Raised when the application watchdog times out."""

    pass


class ApplicationWatchdog:
    """
    A multisource watchdog to monitor the liveness of the application.
    Watchdog is disabled if timeout_seconds is None.
    """

    def __init__(
        self,
        timeout_seconds: Optional[float],
    ):
        if timeout_seconds is not None and timeout_seconds <= 0:
            timeout_seconds = None
        self.timeout_seconds = timeout_seconds

        self._last_pet_times: Dict[str, float] = {}
        self._running_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def run(self):
        """
        The main execution loop for the watchdog.
        Checks periodically if the application has recently been "pet".
        """
        if self.timeout_seconds is None:
            logger.info("Application watchdog is disabled by configuration.")
            return

        logger.info(
            f"Application watchdog started with a {self.timeout_seconds:.2f}s timeout."
        )

        check_interval_secs = min(5.0, self.timeout_seconds / 4)

        try:
            while True:
                await asyncio.sleep(check_interval_secs)

                async with self._lock:
                    if not self._running_event.is_set():
                        logger.debug("Watchdog is paused. Skipping liveness check.")
                        continue

                    now = time.monotonic()
                    for source_name, last_pet in self._last_pet_times.items():
                        time_since_last_pet = now - last_pet
                        if time_since_last_pet > self.timeout_seconds:
                            logger.critical(
                                f"WATCHDOG TIMEOUT: Source '{source_name}' has not been pet in {time_since_last_pet:.2f}s "
                                f"(limit: {self.timeout_seconds:.2f}s). Initiating graceful shutdown."
                            )
                            # raise WatchdogTimeoutError

                            sys.stdout.flush()
                            sys.stderr.flush()

                            # Terminate the entire process immediately.
                            # This will bypass the hung thread and allow systemd to restart.
                            # Temporary solution until a better long-term solution for
                            # stopping the hung NFS write in a thread is devised
                            os._exit(1)

        except asyncio.CancelledError:
            logger.info("Watchdog was cancelled.")

        finally:
            logger.info("Application watchdog is shutting down.")

    async def pet(self, source_name: str):
        """
        Resets the watchdog timer, signaling that the application is alive.
        """
        if self.timeout_seconds is None:
            return

        async with self._lock:
            if source_name not in self._last_pet_times:
                logger.info(f"Registering watchdog source {source_name}")

            self._last_pet_times[source_name] = time.monotonic()

    async def pause(self):
        """
        Pauses the watchdog, preventing it from timing out.
        Should be called when the application enters a legitimate long-wait state.
        """
        if self.timeout_seconds is None:
            return

        async with self._lock:
            if self._running_event.is_set():
                logger.warning("Application watchdog is being PAUSED.")
                self._running_event.clear()

    async def start(self):
        """
        Starts the watchdog and resets its timer.
        Should be called when the application leaves a long-wait state.
        """
        if self.timeout_seconds is None:
            return

        async with self._lock:
            if not self._running_event.is_set():
                logger.info("Application watchdog is being STARTED.")
                self._running_event.set()
                now = time.monotonic()
                for source_name in self._last_pet_times:
                    self._last_pet_times[source_name] = now
