import asyncio
import json
import logging
from contextlib import asynccontextmanager

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)
from websockets.asyncio.client import ClientConnection

from zmsclient.zmc.v1.models import Subscription, Event, Error
from zmsclient.zmc.client_asyncio import ZmsZmcClientAsyncio

logger = logging.getLogger(__name__)


class ZmsEventSubscriber:
    def __init__(
        self,
        zmsclient: ZmsZmcClientAsyncio,
        subscription: Subscription,
        reconnect_on_error: bool = False,
    ):
        self.zmsclient = zmsclient
        self.subscription = subscription
        self.reconnect_on_error = reconnect_on_error

    @asynccontextmanager
    async def _subscription_manager(self):
        subscription_id = None
        try:
            logger.info("Creating ZMS subscription...")
            response = await self.zmsclient.create_subscription(body=self.subscription)
            subscription_result = response.parsed

            if isinstance(subscription_result, Error):
                # If we can't even create the subscription, we can't proceed.
                raise RuntimeError(
                    f"Failed to create ZMS subscription: {subscription_result.error}"
                )

            subscription_id = subscription_result.id
            logger.debug(f"Created subscription with id: {subscription_id}")

            yield subscription_id

        finally:
            if subscription_id:
                logger.info(f"Cleaning up subscription {subscription_id}.")
                try:
                    await self.zmsclient.delete_subscription(
                        subscription_id=subscription_id
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to delete subscription {subscription_id}: {e}"
                    )

    async def _listen_for_events(self, subscription_id: str):
        """
        Handles a single WebSocket connection and listens for events.
        """
        ws_url = self._build_ws_url(subscription_id)
        headers = {"X-Api-Token": self.zmsclient.token}
        ws = None

        try:
            ws = await websockets.connect(ws_url, additional_headers=headers)
            logger.info(f"WebSocket connected for {subscription_id}")
            self.on_open(ws)

            async for message in ws:
                evt = self._parse_event(message)
                if evt:
                    await self.on_event(ws, evt, message)
                else:
                    logger.error("Received invalid event data. Closing connection.")
                    break

        except asyncio.CancelledError:
            logger.info(f"WebSocket listener for {subscription_id} cancelled.")
            raise

        finally:
            if ws:
                await ws.close()

    async def run_async(self):
        """
        Manages the subscription resource and the
        reconnect logic for the WebSocket connection.
        """
        logger.info("ZMS Event Subscriber starting...")
        try:
            while True:
                try:
                    async with self._subscription_manager() as sub_id:
                        await self._listen_for_events(sub_id)

                        logger.info("WebSocket connection closed.")

                except (
                    ConnectionClosed,
                    ConnectionClosedError,
                    ConnectionClosedOK,
                ) as e:
                    logger.warning(
                        f"WebSocket connection lost: {type(e).__name__}. Preparing to reconnect."
                    )

                except Exception as e:
                    logger.error(
                        f"An unexpected error occurred in the listener: {e}",
                    )

                if not self.reconnect_on_error:
                    logger.info("Reconnect is disabled. Exiting.")
                    break

                try:
                    logger.info("Attempting to reconnect in 10 seconds...")
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    logger.info("Reconnect wait was cancelled. Shutting down loop.")
                    break

        except asyncio.CancelledError:
            logger.info(
                "Subscription task cancelled. Shutting down the reconnect loop."
            )

        except Exception as e:
            logger.critical(
                f"Subscriber failed critically during setup or loop: {e}", exc_info=True
            )
        finally:
            logger.info("ZMS Event Subscriber has shut down.")

    def _parse_event(self, msg):
        logger.debug(f"Received raw message from WebSocket: {msg!r}")

        if not msg:
            logger.debug("Received an empty message, ignoring.")
            return None

        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            logger.warning(f"Received a message that was not valid JSON: {msg!r}")
            return None

        if not isinstance(data, dict):
            logger.warning(
                f"Received valid JSON, but it was not a dictionary (was {data}). Ignoring."
            )
            return None

        return Event.from_dict(src_dict=data)

    def _build_ws_url(self, id: str):
        ws_url = self.zmsclient._base_url + "/subscriptions/" + id + "/events"
        return ws_url.replace("http", "ws")

    def on_open(self, ws: ClientConnection):
        pass

    async def on_event(self, ws: ClientConnection, evt: Event, message: bytes | str):
        pass
