"""Broker Server handling producer publishing, subscriptions, and dispatch routing."""

import socket
import threading
from typing import Any, Dict, List, Optional, Tuple

from broker.exclusive_dispatcher import ExclusiveDispatcher
from broker.shared_dispatcher import SharedDispatcher
from common.dispatcher_base import BaseDispatcher
from common.framing import read_frame, write_frame
from common.protocol import (
    Commands,
    ErrorCodes,
    MessageId,
    SubscriptionModes,
    make_error,
    make_ok,
)
from common.tcp_service import BaseTCPServer
from storage.log_manager import FencedError
from storage.storage_client import StorageClient


class BrokerServer(BaseTCPServer):
    """Stateless messaging broker node.
    
    Responsibilities:
    - Accepts producer connections (PUBLISH) and writes to storage.
    - Accepts consumer connections (SUBSCRIBE) and assigns them to dispatchers.
    - Manages Exclusive (and Shared) dispatchers with cursor persistence.
    - Routes ACKs from consumers back to the appropriate dispatcher.
    """

    def __init__(
        self,
        host: str,
        port: int,
        storage_host: str,
        storage_port: int,
        broker_id: str = "broker-1",
        fencing_token: int = 1,
        name: str = "BrokerService"
    ):
        super().__init__(host, port, name=name)
        self.broker_id = broker_id
        self.fencing_token = fencing_token
        self.storage_host = storage_host
        self.storage_port = storage_port

        self.storage_client: Optional[StorageClient] = None

        self._dispatcher_lock = threading.Lock()
        # (topic, subscription) -> BaseDispatcher
        self._dispatchers: Dict[Tuple[str, str], BaseDispatcher] = {}
        # socket -> [(topic, subscription, consumer_id)]
        self._socket_consumers: Dict[socket.socket, List[Tuple[str, str, str]]] = {}

    def start(self) -> int:
        """Starts storage connection and begins listening for client connections."""
        self.storage_client = StorageClient(self.storage_host, self.storage_port)
        self.storage_client.connect()
        bound_port = super().start()
        self.logger.info(f"Broker {self.broker_id} connected to storage at {self.storage_host}:{self.storage_port}")
        return bound_port

    def handle_connection(self, client_sock: socket.socket, client_addr: Tuple[str, int]) -> None:
        """Custom connection loop that cleans up subscriptions on client disconnect."""
        try:
            while self._is_running.is_set():
                frame = read_frame(client_sock)
                if frame is None:
                    break

                response = self.handle_request(frame, client_sock)
                if response is not None:
                    write_frame(client_sock, response)
        finally:
            self._cleanup_client_subscriptions(client_sock)

    def _cleanup_client_subscriptions(self, client_sock: socket.socket) -> None:
        """Detaches disconnected consumer from all attached dispatchers."""
        with self._dispatcher_lock:
            subs = self._socket_consumers.pop(client_sock, [])
            for topic, sub_name, consumer_id in subs:
                dispatcher = self._dispatchers.get((topic, sub_name))
                if dispatcher:
                    dispatcher.remove_consumer(consumer_id)

    def handle_request(self, request: Dict[str, Any], client_sock: socket.socket) -> Optional[Dict[str, Any]]:
        cmd = request.get("type")
        req_id = request.get("req_id")

        if cmd == Commands.PUBLISH:
            return self._handle_publish(request, req_id)
        elif cmd == Commands.SUBSCRIBE:
            return self._handle_subscribe(request, req_id, client_sock)
        elif cmd == Commands.ACK:
            return self._handle_ack(request, req_id)

        return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unknown broker command: {cmd}")

    def _handle_publish(self, request: Dict[str, Any], req_id: Optional[str]) -> Dict[str, Any]:
        topic = request.get("topic")
        payload = request.get("payload")

        if not topic or payload is None:
            return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic or payload in PUBLISH request")

        if not self.storage_client:
            return make_error(req_id, ErrorCodes.INTERNAL_ERROR, "Broker not connected to storage")

        try:
            offset = self.storage_client.append(topic, payload, fencing_token=self.fencing_token)
            msg_id = MessageId(topic=topic, offset=offset)

            return make_ok(
                req_id,
                Commands.PUBLISH_OK,
                topic=topic,
                message_id=msg_id.to_dict()
            )
        except FencedError as e:
            self.logger.error(f"Broker {self.broker_id} was fenced out: {e}")
            return make_error(req_id, ErrorCodes.FENCED, f"Broker fenced out: {e}")
        except Exception as e:
            self.logger.exception(f"Unexpected error publishing message: {e}")
            return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

    def _handle_subscribe(
        self,
        request: Dict[str, Any],
        req_id: Optional[str],
        client_sock: socket.socket
    ) -> Dict[str, Any]:
        topic = request.get("topic")
        subscription = request.get("subscription")
        mode = request.get("mode", SubscriptionModes.EXCLUSIVE)
        consumer_id = request.get("consumer_id", "consumer-default")

        if not topic or not subscription:
            return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic or subscription")

        key = (topic, subscription)
        with self._dispatcher_lock:
            dispatcher = self._dispatchers.get(key)
            if not dispatcher:
                if mode == SubscriptionModes.EXCLUSIVE:
                    dispatcher = ExclusiveDispatcher(
                        topic=topic,
                        subscription_name=subscription,
                        storage_client=self.storage_client,
                        fencing_token=self.fencing_token
                    )
                    self._dispatchers[key] = dispatcher
                elif mode == SubscriptionModes.SHARED:
                    dispatcher = SharedDispatcher(
                        topic=topic,
                        subscription_name=subscription,
                        storage_client=self.storage_client,
                        fencing_token=self.fencing_token
                    )
                    self._dispatchers[key] = dispatcher
                else:
                    return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unsupported subscription mode: {mode}")

            attached = dispatcher.add_consumer(consumer_id, client_sock)
            if not attached:
                return make_error(
                    req_id,
                    ErrorCodes.CONSUMER_BUSY,
                    f"Subscription '{subscription}' is EXCLUSIVE and currently busy."
                )

            if client_sock not in self._socket_consumers:
                self._socket_consumers[client_sock] = []
            self._socket_consumers[client_sock].append((topic, subscription, consumer_id))

        return make_ok(
            req_id,
            Commands.SUBSCRIBE_OK,
            topic=topic,
            subscription=subscription,
            mode=mode
        )

    def _handle_ack(self, request: Dict[str, Any], req_id: Optional[str]) -> Dict[str, Any]:
        topic = request.get("topic")
        subscription = request.get("subscription")
        msg_id_data = request.get("message_id")
        consumer_id = request.get("consumer_id", "")

        if not topic or not subscription or not msg_id_data:
            return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic, subscription, or message_id")

        key = (topic, subscription)
        with self._dispatcher_lock:
            dispatcher = self._dispatchers.get(key)

        if not dispatcher:
            return make_error(req_id, ErrorCodes.BAD_REQUEST, f"No active dispatcher for {key}")

        msg_id = MessageId.from_dict(msg_id_data)
        success = dispatcher.on_ack(msg_id, consumer_id)
        if success:
            return make_ok(
                req_id,
                Commands.ACK_OK,
                topic=topic,
                subscription=subscription,
                offset=msg_id.offset
            )
        else:
            return make_error(req_id, ErrorCodes.BAD_REQUEST, "Failed to process ack")

    def stop(self) -> None:
        """Shuts down broker, closes all dispatchers, and closes storage client."""
        super().stop()
        with self._dispatcher_lock:
            for dispatcher in self._dispatchers.values():
                try:
                    dispatcher.close()
                except Exception:
                    pass
            self._dispatchers.clear()

        if self.storage_client:
            try:
                self.storage_client.close()
            except Exception:
                pass
            self.storage_client = None
