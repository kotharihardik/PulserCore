"""Broker Server handling producer publishing, subscriptions, dispatch routing, and topic leases."""

import socket
import threading
import time
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
from coordinator.coordinator_client import CoordinatorClient
from coordinator.lease_manager import LeaseError
from storage.log_manager import FencedError
from storage.storage_client import StorageClient


class NotOwnerError(Exception):
    """Raised when this broker does not hold the active lease for a topic."""
    def __init__(self, message: str, owner_address: Optional[str] = None):
        super().__init__(message)
        self.owner_address = owner_address


class BrokerServer(BaseTCPServer):
    """Stateless messaging broker node with lease-based topic ownership.
    
    Responsibilities:
    - Acquires and renews time-bound topic leases from the Ownership Coordinator.
    - Watches standby topics and safely takes over with a higher fencing token (N+1) on failover.
    - Rebuilds topic state (last offset, durable cursors) from decoupled Storage on takeover.
    - Accepts producer connections (PUBLISH) and consumer connections (SUBSCRIBE/ACK).
    - Relinquishes ownership locally if fenced out by Storage or if lease renewal fails.
    """

    def __init__(
        self,
        host: str,
        port: int,
        storage_host: str,
        storage_port: int,
        broker_id: str = "broker-1",
        fencing_token: int = 1,
        ack_timeout_sec: float = 3.0,
        coordinator_host: Optional[str] = None,
        coordinator_port: Optional[int] = None,
        lease_ttl_ms: int = 2000,
        renew_interval_sec: Optional[float] = None,
        watch_topics: Optional[List[str]] = None,
        name: str = "BrokerService"
    ):
        super().__init__(host, port, name=name)
        self.broker_id = broker_id
        self.fencing_token = fencing_token
        self.ack_timeout_sec = ack_timeout_sec
        self.storage_host = storage_host
        self.storage_port = storage_port

        # Coordinator settings (None = legacy single-broker static ownership mode)
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.lease_ttl_ms = lease_ttl_ms
        self.renew_interval_sec = (
            renew_interval_sec
            if renew_interval_sec is not None
            else max(0.05, (lease_ttl_ms / 1000.0) / 3.0)
        )
        self.watch_topics: List[str] = list(watch_topics) if watch_topics else []

        self.storage_client: Optional[StorageClient] = None
        self.coordinator_client: Optional[CoordinatorClient] = None

        self._topic_lock = threading.Lock()
        # topic -> active fencing token held by this broker
        self._topic_tokens: Dict[str, int] = {}
        self._lease_thread: Optional[threading.Thread] = None

        self._dispatcher_lock = threading.Lock()
        # (topic, subscription) -> BaseDispatcher
        self._dispatchers: Dict[Tuple[str, str], BaseDispatcher] = {}
        # socket -> [(topic, subscription, consumer_id)]
        self._socket_consumers: Dict[socket.socket, List[Tuple[str, str, str]]] = {}

    @property
    def coordinator_enabled(self) -> bool:
        return self.coordinator_host is not None and self.coordinator_port is not None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def start(self) -> int:
        """Starts storage & coordinator connections and begins listening for client traffic."""
        self.storage_client = StorageClient(self.storage_host, self.storage_port)
        self.storage_client.connect()

        if self.coordinator_enabled:
            self.coordinator_client = CoordinatorClient(self.coordinator_host, self.coordinator_port)
            self.coordinator_client.connect()

        bound_port = super().start()

        if self.coordinator_enabled:
            self._lease_thread = threading.Thread(
                target=self._lease_maintenance_loop,
                name=f"{self.name}-LeaseLoop",
                daemon=True
            )
            self._lease_thread.start()

        self.logger.info(
            f"Broker {self.broker_id} listening at {self.address} "
            f"(storage={self.storage_host}:{self.storage_port}, coordinator={self.coordinator_enabled})"
        )
        return bound_port

    def acquire_topic(self, topic: str) -> int:
        """Acquires ownership lease for `topic` and rebuilds topic state from Storage."""
        if not self.coordinator_enabled or not self.coordinator_client:
            return self.fencing_token

        try:
            token, _ = self.coordinator_client.acquire_lease(
                topic=topic,
                broker_id=self.broker_id,
                lease_ttl_ms=self.lease_ttl_ms,
                broker_address=self.address
            )
        except LeaseError as e:
            owner = self.coordinator_client.get_owner(topic)
            owner_addr = owner.get("broker_address") if owner else None
            raise NotOwnerError(str(e), owner_address=owner_addr) from e

        with self._topic_lock:
            prev_token = self._topic_tokens.get(topic)
            self._topic_tokens[topic] = token
            if topic not in self.watch_topics:
                self.watch_topics.append(topic)

        # Rebuild state from storage on takeover
        last_off = self.storage_client.last_offset(topic) if self.storage_client else -1
        if prev_token != token:
            self.logger.info(
                f"Broker {self.broker_id} acquired topic '{topic}' with fencing_token={token} "
                f"(rebuilt last_offset={last_off})"
            )
            # Update token on any existing dispatchers or clear stale ones
            with self._dispatcher_lock:
                for (t, _sub), disp in self._dispatchers.items():
                    if t == topic:
                        disp.fencing_token = token

        return token

    def get_owned_token(self, topic: str) -> Optional[int]:
        """Returns the broker's current fencing token for `topic`, or None if not owned."""
        if not self.coordinator_enabled:
            return self.fencing_token
        with self._topic_lock:
            return self._topic_tokens.get(topic)

    def _ensure_topic_owned(self, topic: str) -> int:
        """Returns active fencing token for `topic`, acquiring the lease if unowned."""
        if not self.coordinator_enabled:
            return self.fencing_token

        with self._topic_lock:
            token = self._topic_tokens.get(topic)
        if token is not None:
            return token

        return self.acquire_topic(topic)

    def _demote_topic(self, topic: str, reason: str) -> None:
        """Relinquishes local ownership and stops dispatchers for `topic`."""
        with self._topic_lock:
            removed = self._topic_tokens.pop(topic, None)

        if removed is not None:
            self.logger.warning(f"Broker {self.broker_id} demoted from topic '{topic}': {reason}")

        with self._dispatcher_lock:
            keys_to_close = [k for k in self._dispatchers if k[0] == topic]
            for key in keys_to_close:
                disp = self._dispatchers.pop(key)
                try:
                    disp.close()
                except Exception:
                    pass

    def _lease_maintenance_loop(self) -> None:
        """Background loop renewing owned leases and watching standby topics for failover."""
        while self._is_running.is_set():
            time.sleep(self.renew_interval_sec)
            if not self._is_running.is_set() or not self.coordinator_client:
                break

            # 1. Renew all currently owned topic leases
            with self._topic_lock:
                owned_snapshot = list(self._topic_tokens.items())

            for topic, token in owned_snapshot:
                if not self._is_running.is_set():
                    break
                try:
                    self.coordinator_client.renew_lease(
                        topic=topic,
                        broker_id=self.broker_id,
                        token=token,
                        lease_ttl_ms=self.lease_ttl_ms
                    )
                except LeaseError as e:
                    self._demote_topic(topic, f"lease renewal rejected ({e})")
                except Exception as e:
                    self.logger.error(f"Error renewing lease for '{topic}': {e}")

            # 2. Watch standby topics and take over if owner's lease expired
            with self._topic_lock:
                unowned_watched = [t for t in self.watch_topics if t not in self._topic_tokens]

            for topic in unowned_watched:
                if not self._is_running.is_set():
                    break
                try:
                    owner = self.coordinator_client.get_owner(topic)
                    if owner is None:
                        self.acquire_topic(topic)
                except NotOwnerError:
                    # Another standby broker won the race cleanly
                    pass
                except Exception as e:
                    self.logger.debug(f"Standby watch check failed for '{topic}': {e}")

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
            token = self._ensure_topic_owned(topic)
            offset = self.storage_client.append(topic, payload, fencing_token=token)
            msg_id = MessageId(topic=topic, offset=offset)

            return make_ok(
                req_id,
                Commands.PUBLISH_OK,
                topic=topic,
                message_id=msg_id.to_dict()
            )
        except NotOwnerError as e:
            err = make_error(req_id, ErrorCodes.NOT_OWNER, str(e))
            if e.owner_address:
                err["owner_address"] = e.owner_address
            return err
        except FencedError as e:
            self.logger.error(f"Broker {self.broker_id} was fenced out on '{topic}': {e}")
            self._demote_topic(topic, f"fenced by storage ({e})")
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

        try:
            token = self._ensure_topic_owned(topic)
        except NotOwnerError as e:
            err = make_error(req_id, ErrorCodes.NOT_OWNER, str(e))
            if e.owner_address:
                err["owner_address"] = e.owner_address
            return err

        key = (topic, subscription)
        with self._dispatcher_lock:
            dispatcher = self._dispatchers.get(key)
            if not dispatcher:
                if mode == SubscriptionModes.EXCLUSIVE:
                    dispatcher = ExclusiveDispatcher(
                        topic=topic,
                        subscription_name=subscription,
                        storage_client=self.storage_client,
                        fencing_token=token,
                        ack_timeout_sec=self.ack_timeout_sec
                    )
                    self._dispatchers[key] = dispatcher
                elif mode == SubscriptionModes.SHARED:
                    dispatcher = SharedDispatcher(
                        topic=topic,
                        subscription_name=subscription,
                        storage_client=self.storage_client,
                        fencing_token=token,
                        ack_timeout_sec=self.ack_timeout_sec
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

        if self.coordinator_enabled and self.get_owned_token(topic) is None:
            return make_error(req_id, ErrorCodes.NOT_OWNER, f"Broker {self.broker_id} no longer owns '{topic}'")

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

    def crash(self) -> None:
        """Simulates an abrupt process crash (stops server without releasing coordinator leases)."""
        self.stop(release_leases=False)

    def stop(self, release_leases: bool = True) -> None:
        """Shuts down broker, closes dispatchers, and optionally releases coordinator leases."""
        super().stop()

        if self._lease_thread and self._lease_thread.is_alive():
            self._lease_thread.join(timeout=1.0)

        with self._dispatcher_lock:
            for dispatcher in self._dispatchers.values():
                try:
                    dispatcher.close()
                except Exception:
                    pass
            self._dispatchers.clear()

        if release_leases and self.coordinator_enabled and self.coordinator_client:
            with self._topic_lock:
                owned = list(self._topic_tokens.items())
                self._topic_tokens.clear()
            for topic, token in owned:
                try:
                    self.coordinator_client.release_lease(topic, self.broker_id, token)
                except Exception:
                    pass

        if self.coordinator_client:
            try:
                self.coordinator_client.close()
            except Exception:
                pass
            self.coordinator_client = None

        if self.storage_client:
            try:
                self.storage_client.close()
            except Exception:
                pass
            self.storage_client = None
