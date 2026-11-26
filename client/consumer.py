"""Consumer client SDK with socket demultiplexing and automatic broker failover reconnect."""

import json
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from client.discovery import lookup_topic_owner, parse_broker_address
from common.framing import read_frame, write_frame
from common.protocol import Commands, ErrorCodes, MessageId, SubscriptionModes


class ConsumerError(Exception):
    """Base exception for consumer client operations."""
    pass


class ConsumerBusyError(ConsumerError):
    """Raised when an exclusive subscription is already held by another active consumer."""
    pass


@dataclass
class ConsumerMessage:
    """Represents a message delivered to a consumer."""
    message_id: MessageId
    topic: str
    subscription: str
    payload: str
    redelivery_count: int = 0

    @property
    def value(self) -> Any:
        """Attempts to deserialize JSON payload, falling back to raw string."""
        try:
            return json.loads(self.payload)
        except Exception:
            return self.payload


class Consumer:
    """Consumer SDK with background socket demultiplexing and automatic failover reconnect.
    
    Separates unsolicited asynchronous push messages (DELIVER) from
    synchronous command responses (SUBSCRIBE_OK, ACK_OK), and reconnects via
    Coordinator GET_OWNER lookup if the owning broker fails.
    """

    def __init__(
        self,
        broker_host: Optional[str] = None,
        broker_port: Optional[int] = None,
        topic: str = "",
        subscription: str = "",
        mode: str = SubscriptionModes.EXCLUSIVE,
        consumer_id: Optional[str] = None,
        timeout: float = 10.0,
        coordinator_host: Optional[str] = None,
        coordinator_port: Optional[int] = None,
        fallback_brokers: Optional[List[Tuple[str, int]]] = None,
        max_retries: int = 25,
        base_backoff_sec: float = 0.1
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.subscription = subscription
        self.mode = mode
        self.consumer_id = consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
        self.timeout = timeout

        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.fallback_brokers = list(fallback_brokers) if fallback_brokers else []
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self._auto_reconnect = (coordinator_host is not None) or bool(self.fallback_brokers)

        self._sock: Optional[socket.socket] = None
        self._running = threading.Event()
        self._subscribed_event = threading.Event()
        self._receiver_thread: Optional[threading.Thread] = None

        # Queue for incoming DELIVER frames
        self._incoming_messages: queue.Queue[ConsumerMessage] = queue.Queue()
        # Correlation table for RPCs: req_id -> queue.Queue
        self._pending_rpc: Dict[str, queue.Queue] = {}
        self._rpc_lock = threading.Lock()
        self._is_subscribed = False
        self.reconnect_count: int = 0

    def _connect_and_subscribe_once(self, host: str, port: int) -> None:
        """Opens socket to `(host, port)` and completes SUBSCRIBE handshake synchronously."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((host, port))

        subscribe_req = {
            "type": Commands.SUBSCRIBE,
            "req_id": str(uuid.uuid4()),
            "topic": self.topic,
            "subscription": self.subscription,
            "mode": self.mode,
            "consumer_id": self.consumer_id
        }
        write_frame(sock, subscribe_req)
        resp = read_frame(sock)
        if resp is None:
            sock.close()
            raise ConnectionError("Broker closed socket during SUBSCRIBE")

        if resp.get("type") == Commands.ERROR:
            sock.close()
            err_code = resp.get("error_code")
            msg = resp.get("message", "Subscription rejected")
            if err_code == ErrorCodes.CONSUMER_BUSY:
                raise ConsumerBusyError(f"Subscription '{self.subscription}' is busy: {msg}")
            if err_code == ErrorCodes.NOT_OWNER:
                raise ConnectionError(f"NOT_OWNER:{resp.get('owner_address', '')}")
            raise ConsumerError(f"Subscribe failed [{err_code}]: {msg}")

        self.broker_host = host
        self.broker_port = port
        self._sock = sock
        self._is_subscribed = True
        self._subscribed_event.set()

    def _discover_and_subscribe(self) -> bool:
        """Queries Coordinator / fallback brokers with retry + backoff until subscribed."""
        hint_address: Optional[str] = None
        for attempt in range(self.max_retries):
            if not self._running.is_set() and attempt > 0:
                return False

            candidates: List[Tuple[str, int]] = []
            if hint_address:
                try:
                    candidates.append(parse_broker_address(hint_address))
                except Exception:
                    pass
                hint_address = None

            if self.coordinator_host and self.coordinator_port:
                owner = lookup_topic_owner(
                    self.coordinator_host, self.coordinator_port, self.topic, timeout=self.timeout
                )
                if owner and owner not in candidates:
                    candidates.append(owner)

            for fb in self.fallback_brokers:
                if fb not in candidates:
                    candidates.append(fb)

            if self.broker_host and self.broker_port and (self.broker_host, self.broker_port) not in candidates:
                candidates.append((self.broker_host, self.broker_port))

            for host, port in candidates:
                try:
                    self._connect_and_subscribe_once(host, port)
                    return True
                except ConsumerBusyError:
                    raise
                except ConsumerError:
                    raise
                except Exception as e:
                    msg = str(e)
                    if msg.startswith("NOT_OWNER:"):
                        hint_address = msg.split(":", 1)[1] or None
                    continue

            backoff = min(self.base_backoff_sec * (1.5 ** attempt), 0.5)
            time.sleep(backoff)

        return False

    def connect(self) -> None:
        """Connects, registers subscription with the owning broker, and starts receiver thread."""
        self._running.set()
        if not self._auto_reconnect:
            if not self.broker_host or not self.broker_port:
                raise ConnectionError("broker_host and broker_port required when coordinator is not set.")
            try:
                self._connect_and_subscribe_once(self.broker_host, self.broker_port)
            except Exception:
                self._running.clear()
                raise
        else:
            try:
                ok = self._discover_and_subscribe()
            except Exception:
                self._running.clear()
                raise
            if not ok:
                self._running.clear()
                raise ConnectionError(f"Failed to discover and subscribe to '{self.topic}'")

        self._receiver_thread = threading.Thread(
            target=self._receiver_loop,
            name=f"ConsumerRecv-{self.consumer_id}",
            daemon=True
        )
        self._receiver_thread.start()

    def _receiver_loop(self) -> None:
        """Continuously reads frames, demultiplexing DELIVER vs RPC replies and reconnecting on drop."""
        while self._running.is_set():
            sock = self._sock
            if not sock:
                break

            try:
                frame = read_frame(sock)
                if frame is None:
                    raise ConnectionError("Broker socket closed")

                cmd = frame.get("type")
                if cmd == Commands.DELIVER:
                    msg_id_data = frame.get("message_id", {})
                    msg = ConsumerMessage(
                        message_id=MessageId.from_dict(msg_id_data),
                        topic=frame.get("topic", self.topic),
                        subscription=frame.get("subscription", self.subscription),
                        payload=frame.get("payload", ""),
                        redelivery_count=frame.get("redelivery_count", 0)
                    )
                    self._incoming_messages.put(msg)
                else:
                    req_id = frame.get("req_id")
                    with self._rpc_lock:
                        rpc_q = self._pending_rpc.get(req_id)
                    if rpc_q:
                        rpc_q.put(frame)

            except Exception:
                self._is_subscribed = False
                self._subscribed_event.clear()
                try:
                    sock.close()
                except Exception:
                    pass
                self._sock = None

                # Fail fast any in-flight RPC waiters
                with self._rpc_lock:
                    for q in self._pending_rpc.values():
                        try:
                            q.put_nowait({"type": Commands.ERROR, "error_code": "DISCONNECTED", "message": "Broker disconnected"})
                        except Exception:
                            pass

                if not self._running.is_set() or not self._auto_reconnect:
                    break

                # Broker failed -> Reconnect via Coordinator GET_OWNER with retry + backoff
                if self._discover_and_subscribe():
                    self.reconnect_count += 1
                    continue
                break

        self._running.clear()

    def _send_rpc(self, request: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
        """Synchronously transmits an RPC request and waits for matching response by req_id."""
        if not self._sock or not self._running.is_set():
            raise ConnectionError("Consumer is not connected.")

        req_id = request.get("req_id")
        rpc_q: queue.Queue = queue.Queue(maxsize=1)
        with self._rpc_lock:
            self._pending_rpc[req_id] = rpc_q

        try:
            write_frame(self._sock, request)
            wait_time = timeout if timeout is not None else self.timeout
            resp = rpc_q.get(timeout=wait_time)
            return resp
        except queue.Empty:
            raise TimeoutError(f"Timed out waiting for response to request {req_id}")
        finally:
            with self._rpc_lock:
                self._pending_rpc.pop(req_id, None)

    def receive(self, timeout: float = 5.0) -> Optional[ConsumerMessage]:
        """Returns the next message delivered by the broker."""
        if not self._running.is_set() and self._incoming_messages.empty():
            raise ConnectionError("Consumer is not subscribed.")

        try:
            return self._incoming_messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def ack(self, message: Union[ConsumerMessage, MessageId]) -> None:
        """Acknowledges successful message processing (retrying across broker failover if enabled)."""
        msg_id = message.message_id if isinstance(message, ConsumerMessage) else message

        attempts = 5 if self._auto_reconnect else 1
        for _ in range(attempts):
            if self._auto_reconnect and not self._is_subscribed:
                if not self._subscribed_event.wait(timeout=self.timeout):
                    raise ConnectionError("Timed out waiting for consumer reconnect during ack")

            ack_req = {
                "type": Commands.ACK,
                "req_id": str(uuid.uuid4()),
                "topic": self.topic,
                "subscription": self.subscription,
                "message_id": msg_id.to_dict(),
                "consumer_id": self.consumer_id
            }

            try:
                resp = self._send_rpc(ack_req)
            except (ConnectionError, OSError, TimeoutError):
                if not self._auto_reconnect:
                    raise
                time.sleep(0.1)
                continue

            if resp.get("type") == Commands.ERROR:
                err_code = resp.get("error_code")
                if self._auto_reconnect and err_code in ("DISCONNECTED", ErrorCodes.NOT_OWNER, ErrorCodes.FENCED):
                    time.sleep(0.1)
                    continue
                raise ConsumerError(f"Ack failed [{err_code}]: {resp.get('message')}")
            return

    def close(self) -> None:
        """Gracefully disconnects consumer and closes network sockets."""
        self._running.clear()
        self._is_subscribed = False
        self._subscribed_event.clear()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

        if self._receiver_thread and self._receiver_thread.is_alive():
            self._receiver_thread.join(timeout=1.0)

    def __enter__(self) -> "Consumer":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
