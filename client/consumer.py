"""Consumer client SDK with socket demultiplexing for async deliveries and synchronous RPCs."""

import json
import queue
import socket
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

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
    """Consumer SDK with background socket demultiplexing.
    
    Separates unsolicited asynchronous push messages (DELIVER) from
    synchronous command responses (SUBSCRIBE_OK, ACK_OK).
    """

    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        topic: str,
        subscription: str,
        mode: str = SubscriptionModes.EXCLUSIVE,
        consumer_id: Optional[str] = None,
        timeout: float = 10.0
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.subscription = subscription
        self.mode = mode
        self.consumer_id = consumer_id or f"consumer-{uuid.uuid4().hex[:8]}"
        self.timeout = timeout

        self._sock: Optional[socket.socket] = None
        self._running = threading.Event()
        self._receiver_thread: Optional[threading.Thread] = None

        # Queue for incoming DELIVER frames
        self._incoming_messages: queue.Queue[ConsumerMessage] = queue.Queue()
        # Correlation table for RPCs: req_id -> queue.Queue
        self._pending_rpc: Dict[str, queue.Queue] = {}
        self._rpc_lock = threading.Lock()
        self._is_subscribed = False

    def connect(self) -> None:
        """Connects socket, starts demultiplexing receiver, and issues SUBSCRIBE command."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)
        self._sock.connect((self.broker_host, self.broker_port))

        self._running.set()
        self._receiver_thread = threading.Thread(
            target=self._receiver_loop,
            name=f"ConsumerRecv-{self.consumer_id}",
            daemon=True
        )
        self._receiver_thread.start()

        # Send SUBSCRIBE RPC
        req_id = str(uuid.uuid4())
        subscribe_req = {
            "type": Commands.SUBSCRIBE,
            "req_id": req_id,
            "topic": self.topic,
            "subscription": self.subscription,
            "mode": self.mode,
            "consumer_id": self.consumer_id
        }

        resp = self._send_rpc(subscribe_req)
        if resp.get("type") == Commands.ERROR:
            err_code = resp.get("error_code")
            msg = resp.get("message", "Subscription rejected")
            self.close()
            if err_code == ErrorCodes.CONSUMER_BUSY:
                raise ConsumerBusyError(f"Subscription '{self.subscription}' is busy: {msg}")
            raise ConsumerError(f"Subscribe failed [{err_code}]: {msg}")

        self._is_subscribed = True

    def _receiver_loop(self) -> None:
        """Continuously reads frames from socket and demultiplexes DELIVER vs RPC replies."""
        while self._running.is_set():
            try:
                frame = read_frame(self._sock)
                if frame is None:
                    break  # Socket closed

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
                    # Correlated RPC response (SUBSCRIBE_OK, ACK_OK, ERROR)
                    req_id = frame.get("req_id")
                    with self._rpc_lock:
                        rpc_q = self._pending_rpc.get(req_id)
                    if rpc_q:
                        rpc_q.put(frame)

            except Exception:
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
        """Returns the next message delivered by the broker.
        
        Blocks up to `timeout` seconds. Returns None if no message arrives.
        """
        if not self._is_subscribed:
            raise ConnectionError("Consumer is not subscribed.")

        try:
            return self._incoming_messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def ack(self, message: Union[ConsumerMessage, MessageId]) -> None:
        """Acknowledges successful message processing."""
        if isinstance(message, ConsumerMessage):
            msg_id = message.message_id
        else:
            msg_id = message

        req_id = str(uuid.uuid4())
        ack_req = {
            "type": Commands.ACK,
            "req_id": req_id,
            "topic": self.topic,
            "subscription": self.subscription,
            "message_id": msg_id.to_dict(),
            "consumer_id": self.consumer_id
        }

        resp = self._send_rpc(ack_req)
        if resp.get("type") == Commands.ERROR:
            raise ConsumerError(f"Ack failed [{resp.get('error_code')}]: {resp.get('message')}")

    def close(self) -> None:
        """Gracefully disconnects consumer and closes network sockets."""
        self._running.clear()
        self._is_subscribed = False
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
