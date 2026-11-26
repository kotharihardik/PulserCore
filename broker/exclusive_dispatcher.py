"""Exclusive Dispatcher implementation guaranteeing single-consumer strict FIFO ordering."""

import threading
import time
from typing import Any, Dict, List, Optional

from common.cursor import Cursor
from common.dispatcher_base import BaseDispatcher
from common.framing import write_frame
from common.logger import setup_logger
from common.protocol import Commands, MessageId
from broker.unacked_tracker import UnackedTracker
from storage.log_manager import FencedError
from storage.storage_client import StorageClient


class ExclusiveDispatcher(BaseDispatcher):
    """Dispatcher for EXCLUSIVE subscriptions.
    
    Guarantees:
    1. Single Active Consumer: At most one consumer can connect. A second consumer
       attempting to subscribe receives CONSUMER_BUSY.
    2. Strict FIFO Ordering: Messages are fetched sequentially starting from
       (cursor.mark_delete_position + 1) and delivered in offset order.
    3. Resumability: When a consumer restarts, it resumes from the next unacknowledged offset.
    """

    def __init__(
        self,
        topic: str,
        subscription_name: str,
        storage_client: StorageClient,
        fencing_token: int = 1,
        save_interval_acks: int = 1,
        ack_timeout_sec: float = 3.0
    ):
        super().__init__(topic, subscription_name)
        self.storage_client = storage_client
        self.fencing_token = fencing_token
        self.save_interval_acks = save_interval_acks
        self.ack_timeout_sec = ack_timeout_sec
        self.logger = setup_logger(f"ExclusiveDispatcher-{topic}-{subscription_name}")

        self._lock = threading.Lock()
        self._active_consumer_id: Optional[str] = None
        self._consumer_sock: Optional[Any] = None

        # Min-heap ack tracker for at-least-once timeout redeliveries
        self.tracker = UnackedTracker(default_timeout_sec=ack_timeout_sec)

        # Load cursor from storage upon initialization
        self.cursor: Cursor = self.storage_client.load_cursor(topic, subscription_name)
        self._last_dispatched_offset: int = self.cursor.mark_delete_position
        self.logger.info(
            f"Loaded cursor for {subscription_name}: mark_delete={self.cursor.mark_delete_position}"
        )

        self._unpersisted_acks = 0
        self._dispatch_thread: Optional[threading.Thread] = None
        self._is_running = threading.Event()

    def add_consumer(self, consumer_id: str, connection: Any) -> bool:
        """Attaches a consumer. Rejects if another consumer is already active."""
        with self._lock:
            if self._active_consumer_id is not None and self._active_consumer_id != consumer_id:
                # Exclusive mode only permits one active consumer!
                return False

            self._active_consumer_id = consumer_id
            self._consumer_sock = connection
            # Reset dispatch pointer to current mark-delete position so unacked messages stream
            self._last_dispatched_offset = self.cursor.mark_delete_position
            self.logger.info(f"Consumer '{consumer_id}' attached to '{self.subscription_name}' at offset {self._last_dispatched_offset}")

            # Start dispatch loop if not already running
            if not self._is_running.is_set():
                self._is_running.set()
                self._dispatch_thread = threading.Thread(
                    target=self._run_dispatch_loop,
                    name=f"Dispatch-{self.topic}-{self.subscription_name}",
                    daemon=True
                )
                self._dispatch_thread.start()

            return True

    def remove_consumer(self, consumer_id: str) -> None:
        """Detaches consumer on disconnect."""
        with self._lock:
            if self._active_consumer_id == consumer_id:
                self.logger.info(f"Consumer '{consumer_id}' disconnected from '{self.subscription_name}'")
                self._active_consumer_id = None
                self._consumer_sock = None
                self._persist_cursor_now()
                self.tracker.clear()
                # Rewind dispatch pointer back to last acknowledged offset
                self._last_dispatched_offset = self.cursor.mark_delete_position

    def get_active_consumers(self) -> List[str]:
        with self._lock:
            if self._active_consumer_id:
                return [self._active_consumer_id]
            return []

    def dispatch(self, message: Dict[str, Any]) -> bool:
        """Direct push of a message to the active consumer."""
        with self._lock:
            if not self._consumer_sock or not self._active_consumer_id:
                return False

            try:
                write_frame(self._consumer_sock, message)
                offset = message["message_id"]["offset"]
                msg_id = MessageId(self.topic, offset)
                self.tracker.track(
                    msg_id,
                    consumer_id=self._active_consumer_id,
                    payload=message.get("payload")
                )
                return True
            except Exception as e:
                self.logger.warning(f"Failed to push message to consumer {self._active_consumer_id}: {e}")
                self._active_consumer_id = None
                self._consumer_sock = None
                self._last_dispatched_offset = self.cursor.mark_delete_position
                return False

    def on_ack(self, message_id: MessageId, consumer_id: str) -> bool:
        """Advances cursor when the consumer acknowledges a message."""
        with self._lock:
            if consumer_id != self._active_consumer_id and self._active_consumer_id is not None:
                self.logger.warning(f"Rejecting ACK from non-active consumer {consumer_id}")
                return False

            self.tracker.ack(message_id)
            self.cursor.ack(message_id.offset)
            self._unpersisted_acks += 1

            if self._unpersisted_acks >= self.save_interval_acks:
                self._persist_cursor_now()

            return True

    def _persist_cursor_now(self) -> None:
        """Checkpoints the in-memory cursor to the durable Storage Node."""
        try:
            self.storage_client.save_cursor(
                self.topic,
                self.subscription_name,
                self.cursor,
                self.fencing_token
            )
            self._unpersisted_acks = 0
            self.logger.debug(
                f"Checkpointed cursor {self.subscription_name} at offset {self.cursor.mark_delete_position}"
            )
        except FencedError as e:
            self.logger.error(f"ExclusiveDispatcher fenced out while saving cursor: {e}")
            self._is_running.clear()
        except Exception as e:
            self.logger.error(f"Failed to save cursor to storage: {e}")

    def _run_dispatch_loop(self) -> None:
        """Continuously pulls new messages from storage and redelivers timed-out messages."""
        while self._is_running.is_set():
            time.sleep(0.01)

            # 1. Check for expired in-flight messages requiring redelivery
            expired_entries = self.tracker.expired()
            for exp in expired_entries:
                deliver_frame = {
                    "type": Commands.DELIVER,
                    "topic": self.topic,
                    "subscription": self.subscription_name,
                    "message_id": exp.msg_id.to_dict(),
                    "payload": exp.payload,
                    "redelivery_count": exp.redelivery_count
                }
                self.dispatch(deliver_frame)

            with self._lock:
                if not self._consumer_sock:
                    continue
                start_offset = self._last_dispatched_offset + 1

            try:
                messages, _ = self.storage_client.read(self.topic, from_offset=start_offset, max_count=50)
                if not messages:
                    continue

                for msg_record in messages:
                    offset = msg_record["offset"]
                    payload = msg_record["payload"]

                    deliver_frame = {
                        "type": Commands.DELIVER,
                        "topic": self.topic,
                        "subscription": self.subscription_name,
                        "message_id": {"topic": self.topic, "offset": offset},
                        "payload": payload,
                        "redelivery_count": 0
                    }

                    success = self.dispatch(deliver_frame)
                    if not success:
                        break  # Consumer disconnected

                    with self._lock:
                        self._last_dispatched_offset = offset

            except Exception as e:
                self.logger.error(f"Error in dispatch loop for {self.subscription_name}: {e}")
                time.sleep(0.2)

    def close(self) -> None:
        """Stops dispatch loop and persists cursor."""
        self._is_running.clear()
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=1.0)
        with self._lock:
            self._persist_cursor_now()
