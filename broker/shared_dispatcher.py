"""Shared Dispatcher implementation distributing messages across a pool of competing consumers."""

import collections
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Set

from common.cursor import Cursor
from common.dispatcher_base import BaseDispatcher
from common.framing import write_frame
from common.logger import setup_logger
from common.protocol import Commands, MessageId
from broker.unacked_tracker import UnackedTracker
from storage.log_manager import FencedError
from storage.storage_client import StorageClient


class SharedDispatcher(BaseDispatcher):
    """Dispatcher for SHARED subscriptions (Worker Pool pattern).
    
    Guarantees:
    1. Multi-Consumer Scale: Multiple active consumers share the subscription concurrently.
    2. Round-Robin Distribution: Workload is evenly partitioned across available connected consumers.
    3. Resilient Re-pooling: If any consumer disconnects before acknowledging an in-flight message,
       that message is automatically returned to the pending pool and re-dispatched to another worker.
    4. Out-of-Order Compaction: Leverages the Cursor's mark_delete_position and individual_acks.
    """

    def __init__(
        self,
        topic: str,
        subscription_name: str,
        storage_client: StorageClient,
        fencing_token: int = 1,
        save_interval_acks: int = 10,
        ack_timeout_sec: float = 3.0
    ):
        super().__init__(topic, subscription_name)
        self.storage_client = storage_client
        self.fencing_token = fencing_token
        self.save_interval_acks = save_interval_acks
        self.ack_timeout_sec = ack_timeout_sec
        self.logger = setup_logger(f"SharedDispatcher-{topic}-{subscription_name}")

        self._lock = threading.Lock()
        # consumer_id -> socket
        self._consumers: Dict[str, Any] = {}
        self._consumer_order: List[str] = []
        self._rr_index: int = 0

        # Min-heap tracker for timeout redelivery
        self.tracker = UnackedTracker(default_timeout_sec=ack_timeout_sec)

        # In-flight tracking:
        # consumer_id -> set of message offsets in-flight
        self._consumer_in_flight: Dict[str, Set[int]] = collections.defaultdict(set)
        # offset -> raw message dict (payload, redelivery_count)
        self._in_flight_messages: Dict[int, Dict[str, Any]] = {}
        # Queue of messages needing redelivery due to consumer disconnections
        self._requeue_pool: Deque[Dict[str, Any]] = collections.deque()

        # Load cursor from storage upon initialization
        self.cursor: Cursor = self.storage_client.load_cursor(topic, subscription_name)
        self._last_dispatched_offset: int = self.cursor.mark_delete_position
        self.logger.info(
            f"Loaded cursor for shared {subscription_name}: mark_delete={self.cursor.mark_delete_position}"
        )

        self._unpersisted_acks = 0
        self._dispatch_thread: Optional[threading.Thread] = None
        self._is_running = threading.Event()

    def add_consumer(self, consumer_id: str, connection: Any) -> bool:
        """Attaches a new consumer to the shared pool. Always succeeds."""
        with self._lock:
            if consumer_id not in self._consumers:
                self._consumer_order.append(consumer_id)
            self._consumers[consumer_id] = connection
            self.logger.info(
                f"Consumer '{consumer_id}' joined shared subscription '{self.subscription_name}'. "
                f"Total active workers: {len(self._consumers)}"
            )

            if not self._is_running.is_set():
                self._is_running.set()
                self._dispatch_thread = threading.Thread(
                    target=self._run_dispatch_loop,
                    name=f"SharedDispatch-{self.topic}-{self.subscription_name}",
                    daemon=True
                )
                self._dispatch_thread.start()

            return True

    def remove_consumer(self, consumer_id: str) -> None:
        """Detaches a consumer and returns all its unacknowledged messages to the queue."""
        with self._lock:
            if consumer_id in self._consumers:
                del self._consumers[consumer_id]
            if consumer_id in self._consumer_order:
                self._consumer_order.remove(consumer_id)

            # Re-pool all in-flight unacknowledged messages for other workers!
            unacked_offsets = self._consumer_in_flight.pop(consumer_id, set())
            requeued_count = 0
            for offset in sorted(list(unacked_offsets)):
                if offset in self._in_flight_messages and not self.cursor.is_acked(offset):
                    msg_data = self._in_flight_messages.pop(offset)
                    msg_data["redelivery_count"] = msg_data.get("redelivery_count", 0) + 1
                    self._requeue_pool.append(msg_data)
                    requeued_count += 1

            self.logger.info(
                f"Consumer '{consumer_id}' disconnected from shared '{self.subscription_name}'. "
                f"Re-queued {requeued_count} unacked messages."
            )
            self._persist_cursor_now()

    def get_active_consumers(self) -> List[str]:
        with self._lock:
            return list(self._consumers.keys())

    def dispatch(self, message: Dict[str, Any]) -> bool:
        """Delivers a message to the next eligible consumer via round-robin."""
        with self._lock:
            if not self._consumers or not self._consumer_order:
                return False

            num_workers = len(self._consumer_order)
            # Find next connected worker
            for _ in range(num_workers):
                self._rr_index = self._rr_index % num_workers
                consumer_id = self._consumer_order[self._rr_index]
                sock = self._consumers.get(consumer_id)
                self._rr_index = (self._rr_index + 1) % num_workers

                if sock:
                    offset = message["message_id"]["offset"]
                    msg_id = MessageId(self.topic, offset)
                    try:
                        write_frame(sock, message)
                        # Record as in-flight
                        self._consumer_in_flight[consumer_id].add(offset)
                        self._in_flight_messages[offset] = message
                        self.tracker.track(
                            msg_id,
                            consumer_id=consumer_id,
                            payload=message.get("payload")
                        )
                        return True
                    except Exception as e:
                        self.logger.warning(f"Failed to push message to shared consumer {consumer_id}: {e}")
                        # Socket dead, will be removed on disconnect
                        continue

            return False

    def on_ack(self, message_id: MessageId, consumer_id: str) -> bool:
        """Records individual ack, updates cursor, and removes from in-flight tracking."""
        with self._lock:
            offset = message_id.offset

            # Remove from in-flight and unacked tracker
            if consumer_id in self._consumer_in_flight:
                self._consumer_in_flight[consumer_id].discard(offset)
            self._in_flight_messages.pop(offset, None)
            self.tracker.ack(message_id)

            # Advance cursor (mark-delete position & individual acks)
            self.cursor.ack(offset)
            self._unpersisted_acks += 1

            if self._unpersisted_acks >= self.save_interval_acks:
                self._persist_cursor_now()

            return True

    def _persist_cursor_now(self) -> None:
        """Persists cursor to storage node."""
        try:
            self.storage_client.save_cursor(
                self.topic,
                self.subscription_name,
                self.cursor,
                self.fencing_token
            )
            self._unpersisted_acks = 0
            self.logger.debug(
                f"Checkpointed shared cursor {self.subscription_name} at mark-delete {self.cursor.mark_delete_position}"
            )
        except FencedError as e:
            self.logger.error(f"SharedDispatcher fenced out while saving cursor: {e}")
            self._is_running.clear()
        except Exception as e:
            self.logger.error(f"Failed to save shared cursor: {e}")

    def _run_dispatch_loop(self) -> None:
        """Background worker distributing messages from requeue pool, timeout tracker, and storage."""
        while self._is_running.is_set():
            time.sleep(0.01)

            # 1. Check for expired messages from frozen/slow consumers
            expired_entries = self.tracker.expired()
            for exp in expired_entries:
                requeue_frame = {
                    "type": Commands.DELIVER,
                    "topic": self.topic,
                    "subscription": self.subscription_name,
                    "message_id": exp.msg_id.to_dict(),
                    "payload": exp.payload,
                    "redelivery_count": exp.redelivery_count
                }
                with self._lock:
                    if exp.consumer_id in self._consumer_in_flight:
                        self._consumer_in_flight[exp.consumer_id].discard(exp.msg_id.offset)
                    self._in_flight_messages.pop(exp.msg_id.offset, None)
                    self._requeue_pool.append(requeue_frame)

            # 2. First priority: Re-dispatch any unacked messages from disconnected/timed-out workers
            with self._lock:
                has_requeue = len(self._requeue_pool) > 0
                has_workers = len(self._consumers) > 0

            if has_requeue and has_workers:
                with self._lock:
                    msg = self._requeue_pool.popleft() if self._requeue_pool else None
                if msg:
                    success = self.dispatch(msg)
                    if not success:
                        with self._lock:
                            self._requeue_pool.appendleft(msg)
                    continue

            # 2. Second priority: Read new messages from storage
            with self._lock:
                if not self._consumers:
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
                        # No workers available, stop pulling
                        break

                    with self._lock:
                        self._last_dispatched_offset = offset

            except Exception as e:
                self.logger.error(f"Error in shared dispatch loop: {e}")
                time.sleep(0.2)

    def close(self) -> None:
        """Stops dispatch loop and persists cursor."""
        self._is_running.clear()
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=1.0)
        with self._lock:
            self._persist_cursor_now()
