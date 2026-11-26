"""UnackedTracker module using a min-heap priority queue for efficient timeout redeliveries."""

import heapq
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from common.protocol import MessageId


@dataclass(order=True)
class HeapEntry:
    deadline: float
    # Avoid comparing dictionaries/MessageIds directly if deadlines match
    entry_id: int
    msg_id: MessageId = field(compare=False)
    consumer_id: str = field(compare=False)
    payload: Any = field(default=None, compare=False)


@dataclass
class ExpiredMessage:
    msg_id: MessageId
    consumer_id: str
    payload: Any
    redelivery_count: int


class UnackedTracker:
    """Tracks in-flight messages awaiting acknowledgments using a min-heap.
    
    Guarantees:
    1. Efficient Expiry: O(1) peek at the root of the min-heap to check if any message expired.
    2. Lazy Ack Deletion: Acknowledged messages are recorded in an acked set; removed from heap on pop.
    3. Redelivery Metrics: Tracks total redelivery count per message for monitoring and observability.
    """

    def __init__(self, default_timeout_sec: float = 3.0):
        self.default_timeout_sec = default_timeout_sec
        self._lock = threading.Lock()
        # Min-heap ordered by deadline
        self._heap: List[HeapEntry] = []
        # msg_id -> entry_id (tracks current active version in heap)
        self._active_entries: Dict[MessageId, int] = {}
        # msg_id -> redelivery_count
        self._redelivery_counts: Dict[MessageId, int] = {}
        self._seq: int = 0

    def track(
        self,
        msg_id: MessageId,
        consumer_id: str,
        timeout_sec: Optional[float] = None,
        payload: Any = None,
        now: Optional[float] = None
    ) -> float:
        """Starts tracking an in-flight message with an expiration deadline.
        
        Returns:
            float: The computed deadline timestamp.
        """
        ttl = timeout_sec if timeout_sec is not None else self.default_timeout_sec
        base_time = now if now is not None else time.time()
        deadline = base_time + ttl

        with self._lock:
            self._seq += 1
            entry_id = self._seq
            self._active_entries[msg_id] = entry_id

            entry = HeapEntry(
                deadline=deadline,
                entry_id=entry_id,
                msg_id=msg_id,
                consumer_id=consumer_id,
                payload=payload
            )
            heapq.heappush(self._heap, entry)
            return deadline

    def ack(self, msg_id: MessageId) -> bool:
        """Acknowledges a message, canceling its timeout deadline.
        
        Returns:
            bool: True if the message was actively being tracked, False otherwise.
        """
        with self._lock:
            if msg_id in self._active_entries:
                del self._active_entries[msg_id]
                self._redelivery_counts.pop(msg_id, None)
                return True
            return False

    def is_tracked(self, msg_id: MessageId) -> bool:
        """Returns True if the message is currently in-flight."""
        with self._lock:
            return msg_id in self._active_entries

    def get_redelivery_count(self, msg_id: MessageId) -> int:
        """Returns the number of times this message has been redelivered."""
        with self._lock:
            return self._redelivery_counts.get(msg_id, 0)

    def expired(self, now: Optional[float] = None) -> List[ExpiredMessage]:
        """Pops and returns all messages whose deadlines have passed.
        
        Returns:
            List[ExpiredMessage]: Messages ready for redelivery.
        """
        current_time = now if now is not None else time.time()
        expired_list: List[ExpiredMessage] = []

        with self._lock:
            while self._heap and self._heap[0].deadline <= current_time:
                top: HeapEntry = heapq.heappop(self._heap)

                # Lazy deletion check: verify this entry is still active and was not acked or re-tracked
                if self._active_entries.get(top.msg_id) == top.entry_id:
                    # Message genuinely timed out!
                    del self._active_entries[top.msg_id]
                    count = self._redelivery_counts.get(top.msg_id, 0) + 1
                    self._redelivery_counts[top.msg_id] = count

                    expired_list.append(
                        ExpiredMessage(
                            msg_id=top.msg_id,
                            consumer_id=top.consumer_id,
                            payload=top.payload,
                            redelivery_count=count
                        )
                    )

        return expired_list

    def clear(self) -> None:
        """Clears all tracking state."""
        with self._lock:
            self._heap.clear()
            self._active_entries.clear()
            self._redelivery_counts.clear()
