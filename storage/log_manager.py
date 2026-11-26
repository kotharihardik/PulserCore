"""Append-only log storage engine with fsync durability and fencing token enforcement."""

import json
import os
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from common.cursor import Cursor

RECORD_HEADER_FORMAT = "!I"
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FORMAT)


class FencedError(Exception):
    """Raised when a write is rejected due to a stale fencing token."""
    def __init__(self, message: str, current_token: int, attempted_token: int):
        super().__init__(message)
        self.current_token = current_token
        self.attempted_token = attempted_token


class LogManager:
    """Manages append-only disk logs and topic fencing states.
    
    Guarantees:
    1. Durability: Every appended message is flushed and fsynced to disk before returning.
    2. Strict Monotonicity: Offsets are continuous integers starting from 0.
    3. Fencing Enforcement: Rejects any broker write carrying a fencing token lower than
       the highest token ever witnessed for that topic.
    """

    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

        self._lock = threading.Lock()
        # topic -> file handle
        self._file_handles: Dict[str, Any] = {}
        # topic -> in-memory index: list of file seek positions relative to base_offset
        self._offset_positions: Dict[str, List[int]] = {}
        # topic -> earliest offset retained on disk (0 initially, advances on retention deletion)
        self._base_offsets: Dict[str, int] = {}
        # topic -> highest fencing token seen
        self._highest_tokens: Dict[str, int] = {}
        # topic -> last appended offset
        self._last_offsets: Dict[str, int] = {}
        # topic -> cached metadata dict (retention rules, subscriptions)
        self._topic_meta: Dict[str, Dict[str, Any]] = {}

        self._recover_all_topics()

    def _meta_path(self, topic: str) -> str:
        return os.path.join(self.data_dir, f"{topic}.meta.json")

    def _cursor_path(self, topic: str, subscription: str) -> str:
        return os.path.join(self.data_dir, f"{topic}_{subscription}.cursor.json")

    def _log_path(self, topic: str) -> str:
        return os.path.join(self.data_dir, f"{topic}.log")

    def _recover_all_topics(self) -> None:
        """Discovers existing topics in the data directory and rebuilds indices on startup."""
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".log"):
                topic = filename[:-4]
                self._load_topic(topic)

    def _load_topic(self, topic: str) -> None:
        """Scans the topic log file to reconstruct offsets and reads metadata for fencing tokens."""
        meta_file = self._meta_path(topic)
        meta: Dict[str, Any] = {}
        highest_token = 0
        meta_last_offset = -1
        meta_base_offset = 0
        if os.path.exists(meta_file):
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    highest_token = int(meta.get("highest_fencing_token", 0))
                    meta_last_offset = int(meta.get("last_offset", -1))
                    meta_base_offset = int(meta.get("base_offset", 0))
            except Exception:
                meta = {}
                highest_token = 0

        self._highest_tokens[topic] = highest_token
        self._topic_meta[topic] = {
            "topic": topic,
            "highest_fencing_token": highest_token,
            "subscriptions": list(meta.get("subscriptions", [])),
            "retention_time_sec": meta.get("retention_time_sec"),
            "retention_size_bytes": meta.get("retention_size_bytes"),
        }

        log_file = self._log_path(topic)
        positions: List[int] = []
        base_offset: Optional[int] = None
        last_offset = meta_last_offset

        if os.path.exists(log_file):
            with open(log_file, "rb") as f:
                pos = 0
                while True:
                    header = f.read(RECORD_HEADER_SIZE)
                    if len(header) < RECORD_HEADER_SIZE:
                        break  # Clean end of file or truncated record

                    rec_len = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
                    rec_bytes = f.read(rec_len)
                    if len(rec_bytes) < rec_len:
                        break  # Incomplete record write on crash; stop at last valid record

                    try:
                        rec = json.loads(rec_bytes.decode("utf-8"))
                        rec_offset = int(rec["offset"])
                    except Exception:
                        break

                    if base_offset is None:
                        base_offset = rec_offset
                    positions.append(pos)
                    last_offset = rec_offset
                    pos = f.tell()

        if base_offset is None:
            base_offset = (last_offset + 1) if last_offset >= 0 else meta_base_offset

        self._offset_positions[topic] = positions
        self._base_offsets[topic] = base_offset
        self._last_offsets[topic] = last_offset

        # Open in append+binary mode with write access
        handle = open(log_file, "a+b")
        handle.seek(0, os.SEEK_END)
        self._file_handles[topic] = handle

    def _persist_meta(self, topic: str) -> None:
        """Atomically saves topic metadata (fencing token, offsets, retention, subscriptions) with fsync."""
        meta_file = self._meta_path(topic)
        temp_file = f"{meta_file}.tmp"
        existing = self._topic_meta.get(topic, {})
        data = {
            "topic": topic,
            "highest_fencing_token": self._highest_tokens.get(topic, 0),
            "base_offset": self._base_offsets.get(topic, 0),
            "last_offset": self._last_offsets.get(topic, -1),
            "subscriptions": list(existing.get("subscriptions", [])),
            "retention_time_sec": existing.get("retention_time_sec"),
            "retention_size_bytes": existing.get("retention_size_bytes"),
        }
        self._topic_meta[topic] = data
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, meta_file)

    def _ensure_topic_initialized(self, topic: str) -> None:
        if topic not in self._file_handles:
            self._load_topic(topic)

    def append(
        self,
        topic: str,
        payload: str,
        fencing_token: int,
        timestamp: Optional[float] = None
    ) -> int:
        """Appends a message to the topic log with fsync durability and fencing check.
        
        Args:
            topic: Target topic name.
            payload: Raw string or serialized payload.
            fencing_token: The fencing token of the broker attempting this append.
            timestamp: Optional creation timestamp (defaults to time.time()).
            
        Returns:
            int: The assigned continuous 0-based offset of the message.
            
        Raises:
            FencedError: If the broker's fencing token is lower than the current highest token.
        """
        with self._lock:
            self._ensure_topic_initialized(topic)

            current_token = self._highest_tokens.get(topic, 0)
            if fencing_token < current_token:
                raise FencedError(
                    f"Fenced: broker token {fencing_token} is stale. Highest known token is {current_token}.",
                    current_token=current_token,
                    attempted_token=fencing_token
                )

            # If the broker has a strictly higher fencing token, update and persist it
            if fencing_token > current_token:
                self._highest_tokens[topic] = fencing_token
                self._persist_meta(topic)

            next_offset = self._last_offsets.get(topic, -1) + 1
            msg_ts = timestamp if timestamp is not None else time.time()
            record = {
                "offset": next_offset,
                "payload": payload,
                "timestamp": msg_ts,
                "fencing_token": fencing_token
            }
            record_bytes = json.dumps(record).encode("utf-8")
            header = struct.pack(RECORD_HEADER_FORMAT, len(record_bytes))

            handle = self._file_handles[topic]
            handle.seek(0, os.SEEK_END)
            seek_pos = handle.tell()

            # 1. Write header and payload
            handle.write(header + record_bytes)
            # 2. Flush OS user-space buffer to OS kernel
            handle.flush()
            # 3. Force kernel to flush dirty blocks to physical disk
            os.fsync(handle.fileno())

            # Update indices
            self._offset_positions[topic].append(seek_pos)
            self._last_offsets[topic] = next_offset

            return next_offset

    def read(self, topic: str, from_offset: int, max_count: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """Reads a sequence of messages starting from `from_offset`.
        
        Args:
            topic: Topic name.
            from_offset: The offset to begin reading from (0-indexed).
            max_count: Maximum number of messages to return.
            
        Returns:
            Tuple[List[Dict], int]: List of deserialized message dicts and the next_offset.
        """
        with self._lock:
            self._ensure_topic_initialized(topic)

            if from_offset < 0:
                return [], from_offset

            positions = self._offset_positions.get(topic, [])
            base = self._base_offsets.get(topic, 0)
            effective_from = max(from_offset, base)
            start_idx = effective_from - base

            if start_idx >= len(positions):
                # Nothing to read
                return [], effective_from

            end_idx = min(start_idx + max_count, len(positions))
            start_pos = positions[start_idx]

            log_file = self._log_path(topic)
            messages = []

            with open(log_file, "rb") as f:
                f.seek(start_pos)
                idx = start_idx
                while idx < end_idx:
                    header = f.read(RECORD_HEADER_SIZE)
                    if len(header) < RECORD_HEADER_SIZE:
                        break
                    rec_len = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
                    rec_bytes = f.read(rec_len)
                    if len(rec_bytes) < rec_len:
                        break

                    record = json.loads(rec_bytes.decode("utf-8"))
                    messages.append(record)
                    idx += 1

            next_offset = effective_from + len(messages)
            return messages, next_offset

    def base_offset(self, topic: str) -> int:
        """Returns the earliest retained offset still stored on disk for the topic."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            return self._base_offsets.get(topic, 0)

    def last_offset(self, topic: str) -> int:
        """Returns the latest appended offset for the topic, or -1 if empty."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            return self._last_offsets.get(topic, -1)

    def get_highest_token(self, topic: str) -> int:
        """Returns the highest recorded fencing token for the topic."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            return self._highest_tokens.get(topic, 0)

    def save_cursor(self, topic: str, subscription: str, cursor: Cursor, fencing_token: int) -> None:
        """Saves subscription cursor durably to disk with fsync and fencing check.
        
        Args:
            topic: Topic name.
            subscription: Subscription identifier.
            cursor: The Cursor object containing mark_delete_position and individual_acks.
            fencing_token: The broker's fencing token.
            
        Raises:
            FencedError: If broker's fencing token is lower than storage highest token.
        """
        with self._lock:
            self._ensure_topic_initialized(topic)

            current_token = self._highest_tokens.get(topic, 0)
            if fencing_token < current_token:
                raise FencedError(
                    f"Fenced: broker token {fencing_token} is stale. Highest known token is {current_token}.",
                    current_token=current_token,
                    attempted_token=fencing_token
                )

            if fencing_token > current_token:
                self._highest_tokens[topic] = fencing_token
                self._persist_meta(topic)

            cursor_file = self._cursor_path(topic, subscription)
            temp_file = f"{cursor_file}.tmp"
            data = cursor.to_dict()
            data["topic"] = topic
            data["subscription"] = subscription
            data["updated_at"] = time.time()

            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, cursor_file)

            # Register subscription in topic metadata so retention tracks all subscribers
            meta = self._topic_meta.setdefault(topic, {"subscriptions": []})
            subs = meta.setdefault("subscriptions", [])
            if subscription not in subs:
                subs.append(subscription)
                self._persist_meta(topic)

    def load_cursor(self, topic: str, subscription: str) -> Cursor:
        """Loads subscription cursor from disk. Returns default Cursor if none exists."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            cursor_file = self._cursor_path(topic, subscription)
            if not os.path.exists(cursor_file):
                return Cursor(mark_delete_position=-1, individual_acks=set())

            try:
                with open(cursor_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return Cursor.from_dict(data)
            except Exception:
                return Cursor(mark_delete_position=-1, individual_acks=set())

    def save_topic_metadata(self, topic: str, metadata: Dict[str, Any]) -> None:
        """Persists topic metadata such as subscription list and retention settings."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            existing = self._topic_meta.get(topic, {})
            existing.update(metadata)
            self._topic_meta[topic] = existing
            self._persist_meta(topic)

    def get_topic_metadata(self, topic: str) -> Dict[str, Any]:
        """Retrieves topic metadata."""
        with self._lock:
            self._ensure_topic_initialized(topic)
            meta_file = self._meta_path(topic)
            if os.path.exists(meta_file):
                try:
                    with open(meta_file, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    pass
            return {
                "topic": topic,
                "highest_fencing_token": self._highest_tokens.get(topic, 0),
                "base_offset": self._base_offsets.get(topic, 0),
                "last_offset": self._last_offsets.get(topic, -1),
                "subscriptions": list(self._topic_meta.get(topic, {}).get("subscriptions", [])),
                "retention_time_sec": self._topic_meta.get(topic, {}).get("retention_time_sec"),
                "retention_size_bytes": self._topic_meta.get(topic, {}).get("retention_size_bytes"),
            }

    def set_retention_policy(
        self,
        topic: str,
        retention_time_sec: Optional[float] = None,
        retention_size_bytes: Optional[int] = None
    ) -> None:
        """Configures time-based and/or size-based retention rules for a topic."""
        self.save_topic_metadata(topic, {
            "retention_time_sec": retention_time_sec,
            "retention_size_bytes": retention_size_bytes
        })

    def _discover_subscriptions(self, topic: str) -> List[str]:
        """Finds all subscriptions known for a topic via metadata and cursor files on disk."""
        subs: Set[str] = set(self._topic_meta.get(topic, {}).get("subscriptions", []))
        prefix = f"{topic}_"
        suffix = ".cursor.json"
        for fname in os.listdir(self.data_dir):
            if fname.startswith(prefix) and fname.endswith(suffix):
                sub_name = fname[len(prefix):-len(suffix)]
                if sub_name:
                    subs.add(sub_name)
        return sorted(subs)

    def _load_cursor_unlocked(self, topic: str, subscription: str) -> Cursor:
        cursor_file = self._cursor_path(topic, subscription)
        if not os.path.exists(cursor_file):
            return Cursor(mark_delete_position=-1, individual_acks=set())
        try:
            with open(cursor_file, "r", encoding="utf-8") as f:
                return Cursor.from_dict(json.load(f))
        except Exception:
            return Cursor(mark_delete_position=-1, individual_acks=set())

    def apply_retention(
        self,
        topic: str,
        retention_time_sec: Optional[float] = None,
        retention_size_bytes: Optional[int] = None,
        now: Optional[float] = None
    ) -> Dict[str, Any]:
        """Safely deletes old log entries that are past retention AND acked by every subscription.
        
        Golden Rule of Pulsar Retention:
        A message is deleted ONLY IF:
          1. It is past the time-based and/or size-based retention policy, AND
          2. Every active subscription has acknowledged at or past that offset
             (offset <= min(cursor.mark_delete_position for all subscriptions)).
        """
        with self._lock:
            self._ensure_topic_initialized(topic)

            meta = self._topic_meta.get(topic, {})
            eff_time = retention_time_sec if retention_time_sec is not None else meta.get("retention_time_sec")
            eff_size = retention_size_bytes if retention_size_bytes is not None else meta.get("retention_size_bytes")

            subs = self._discover_subscriptions(topic)
            if subs:
                cursors = [self._load_cursor_unlocked(topic, s) for s in subs]
                safe_acked_upto = min(c.mark_delete_position for c in cursors)
            else:
                safe_acked_upto = self._last_offsets.get(topic, -1)

            log_file = self._log_path(topic)
            current_bytes = os.path.getsize(log_file) if os.path.exists(log_file) else 0

            if eff_time is None and eff_size is None:
                return {
                    "deleted_count": 0,
                    "new_base_offset": self._base_offsets.get(topic, 0),
                    "remaining_count": len(self._offset_positions.get(topic, [])),
                    "remaining_bytes": current_bytes,
                    "safe_acked_upto": safe_acked_upto
                }

            # Read raw records from head to evaluate retention eligibility
            records: List[Tuple[int, float, bytes]] = []
            if os.path.exists(log_file):
                with open(log_file, "rb") as f:
                    while True:
                        header = f.read(RECORD_HEADER_SIZE)
                        if len(header) < RECORD_HEADER_SIZE:
                            break
                        rec_len = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
                        rec_bytes = f.read(rec_len)
                        if len(rec_bytes) < rec_len:
                            break
                        rec = json.loads(rec_bytes.decode("utf-8"))
                        records.append((int(rec["offset"]), float(rec["timestamp"]), header + rec_bytes))

            current_time = now if now is not None else time.time()
            remaining_bytes = sum(len(raw) for _, _, raw in records)
            delete_count = 0

            for rec_offset, rec_ts, raw_frame in records:
                # Never delete unacknowledged data!
                if rec_offset > safe_acked_upto:
                    break

                past_time = eff_time is not None and (current_time - rec_ts) >= eff_time
                past_size = eff_size is not None and remaining_bytes > eff_size

                if not (past_time or past_size):
                    break

                delete_count += 1
                remaining_bytes -= len(raw_frame)

            if delete_count > 0:
                kept_records = records[delete_count:]
                temp_log = f"{log_file}.tmp"
                new_positions: List[int] = []

                with open(temp_log, "wb") as f:
                    pos = 0
                    for _, _, raw_frame in kept_records:
                        new_positions.append(pos)
                        f.write(raw_frame)
                        pos += len(raw_frame)
                    f.flush()
                    os.fsync(f.fileno())

                # Swap log file atomically and reopen append handle
                old_handle = self._file_handles.pop(topic, None)
                if old_handle:
                    try:
                        old_handle.close()
                    except Exception:
                        pass

                os.replace(temp_log, log_file)
                new_handle = open(log_file, "a+b")
                new_handle.seek(0, os.SEEK_END)
                self._file_handles[topic] = new_handle

                self._offset_positions[topic] = new_positions
                if kept_records:
                    self._base_offsets[topic] = kept_records[0][0]
                else:
                    self._base_offsets[topic] = self._last_offsets.get(topic, -1) + 1

                self._persist_meta(topic)

            return {
                "deleted_count": delete_count,
                "new_base_offset": self._base_offsets.get(topic, 0),
                "remaining_count": len(self._offset_positions.get(topic, [])),
                "remaining_bytes": remaining_bytes,
                "safe_acked_upto": safe_acked_upto
            }

    def close(self) -> None:
        """Flushes and closes all open file handles."""
        with self._lock:
            for handle in self._file_handles.values():
                try:
                    handle.flush()
                    os.fsync(handle.fileno())
                    handle.close()
                except Exception:
                    pass
            self._file_handles.clear()

