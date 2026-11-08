"""Append-only log storage engine with fsync durability and fencing token enforcement."""

import json
import os
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

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
        # topic -> in-memory index: list of file seek positions [pos_0, pos_1, pos_2, ...]
        self._offset_positions: Dict[str, List[int]] = {}
        # topic -> highest fencing token seen
        self._highest_tokens: Dict[str, int] = {}
        # topic -> last appended offset
        self._last_offsets: Dict[str, int] = {}

        self._recover_all_topics()

    def _meta_path(self, topic: str) -> str:
        return os.path.join(self.data_dir, f"{topic}.meta.json")

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
        highest_token = 0
        if os.path.exists(meta_file):
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    highest_token = int(meta.get("highest_fencing_token", 0))
            except Exception:
                highest_token = 0
        self._highest_tokens[topic] = highest_token

        log_file = self._log_path(topic)
        positions: List[int] = []
        last_offset = -1

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

                    positions.append(pos)
                    last_offset += 1
                    pos = f.tell()

        self._offset_positions[topic] = positions
        self._last_offsets[topic] = last_offset

        # Open in append+binary mode with write access
        handle = open(log_file, "a+b")
        handle.seek(0, os.SEEK_END)
        self._file_handles[topic] = handle

    def _persist_meta(self, topic: str) -> None:
        """Atomically saves topic metadata (highest fencing token) with fsync."""
        meta_file = self._meta_path(topic)
        temp_file = f"{meta_file}.tmp"
        data = {
            "topic": topic,
            "highest_fencing_token": self._highest_tokens[topic]
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, meta_file)

    def _ensure_topic_initialized(self, topic: str) -> None:
        if topic not in self._file_handles:
            self._load_topic(topic)

    def append(self, topic: str, payload: str, fencing_token: int) -> int:
        """Appends a message to the topic log with fsync durability and fencing check.
        
        Args:
            topic: Target topic name.
            payload: Raw string or serialized payload.
            fencing_token: The fencing token of the broker attempting this append.
            
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
            record = {
                "offset": next_offset,
                "payload": payload,
                "timestamp": time.time(),
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

            positions = self._offset_positions.get(topic, [])
            total_msgs = len(positions)

            if from_offset < 0 or from_offset >= total_msgs:
                # Nothing to read
                return [], from_offset

            end_offset = min(from_offset + max_count, total_msgs)
            start_pos = positions[from_offset]

            log_file = self._log_path(topic)
            messages = []

            with open(log_file, "rb") as f:
                f.seek(start_pos)
                current_offset = from_offset
                while current_offset < end_offset:
                    header = f.read(RECORD_HEADER_SIZE)
                    if len(header) < RECORD_HEADER_SIZE:
                        break
                    rec_len = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
                    rec_bytes = f.read(rec_len)
                    if len(rec_bytes) < rec_len:
                        break

                    record = json.loads(rec_bytes.decode("utf-8"))
                    messages.append(record)
                    current_offset += 1

            next_offset = from_offset + len(messages)
            return messages, next_offset

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

