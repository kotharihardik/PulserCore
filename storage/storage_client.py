"""Client SDK for interacting with the PulserCore Storage Service over TCP."""

import uuid
from typing import Any, Dict, List, Optional, Tuple

from common.protocol import Commands, ErrorCodes
from common.tcp_service import TCPClient
from storage.log_manager import FencedError


class StorageClient:
    """Synchronous network client for the decoupled storage node."""

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._tcp_client: Optional[TCPClient] = None

    def connect(self) -> None:
        """Connects to the storage service."""
        self._tcp_client = TCPClient(self.host, self.port, timeout=self.timeout)
        self._tcp_client.connect()

    def close(self) -> None:
        """Closes the underlying TCP connection."""
        if self._tcp_client:
            self._tcp_client.close()
            self._tcp_client = None

    def __enter__(self) -> "StorageClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def append(self, topic: str, payload: str, fencing_token: int) -> int:
        """Appends a message to the storage log.
        
        Returns:
            int: The continuous 0-based offset of the persisted message.
            
        Raises:
            FencedError: If write was rejected because the broker's fencing token is stale.
            RuntimeError: For other storage errors.
        """
        if not self._tcp_client:
            raise ConnectionError("StorageClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.APPEND,
            "req_id": req_id,
            "topic": topic,
            "payload": payload,
            "fencing_token": fencing_token
        }
        resp = self._tcp_client.send_request(req)

        if resp.get("type") == Commands.ERROR:
            err_code = resp.get("error_code")
            msg = resp.get("message", "Unknown storage error")
            if err_code == ErrorCodes.FENCED:
                raise FencedError(msg, current_token=-1, attempted_token=fencing_token)
            raise RuntimeError(f"Storage append failed [{err_code}]: {msg}")

        return int(resp["offset"])

    def read(self, topic: str, from_offset: int, max_count: int = 100) -> Tuple[List[Dict[str, Any]], int]:
        """Reads a range of messages starting from `from_offset`.
        
        Returns:
            Tuple[List[Dict], int]: List of message dictionaries and the next readable offset.
        """
        if not self._tcp_client:
            raise ConnectionError("StorageClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.READ,
            "req_id": req_id,
            "topic": topic,
            "from_offset": from_offset,
            "max_count": max_count
        }
        resp = self._tcp_client.send_request(req)

        if resp.get("type") == Commands.ERROR:
            raise RuntimeError(f"Storage read failed: {resp.get('message')}")

        return resp.get("messages", []), int(resp.get("next_offset", from_offset))

    def last_offset(self, topic: str) -> int:
        """Queries the latest offset written to the topic."""
        if not self._tcp_client:
            raise ConnectionError("StorageClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.LAST_OFFSET,
            "req_id": req_id,
            "topic": topic
        }
        resp = self._tcp_client.send_request(req)

        if resp.get("type") == Commands.ERROR:
            raise RuntimeError(f"Storage last_offset failed: {resp.get('message')}")

        return int(resp.get("last_offset", -1))

