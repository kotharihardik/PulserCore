"""Producer client SDK with topic discovery and automatic failover reconnection."""

import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

from client.discovery import lookup_topic_owner, parse_broker_address
from common.protocol import Commands, ErrorCodes, MessageId
from common.tcp_service import TCPClient


class PublishError(Exception):
    """Raised when a message publication request fails."""
    def __init__(self, error_code: str, message: str):
        super().__init__(f"Publish failed [{error_code}]: {message}")
        self.error_code = error_code
        self.message = message


class Producer:
    """High-level client SDK for publishing messages to PulserCore brokers.
    
    Supports:
    - Direct connection to a known broker (Phases 3-7).
    - Dynamic topic owner lookup via Coordinator (GET_OWNER) and automatic
      reconnect with exponential backoff on NOT_OWNER, FENCED, or socket drop (Phase 9).
    """

    def __init__(
        self,
        broker_host: Optional[str] = None,
        broker_port: Optional[int] = None,
        timeout: float = 10.0,
        coordinator_host: Optional[str] = None,
        coordinator_port: Optional[int] = None,
        fallback_brokers: Optional[List[Tuple[str, int]]] = None,
        max_retries: int = 20,
        base_backoff_sec: float = 0.1
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.timeout = timeout
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.fallback_brokers = list(fallback_brokers) if fallback_brokers else []
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec

        self._tcp_client: Optional[TCPClient] = None
        self._auto_reconnect = (coordinator_host is not None) or bool(self.fallback_brokers)
        self.reconnect_count: int = 0

    def connect(self, topic: Optional[str] = None) -> None:
        """Establishes TCP connection with the target or discovered owning broker."""
        if self.broker_host and self.broker_port:
            self._connect_to(self.broker_host, self.broker_port)
            return

        if topic and self.coordinator_host and self.coordinator_port:
            owner = lookup_topic_owner(self.coordinator_host, self.coordinator_port, topic, timeout=self.timeout)
            if owner:
                self._connect_to(owner[0], owner[1])
                return

        if self.fallback_brokers:
            host, port = self.fallback_brokers[0]
            self._connect_to(host, port)

    def _connect_to(self, host: str, port: int) -> None:
        if self._tcp_client:
            self._tcp_client.close()
        self.broker_host = host
        self.broker_port = port
        self._tcp_client = TCPClient(host, port, timeout=self.timeout)
        self._tcp_client.connect()

    def _resolve_and_reconnect(self, topic: str, attempt: int, hint_address: Optional[str] = None) -> bool:
        """Attempts to discover the current topic owner and reconnect."""
        if self._tcp_client:
            self._tcp_client.close()
            self._tcp_client = None

        backoff = min(self.base_backoff_sec * (1.5 ** attempt), 0.5)
        time.sleep(backoff)

        candidates: List[Tuple[str, int]] = []
        if hint_address:
            try:
                candidates.append(parse_broker_address(hint_address))
            except Exception:
                pass

        if self.coordinator_host and self.coordinator_port:
            owner = lookup_topic_owner(self.coordinator_host, self.coordinator_port, topic, timeout=self.timeout)
            if owner and owner not in candidates:
                candidates.append(owner)

        for fb in self.fallback_brokers:
            if fb not in candidates:
                candidates.append(fb)

        for host, port in candidates:
            try:
                self._connect_to(host, port)
                self.reconnect_count += 1
                return True
            except Exception:
                continue

        return False

    def send(self, topic: str, payload: Union[str, Dict[str, Any], bytes], timeout: Optional[float] = None) -> MessageId:
        """Publishes a message to `topic`, automatically failing over if auto-reconnect is enabled."""
        if isinstance(payload, dict):
            payload_str = json.dumps(payload)
        elif isinstance(payload, bytes):
            payload_str = payload.decode("utf-8")
        else:
            payload_str = str(payload)

        if not self._tcp_client:
            if not self._auto_reconnect:
                raise ConnectionError("Producer is not connected. Call connect() first.")
            self._resolve_and_reconnect(topic, attempt=0)

        attempts = self.max_retries if self._auto_reconnect else 1
        last_error: Optional[Exception] = None

        for attempt in range(attempts):
            if not self._tcp_client:
                self._resolve_and_reconnect(topic, attempt)
                continue

            req = {
                "type": Commands.PUBLISH,
                "req_id": str(uuid.uuid4()),
                "topic": topic,
                "payload": payload_str
            }

            try:
                resp = self._tcp_client.send_request(req, timeout=timeout)
            except (ConnectionError, OSError, TimeoutError) as e:
                last_error = e
                if not self._auto_reconnect:
                    raise
                self._resolve_and_reconnect(topic, attempt)
                continue

            if resp.get("type") == Commands.ERROR:
                err_code = resp.get("error_code", ErrorCodes.INTERNAL_ERROR)
                err_msg = resp.get("message", "Unknown broker error")
                if self._auto_reconnect and err_code in (ErrorCodes.NOT_OWNER, ErrorCodes.FENCED):
                    last_error = PublishError(err_code, err_msg)
                    self._resolve_and_reconnect(topic, attempt, hint_address=resp.get("owner_address"))
                    continue
                raise PublishError(error_code=err_code, message=err_msg)

            if resp.get("type") != Commands.PUBLISH_OK:
                raise PublishError(
                    error_code=ErrorCodes.INTERNAL_ERROR,
                    message=f"Unexpected response type: {resp.get('type')}"
                )

            raw_msg_id = resp.get("message_id")
            if isinstance(raw_msg_id, dict):
                return MessageId.from_dict(raw_msg_id)
            elif isinstance(raw_msg_id, str):
                return MessageId.from_string(raw_msg_id)
            raise PublishError(ErrorCodes.INTERNAL_ERROR, f"Malformed message_id in response: {raw_msg_id}")

        if isinstance(last_error, PublishError):
            raise last_error
        raise ConnectionError(f"Producer failed to publish to '{topic}' after {attempts} attempts: {last_error}")

    def close(self) -> None:
        """Closes the TCP connection to the broker."""
        if self._tcp_client:
            self._tcp_client.close()
            self._tcp_client = None

    def __enter__(self) -> "Producer":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
