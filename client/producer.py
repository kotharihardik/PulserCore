"""Producer client SDK for publishing messages to PulserCore brokers."""

import json
import uuid
from typing import Any, Dict, Optional, Union

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
    
    Usage:
        with Producer(broker_host="127.0.0.1", broker_port=5001) as producer:
            msg_id = producer.send("orders", {"order_id": 101, "price": 49.99})
            print(f"Published order at offset {msg_id.offset}")
    """

    def __init__(self, broker_host: str, broker_port: int, timeout: float = 10.0):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.timeout = timeout
        self._tcp_client: Optional[TCPClient] = None

    def connect(self) -> None:
        """Establishes TCP connection with the broker."""
        self._tcp_client = TCPClient(self.broker_host, self.broker_port, timeout=self.timeout)
        self._tcp_client.connect()

    def send(self, topic: str, payload: Union[str, Dict[str, Any], bytes], timeout: Optional[float] = None) -> MessageId:
        """Publishes a message to the specified topic and waits for broker durability confirmation.
        
        Args:
            topic: Destination topic name.
            payload: Payload string, dictionary (auto-serialized to JSON), or UTF-8 bytes.
            timeout: Optional per-request timeout in seconds.
            
        Returns:
            MessageId: The unique (topic, offset) identifier assigned to this message.
            
        Raises:
            PublishError: If the broker or storage rejects the publication.
            ConnectionError: If network connection drops.
        """
        if not self._tcp_client:
            raise ConnectionError("Producer is not connected. Call connect() first.")

        if isinstance(payload, dict):
            payload_str = json.dumps(payload)
        elif isinstance(payload, bytes):
            payload_str = payload.decode("utf-8")
        else:
            payload_str = str(payload)

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.PUBLISH,
            "req_id": req_id,
            "topic": topic,
            "payload": payload_str
        }

        resp = self._tcp_client.send_request(req, timeout=timeout)

        if resp.get("type") == Commands.ERROR:
            raise PublishError(
                error_code=resp.get("error_code", ErrorCodes.INTERNAL_ERROR),
                message=resp.get("message", "Unknown broker error")
            )

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
        else:
            raise PublishError(
                error_code=ErrorCodes.INTERNAL_ERROR,
                message=f"Malformed message_id in response: {raw_msg_id}"
            )

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

