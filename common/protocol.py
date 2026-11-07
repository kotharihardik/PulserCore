"""Protocol definitions, constants, and MessageId schema for PulserCore."""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


class Commands:
    # Producer / Consumer <-> Broker
    PUBLISH = "PUBLISH"
    PUBLISH_OK = "PUBLISH_OK"
    SUBSCRIBE = "SUBSCRIBE"
    SUBSCRIBE_OK = "SUBSCRIBE_OK"
    DELIVER = "DELIVER"
    ACK = "ACK"
    ACK_OK = "ACK_OK"

    # Broker <-> Storage
    APPEND = "APPEND"
    APPEND_OK = "APPEND_OK"
    READ = "READ"
    READ_OK = "READ_OK"
    LAST_OFFSET = "LAST_OFFSET"
    LAST_OFFSET_OK = "LAST_OFFSET_OK"
    SAVE_CURSOR = "SAVE_CURSOR"
    SAVE_CURSOR_OK = "SAVE_CURSOR_OK"
    LOAD_CURSOR = "LOAD_CURSOR"
    LOAD_CURSOR_OK = "LOAD_CURSOR_OK"
    TRUNCATE = "TRUNCATE"
    TRUNCATE_OK = "TRUNCATE_OK"

    # Broker/Client <-> Coordinator
    ACQUIRE_LEASE = "ACQUIRE_LEASE"
    ACQUIRE_LEASE_OK = "ACQUIRE_LEASE_OK"
    RENEW_LEASE = "RENEW_LEASE"
    RENEW_LEASE_OK = "RENEW_LEASE_OK"
    RELEASE_LEASE = "RELEASE_LEASE"
    RELEASE_LEASE_OK = "RELEASE_LEASE_OK"
    GET_OWNER = "GET_OWNER"
    GET_OWNER_OK = "GET_OWNER_OK"

    # Generic Error
    ERROR = "ERROR"


class ErrorCodes:
    NOT_OWNER = "NOT_OWNER"
    FENCED = "FENCED"
    CONSUMER_BUSY = "CONSUMER_BUSY"
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    INVALID_OFFSET = "INVALID_OFFSET"
    LEASE_NOT_FOUND = "LEASE_NOT_FOUND"
    BAD_REQUEST = "BAD_REQUEST"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    TIMEOUT = "TIMEOUT"


class SubscriptionModes:
    EXCLUSIVE = "EXCLUSIVE"
    SHARED = "SHARED"


@dataclass(frozen=True)
class MessageId:
    """Represents a unique message identifier in PulserCore.
    
    Since storage is separated per topic, (topic, offset) uniquely identifies
    any message in the system.
    """
    topic: str
    offset: int

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic, "offset": self.offset}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageId":
        return cls(topic=str(data["topic"]), offset=int(data["offset"]))

    def to_string(self) -> str:
        return f"{self.topic}:{self.offset}"

    @classmethod
    def from_string(cls, id_str: str) -> "MessageId":
        parts = id_str.split(":", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid MessageId string: '{id_str}'")
        return cls(topic=parts[0], offset=int(parts[1]))

    def __str__(self) -> str:
        return self.to_string()


def make_error(req_id: Optional[str], error_code: str, message: str) -> Dict[str, Any]:
    """Helper to generate standard ERROR response frames."""
    resp = {
        "type": Commands.ERROR,
        "error_code": error_code,
        "message": message,
    }
    if req_id is not None:
        resp["req_id"] = req_id
    return resp


def make_ok(req_id: Optional[str], cmd_type: str, **kwargs) -> Dict[str, Any]:
    """Helper to generate standard OK response frames."""
    resp = {"type": cmd_type}
    if req_id is not None:
        resp["req_id"] = req_id
    resp.update(kwargs)
    return resp

