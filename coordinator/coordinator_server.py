"""Coordinator TCP Server exposing lease acquisition, renewal, release, and discovery APIs."""

import socket
from typing import Any, Dict, Optional

from common.protocol import Commands, ErrorCodes, make_error, make_ok
from common.tcp_service import BaseTCPServer
from coordinator.lease_manager import LeaseError, LeaseManager


class CoordinatorServer(BaseTCPServer):
    """Network-facing Ownership Coordinator node."""

    def __init__(self, host: str, port: int, data_dir: str, name: str = "CoordinatorService"):
        super().__init__(host, port, name=name)
        self.lease_manager = LeaseManager(data_dir)

    def handle_request(self, request: Dict[str, Any], client_sock: socket.socket) -> Optional[Dict[str, Any]]:
        cmd = request.get("type")
        req_id = request.get("req_id")

        if cmd == Commands.ACQUIRE_LEASE:
            topic = request.get("topic")
            broker_id = request.get("broker_id")
            lease_ttl_ms = request.get("lease_ttl_ms", 5000)

            if not topic or not broker_id:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic or broker_id")

            try:
                token, expires_at = self.lease_manager.acquire(topic, broker_id, int(lease_ttl_ms))
                return make_ok(
                    req_id,
                    Commands.ACQUIRE_LEASE_OK,
                    topic=topic,
                    broker_id=broker_id,
                    fencing_token=token,
                    expires_at=expires_at
                )
            except LeaseError as e:
                return make_error(req_id, ErrorCodes.NOT_OWNER, str(e))
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.RENEW_LEASE:
            topic = request.get("topic")
            broker_id = request.get("broker_id")
            token = request.get("fencing_token")
            lease_ttl_ms = request.get("lease_ttl_ms", 5000)

            if not topic or not broker_id or token is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic, broker_id, or fencing_token")

            try:
                expires_at = self.lease_manager.renew(topic, broker_id, int(token), int(lease_ttl_ms))
                return make_ok(
                    req_id,
                    Commands.RENEW_LEASE_OK,
                    topic=topic,
                    broker_id=broker_id,
                    fencing_token=int(token),
                    expires_at=expires_at
                )
            except LeaseError as e:
                return make_error(req_id, ErrorCodes.LEASE_NOT_FOUND, str(e))
            except Exception as e:
                return make_error(req_id, ErrorCodes.INTERNAL_ERROR, str(e))

        elif cmd == Commands.RELEASE_LEASE:
            topic = request.get("topic")
            broker_id = request.get("broker_id")
            token = request.get("fencing_token")

            if not topic or not broker_id or token is None:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic, broker_id, or fencing_token")

            success = self.lease_manager.release(topic, broker_id, int(token))
            return make_ok(req_id, Commands.RELEASE_LEASE_OK, topic=topic, released=success)

        elif cmd == Commands.GET_OWNER:
            topic = request.get("topic")
            if not topic:
                return make_error(req_id, ErrorCodes.BAD_REQUEST, "Missing topic")

            owner_info = self.lease_manager.get_owner(topic)
            if owner_info:
                return make_ok(
                    req_id,
                    Commands.GET_OWNER_OK,
                    topic=topic,
                    broker_id=owner_info["broker_id"],
                    fencing_token=owner_info["fencing_token"],
                    expires_at=owner_info["expires_at"]
                )
            else:
                return make_ok(
                    req_id,
                    Commands.GET_OWNER_OK,
                    topic=topic,
                    broker_id=None,
                    fencing_token=None,
                    expires_at=None
                )

        return make_error(req_id, ErrorCodes.BAD_REQUEST, f"Unknown coordinator command: {cmd}")

