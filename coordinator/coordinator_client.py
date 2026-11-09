"""Client SDK for interacting with the PulserCore Ownership Coordinator over TCP."""

import uuid
from typing import Any, Dict, Optional, Tuple

from common.protocol import Commands, ErrorCodes
from common.tcp_service import TCPClient
from coordinator.lease_manager import LeaseError


class CoordinatorClient:
    """Synchronous network client for the Ownership Coordinator."""

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._tcp_client: Optional[TCPClient] = None

    def connect(self) -> None:
        """Connects to the Coordinator service."""
        self._tcp_client = TCPClient(self.host, self.port, timeout=self.timeout)
        self._tcp_client.connect()

    def close(self) -> None:
        """Closes the underlying TCP socket."""
        if self._tcp_client:
            self._tcp_client.close()
            self._tcp_client = None

    def __enter__(self) -> "CoordinatorClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def acquire_lease(self, topic: str, broker_id: str, lease_ttl_ms: int = 5000) -> Tuple[int, float]:
        """Requests topic ownership.
        
        Returns:
            Tuple[int, float]: (fencing_token, expires_at)
            
        Raises:
            LeaseError: If topic is currently locked by another broker.
        """
        if not self._tcp_client:
            raise ConnectionError("CoordinatorClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.ACQUIRE_LEASE,
            "req_id": req_id,
            "topic": topic,
            "broker_id": broker_id,
            "lease_ttl_ms": lease_ttl_ms
        }
        resp = self._tcp_client.send_request(req)

        if resp.get("type") == Commands.ERROR:
            raise LeaseError(f"Lease acquire failed [{resp.get('error_code')}]: {resp.get('message')}")

        return int(resp["fencing_token"]), float(resp["expires_at"])

    def renew_lease(self, topic: str, broker_id: str, token: int, lease_ttl_ms: int = 5000) -> float:
        """Renews an existing topic ownership lease.
        
        Returns:
            float: New expiration timestamp.
            
        Raises:
            LeaseError: If lease expired or was taken by another broker.
        """
        if not self._tcp_client:
            raise ConnectionError("CoordinatorClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.RENEW_LEASE,
            "req_id": req_id,
            "topic": topic,
            "broker_id": broker_id,
            "fencing_token": token,
            "lease_ttl_ms": lease_ttl_ms
        }
        resp = self._tcp_client.send_request(req)

        if resp.get("type") == Commands.ERROR:
            raise LeaseError(f"Lease renew failed [{resp.get('error_code')}]: {resp.get('message')}")

        return float(resp["expires_at"])

    def release_lease(self, topic: str, broker_id: str, token: int) -> bool:
        """Releases the topic lease voluntarily."""
        if not self._tcp_client:
            raise ConnectionError("CoordinatorClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.RELEASE_LEASE,
            "req_id": req_id,
            "topic": topic,
            "broker_id": broker_id,
            "fencing_token": token
        }
        resp = self._tcp_client.send_request(req)
        return bool(resp.get("released", False))

    def get_owner(self, topic: str) -> Optional[Dict[str, Any]]:
        """Queries the current topic owner."""
        if not self._tcp_client:
            raise ConnectionError("CoordinatorClient is not connected.")

        req_id = str(uuid.uuid4())
        req = {
            "type": Commands.GET_OWNER,
            "req_id": req_id,
            "topic": topic
        }
        resp = self._tcp_client.send_request(req)
        if resp.get("broker_id") is None:
            return None
        return {
            "broker_id": resp.get("broker_id"),
            "fencing_token": resp.get("fencing_token"),
            "expires_at": resp.get("expires_at")
        }

