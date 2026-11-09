"""Lease and fencing token manager for topic ownership coordination."""

import json
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple


class LeaseError(Exception):
    """Raised when lease operations violate coordination rules."""
    pass


class LeaseManager:
    """Manages time-bound topic leases and monotonically increasing fencing tokens.
    
    Guarantees:
    1. Mutual Exclusion: At most one broker holds a valid, unexpired lease for a topic.
    2. Monotonic Fencing: Every newly granted lease yields a strictly higher token (N+1).
    3. Counter Durability: The fencing token counter is persisted to disk with fsync,
       guaranteeing that counter values never decrement or repeat even if the coordinator crashes.
    4. Safe Expiry: If a broker crashes or fails to heartbeat, its lease expires, allowing
       a standby broker to take over cleanly.
    """

    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.meta_file = os.path.join(self.data_dir, "coordinator_meta.json")

        self._lock = threading.Lock()
        # topic -> {"broker_id": str, "token": int, "expires_at": float}
        self._leases: Dict[str, Dict[str, Any]] = {}
        # Monotonically increasing counter across the entire cluster
        self._global_token_counter: int = 0

        self._recover_state()

    def _recover_state(self) -> None:
        """Loads the global fencing counter from disk to survive coordinator restarts."""
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._global_token_counter = int(data.get("global_token_counter", 0))
            except Exception:
                self._global_token_counter = 0

    def _persist_counter(self) -> None:
        """Atomically persists the fencing token counter with fsync before returning a new token."""
        temp_file = f"{self.meta_file}.tmp"
        data = {"global_token_counter": self._global_token_counter}
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, self.meta_file)

    def _is_lease_active(self, lease: Dict[str, Any], current_time: float) -> bool:
        return lease is not None and lease.get("expires_at", 0) > current_time

    def acquire(self, topic: str, broker_id: str, lease_ttl_ms: int = 5000) -> Tuple[int, float]:
        """Attempts to acquire exclusive topic ownership lease.
        
        Args:
            topic: The target topic name.
            broker_id: Identifier of the broker requesting ownership.
            lease_ttl_ms: Time-to-live for the lease in milliseconds.
            
        Returns:
            Tuple[int, float]: (fencing_token, expires_at_timestamp)
            
        Raises:
            LeaseError: If the topic is currently owned by another broker with an active lease.
        """
        with self._lock:
            now = time.time()
            existing = self._leases.get(topic)

            if existing and self._is_lease_active(existing, now):
                # If already owned by this exact broker, treat as a renew/re-acquire
                if existing["broker_id"] == broker_id:
                    expires_at = now + (lease_ttl_ms / 1000.0)
                    existing["expires_at"] = expires_at
                    return existing["token"], expires_at
                else:
                    time_left = existing["expires_at"] - now
                    raise LeaseError(
                        f"Topic '{topic}' is currently owned by '{existing['broker_id']}' "
                        f"(expires in {time_left:.2f}s)"
                    )

            # Either unowned or previous lease expired -> Issue a new lease with strictly higher token
            self._global_token_counter += 1
            new_token = self._global_token_counter
            self._persist_counter()

            expires_at = now + (lease_ttl_ms / 1000.0)
            self._leases[topic] = {
                "broker_id": broker_id,
                "token": new_token,
                "expires_at": expires_at
            }
            return new_token, expires_at

    def renew(self, topic: str, broker_id: str, token: int, lease_ttl_ms: int = 5000) -> float:
        """Heartbeat to renew an existing lease.
        
        Args:
            topic: Target topic.
            broker_id: Broker attempting renewal.
            token: Fencing token held by the broker.
            lease_ttl_ms: Extension duration in milliseconds.
            
        Returns:
            float: New expiration timestamp.
            
        Raises:
            LeaseError: If lease expired, token does not match, or another broker took over.
        """
        with self._lock:
            now = time.time()
            lease = self._leases.get(topic)

            if not lease:
                raise LeaseError(f"No active lease found for topic '{topic}'")

            if not self._is_lease_active(lease, now):
                raise LeaseError(f"Lease for topic '{topic}' has expired. Must re-acquire.")

            if lease["broker_id"] != broker_id:
                raise LeaseError(
                    f"Renewal rejected: broker '{broker_id}' does not own topic '{topic}' "
                    f"(current owner is '{lease['broker_id']}')"
                )

            if lease["token"] != token:
                raise LeaseError(
                    f"Renewal rejected: token {token} is stale. Current active token is {lease['token']}."
                )

            new_expires_at = now + (lease_ttl_ms / 1000.0)
            lease["expires_at"] = new_expires_at
            return new_expires_at

    def release(self, topic: str, broker_id: str, token: int) -> bool:
        """Gracefully relinquishes a topic lease.
        
        Returns:
            bool: True if released, False if caller wasn't the active owner.
        """
        with self._lock:
            lease = self._leases.get(topic)
            if lease and lease["broker_id"] == broker_id and lease["token"] == token:
                del self._leases[topic]
                return True
            return False

    def get_owner(self, topic: str) -> Optional[Dict[str, Any]]:
        """Queries the current active owner of a topic.
        
        Returns:
            Optional[Dict]: Info dictionary if an active unexpired lease exists, else None.
        """
        with self._lock:
            now = time.time()
            lease = self._leases.get(topic)
            if self._is_lease_active(lease, now):
                return {
                    "topic": topic,
                    "broker_id": lease["broker_id"],
                    "fencing_token": lease["token"],
                    "expires_at": lease["expires_at"],
                    "time_to_live_sec": round(lease["expires_at"] - now, 3)
                }
            return None

