"""Shared client discovery utilities for locating topic-owning brokers via Coordinator."""

from typing import List, Optional, Tuple

from coordinator.coordinator_client import CoordinatorClient


def parse_broker_address(address: str) -> Tuple[str, int]:
    """Parses a 'host:port' string into (host, port)."""
    host, port_str = address.rsplit(":", 1)
    return host, int(port_str)


def lookup_topic_owner(
    coordinator_host: str,
    coordinator_port: int,
    topic: str,
    timeout: float = 5.0
) -> Optional[Tuple[str, int]]:
    """Queries the Coordinator for the current owner's (host, port) for `topic`."""
    try:
        with CoordinatorClient(coordinator_host, coordinator_port, timeout=timeout) as coord:
            owner = coord.get_owner(topic)
            if owner and owner.get("broker_address"):
                return parse_broker_address(owner["broker_address"])
    except Exception:
        return None
    return None

