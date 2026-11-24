"""Subscription cursor data structure supporting mark-delete position and individual acks."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set


@dataclass
class Cursor:
    """Tracks consumption progress for a topic subscription.
    
    A Cursor combines:
    1. mark_delete_position: The highest contiguous offset where ALL messages up to
       and including this offset have been acknowledged.
    2. individual_acks: A set of discrete acknowledged offsets strictly greater than
       mark_delete_position. (Used in Shared subscriptions when messages finish out-of-order).
       
    Automatic Compaction:
    Whenever the gap between mark_delete_position and individual_acks is filled,
    mark_delete_position automatically advances forward, and newly contiguous offsets
    are purged from individual_acks to minimize memory footprint.
    """
    mark_delete_position: int = -1
    individual_acks: Set[int] = field(default_factory=set)

    def ack(self, offset: int) -> None:
        """Records an acknowledgment for a specific message offset."""
        if offset <= self.mark_delete_position:
            # Already cumulatively acknowledged; nothing to do
            return

        if offset == self.mark_delete_position + 1:
            # Contiguous progression: advance mark_delete_position
            self.mark_delete_position = offset
            # Compact any contiguous individual acks immediately following it
            while (self.mark_delete_position + 1) in self.individual_acks:
                self.mark_delete_position += 1
                self.individual_acks.remove(self.mark_delete_position)
        else:
            # Out-of-order ack: store in individual acks set
            self.individual_acks.add(offset)

    def is_acked(self, offset: int) -> bool:
        """Checks if a given offset has been acknowledged."""
        if offset <= self.mark_delete_position:
            return True
        return offset in self.individual_acks

    def to_dict(self) -> Dict[str, Any]:
        """Serializes cursor state for JSON transport/storage."""
        return {
            "mark_delete_position": self.mark_delete_position,
            "individual_acks": sorted(list(self.individual_acks))
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Cursor":
        """Reconstructs cursor state from a dictionary."""
        mdp = int(data.get("mark_delete_position", -1))
        # Also support alias "mark_delete_offset" from protocol.md
        if "mark_delete_offset" in data and "mark_delete_position" not in data:
            mdp = int(data["mark_delete_offset"])
        ind_acks = set(int(x) for x in data.get("individual_acks", []))
        return cls(mark_delete_position=mdp, individual_acks=ind_acks)

