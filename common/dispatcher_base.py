"""Dispatcher abstract base class defining the contract for subscription dispatching."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from common.protocol import MessageId


class BaseDispatcher(ABC):
    """Abstract interface for topic-subscription dispatching logic.
    
    Decouples message dispatching algorithms (Exclusive single-consumer FIFO vs.
    Shared multi-consumer Round-Robin) from underlying network connection management.
    """

    def __init__(self, topic: str, subscription_name: str):
        self.topic = topic
        self.subscription_name = subscription_name

    @abstractmethod
    def add_consumer(self, consumer_id: str, connection: Any) -> bool:
        """Attaches a consumer to this subscription.
        
        Returns:
            bool: True if attached successfully, False if rejected (e.g., Exclusive is busy).
        """
        pass

    @abstractmethod
    def remove_consumer(self, consumer_id: str) -> None:
        """Detaches a consumer (e.g., upon socket disconnect)."""
        pass

    @abstractmethod
    def dispatch(self, message: Dict[str, Any]) -> bool:
        """Dispatches a message to an eligible connected consumer.
        
        Returns:
            bool: True if dispatched to a consumer, False if no consumer was available.
        """
        pass

    @abstractmethod
    def on_ack(self, message_id: MessageId, consumer_id: str) -> bool:
        """Records an explicit acknowledgment from a consumer."""
        pass

    @abstractmethod
    def get_active_consumers(self) -> List[str]:
        """Returns the list of active consumer identifiers."""
        pass

