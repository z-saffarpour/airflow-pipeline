"""
Abstract Base Class for Message Producer.
Provides contract for implementing different message brokers.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class MessageProducer(ABC):
    """
    Abstract interface for producing messages to a message broker.
    Implement this interface to add support for different message brokers.
    """
    
    @abstractmethod
    def produce(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Produce a message to the message broker.
        
        Args:
            topic: Topic/queue name
            key: Message key
            value: Message value
            headers: Optional message headers
        """
        pass
    
    @abstractmethod
    def flush(self, timeout: float = 30.0) -> None:
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum time to wait (seconds)
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the producer and cleanup resources."""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """
        Get producer statistics.
        
        Returns:
            Dictionary with producer stats
        """
        pass
    
    @abstractmethod
    def send_batch_to_kafka(
        self,
        batch: list,
        topic: str,
        source_name: str,
        key_column: Optional[str],
        execution_date: str,
        batch_number: int,
        snapshot_id: int,
    ) -> None:
        """
        Send a batch of records to Kafka.

        Args:
            batch: List of row dicts to send
            topic: Kafka topic name
            source_name: Source identifier for headers
            key_column: Column for message key (or None for auto-detect)
            execution_date: Execution date
            batch_number: Batch number for headers
        """
        pass