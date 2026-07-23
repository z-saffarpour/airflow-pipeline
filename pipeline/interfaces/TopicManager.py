"""
Abstract Base Class for Topic Manager.
Provides contract for managing message broker topics/queues.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class TopicManager(ABC):
    """
    Abstract interface for managing message broker topics/queues.
    Includes topic management and monitoring capabilities.
    """
    
    @abstractmethod
    def ensure_topic_exists(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Ensure a topic exists, creating it if necessary.
        
        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Additional topic configuration
        """
        pass

    @abstractmethod
    def get_topic_stats(self, topic_name: str) -> Dict:
        """
        Get general topic statistics (number of messages, partitions, etc.).
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary with topic statistics
        """
        pass

    @abstractmethod
    def check_consumer_lag(
        self,
        topic_name: str,
        consumer_group: str
    ) -> Dict:
        """
        Check Lag in Kafka Consumer Group.
        
        Args:
            topic_name: Name of the topic
            consumer_group: Consumer group ID
        
        Returns:
            Dictionary with lag information for each partition
        """
        pass

    @abstractmethod
    def sample_messages(
        self,
        topic_name: str,
        partition: int = 0,
        count: int = 10,
        offset: str = 'latest'
    ) -> List[Dict]:
        """
        Sample messages from topic.
        
        Args:
            topic_name: Topic name
            partition: Partition number
            count: Number of messages to sample
            offset: 'earliest' or 'latest'
            
        Returns:
            List of sampled messages
        """
        pass

    @abstractmethod
    def check_health(
        self,
        topic_name: str,
        consumer_group: str,
        expected_daily_records: int = 5000000
    ) -> Dict:
        """
        Check overall pipeline health.
        
        Args:
            topic_name: Name of the topic
            consumer_group: Consumer group ID
            expected_daily_records: Expected daily record count for threshold calculation
            
        Returns:
            Health report including topic status, lag info, and recommendations
        """
        pass
