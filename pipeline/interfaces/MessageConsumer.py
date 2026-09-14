"""
Abstract Base Class for Message Consumer.
Provides contract for implementing different message broker consumers.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple


class MessageConsumer(ABC):
    """
    Abstract interface for consuming messages from a message broker.
    Implement this interface to add support for different message brokers.
    """

    @abstractmethod
    def plan_partition_chunks(self, topic: str) -> List[Dict[str, Any]]:
        """
        List a topic's partitions/shards for parallel mapped sync tasks.

        Args:
            topic: Topic/queue name

        Returns:
            List of chunk descriptors, one per partition/shard (exact keys
            depend on the implementation, e.g. chunk_no, partition_id,
            topic, row_count).
        """
        pass

    @abstractmethod
    def iter_batches(
        self,
        topic: str,
        assigned_partitions: Optional[Sequence[int]] = None,
    ) -> Generator[Tuple[List[Dict[str, Any]], Dict[Any, Any], Any], None, None]:
        """
        Stream deserialized row batches with a live consumer for offset commits.

        Args:
            topic: Topic/queue name
            assigned_partitions: Optional partition subset (manual assign,
                for parallel mapped tasks); None consumes via normal
                subscription/rebalancing.

        Yields:
            (batch_rows, offsets_to_commit, consumer) — process the batch,
            then call ``commit_offsets(consumer, offsets_to_commit)`` before
            requesting the next one.
        """
        pass

    @abstractmethod
    def commit_offsets(self, consumer: Any, offsets: Dict[Any, Any]) -> None:
        """
        Commit the highest processed offset per partition (synchronous).

        Args:
            consumer: The live consumer object yielded by ``iter_batches``
            offsets: Offsets to commit, as yielded by ``iter_batches``
        """
        pass
