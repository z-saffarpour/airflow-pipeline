"""
Configuration for MSSQL → Kafka sync (query-based, Gen-2 style).
"""
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class MSSQLToKafkaSyncConfig:
    """
    Sync settings for reading an MSSQL query and producing to a Kafka topic.

    Destination topic / partitions come from ``KafkaTopicConfig`` passed to
    the DAG factory. This config covers source query, keying, batching, and
    optional NTILE chunk planning (same pattern as MasterDataSyncConfig).
    """

    source_name: str
    source_query: str
    source_query_count: str
    key_column: str

    primary_keys: Optional[Tuple[str, ...]] = None
    use_dynamic_tasks: bool = False
    task_chunk_size: int = 100_000
    chunk_column: Optional[str] = None
    max_parallel_chunks: int = 8
    max_global_parallel_chunks: int = 32
    batch_size: int = 10_000
    date_offset: int = 0

    def __post_init__(self) -> None:
        if not self.source_name:
            raise ValueError("'source_name' is required")
        if not self.source_query or not self.source_query.strip():
            raise ValueError("'source_query' is required")
        if not self.source_query_count or not self.source_query_count.strip():
            raise ValueError("'source_query_count' is required")
        if not self.key_column:
            raise ValueError("'key_column' is required")
        if self.batch_size < 1:
            raise ValueError("'batch_size' must be >= 1")
        if self.task_chunk_size < 1:
            raise ValueError("'task_chunk_size' must be >= 1")
        if self.max_parallel_chunks < 1:
            raise ValueError("'max_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < 1:
            raise ValueError("'max_global_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < self.max_parallel_chunks:
            raise ValueError(
                "'max_global_parallel_chunks' must be >= 'max_parallel_chunks'"
            )
        if self.use_dynamic_tasks and not (
            self.chunk_column
            or (self.primary_keys and len(self.primary_keys) > 0)
            or self.key_column
        ):
            raise ValueError(
                "'chunk_column', 'primary_keys', or 'key_column' is required "
                "when use_dynamic_tasks=True"
            )
