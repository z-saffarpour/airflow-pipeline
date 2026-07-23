"""
Configuration for Kafka → MSSQL replication-style sync.
"""
from dataclasses import dataclass
from typing import Optional, Tuple

UniqueKeyColumns = Tuple[str, ...]
UniqueKeys = Tuple[UniqueKeyColumns, ...]


@dataclass(frozen=True)
class KafkaSyncConfig:
    """
    Sync settings for consuming a Kafka topic and upserting into an MSSQL
    target table — same MERGE semantics as MasterDataSyncConfig / MongoSyncConfig.

    Offset tracking uses ``consumer_group`` (at-least-once: commit after
    successful MSSQL upsert per batch).
    """

    source_name: str
    kafka_topic: str
    consumer_group: str
    target_schema: str
    target_table: str
    primary_keys: Optional[Tuple[str, ...]]

    # Consume behaviour
    auto_offset_reset: str = "earliest"
    poll_timeout_sec: float = 1.0
    max_idle_polls: int = 10
    max_messages_per_run: Optional[int] = None
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 300_000

    # Message → row mapping
    value_columns: Optional[Tuple[str, ...]] = None
    exclude_columns: Tuple[str, ...] = ("version_id",)

    # MSSQL upsert
    staging_schema: Optional[str] = None
    unique_keys: Optional[UniqueKeys] = None
    resolve_unique_key_conflicts: bool = True
    use_hash_change_detection: bool = True
    delete_missing: bool = False
    delete_scope_column: Optional[str] = None
    batch_size: int = 10_000

    # Parallel consume by Kafka partition
    use_dynamic_tasks: bool = False
    max_parallel_chunks: int = 8
    max_global_parallel_chunks: int = 32

    def __post_init__(self) -> None:
        if not self.kafka_topic:
            raise ValueError("'kafka_topic' is required")
        if not self.consumer_group:
            raise ValueError("'consumer_group' is required")
        if self.auto_offset_reset not in ("earliest", "latest"):
            raise ValueError("'auto_offset_reset' must be 'earliest' or 'latest'")
        if self.poll_timeout_sec <= 0:
            raise ValueError("'poll_timeout_sec' must be > 0")
        if self.max_idle_polls < 1:
            raise ValueError("'max_idle_polls' must be >= 1")
        if self.max_messages_per_run is not None and self.max_messages_per_run < 1:
            raise ValueError("'max_messages_per_run' must be >= 1 when set")
        if self.max_parallel_chunks < 1:
            raise ValueError("'max_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < 1:
            raise ValueError("'max_global_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < self.max_parallel_chunks:
            raise ValueError(
                "'max_global_parallel_chunks' must be >= 'max_parallel_chunks'"
            )
        if self.delete_missing and not (
            self.delete_scope_column
            or (self.primary_keys and len(self.primary_keys) > 0)
        ):
            raise ValueError(
                "'delete_scope_column' or 'primary_keys' is required when "
                "delete_missing=True"
            )
