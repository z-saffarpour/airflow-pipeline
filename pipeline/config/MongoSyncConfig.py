"""
Configuration for MongoDB → MSSQL replication-style sync.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

UniqueKeyColumns = Tuple[str, ...]
UniqueKeys = Tuple[UniqueKeyColumns, ...]
MongoSort = Tuple[Tuple[str, int], ...]
MongoPipeline = Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class MongoSyncConfig:
    """
    Sync settings for reading a MongoDB collection (or aggregation) and
    upserting into an MSSQL target table — same MERGE semantics as MasterDataSyncConfig.
    """

    source_name: str
    collection: str
    target_schema: str
    target_table: str
    primary_keys: Optional[Tuple[str, ...]]
    database: Optional[str] = None
    filter_query: Optional[Dict[str, Any]] = None
    projection: Optional[Dict[str, Any]] = None
    sort: Optional[MongoSort] = None
    aggregation_pipeline: Optional[MongoPipeline] = None
    rename_id_to: Optional[str] = "id"
    serialize_nested: bool = True
    staging_schema: Optional[str] = None
    unique_keys: Optional[UniqueKeys] = None
    resolve_unique_key_conflicts: bool = True
    use_hash_change_detection: bool = True
    use_dynamic_tasks: bool = False
    task_chunk_size: int = 100_000
    chunk_column: Optional[str] = None
    delete_scope_column: Optional[str] = None
    max_parallel_chunks: int = 8
    max_global_parallel_chunks: int = 32
    delete_missing: bool = False
    batch_size: int = 10_000

    def __post_init__(self) -> None:
        if not self.collection:
            raise ValueError("'collection' is required")
        if self.max_parallel_chunks < 1:
            raise ValueError("'max_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < 1:
            raise ValueError("'max_global_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < self.max_parallel_chunks:
            raise ValueError(
                "'max_global_parallel_chunks' must be >= 'max_parallel_chunks'"
            )
        if self.aggregation_pipeline and self.use_dynamic_tasks:
            raise ValueError(
                "use_dynamic_tasks is not supported with aggregation_pipeline; "
                "use find-based filter_query/projection instead"
            )
