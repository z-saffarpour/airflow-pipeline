from dataclasses import dataclass
from typing import Optional, Tuple

UniqueKeyColumns = Tuple[str, ...]
UniqueKeys = Tuple[UniqueKeyColumns, ...]

@dataclass(frozen=True)
class MasterDataSyncConfig:
    source_name: str
    source_query:str
    source_query_count:str
    target_schema:str
    target_table:str
    primary_keys: Optional[Tuple[str, ...]]
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
    delete_missing : bool = False
    batch_size: int = 10_000

    def __post_init__(self) -> None:
        if self.max_parallel_chunks < 1:
            raise ValueError("'max_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < 1:
            raise ValueError("'max_global_parallel_chunks' must be >= 1")
        if self.max_global_parallel_chunks < self.max_parallel_chunks:
            raise ValueError(
                "'max_global_parallel_chunks' must be >= 'max_parallel_chunks'"
            )
