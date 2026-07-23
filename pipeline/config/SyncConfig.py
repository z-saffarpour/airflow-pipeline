"""
Base configuration model for defining common operational and scheduling
parameters used across all data pipeline DAGs.
This class centralizes shared Airflow configuration such as scheduling,
retry policies, execution limits, metadata, and resource controls.
Pipeline-specific configuration classes (e.g., QuerySyncConfig, TableSyncConfig)
should inherit from this class to ensure consistency across pipelines and
to avoid duplication of common settings.
By standardizing these parameters in a base configuration, the data platform
ensures uniform behavior for scheduling, retries, concurrency management,
and operational metadata across all pipeline implementations.

Features:
    Shared scheduling configuration for Airflow DAGs
    Standardized retry and failure handling policies
    Centralized execution timeout and resource controls
    Consistent metadata fields (owner, tags, description)
    Concurrency and parallelism limits for DAG execution
    Pool assignment for resource management
    Reusable foundation for all pipeline configuration models
    Centralized database hook resolution (db_config + hook_class)
Usage:
    Pipeline-specific configuration classes should inherit from this class:
    
    @dataclass
    class QuerySyncConfig(BaseSyncConfig):
        kafka_topic: str
        query: str

    @dataclass
    class TableSyncConfig(BaseSyncConfig):
        table_name: str
        primary_key_column: str

Example instantiation:
    config = QuerySyncConfig(
        dag_id="orders_query_sync",
        kafka_topic="orders.topic",
        query="SELECT * FROM dbo.orders WHERE updated_at > :watermark"
    )

Author: Senior Data Engineer
Version: 5.0
"""
from dataclasses import dataclass, field
from typing import Optional

from pipeline.config.DatabaseHookConfig import DatabaseHookConfig

@dataclass
class SyncConfig:    
    # -------------------------------------------------------------------------
    # Database hook configuration — centralized here to avoid duplication
    # in child classes (QuerySyncConfig, TableSyncConfig)
    # -------------------------------------------------------------------------
    db_config: DatabaseHookConfig = field(
        default_factory=lambda: DatabaseHookConfig(database_type="mssql")
    )
    
    #
    is_send_kafka: bool = True
    is_send_clickhouse: bool = False
    fail_on_error: bool = True
    
    # Date config
    date_offset: Optional[int] = None
    
    # Batch config
    batch_size: int = 10000
    
    # -------------------------------------------------------------------------
    # Backward-compatible property: DAG factories use config.hook_class
    # This delegates to db_config so all consumers continue to work unchanged.
    # -------------------------------------------------------------------------
    @property
    def hook_class(self):
        """Unified access to resolved hook class."""
        return self.db_config.hook_class
    
    def __post_init__(self):
        # Ensure db_config is always initialized with correct database_type            
        if not self.db_config.database_type:
            self.db_config.database_type = "mssql"
        if self.db_config.hook_class is None:
            self.db_config._resolve_hook_class()
            
        # --- numeric validations ---
        if self.batch_size <= 0:
            raise ValueError("'batch_size' must be > 0")