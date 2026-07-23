
"""
Configuration object used to define and control a SQL Server → Kafka
synchronization DAG in Apache Airflow.

This dataclass centralizes all operational, scheduling, retry,
and infrastructure settings required for executing a query on
SQL Server and streaming the results to Kafka in batches.

Features:
    - Centralized configuration for Airflow DAG generation
    - Incremental query support using watermark patterns
    - Batch-based data transfer to Kafka for large datasets
    - Kafka topic auto-creation configuration
    - Retry and failure handling controls
    - DAG scheduling and concurrency management
    - Data quality guard using minimum expected record checks
    - Extensible hook mechanism for custom SQL Server hooks
    - Airflow metadata management (tags, owner, description)

Usage:
    Example configuration for creating a Query → Kafka sync DAG:

        config = QuerySyncConfig(
            dag_id="orders_incremental_sync",
            mssql_conn_id="mssql_prod",
            kafka_conn_id="kafka_cluster",
            kafka_topic="orders_topic",
            source_name="orders",
            query=\"""
                SELECT *
                FROM dbo.orders
                WHERE updated_at > "{{ds}}"
            \""",
            key_column="order_id",
            batch_size=50000,
            schedule="@hourly",
            min_expected_records=1,
        )

    The configuration instance is then passed to a DAG factory
    or pipeline builder that constructs the Airflow DAG.

Author: Senior Data Engineer
Version: 4.0
"""

from dataclasses import dataclass, field
from typing import Dict, Any

from pipeline.config import QueryConfiguration
from pipeline.config.SyncConfig import SyncConfig

# ============================================================================
# CONFIGURATION DATACLASS
# ============================================================================

@dataclass
class QuerySyncConfig(SyncConfig):
    """
    Configuration for a SQL Server Query → Kafka sync DAG.

    Required:
        dag_id        : Unique DAG identifier in Airflow.
        mssql_conn_id : Airflow Connection ID for SQL Server.
        kafka_conn_id : Airflow Connection ID for Kafka.
        kafka_topic   : Target Kafka topic name.
        query         : SQL query to execute (supports :watermark placeholder).

    Optional:
        schedule              : Cron expression or timedelta (default: daily).
        start_date            : DAG start date (default: 2025-01-01).
        catchup               : Enable backfill (default: False).
        retries               : Number of task retries (default: 2).
        retry_delay           : Delay between retries (default: 5 min).
        batch_size            : Rows per Kafka batch (default: 1000).
        kafka_topic_partitions: Partitions for auto-created topic (default: 1).
        kafka_replication     : Replication factor for auto-created topic (default: 1).
        tags                  : DAG tags for Airflow UI filtering.
        owner                 : DAG owner (default: "data-engineering").
        description           : DAG description shown in Airflow UI.
        db_config             : DatabaseHookConfig instance (default: mssql).
 
    Note:
        hook_class and db_config are inherited from BaseSyncConfig.
        No duplication needed here.       
    """
        
    # Required query fields
    query_config : QueryConfiguration = None

    
    def __post_init__(self):    
        super().__post_init__()

        if self.dag_config is None and self.dag_config:
            if self.dag_config.tags is None:       
                self.dag_config.tags = field(default_factory=lambda: ["mssql", "kafka", "query"])
            if self.dag_config.description is None: 
                self.dag_config.description = "SQL query to Kafka sync"
              
                   
    def to_params_dict(self) -> Dict[str, Any]:
        """Convert to Airflow params dict for backward compatibility."""
        return {
            'mssql_conn_id': self.connection_config.mssql_conn_id,
            'kafka_conn_id': self.connection_config.kafka_conn_id,
            'source_name': self.query_config.source_name,
            'query': self.query_config.query,
            'query_params': self.query_config.query_params,
            'key_column': self.query_config.key_column,
            'min_expected_records': self.query_config.min_expected_records,
            'kafka_topic': self.kafka_topic_config.kafka_topic,
            'num_partitions': self.kafka_topic_config.kafka_topic_partitions,
            'replication_factor': self.kafka_topic_config.kafka_replication,
            'date_offset': self.date_offset,
            'batch_size': self.batch_size,
        }