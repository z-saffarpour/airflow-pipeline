"""
Configuration object for defining a SQL Server table → Kafka
synchronization DAG in Apache Airflow.

This configuration supports both full and incremental table
replication from SQL Server into Kafka, with built-in batching,
execution control, and data consistency guarantees.

The class is designed to be consumed by a DAG factory function
(e.g. create_table_sync_dag) and should not be executed directly.

Features:
    - Full or incremental table synchronization
    - Incremental loading using execution_date and date offsets
    - Support for integer (YYYYMMDD) and DATE/DATETIME columns
    - Batch-based extraction for large tables
    - Exactly-once delivery semantics using idempotent Kafka producer
    - Primary key–based record identification
    - Optional column-level projection
    - Kafka topic auto-creation support
    - Airflow retry, timeout, and concurrency controls
    - Extensible SQL Server hook override mechanism

Usage:
    Example configuration for a dimension table sync:

        config = TableSyncConfig(
            dag_id="dim_customer_sync",
            mssql_conn_id="mssql_prod",
            kafka_conn_id="kafka_prod",
            kafka_topic="dim.customer",
            table_name="DimCustomer",
            primary_key_column="CustomerID",
            date_column="UpdateDate",
            date_column_type="date",
            batch_size=50000,
            schedule="@daily",
        )

    The configuration instance is passed to a DAG factory:

        dag = create_table_sync_dag(config)

Author: Senior Data Engineer
Version: 4.0
"""

from dataclasses import dataclass, field
from typing import Dict, Any

from pipeline.config import TableConfiguration
from pipeline.config.SyncConfig import SyncConfig

# ============================================================================
# CONFIGURATION DATACLASS
# ============================================================================

@dataclass
class TableSyncConfig(SyncConfig):
    """
    Configuration for a SQL Server Full Table → Kafka sync DAG.
    """
    
    # Required
    table_config : TableConfiguration = None
    
    def __post_init__(self):                 
        super().__post_init__()
        
        if self.dag_config is None and self.dag_config:
            if self.dag_config.tags is None:
                self.dag_config.tags = field(default_factory=lambda: ["mssql", "kafka", "table"])
            if self.dag_config.description is None: 
                self.dag_config.description = "SQL Server table to Kafka sync"
                         
    def to_params_dict(self) -> Dict[str, Any]:
        """Convert to Airflow params dict for backward compatibility."""
        return {
            'mssql_conn_id': self.connection_config.mssql_conn_id,
            'kafka_conn_id': self.connection_config.kafka_conn_id,
            'table_name': self.table_config.table_name,
            'primary_key_column': self.table_config.primary_key_column,
            'date_column': self.table_config.date_column,
            'date_column_type': self.table_config.date_column_type,
            'columns': self.table_config.columns,
            'batch_size': self.batch_size,
        }
