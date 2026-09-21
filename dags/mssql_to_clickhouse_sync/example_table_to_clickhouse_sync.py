"""
Airflow DAG: MSSQL → ClickHouse table sync (example)
====================================================
Reads from an MSSQL table/query and bulk-inserts into ClickHouse
(ReplacingMergeTree-friendly INSERT with version_id).

Uses mssql_to_clickhouse_sync_dag_factory.

Required Airflow connections:
  - mssql_default      (source)
  - clickhouse_default     (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from pipeline.utils.parse_config import parse_int, parse_str

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.mssql_to_clickhouse_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="mssql_example_table_to_clickhouse_sync",
    description="Example: sync MSSQL table to ClickHouse (bulk INSERT)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=parse_int("max_active_runs_mssql_example_table_ch", 1),
    retries=parse_int("retries_mssql_example_table_ch", 2),
    retry_delay=timedelta(
        minutes=parse_int("retry_delay_minutes_mssql_example_table_ch", 5)
    ),
    execution_timeout=timedelta(
        hours=parse_int("execution_timeout_hours_mssql_example_table_ch", 8)
    ),
    tags=["mssql", "clickhouse", "sync", "mssql-to-clickhouse"],
    pool="mssql_to_clickhouse_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=parse_str("mssql_source_conn_id", "mssql_default"),
    clickhouse_conn_id=parse_str("clickhouse_target_conn_id", "clickhouse_default"),
)

# Replace SELECT / COUNT / keys / target with your real MSSQL source and ClickHouse target.
# Note: target_schema is the ClickHouse database name.
# Prefer ReplacingMergeTree (or similar) on the target so INSERT acts as upsert via version_id.
sync_config = MasterDataSyncConfig(
    source_name="mssql_example_table",
    source_query="""
            SELECT id, code, name, updated_at
            FROM dbo.ExampleTable
          """,
    source_query_count="""
            SELECT COUNT(1) AS CNT
            FROM dbo.ExampleTable
          """,
    primary_keys=("id",),
    target_schema="default",
    target_table="example_table",
    # delete_missing / staging / hash detection are not used for ClickHouse path
    use_hash_change_detection=False,
    delete_missing=False,
    # For large tables enable chunking (SQL Server NTILE):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=parse_int("batch_size_mssql_example_table_ch", 10000),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)

dag
