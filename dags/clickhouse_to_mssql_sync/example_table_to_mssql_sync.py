"""
Airflow DAG: ClickHouse → MSSQL table sync (example)
====================================================
Reads from a ClickHouse table/query and upserts into MSSQL.

Uses clickhouse_to_mssql_sync_dag_factory.

Required Airflow connections:
  - clickhouse_default  (source)
  - mssql_default   (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.clickhouse_to_mssql_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="clickhouse_example_table_to_mssql_sync",
    description="Example: sync ClickHouse table to MSSQL (upsert)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_clickhouse_example_table", default_var=1)
    ),
    retries=int(Variable.get("retries_clickhouse_example_table", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get(
                "retry_delay_minutes_clickhouse_example_table", default_var=5
            )
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get(
                "execution_timeout_hours_clickhouse_example_table", default_var=8
            )
        )
    ),
    tags=["clickhouse", "mssql", "clickhouse-sync"],
    pool="clickhouse_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    clickhouse_conn_id=Variable.get(
        "clickhouse_source_conn_id", default_var="clickhouse_default"
    ),
    mssql_conn_id=Variable.get(
        "mssql_target_conn_id", default_var="mssql_default"
    ),
)

# Replace SELECT / COUNT / keys / target with your real ClickHouse source and MSSQL target.
# Use ClickHouse SQL dialect (backticks for identifiers, count(), etc.).
sync_config = MasterDataSyncConfig(
    source_name="clickhouse_example_table",
    source_query="""
            SELECT id, code, name, updated_at
            FROM example_table
          """,
    source_query_count="""
            SELECT count() AS CNT
            FROM example_table
          """,
    primary_keys=("id",),
    target_schema="dbo",
    target_table="ExampleTableFromClickHouse",
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=bool(
        int(Variable.get("delete_missing_clickhouse_example_table", default_var=0))
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For large tables enable chunking (requires ClickHouse window functions / ntile):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=int(
        Variable.get("batch_size_clickhouse_example_table", default_var=10000)
    ),
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
