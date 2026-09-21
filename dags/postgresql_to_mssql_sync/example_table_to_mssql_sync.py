"""
Airflow DAG: PostgreSQL → MSSQL table sync (example)
====================================================
Reads from a PostgreSQL table and upserts into MSSQL.

Uses postgresql_to_mssql_sync_dag_factory.

Required Airflow connections:
  - postgres_source_default  (source)
  - mssql_default        (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from pipeline.utils.parse_config import parse_int, parse_str

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.postgresql_to_mssql_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="postgresql_example_table_to_mssql_sync",
    description="Example: sync PostgreSQL table to MSSQL (upsert)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=parse_int("max_active_runs_postgresql_example_table", 1),
    retries=parse_int("retries_postgresql_example_table", 2),
    retry_delay=timedelta(
        minutes=parse_int("retry_delay_minutes_postgresql_example_table", 5)
    ),
    execution_timeout=timedelta(
        hours=parse_int("execution_timeout_hours_postgresql_example_table", 8)
    ),
    tags=["postgresql", "mssql", "postgresql-to-mssql"],
    pool="postgresql_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    postgres_conn_id=parse_str("postgres_source_conn_id", "postgres_source_default"),
    mssql_conn_id=parse_str("mssql_target_conn_id", "mssql_default"),
)

# Replace SELECT / COUNT / keys / target with your real PostgreSQL source and MSSQL target.
# Note: source_query must use PostgreSQL dialect (double-quoted identifiers if needed).
sync_config = MasterDataSyncConfig(
    source_name="postgresql_example_table",
    source_query="""
            SELECT id, code, name, updated_at
            FROM public.example_table
          """,
    source_query_count="""
            SELECT COUNT(1) AS CNT
            FROM public.example_table
          """,
    primary_keys=("id",),
    target_schema="dbo",
    target_table="ExampleTable",
    staging_schema=parse_str("mssql_staging_schema", "crt"),
    delete_missing=bool(
        parse_int("delete_missing_postgresql_example_table", 0)
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For large tables enable chunking (PostgreSQL NTILE):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=parse_int("batch_size_postgresql_example_table", 10000),
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
