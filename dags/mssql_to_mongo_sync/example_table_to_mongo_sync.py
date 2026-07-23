"""
Airflow DAG: MSSQL → MongoDB collection sync (example)
======================================================
Reads from an MSSQL table/query and upserts into MongoDB (replication-style).

Uses mssql_to_mongo_sync_dag_factory.

Required Airflow connections:
  - mssql_dwh_primary      (source)
  - mongo_target_default   (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.mssql_to_mongo_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="mssql_example_table_to_mongo_sync",
    description="Example: sync MSSQL table to MongoDB (upsert/replication-style)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_mssql_example_table_mongo", default_var=1)
    ),
    retries=int(Variable.get("retries_mssql_example_table_mongo", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_mssql_example_table_mongo", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_mssql_example_table_mongo", default_var=8)
        )
    ),
    tags=["mssql", "mongo", "mongodb", "replication", "mssql-to-mongo"],
    pool="mssql_to_mongo_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_dwh_primary"),
    mongo_conn_id=Variable.get("mongo_target_conn_id", default_var="mongo_target_default"),
)

# Replace SELECT / COUNT / keys / target with your real MSSQL source and Mongo target.
# Note: target_schema = MongoDB database name, target_table = collection name.
# Primary key field ``id`` is mapped to MongoDB ``_id`` by default.
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
    target_schema="app",
    target_table="example_table",
    staging_schema=Variable.get("mongo_staging_database", default_var="staging"),
    delete_missing=bool(
        int(Variable.get("delete_missing_mssql_example_table_mongo", default_var=0))
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For large tables enable chunking (SQL Server NTILE):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=int(Variable.get("batch_size_mssql_example_table_mongo", default_var=10000)),
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
