"""
Airflow DAG: Masterdata → Store MSSQL sync (example)
====================================================
Reads from the replication MD publisher and upserts into a dynamic
store MSSQL database (store_number Airflow param).

Uses mssql_masterdata_to_mssql_store_sync_dag_factory.create_dag.

Connections are resolved inside the factory (publisher + per-store target).
Pass store_number when triggering the DAG.

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.mssql_masterdata_to_mssql_store_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="example_table_to_store_sync",
    description="Example: sync masterdata table to store MSSQL (upsert)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_example_table_store", default_var=1)
    ),
    retries=int(Variable.get("retries_example_table_store", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_example_table_store", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_example_table_store", default_var=8)
        )
    ),
    tags=["mssql", "store", "master-data", "example"],
    pool="replication_md_store_sync_pool",
)

# Replace SELECT / COUNT / keys / target with your real publisher source
# and store target table.
sync_config = MasterDataSyncConfig(
    source_name="adhoc_example_table",
    source_query="""
            SELECT id, code, name, updated_at
            FROM dbo.ExampleTable WITH (READPAST)
          """,
    source_query_count="""
            SELECT COUNT(1) AS CNT
            FROM dbo.ExampleTable WITH (READPAST)
          """,
    primary_keys=("id",),
    target_schema="dbo",
    target_table="ExampleTable",
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=bool(
        int(Variable.get("delete_missing_example_table_store", default_var=0))
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For large tables enable chunking (SQL Server NTILE):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=int(
        Variable.get("batch_size_example_table_store", default_var=10000)
    ),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
)

dag
