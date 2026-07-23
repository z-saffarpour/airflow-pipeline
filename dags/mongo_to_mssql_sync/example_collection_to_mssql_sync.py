"""
Airflow DAG: MongoDB → MSSQL collection sync (example)
======================================================
Reads from a MongoDB collection and upserts into MSSQL.

Uses mongo_to_mssql_sync_dag_factory.

Required Airflow connections:
  - mongo_source_default  (source)
  - mssql_default     (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MongoSyncConfig import MongoSyncConfig

from template.mongo_to_mssql_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="mongo_example_collection_to_mssql_sync",
    description="Example: sync MongoDB collection to MSSQL (upsert)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_mongo_example_collection", default_var=1)
    ),
    retries=int(Variable.get("retries_mongo_example_collection", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_mongo_example_collection", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_mongo_example_collection", default_var=8)
        )
    ),
    tags=["mongo", "mongodb", "mssql", "mongo-sync"],
    pool="mongo_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    mongo_conn_id=Variable.get("mongo_source_conn_id", default_var="mongo_source_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_default"),
)

# Replace collection / projection / keys / target with your real Mongo source and MSSQL target.
# Field names in projection (after rename_id_to) must match MSSQL destination columns.
sync_config = MongoSyncConfig(
    source_name="mongo_example_collection",
    collection="example_collection",
    database=Variable.get("mongo_source_database", default_var="") or None,
    filter_query={},  # e.g. {"status": "active"} or {"updated_at": {"$gte": ...}}
    projection={
        "_id": 1,
        "code": 1,
        "name": 1,
        "updated_at": 1,
    },
    sort=(("updated_at", 1),),
    rename_id_to="id",
    primary_keys=("id",),
    target_schema="dbo",
    target_table="ExampleCollection",
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=bool(
        int(Variable.get("delete_missing_mongo_example_collection", default_var=0))
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For large collections enable chunking (uses $bucketAuto):
    # use_dynamic_tasks=True,
    # chunk_column="id",  # maps to Mongo _id when rename_id_to="id"
    # task_chunk_size=100_000,
    batch_size=int(
        Variable.get("batch_size_mongo_example_collection", default_var=10000)
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
