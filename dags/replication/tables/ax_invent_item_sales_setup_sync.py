"""
Airflow DAG: ax.InventItemSalesSetup Query to Store
===============================================
This DAG executes a business query on ax.InventItemSalesSetup and sends results to Store.
Uses query_mssql_replication_md_store_sync_dag_factory template tasks.

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable # type: ignore

from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.query_mssql_replication_md_store_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config = DAGConfig(
    dag_id = 'ax_invent_item_sales_setup_sync',
    description = 'masterdata query sync from ax.InventItemSalesSetup to Store',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 7, 14),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs=int(Variable.get("max_active_runs_invent_item_sales_setup", default_var=4)),
    retries=int(Variable.get("retries_invent_item_sales_setup", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_invent_item_sales_setup", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_invent_item_sales_setup", default_var=8))),
    # Tags
    tags = ["mssql","store", "master-data"],
    pool = "replication_md_store_sync_pool"
)

sync_config =  MasterDataSyncConfig(
    source_name = 'adhoc_ax_InventItemSalesSetup',
    source_query= """
            SELECT RECID, INVENTDIMID, INVENTDIMIDDEFAULT, ITEMID, MANDATORYINVENTLOCATION, MANDATORYINVENTSITE,
                   DATAAREAID
            FROM ax.INVENTITEMSALESSETUP WITH (READPAST);
          """,
    source_query_count= """
            SELECT COUNT(1) AS CNT
            FROM ax.INVENTITEMSALESSETUP WITH (READPAST);
          """,
    primary_keys=('ITEMID', 'INVENTDIMID', 'DATAAREAID',),

    use_hash_change_detection=True,
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=int(Variable.get("task_chunk_size_invent_item_sales_setup", default_var=100000)),
    max_parallel_chunks=int(Variable.get("max_parallel_chunks_invent_item_sales_setup", default_var=8)),
    max_global_parallel_chunks=int(Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)),

    target_schema = 'ax',
    target_table = 'InventItemSalesSetup',
    staging_schema = Variable.get("mssql_staging_schema", default_var = "crt"),
    delete_missing=bool(int(Variable.get("delete_missing_invent_item_sales_setup", default_var=0))),
    delete_scope_column='RECID',
    batch_size = int(Variable.get("batch_size_invent_item_sales_setup", default_var = 30000))
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config = dag_config,
    sync_config = sync_config
)

dag