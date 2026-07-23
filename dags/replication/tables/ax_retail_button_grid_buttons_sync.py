"""
Airflow DAG: ax.RetailButtonGridButtons Query to Store
===============================================
This DAG executes a business query on ax.RetailButtonGridButtons and sends results to Store.
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
    dag_id = 'ax_retail_button_grid_buttons_sync',
    description = 'masterdata query sync from ax.RetailButtonGridButtons to Store',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 7, 14),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs=int(Variable.get("max_active_runs_retail_button_grid_buttons", default_var=4)),
    retries=int(Variable.get("retries_retail_button_grid_buttons", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_retail_button_grid_buttons", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_retail_button_grid_buttons", default_var=8))),
    # Tags
    tags = ["mssql","store", "master-data"],
    pool = "replication_md_store_sync_pool"
)

sync_config =  MasterDataSyncConfig(
    source_name = 'adhoc_ax_RetailButtonGridButtons',
    source_query= """
            SELECT RECID, [ACTION], ACTIONPROPERTY, BACKCOLOR, BACKCOLOR2, BORDERCOLOR, BUTTONGRIDID, COL, COLOUR,
                   COLSPAN, DISPLAYTEXT, ENABLECUSTOMFONTFORMPOS, FONTCOLOR, FONTSIZE, FONTSTYLE, GRADIENTMODE, ID,
                   IMAGEALIGNMENT, NEWIMAGEALIGNMENT, NEWTEXTALIGNMENT, PICTUREID, ROWNUM, ROWSPAN,
                   USECUSTOMLOOKANDFEEL
            FROM ax.RETAILBUTTONGRIDBUTTONS WITH (READPAST);
          """,
    source_query_count= """
            SELECT COUNT(1) AS CNT
            FROM ax.RETAILBUTTONGRIDBUTTONS WITH (READPAST);
          """,
    primary_keys=('ID', 'BUTTONGRIDID',),

    use_hash_change_detection=True,
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=int(Variable.get("task_chunk_size_retail_button_grid_buttons", default_var=100000)),
    max_parallel_chunks=int(Variable.get("max_parallel_chunks_retail_button_grid_buttons", default_var=8)),
    max_global_parallel_chunks=int(Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)),

    target_schema = 'ax',
    target_table = 'RetailButtonGridButtons',
    staging_schema = Variable.get("mssql_staging_schema", default_var = "crt"),
    delete_missing=bool(int(Variable.get("delete_missing_retail_button_grid_buttons", default_var=0))),
    delete_scope_column='RECID',
    batch_size = int(Variable.get("batch_size_retail_button_grid_buttons", default_var = 30000))
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config = dag_config,
    sync_config = sync_config
)

dag