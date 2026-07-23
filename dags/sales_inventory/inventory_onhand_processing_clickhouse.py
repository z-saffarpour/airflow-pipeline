"""
Inventory On-Hand Processing Pipeline
======================================

This DAG orchestrates the complete on-hand inventory data processing workflow in ClickHouse.
It handles incremental updates, backfill operations for discontinued items, and maintains
historical snapshots for temporal analysis.

Architecture:
-------------
- Source: inventory.onhand_lite (staging table synced from MSSQL)
- Target: inventory.onhand (main operational table)
- Archive: inventory.onhand_history (point-in-time snapshots)

Key Features:
-------------
1. Delta Detection: Uses RowHash comparison to identify changed records
2. Backfill Logic: Inserts zero-valued records for items removed from source
3. Historical Tracking: Maintains versioned snapshots for trend analysis
4. Audit Trail: Comprehensive logging via AuditLogger for all operations

Data Flow:
----------
vw_onhand_position_snapshot → onhand (incremental merge)
                            → onhand_history (full snapshot)
vw_onhand_backfill_missing → onhand (zero-valued records)

Version Management:
-------------------
Each DAG run generates a unique version_id (timestamp: YYYYMMDDHHMMSS) that tags
all records processed in that execution, enabling point-in-time recovery and lineage tracking.

Owner: Zahra Saffarpour
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from time import time

from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.decorators import task # type: ignore
from airflow.exceptions import AirflowException # type: ignore

from pipeline.database import ClickHouseWriter
from pipeline.utils.validation import validate_clickhouse_conn
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.config.AuditConfig import EventType, EventStatus

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION 
# ============================================================================

CLICKHOUSE_CONN_ID = "clickhouse_default"

# ============================================================================
# TASKS 
# ============================================================================
@task
def validate_clickhouse_connection():
    """
    Validate ClickHouse connection before DAG execution.
    
    Ensures that the ClickHouse connection is available and responsive
    before proceeding with data processing tasks.
    
    Returns:
        Dict: Connection validation result
    """
    result = validate_clickhouse_conn(CLICKHOUSE_CONN_ID)
    return result

@task
def generate_version_task(**context) -> str:
    """
    Generate a unique version timestamp for this DAG run.
    
    This version will be consistent across all tasks in the same DAG execution
    and will be stored as a column in ClickHouse.
    
    Returns:
        str: Version in format YYYYMMDDHHMMSS (e.g., '20260406143025')
    """
    version = datetime.now().strftime('%Y%m%d%H%M%S')
    
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    
    audit.log(
        EventType.DAG_COMPLETED,
        'generate_version',
        EventStatus.SUCCESS,
        {'version': version}
    )
    
    logger.info(f"Generated version for this DAG run: {version}")
    return version
 
@task
def process_onhand_task(version: str, **context) -> Dict[str, Any]:
    """
    Process and merge on-hand inventory data from onhand_lite into onhand table.
    
    This task performs incremental updates to the main inventory table by:
    1. Calculating ActualPhysical = AvailablePhysical + SalesQTY from vw_sales_total
    2. Inserting or updating records where RowHash has changed (delta detection)
    3. Maintaining data consistency with the source system
    
    Args:
        version: Unique version timestamp for this DAG run
        **context: Airflow task context
        
    Returns:
        Dict: Processing result containing status, version, and duration_seconds
        
    Raises:
        AirflowException: If ClickHouse query execution fails
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(
        EventType.PROCESSING_SERVER,
        task_id,
        EventStatus.STARTED,
        {'version': version}
    )
    
    start_time = time()
    
    clickhouse_query= """
    INSERT INTO inventory.onhand (
        InventLocationID, ItemID, InventSiteID, 
        PhysicalInventory, PhysicalReserved, Ordered, 
        OnOrdered, OrderedReserved, OrderedInTotal, 
        TotalAvailable, AvailablePhysical, ActualPhysical, 
        SalesQTY, AdjustmentDatetime, RowHash, version_id
    )
    SELECT ol.InventLocationID,
           ol.ItemID,
           ol.InventSiteID,
           ol.PhysicalInventory,
           ol.PhysicalReserved,
           ol.Ordered,
           ol.OnOrdered,
           ol.OrderedReserved,
           ol.OrderedInTotal,
           ol.TotalAvailable,
           ol.AvailablePhysical,
           ol.ActualPhysical,
           ol.SalesQTY,
           ol.AdjustmentDatetime,
           ol.RowHash,
           {version} AS version_id
    FROM inventory.vw_onhand_position_snapshot as ol
    LEFT JOIN inventory.onhand AS oi ON oi.InventLocationID = ol.InventLocationID
                                        AND oi.ItemID = ol.ItemID
    WHERE oi.RowHash = '' OR oi.RowHash != ol.RowHash;
    """.format(version=version)
    
    try:
        clickhouse_writer = ClickHouseWriter(CLICKHOUSE_CONN_ID)
        result = clickhouse_writer.execute_command(clickhouse_query)
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {
                'version': version,
                'duration_seconds': round(duration, 2)
            }
        )
        
        logger.info(f"Onhand processing completed in {duration:.2f}s with version {version}")
        
        return {
            "status": "success",
            "version": version,
            "duration_seconds": round(duration, 2)
        }
    except Exception as e:
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {'version': version},
            error=str(e)
        )
        
        logger.error(f"Onhand processing failed after {duration:.2f}s: {str(e)}")
        raise AirflowException(f"ClickHouse query execution failed: {str(e)}") from e

@task
def process_backfill_missing_onhand_task(version: str, **context) -> Dict[str, Any]:
    """
    Backfill missing inventory records with zero values.
    
    This task handles items that exist in the onhand table but are no longer
    present in onhand_lite (deleted or discontinued items). It ensures data
    completeness by inserting zero-valued records for these missing items.
    
    Logic:
    - Identifies records in onhand that don't exist in onhand_lite
    - Inserts zero-valued records to maintain historical continuity
    - Uses a default adjustment datetime of '1900-01-01' for tracking
    
    Args:
        version: Unique version timestamp for this DAG run
        **context: Airflow task context
        
    Returns:
        Dict: Processing result containing status, version, and duration_seconds
        
    Raises:
        AirflowException: If ClickHouse query execution fails
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(
        EventType.PROCESSING_SERVER,
        task_id,
        EventStatus.STARTED,
        {'version': version}
    )
    
    start_time = time()
    
    clickhouse_query= """
    INSERT INTO inventory.onhand (
        InventLocationID, ItemID, InventSiteID, 
        PhysicalInventory, PhysicalReserved, Ordered, 
        OnOrdered, OrderedReserved, OrderedInTotal, 
        TotalAvailable, AvailablePhysical, ActualPhysical, 
        SalesQTY, AdjustmentDatetime, RowHash, version_id
    )
    SELECT 
        InventLocationID,
        ItemID,
        InventSiteID,
        0 AS PhysicalInventory,
        0 AS PhysicalReserved,
        0 AS Ordered,
        0 AS OnOrdered,
        0 AS OrderedReserved,
        0 AS OrderedInTotal,
        0 AS TotalAvailable,
        0 AS AvailablePhysical,
        0 AS ActualPhysical,
        0 AS SalesQTY,
        toDateTime('1900-01-01') AS AdjustmentDatetime,
        hex(MD5('0|0')) AS RowHash,
        {version} AS version_id
    FROM inventory.vw_onhand_backfill_missing;
    """.format(version=version)
    
    try:
        clickhouse_writer = ClickHouseWriter(CLICKHOUSE_CONN_ID)
        result = clickhouse_writer.execute_command(clickhouse_query)
        rows_affected = result if isinstance(result, int) else 0
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {
                #'rows_affected': rows_affected,
                'version': version,
                'duration_seconds': round(duration, 2)
            }
        )
        
        logger.info(f"Backfill processing completed in {duration:.2f}s with version {version}")
        
        return {
            "status": "success",
            "version": version,
            "duration_seconds": round(duration, 2)
        }
    except Exception as e:
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {'version': version},
            error=str(e)
        )
        
        logger.error(f"Backfill processing failed after {duration:.2f}s: {str(e)}")
        raise AirflowException(f"ClickHouse query execution failed: {str(e)}") from e

@task
def process_onhand_history_task(version: str, **context) -> Dict[str, Any]:
    """
    Archive current inventory snapshot to historical table.
    
    This task creates a point-in-time snapshot of the current inventory state
    and stores it in the onhand_history table for historical analysis and
    trend tracking purposes.
    
    Logic:
    - Captures complete inventory position from vw_onhand_position_snapshot
    - Includes calculated ActualPhysical (AvailablePhysical + SalesQTY)
    - Tags snapshot with version_id for temporal queries
    
    Args:
        version: Unique version timestamp for this DAG run
        **context: Airflow task context
        
    Returns:
        Dict: Processing result containing status, version, and duration_seconds
        
    Raises:
        AirflowException: If ClickHouse query execution fails
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(
        EventType.PROCESSING_SERVER,
        task_id,
        EventStatus.STARTED,
        {'version': version}
    )
    
    start_time = time()
    
    clickhouse_query= """
    INSERT INTO inventory.onhand_history (
        InventLocationID, ItemID, InventSiteID, 
        PhysicalInventory, PhysicalReserved, Ordered, 
        OnOrdered, OrderedReserved, OrderedInTotal, 
        TotalAvailable, AvailablePhysical, ActualPhysical, 
        SalesQTY, AdjustmentDatetime, RowHash, version_id
    )
    SELECT ol.InventLocationID,
        ol.ItemID,
        ol.InventSiteID,
        ol.PhysicalInventory,
        ol.PhysicalReserved,
        ol.Ordered,
        ol.OnOrdered,
        ol.OrderedReserved,
        ol.OrderedInTotal,
        ol.TotalAvailable,
        ol.AvailablePhysical,
        ol.ActualPhysical,
        ol.SalesQTY,
        ol.AdjustmentDatetime,
        ol.RowHash,
        {version} AS version_id
    FROM inventory.vw_onhand_position_snapshot AS ol
    """.format(version=version)
    
    try:
        clickhouse_writer = ClickHouseWriter(CLICKHOUSE_CONN_ID)
        result = clickhouse_writer.execute_command(clickhouse_query)
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {
                'version': version,
                'duration_seconds': round(duration, 2)
            }
        )
        
        logger.info(f"History snapshot completed in {duration:.2f}s with version {version}")
        
        return {
            "status": "success",
            "version": version,
            "duration_seconds": round(duration, 2)
        }
    except Exception as e:
        duration = time() - start_time
        
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {'version': version},
            error=str(e)
        )
        
        logger.error(f"History snapshot failed after {duration:.2f}s: {str(e)}")
        raise AirflowException(f"ClickHouse query execution failed: {str(e)}") from e


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': "Zahra Saffarpour",
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['Saffarpour.Zahra@okco.ir'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
    'pool': "sales_inventory_default_pool",
}
    
with DAG(
    dag_id="inventory_onhand_processing_clickhouse",
    description="Process on-hand inventory data: incremental updates, backfill missing records, and historical snapshots.",
    default_args=default_args,
    start_date=datetime(2026, 4, 2),
    schedule=None,                   # Triggered by parent DAG
    catchup=False,                   # Don't backfill historical runs
    max_active_runs=1,               # Prevent overlapping executions
    tags=["clickhouse", "inventory", "onhand", "processing"],
) as dag:
    """
    Pipeline Execution Flow
    =======================
    
    This DAG executes a three-phase processing pipeline for on-hand inventory data:
    
    Phase 1: Validation
    -------------------
    - Validate ClickHouse connection availability and responsiveness
    - Ensures infrastructure readiness before data operations
    
    Phase 2: Setup
    --------------
    - Generate unique version_id (timestamp: YYYYMMDDHHMMSS)
    - Version propagates to all downstream tasks for data lineage
    
    Phase 3: Processing (Sequential Pipeline)
    ------------------------------------------
    a) Incremental Update (process_onhand):
       - Merge changed records from vw_onhand_position_snapshot
       - Delta detection via RowHash comparison
       - Updates ActualPhysical = AvailablePhysical + SalesQTY
    
    b) Backfill Missing Records (process_backfill_missing_onhand):
       - Insert zero-valued records for discontinued items
       - Maintains data completeness for items removed from source
    
    c) Historical Snapshot (process_onhand_history):
       - Archive complete inventory state to onhand_history
       - Enables temporal analysis and trend tracking
    
    Dependency Chain
    ----------------
    validate_clickhouse → version → [process_onhand → process_backfill_missing_onhand → process_onhand_history]
    
    Error Handling
    --------------
    - All tasks include comprehensive audit logging
    - Failures are logged with context (version, duration, error details)
    - No automatic retries (retries=0) to prevent duplicate data insertion
    - 8-hour execution timeout for long-running operations
    
    Concurrency Control
    -------------------
    - max_active_runs=1: Prevents overlapping executions
    - Pool: sales_inventory_default_pool (shared resource management)
    """

    with TaskGroup(group_id="Validation") as validation:
        validate_clickhouse = validate_clickhouse_connection()
       
    with TaskGroup(group_id="setup") as Setup:
        version = generate_version_task()
                
    # Phase 4: Processing
    with TaskGroup("processing") as processing:
        process_onhand = process_onhand_task(version)
        process_backfill_missing_onhand = process_backfill_missing_onhand_task(version)
        process_onhand_history = process_onhand_history_task(version)
        
        # Internal dependencies
        process_onhand >> process_backfill_missing_onhand >> process_onhand_history

        
        #Processing pipeline
        validate_clickhouse >> version >> processing
