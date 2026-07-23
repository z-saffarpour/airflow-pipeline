"""
sales_and_inventory_kafka_sync — Daily Sales & Inventory Sync to Kafka

Orchestrates daily extraction of sales and inventory data from multiple sources
and streams results to Kafka for downstream processing.

Sources:
    - On-Hand Inventory   : ERP Primary AX (mssql_erp_primary)

Kafka Topics:
    - KAFKA_TOPIC        : On-hand inventory snapshots

Pipeline Stages:
    1. Validate   : HQ MSSQL (Windows Auth) + ERP Primary AX + Kafka connectivity
    2. Setup      : Ensure all Kafka topics exist (12 partitions, RF=3)
    5. Process    : Parallel execution across chunks and date ranges
                    └── On-hand        → ERP AX stored procedure

Key Features:
    - Chunked processing  : Handles 5000+ servers without hitting XCom size limits
    - Parallel execution  : Up to 30 concurrent chunk tasks
    - Dynamic mapping     : Sales orders mapped over 13-day date range via expand()
    - Dynamic connections : Store connections created at runtime (no pre-config needed)
    - Fault isolation     : Individual store or date failures do not abort the DAG
    - Configurable        : Lookback days and chunk size controlled via Airflow Variables
    - Auto cleanup        : Temp files removed after DAG completion

Author: Senior Data Engineer
Version: 1.0
Created: 2026-04-02
Updated: 2026-04-02
"""
import logging
from datetime import datetime,timedelta
from typing import Dict, Any

from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.exceptions import AirflowException # type: ignore
from airflow.models import Variable # type: ignore

from pipeline.core.MSSQLDataTransferOrchestrator import MSSQLDataTransferOrchestrator
from pipeline.config.AuditConfig import EventType, EventStatus
from pipeline.config.QueryConfiguration import QueryConfiguration
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor
from pipeline.kafka.KafkaTopicManager import KafkaTopicManager
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.utils.validation import validate_mssql_conn, validate_kafka_conn
from pipeline.utils.kafka_utils import get_kafka_brokers

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION 
# ============================================================================
MSSQL_CONN_ID = "mssql_erp_primary"

# Kafka configuration
KAFKA_CONN_ID = "kafka_default"
KAFKA_TOPIC = "ax.query.raw.inventory.onhand_lite"
KAFKA_PARTITION = 12
KAFKA_REPLICATION = 3

BATCH_SIZE = int(Variable.get("batch_size_inventory_onhand", default_var = 100000))
  
# ============================================================================
# TASKS 
# ============================================================================
@task
def validate_mssql_connection_task(**context):
    """
    Validate HQ MSSQL connection before DAG execution.
    
    Ensures Windows Auth connection to HQ server is working.
    Fails fast if HQ is unreachable.
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.STARTED,
        {'conn_id': MSSQL_CONN_ID, 'type': 'mssql'}
    )
    
    result = validate_mssql_conn(MSSQL_CONN_ID)
        
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {'conn_id': MSSQL_CONN_ID, 'type': 'mssql'}
    )
    
    return result

@task
def validate_kafka_task(**context):
    """
    Validate Kafka cluster connectivity.
    
    Ensures Kafka brokers are reachable before processing stores.
    Prevents wasted work if Kafka is down.
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.STARTED,
        {'conn_id': KAFKA_CONN_ID, 'type': 'Kafka'}
    )
    
    result = validate_kafka_conn(KAFKA_CONN_ID)
        
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {'conn_id': KAFKA_CONN_ID, 'type': 'Kafka'}
    )
    
    return result

@task
def setup_kafka_topic_task(**context):
    """
    Ensure Kafka topic exists with proper configuration.
    
    Creates topic if missing or validates existing topic settings.
    12 partitions enable parallel consumption by downstream services.
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    audit.log(EventType.KAFKA_TOPIC_CONFIGURED, task_id, EventStatus.STARTED, {})
    
    kafka_brokers = get_kafka_brokers(KAFKA_CONN_ID)
    manager = KafkaTopicManager(kafka_brokers)
    manager.ensure_topic_exists(
        topic_name=KAFKA_TOPIC,
        num_partitions=KAFKA_PARTITION,        # Parallel consumption capability
        replication_factor=KAFKA_REPLICATION      # High availability
    )
    result = {
        "topic": KAFKA_TOPIC,
        "partitions": KAFKA_PARTITION,
        "conn_id": KAFKA_CONN_ID,
    }
    
    audit.log(EventType.KAFKA_TOPIC_CONFIGURED, task_id, EventStatus.SUCCESS, result)
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
def process_onhand_task(version:str,**context) -> Dict[str, Any]:
    """
    Extract current on-hand inventory snapshot from ERP and publish to Kafka.

    Queries dbo.OKOnhandLiteView on the primary AX ERP for all 'Available'
    stock in OKS-prefixed warehouse locations (InventLocationID LIKE 'OKS%'),
    then streams results to the Kafka onhand topic in batches of 10,000 rows.

    Note:
        - Query reflects a live snapshot; exec_date is used for metadata only.
        - AdjustmentDatetime and Adjusted are hardcoded sentinels (no adjustment applied).
        - ActualAvailablePhysical is an alias of AvailablePhysical for downstream consumers.

    Args:
        **context: Airflow task context (exec_date, dag_id, run_id, task_id).

    Returns:
        Dict[str, Any]: status, exec_date, total_records, batch_count, duration_seconds.

    Raises:
        AirflowException: On invalid query config or orchestrator transfer failure.
    """
    exec_date = ExecutionDateExtractor.get_date_from_context(context, 0)
    
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id

    audit.log(
        EventType.PROCESSING_SERVER,
        task_id,
        EventStatus.STARTED,
        {'exec_date': exec_date}
    )
    
    query = """
        SELECT UPPER( INVENTLOCATIONID ) AS InventLocationID,
            ITEMID AS ItemID,
            INVENTSITEID AS InventSiteID,
            PhysicalInventory,
            PhysicalReserved,
            AvailablePhysical,
            Ordered,
            OnOrdered,
            OrderedReserved,
            OrderedInTotal,
            TotalAvailable,
            CAST('2000-01-01' AS DATETIME) AS AdjustmentDatetime,
            0 AS Adjusted,
            AvailablePhysical AS ActualAvailablePhysical
        FROM dbo.OKOnhandLiteView
        WHERE InventStatusID = 'Available'
            AND InventLocationID LIKE 'OKS%';
    """
    count_query= """
            SELECT COUNT(1) AS CNT 
        FROM dbo.OKOnhandLiteView
        WHERE InventStatusID = 'Available'
            AND InventLocationID LIKE 'OKS%';
    """
    
    kafka_brokers = get_kafka_brokers(KAFKA_CONN_ID)
    try:
        query_config = QueryConfiguration(
            source_name="sales_onhand",
            query=query,
            count_query=count_query,
            key_column='InventLocationID',
        )
    except ValueError as e:
        raise AirflowException(str(e)) from e

    logger.info(f"Starting query transfer for date: {exec_date}")

    orchestrator = MSSQLDataTransferOrchestrator(
        mssql_conn_id=MSSQL_CONN_ID,
        kafka_bootstrap_servers=kafka_brokers,
        clickhouse_conn_id=None,
        is_send_kafka=True,
        is_send_clickhouse=False,
        fail_on_error=True,
        is_connection_string=False,
        version=version
    )

    result = orchestrator.transfer_query_data(
        config=query_config,
        execution_date=exec_date,
        batch_size=BATCH_SIZE,
        kafka_topic=KAFKA_TOPIC,
        clickhouse_database=None,
        clickhouse_table_name=None, 
    )
    
    if result.is_failure:
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {'exec_date': exec_date,'version': version},
            error=result.error_message
        )
        raise AirflowException(f"Transfer failed for date {exec_date}: {result.error_message}")

    audit.log(
        EventType.DATA_TRANSFER_ORCHESTRATOR,
        task_id,
        EventStatus.SUCCESS,
        {
            'exec_date': exec_date,
            'version': version,
            'total_records': result.records_transferred,
            'duration_seconds': result.duration_seconds,
        }
    )

    logger.info(f"[{exec_date}] Transferred {result.records_transferred} records in {result.duration_seconds:.2f}s")

    return {
        "status": "success",
        "exec_date": exec_date,
        "version": version,
        "total_records": result.records_transferred,
        "batch_count": result.batch_count,
        "duration_seconds": result.duration_seconds,
    }

@task(trigger_rule="all_done")
def dag_completion_task(verification: Dict, **context):
    """
    Log final DAG completion status to audit trail.

    Reads verification summary and emits a DAG_COMPLETED audit event
    with SUCCESS or FAILED status based on failed store count.

    Args:
        verification: Result dict from verify_transfers_task.
    """
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    
    audit.log(
        EventType.DAG_COMPLETED,
        'dag_completion',
        EventStatus.SUCCESS if verification['failed'] == 0 else EventStatus.FAILED,
        verification
    )

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
    dag_id="query_inventory_onhand_lite_sync",
    description="Daily sync of on-hand lite inventory data to Kafka.",
    start_date=datetime(2026, 4, 2),
    schedule=None,#"0 2 * * *",      # Daily at 2:00 AM
    catchup=False,                   # Don't backfill historical runs
    max_active_runs=1,               # Prevent overlapping executions
    tags=["mssql","kafka", "inventory"],
    default_args=default_args,
) as dag:
    """
    Execution Flow:
    
    1. Validation (parallel):
       - Validate HQ MSSQL connection
       - Validate Kafka cluster connectivity
    2. Setup:
       - Ensure Kafka topic exists (12 partitions, RF=3)
    
    3. Discovery & Processing (TaskGroup):
       - Retrieve active store list from HQ → save to temp file
       - Split servers into chunks (100 servers/chunk)
       - Process chunks in parallel (max 30 concurrent)* Each chunk: execute SP on stores → stream to Kafka
    
    4. Verification:
       - Aggregate results from all chunks
       - Report success/failure counts
    5. Cleanup:
       - Remove temp server list file
    
    Dependencies:
    [validate_mssql, validate_kafka] >> topic_setup >> discovery_processing >> verification >> cleanup
    """

    with TaskGroup(group_id="Validation") as validation:
        validate_mssql = validate_mssql_connection_task()
        validate_kafka = validate_kafka_task()
        
        # Internal dependencies
        [validate_mssql,  validate_kafka]
       
    with TaskGroup(group_id="setup") as Setup:
        setup_kafka_topic = setup_kafka_topic_task()
        version = generate_version_task()
        
        # Internal dependencies
        [setup_kafka_topic, version]
                
    # Phase 4: Processing
    with TaskGroup("processing") as processing:
        process_onhand = process_onhand_task(version)
    
        # Internal dependencies
        process_onhand
        
    # with TaskGroup(group_id="verify_and_completion") as verify_completion:
    #     completion = dag_completion_task(process_onhand)
        
    #     # Internal dependencies
    #     completion
        
    # Define task dependencies
    validation >> Setup >> processing