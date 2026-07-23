"""
sales_and_inventory_kafka_sync — Daily Sales & Inventory Sync to Kafka

Orchestrates daily extraction of sales and inventory data from multiple sources
and streams results to Kafka for downstream processing.

Sources:
    - PURCH QTY    : ERP Primary AX (mssql_erp_primary)

Kafka Topics:
    - KAFKA_TOPIC        : On-hand inventory snapshots

Pipeline Stages:
    1. Validate   : HQ MSSQL (Windows Auth) + ERP Primary AX + Kafka connectivity
    2. Setup      : Ensure all Kafka topics exist (12 partitions, RF=3)
    5. Process    : Parallel execution across chunks and date ranges
                    └── On-hand        → ERP AX stored procedure

Key Features:

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
from pipeline.database import ClickHouseWriter
from pipeline.kafka.KafkaTopicManager import KafkaTopicManager
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.utils.validation import validate_clickhouse_conn, validate_mssql_conn, validate_kafka_conn

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
KAFKA_TOPIC = "ax.query.raw.inventory.purch"
KAFKA_PARTITION = 3
KAFKA_REPLICATION = 2

CLICKHOUSE_CONN_ID = "clickhouse_default"

BATCH_SIZE = int(Variable.get("batch_size_inventory_purch", default_var = 10000)) 
# ============================================================================
# TASKS 
# ============================================================================
@task
def validate_mssql_connection_task(**context):
    """
    Validate MSSQL connection before DAG execution.
    
    Ensures Windows Auth connection to server is working.
    Fails fast if is unreachable.
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
def validate_clickhouse_task(**context):
    """
    Validate ClickHouse connectivity.

    Ensures the ClickHouse server is reachable before running downstream 
    processing steps. Prevents wasted computation if the database connection 
    is unavailable.

    Logs STARTED and SUCCESS events to AuditLogger using EventType.CONN_VALIDATED.
    Returns the result of the ClickHouse connection validation.
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
        {'conn_id': CLICKHOUSE_CONN_ID, 'type': 'clickhouse'}
    )
    
    result = validate_clickhouse_conn(CLICKHOUSE_CONN_ID)
        
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {'conn_id': CLICKHOUSE_CONN_ID, 'type': 'clickhouse'}
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
    
    manager = KafkaTopicManager(KAFKA_CONN_ID)
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
        EventType.METADATA_GENERATED,
        'generate_version',
        EventStatus.SUCCESS,
        {'version': version}
    )
    
    logger.info(f"Generated version for this DAG run: {version}")
    return version

@task
def truncate_purch_reference_task(**context):
    """
    Truncate purch_reference on ClickHouse cluster before processing.

    Ensures the local table is empty prior to inserting new records during
    process_purch_task execution.
    """
    audit = AuditLogger(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"]
    )

    task_id = context["task_instance"].task_id

    audit.log(
        EventType.DATA_TRANSFER_ORCHESTRATOR,
        task_id,
        EventStatus.STARTED,
        {"table": "inventory.local_purch_reference"}
    )

    clickhouse_writer = ClickHouseWriter(CLICKHOUSE_CONN_ID)

    sql = """
        TRUNCATE TABLE inventory.local_purch_reference
        ON CLUSTER cluster_2S_2R;
    """

    try:
        clickhouse_writer.execute_command(sql)

        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {"table": "inventory.local_purch_reference"}
        )

        return {"status": "success"}

    except Exception as ex:
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {"table": "inventory.local_purch_reference"},
            error=str(ex)
        )
        raise AirflowException(
            f"Failed to truncate purch_reference: {ex}"
        )

@task
def process_purch_task(version:str,**context) -> Dict[str, Any]:
    """
    Extract aggregated purchase quantities for OKS locations and publish to Kafka.

    Queries mssql_erp_primary for PURCHQTY grouped by InventLocationID and ItemID,
    filtered on PURCHASETYPE=0 and InventLocationID LIKE 'OKS%'.
    Results are streamed to KAFKA_TOPIC_PURCH in batches of 1,000 records.

    Note:
        - Single aggregated query, not date-mapped.
        - Uses WITH (READPAST) to avoid blocking on locked rows.

    Args:
        **context: Airflow task context.

    Returns:
        Dict[str, Any]: status, exec_date, total_records, batch_count, duration_seconds.

    Raises:
        AirflowException: On invalid config or transfer failure.
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
        SELECT myPurchTable.INVENTLOCATIONID AS InventLocationID,
               myPurchLine.ITEMID            AS ItemID,
               SUM(myPurchLine.PURCHQTY)     AS PurchQTY
        FROM dbo.PURCHTABLE AS myPurchTable WITH (READPAST)
        INNER JOIN dbo.PURCHLINE AS myPurchLine WITH (READPAST)
            ON myPurchTable.PURCHID = myPurchLine.PURCHID
        WHERE myPurchLine.PURCHASETYPE = 0
          AND myPurchTable.INVENTLOCATIONID LIKE 'OKS%'
        GROUP BY myPurchTable.INVENTLOCATIONID,
                 myPurchLine.ITEMID;
    """
    count_query= """
        SELECT COUNT(1) AS CNT 
        FROM dbo.PURCHTABLE AS myPurchTable WITH (READPAST)
        INNER JOIN dbo.PURCHLINE AS myPurchLine WITH (READPAST)
            ON myPurchTable.PURCHID = myPurchLine.PURCHID
        WHERE myPurchLine.PURCHASETYPE = 0
          AND myPurchTable.INVENTLOCATIONID LIKE 'OKS%';
    """
    

    query_config = QueryConfiguration(
        source_name="purchase_quantity",
        query=query,
        count_query=count_query,
        key_column="InventLocationID",
    )
    
    logger.info("Starting query transfer for purchase quantity data.")

    orchestrator = MSSQLDataTransferOrchestrator(
        mssql_conn_id=MSSQL_CONN_ID,
        kafka_conn_id=KAFKA_CONN_ID,
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        is_send_kafka=True,
        is_send_clickhouse=True,
        fail_on_error=True,
        is_connection_string=False,
        version=version,
    )

    result = orchestrator.transfer_query_data(
        config=query_config,
        execution_date=exec_date,
        batch_size=BATCH_SIZE,
        kafka_topic=KAFKA_TOPIC,
        clickhouse_database="inventory",
        clickhouse_table_name="purch_reference",  
    )

    if result.is_failure:
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {'exec_date': exec_date,'version': version},
            error=result.error_message,
        )
        raise AirflowException(
            f"Transfer failed for purchase quantity data: {result.error_message}"
        )

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

    logger.info(
        f"Transferred {result.records_transferred} records "
        f"in {result.duration_seconds:.2f}s"
    )

    return {
        "status": "success",
        "exec_date": exec_date,
        "version": version,
        "total_records": result.records_transferred,
        "batch_count": result.batch_count,
        "duration_seconds": result.duration_seconds,
    }
       
@task
def backfill_missing_purch_task(version: str, **context):
    """
    Backfill missing purch records into inventory.purch_raw with PurchQTY = 0.

    Inserts rows from inventory.purch that do not exist in inventory.purch_reference,
    attaching the latest version_id from purch_reference.

    Args:
        version (str): Version ID generated earlier in the DAG.
        **context: Airflow context.

    Returns:
        Dict[str, Any]: Summary of inserted records.
    """
    audit = AuditLogger(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"]
    )
    task_id = context["task_instance"].task_id

    audit.log(
        EventType.DATA_TRANSFER_ORCHESTRATOR,
        task_id,
        EventStatus.STARTED,
        {"version": version}
    )

    clickhouse_writer = ClickHouseWriter(CLICKHOUSE_CONN_ID)

    sql_insert = f"""
        INSERT INTO inventory.purch_raw (InventLocationID, ItemID, PurchQTY, version_id)
        SELECT
            p.InventLocationID,
            p.ItemID,
            0 AS PurchQTY,
            {version} AS version_id
        FROM inventory.purch p
        LEFT JOIN inventory.purch_reference pr 
            ON pr.InventLocationID = p.InventLocationID
           AND pr.ItemID = p.ItemID
        WHERE pr.ItemID = '';
    """

    try:
        result = clickhouse_writer.execute_command(sql_insert)
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {"version": version, "status": "backfill_completed"}
        )
        return {
            "status": "success",
            "version": version
        }

    except Exception as ex:
        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.FAILED,
            {"version": version},
            error=str(ex)
        )
        raise AirflowException(f"Backfill failed in backfill_missing_purch_task: {ex}")

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
    dag_id="query_inventory_purch_sync",
    description="Daily sync of purch qty data to Kafka.",
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
       - Validate MSSQL connection
       - Validate Kafka cluster connectivity
       - Validate ClickHouse connectivity

    2. Setup:
       - Ensure Kafka topic exists
       - Generate processing version metadata

    4. Processing (TaskGroup):
       - Truncate table: inventory.local_purch_reference
         Ensures the reference table is clean before loading new data.
       - Process purchase data using version metadata
       - Backfill missing purchase quantity records (WHERE ItemID IS NULL)

    Dependencies:
        [validate_mssql, validate_kafka, validate_clickhouse] 
            >> setup 
            >> truncate_purch_reference 
            >> processing
    """
        
    with TaskGroup(group_id="Validation") as validation:
        validate_mssql = validate_mssql_connection_task()
        validate_kafka = validate_kafka_task()
        validate_clickhouse = validate_clickhouse_task()
        
        # Internal dependencies
        [validate_mssql,  validate_kafka, validate_clickhouse]
       
    with TaskGroup(group_id="setup") as Setup:
        setup_kafka_topic = setup_kafka_topic_task()
        version = generate_version_task()
        
        # Internal dependencies
        [setup_kafka_topic,version]

    with TaskGroup("Pre_Processing") as pre_rocessing:
        truncate_purch_reference = truncate_purch_reference_task()

        # Internal dependencies
        truncate_purch_reference
                
    with TaskGroup("processing") as processing:
        process_purch = process_purch_task(version=version)
        backfill_missing = backfill_missing_purch_task(version=version)

        # Internal dependencies
        process_purch >> backfill_missing

    # Define task dependencies
    validation >> Setup >> pre_rocessing >> processing