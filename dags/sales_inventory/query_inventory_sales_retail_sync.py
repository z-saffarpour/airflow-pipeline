"""
sales_and_inventory_kafka_sync — Daily Sales & Inventory Sync to Kafka

Orchestrates daily extraction of sales and inventory data from multiple sources
and streams results to Kafka for downstream processing.

Sources:
    - Retail Store Sales  : ~5000 store SQL Server instances (mssql_erp_retailpos)

Kafka Topics:
    - KAFKA_TOPIC_SALES_RETAIL : Retail store sales quantities

Pipeline Stages:
    1. Validate   : HQ MSSQL (Windows Auth) + ERP Primary AX + Kafka connectivity
    2. Setup      : Ensure all Kafka topics exist (12 partitions, RF=3)
    3. Discover   : Retrieve active store server list from HQ connection-info server
    4. Chunk      : Split ~5000 servers into chunks to avoid XCom size limits
    5. Process    : Parallel execution across chunks and date ranges
                    ├── Retail sales   → ax.proc_SalesByItem per store server
    6. Verify     : Aggregate transfer results and report per-source failures
    7. Cleanup    : Remove temporary server-list JSON files

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
from typing import Dict, List
import json

from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore

from pipeline.core.MSSQLDataTransferOrchestrator import MSSQLDataTransferOrchestrator
from pipeline.config.AuditConfig import EventType, EventStatus
from pipeline.config.QueryConfiguration import QueryConfiguration
from pipeline.database.ConnectionFactory import ConnectionFactory
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

# HQ server connection (Windows Authentication) - stores metadata about all stores
CONNECTION_INFO_CONN_ID = "mssql_store_connectionInfo"

# Template connection for store credentials (SQL Authentication)
# Contains username/password that will be reused for all stores
STORE_DRIVER_TYPE = 'pyodbc' #'pymssql'
STORE_CONN_ID = "mssql_store_template"
STORE_TIMEOUT_SEC = 300 # 5 minutes
STORE_QUERY_TIMEOUT_SEC = 150 # 2 minutes
STORE_LOGIN_TIMEOUT_SEC = 30 # 30 seconds
STORE_DURATION_THRESHOLD_SEC  = 120 # 2 minutes

# Kafka configuration
KAFKA_CONN_ID = "kafka_default"
KAFKA_TOPIC = "store.query.raw.inventory.sales_retail"
KAFKA_PARTITION = 12
KAFKA_REPLICATION = 3

# Parallel execution limit - prevents overwhelming Airflow scheduler
STORE_MAX_ACTIVE_TIS_PER_DAG = 30

# Chunking configuration
CHUNK_SIZE = 100  # Number of servers per chunk

# Days to look back for sales data (configurable via Airflow Variable)
LOOKBACK_DAYS = int(Variable.get("lookback_days_inventory_sales", default_var = -12))
BATCH_SIZE = int(Variable.get("batch_size_inventory_sales_retail", default_var = 1000))
AIRFLOW_SHARED_PATH = Variable.get("airflow_shared_path", default_var = '/opt/airflow/shared')
# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_sql_credentials(conn_id: str) -> tuple:
    """
    Extract SQL authentication credentials from Airflow Connection.
    
    Args:
        conn_id: Airflow connection ID containing SQL credentials
        
    Returns:
        Tuple of (username, password)
    """
    from airflow.hooks.base import BaseHook # type: ignore
    conn = BaseHook.get_connection(conn_id)
    return conn.login, conn.password

# {
#   "driver": "ODBC Driver 18 for SQL Server"
# }

def get_odbc_driver(conn_id: str) -> str:
    """
    Extract ODBC driver name from Airflow Connection extra field.
    
    Args:
        conn_id: Airflow connection ID
        
    Returns:
        ODBC driver name (e.g., 'ODBC Driver 18 for SQL Server')
    """
    from airflow.hooks.base import BaseHook # type: ignore
    
    conn = BaseHook.get_connection(conn_id)
    
    # Parse extra field
    extra = conn.extra_dejson if hasattr(conn, 'extra_dejson') else {}
    
    # Get driver with fallback to default
    driver = extra.get('driver', 'ODBC Driver 18 for SQL Server')
    
    return driver

def create_dynamic_connection(
    server_ip: str,
    port: int,
    database: str,
    credentials_conn_id: str
) -> str:
    """
    Create connection URI for a store server.
    
    Builds MSSQL connection string using credentials from template connection.
    Handles both named and default SQL Server instances.
    
    Args:
        server_ip: Store server IP address
        port: SQL Server port number
        database: Target database name
        credentials_conn_id: Airflow connection ID containing SQL credentials
        
    Returns:
        Connection URI string (mssql+pymssql://...)
    # mssql+pymssql://sa:MyP%40ssw0rd%21@192.168.1.100,1433/SalesDB?appname=MyApp&timeout=600&login_timeout=30&query_timeout=300
    # mssql+pyodbc://sa:MyP%40ssw0rd%21@192.168.1.100,1433/SalesDB?appname=Airflow-DataPipeline&timeout=600&login_timeout=30&query_timeout=300&driver=ODBC+Driver+17+for+SQL+Server

    """
    sql_user, sql_pass = get_sql_credentials(credentials_conn_id)
    
    from urllib.parse import quote_plus
    
    # URL-encode credentials
    encoded_user = quote_plus(sql_user)
    encoded_pass = quote_plus(sql_pass)
    encoded_appname = quote_plus("Airflow")
    
    # Build base URI based on driver type
    if STORE_DRIVER_TYPE == 'pyodbc':
        odbc_driver = get_odbc_driver(credentials_conn_id)
        encoded_driver = quote_plus(odbc_driver)
        scheme = 'mssql+pyodbc'
        driver_param = f'&driver={encoded_driver}'
    else:
        scheme = 'mssql+pymssql'
        driver_param = ''
    
    # Build connection URI directly
    conn_string = (
        f"{scheme}://{encoded_user}:{encoded_pass}@{server_ip},{port}/{database}"
        f"?appname={encoded_appname}"
        f"&timeout={STORE_TIMEOUT_SEC}"
        f"&login_timeout={STORE_LOGIN_TIMEOUT_SEC}"
        f"&query_timeout={STORE_QUERY_TIMEOUT_SEC}"
        f"{driver_param}"
    )
    
    return conn_string


def process_sales_retail_single(
    server: Dict[str, str], 
    version: str,
    exec_date: str,
    audit: AuditLogger,
    task_id: str
) -> Dict[str, any]:
    """
    Process sales data from a single store server.
    
    Creates temporary connection, executes ax.proc_SalesByItem stored procedure,
    and streams results to Kafka. Catches exceptions to prevent chunk failure.
    
    Args:
        server: Dict with keys: ip, instance, port, database, storenumber.
        exec_date: Execution date in YYYY-MM-DD format passed to ax.proc_SalesByItem_v2.
        audit: AuditLogger instance shared from the parent chunk task.
        task_id: Airflow task_id for audit log correlation.
        
    Returns:
        Dict with keys:
            - success (bool): Whether processing succeeded
            - store (str): Store number
            - records (int): Number of rows processed (if successful)
            - error (str): Error message (if failed)
    """
    server_ip = server["ip"]
    port = server["port"]
    database = server["database"]
    store_number = server["storenumber"]
    
    audit.log( EventType.PROCESSING_SERVER, task_id, EventStatus.STARTED, {'store': store_number, 'exec_date': exec_date} )
    
    try:
        # Create temporary connection
        conn_uri = create_dynamic_connection(
            server_ip=server_ip,
            port=port,
            database=database,
            credentials_conn_id=STORE_CONN_ID
        )
        
        # Initialize orchestrator
        kafka_brokers = get_kafka_brokers(KAFKA_CONN_ID)
        orchestrator = MSSQLDataTransferOrchestrator(
            mssql_conn_id=conn_uri,
            kafka_bootstrap_servers=kafka_brokers,
            clickhouse_conn_id=None,
            is_send_kafka=True,
            is_send_clickhouse=False,
            fail_on_error=True,
            is_connection_string=True,
            version=version,
        )

        # Configure query
        config = QueryConfiguration(
            query=f"EXEC ax.proc_SalesByItem_v2 '{exec_date}'",
            source_name=f"store_{store_number}_sales",
            key_column='InventLocationID',
            query_params=None
        )

        # Execute and stream
        result = orchestrator.transfer_query_data(
            config=config,
            execution_date=exec_date,
            batch_size=BATCH_SIZE,
            kafka_topic=KAFKA_TOPIC,
            clickhouse_database=None,
            clickhouse_table_name=None, 
        )
        is_slow = result.duration_seconds > STORE_DURATION_THRESHOLD_SEC

        if not result.success:
            audit.log(EventType.DATA_TRANSFER_ORCHESTRATOR, task_id, EventStatus.FAILED, {'store': store_number, 'duration_seconds': result.duration_seconds}, error=result.error_message)
            logger.error(f"Transfer failed for store {store_number} after {result.duration_seconds:.2f}s: {result.error_message}")
            return {"success": False, "store": store_number, "error": result.error_message}
        
        if is_slow:
            audit.log(EventType.DATA_TRANSFER_ORCHESTRATOR, task_id, EventStatus.WARNING, {'store': store_number, 'records': result.records_transferred, 'duration_seconds': result.duration_seconds},f"Store {store_number} transfer took {result.duration_seconds:.2f}s (>{STORE_DURATION_THRESHOLD_SEC}s threshold)")
            logger.warning(f"Store {store_number} transfer took {result.duration_seconds:.2f}s (>{STORE_DURATION_THRESHOLD_SEC}s threshold)")
            
        audit.log(EventType.PROCESSING_SERVER, task_id, EventStatus.SUCCESS, {'store': store_number, 'records': result.records_transferred, 'duration_seconds': result.duration_seconds})
        logger.info(f"Successfully processed store {store_number}: {result.records_transferred} rows in {result.duration_seconds:.2f}s")
        return {"success": True, "store": store_number, "records": result.records_transferred, "duration_seconds": result.duration_seconds, "is_slow": is_slow}
        
    except Exception as e:
        audit.log( EventType.PROCESSING_SERVER, task_id, EventStatus.FAILED, {'store': store_number}, error=str(e))
        logger.error(f"Failed processing store {store_number}: {str(e)}")
        return {"success": False, "store": store_number, "error": str(e)}
    
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
        {'conn_id': CONNECTION_INFO_CONN_ID, 'type': 'mssql'}
    )
    
    result = validate_mssql_conn(CONNECTION_INFO_CONN_ID)
        
    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {'conn_id': CONNECTION_INFO_CONN_ID, 'type': 'mssql'}
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
def get_store_servers_task(**context) -> str:
    """
    Retrieve active store servers from HQ and save to temp file.

    Queries retail.ActiveConnectionInfo on the HQ server and writes
    the result as JSON to a temp file to avoid XCom size limits.

    Returns:
        str: Path to temp JSON file (format: /opt/airflow/shared/{dag_id}_{run_id}_servers.json).
    """
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    audit = AuditLogger(
        dag_id=dag_id,
        run_id=run_id
    )
    task_id = context['task_instance'].task_id
    
    audit.log(EventType.STORE_LIST_RETRIEVED, task_id, EventStatus.STARTED, {})

    query = """
            SELECT ServerDatabaseIP AS ServerIP, DatabaseInstanceName AS InstanceName, DatabaseName, '49159' AS Port, StoreNumber
            FROM retail.ActiveConnectionInfo
        """
        
    factory = ConnectionFactory(conn_id = CONNECTION_INFO_CONN_ID)
    rows  = factory.execute_query(query)
    
    servers = [
        {
            "ip": row["ServerIP"],
            "instance": row["InstanceName"],
            "port": row["Port"],
            "database": row["DatabaseName"],
            "storenumber": row["StoreNumber"]
        }
        for row in rows
    ]
    
    # Save to temp file
    file_path = f"/{AIRFLOW_SHARED_PATH}/{dag_id}_{run_id}_servers.json"
    with open(file_path, "w") as f:
        json.dump(servers, f)
    
    audit.log(
        EventType.STORE_LIST_RETRIEVED,
        task_id,
        EventStatus.SUCCESS,
        {'store_count': len(servers), 'file': file_path}
    )
    
    logger.info(f"Retrieved {len(servers)} servers, saved to {file_path}")
    return file_path
 
@task
def create_chunks_task(server_file: str) -> List[Dict[str, int]]:
    """
    Split server list into chunks for parallel processing.
    
    Reads server list from file and creates chunk definitions with
    start/end indices. Each chunk will be processed by a separate task.
    
    Args:
        server_file: Path to JSON file containing server list
        
    Returns:
        List of dicts with keys: start_index, end_index.
        Example: [{"start_index": 0, "end_index": 100}, ...]
    """
    with open(server_file, "r") as f:
        servers = json.load(f)
    
    total = len(servers)
    chunks = []
    
    for i in range(0, total, CHUNK_SIZE):
        chunks.append({
            "start_index": i,
            "end_index": min(i + CHUNK_SIZE, total)
        })
    
    logger.info(f"Created {len(chunks)} chunks for {total} servers")
    return chunks

@task(max_active_tis_per_dag=STORE_MAX_ACTIVE_TIS_PER_DAG)
def process_sales_retail_task(server_file: str, version:str , start_index: int, end_index: int, **context) -> List[Dict[str, any]]:
    """
    Process a chunk of servers in parallel.
    
    Reads specified range of servers from file and processes each one.
    Individual server failures are caught and reported without failing the chunk.
    
    Args:
        server_file: Path to JSON file containing server list
        start_index: Starting index in server list (inclusive)
        end_index: Ending index in server list (exclusive)
        
    Returns:
        List of result dicts from process_single_server for each server in chunk
    """
    # Initialize audit logger
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    try:
        audit.log(EventType.CHUNK_PROCESSING, task_id, EventStatus.STARTED, {'start_index': start_index, 'end_index': end_index} )
        with open(server_file, "r") as f:
            servers = json.load(f)
        
        results = []
        chunk_servers = servers[start_index:end_index]
        
        logger.info(f"Processing chunk: stores {start_index} to {end_index-1} ({len(chunk_servers)} servers)")
        exec_date = ExecutionDateExtractor.get_date_from_context(context, LOOKBACK_DAYS)
        
        for server in chunk_servers:
            result = process_sales_retail_single(server, version, exec_date, audit, task_id)
            results.append(result)
        success_count = sum(1 for r in results if r.get("success"))
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.SUCCESS,
            {
                'total': len(results),
                'successful': success_count,
                'failed': len(results) - success_count
            }
        )
        
        return results
        
    except Exception as e:
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.FAILED,
            error=str(e)
        )
        raise

@task #(trigger_rule="all_done")
def verify_transfers_task(chunk_results: List[List[Dict]], **context) -> Dict[str, any]:
    """
    Aggregate and verify results from all chunks.
    
    Flattens results from all chunk tasks, counts successes/failures,
    and logs failed stores for investigation.
    
    Args:
        chunk_results: List of result lists from all process_chunk tasks
        
    Returns:
        Dict with keys:
            - total_servers (int): Total number of servers processed
            - successful (int): Number of successful transfers
            - failed (int): Number of failed transfers
            - failed_stores (list): Store numbers that failed
            - status (str): 'completed' or 'completed_with_errors'
    """
        
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
        
    all_results = []
    for chunk in chunk_results:
        all_results.extend(chunk)
    
    successful = sum(1 for r in all_results if r.get("success"))
    failed = len(all_results) - successful
    failed_stores = [r["store"] for r in all_results if not r.get("success")]
    slow_stores = [
        {"store": r["store"], "duration_seconds": r["duration_seconds"]}
        for r in all_results
        if r.get("success") and r.get("is_slow")
    ]
    status = "completed_with_errors" if failed > 0 else "completed"

    logger.info(f"Processed {len(all_results)} stores: {successful} successful, {failed} failed, {len(slow_stores)} slow")
    
    audit.log(
        EventType.VALIDATION,
        task_id,
        EventStatus.SUCCESS if failed == 0 else EventStatus.FAILED,
        {
            'total_servers': len(all_results),
            'successful': successful,
            'failed': failed,
            'failed_stores': failed_stores, #[:10]  # First 10 only
            'slow_stores': slow_stores
        }
    )
        
    if failed > 0:
        logger.error(f"Failed stores: {failed_stores}")
    if slow_stores:
        logger.warning(f"Slow stores (>{STORE_DURATION_THRESHOLD_SEC}s): {slow_stores}")

    return {
        "total_servers": len(all_results),
        "successful": successful,
        "failed": failed,
        "failed_stores": failed_stores,
        "slow_stores": slow_stores,
        "status": status
    }

@task(trigger_rule="all_done")
def cleanup_temp_files_task(server_file: str, **context) -> Dict[str, str]:
    """
    Clean up temporary server list file.
    
    Removes temp JSON file created by get_store_servers to prevent disk space issues.
    Runs even if previous tasks fail (trigger_rule="all_done").
    
    Args:
        server_file: Path to temp JSON file
        verification_result: Result from verify_transfers (for dependency only)
        
    Returns:
        Dict with keys: status ('success'/'not_found'/'failed'), file, error (optional)
    """
    import os
        
    audit = AuditLogger(
        dag_id=context['dag'].dag_id,
        run_id=context['run_id']
    )
    task_id = context['task_instance'].task_id
    
    try:
        audit.log(
            EventType.FILE_OPERATION,
            task_id,
            EventStatus.STARTED,
            {'file': server_file}
        )
        
        if os.path.exists(server_file):
            os.remove(server_file)
            audit.log(
                EventType.FILE_OPERATION,
                task_id,
                EventStatus.SUCCESS,
                {'file': server_file, 'action': 'deleted'}
            )
            logger.info(f"Cleaned up temp file: {server_file}")
            return {"status": "success", "file": server_file}
        else:
            audit.log(
                EventType.FILE_OPERATION,
                task_id,
                EventStatus.SUCCESS,
                {'file': server_file, 'action': 'not_found'}
            )
            logger.warning(f"Temp file not found: {server_file}")
            return {"status": "not_found", "file": server_file}
    except Exception as e:
        audit.log(
            EventType.FILE_OPERATION,
            task_id,
            EventStatus.FAILED,
            {'file': server_file},
            error=str(e)
        )
        logger.error(f"Failed to cleanup {server_file}: {str(e)}")
        return {"status": "failed", "file": server_file, "error": str(e)}

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
    dag_id="query_inventory_sales_retail_sync",
    description="Daily sync of retail store data to Kafka.",
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
        [setup_kafka_topic,version]

    # Phase 3: Discovery
    with TaskGroup("discovery") as discovery:
        store_server_file = get_store_servers_task()
        store_chunk = create_chunks_task(store_server_file)
                    
        # Internal dependencies
        store_server_file >> store_chunk
                
    # Phase 4: Processing
    with TaskGroup("processing") as processing:        
        process_sales_retail= process_sales_retail_task.partial(server_file=store_server_file,version=version).expand_kwargs(store_chunk)
    
        # Internal dependencies
        process_sales_retail
        
    with TaskGroup(group_id="verify_and_completion") as verify_completion:
        verification = verify_transfers_task(process_sales_retail)
        cleanup = cleanup_temp_files_task(store_server_file)
        completion = dag_completion_task(verification)
        
        # Internal dependencies
        verification >> cleanup >> completion
        
    # Define task dependencies
    validation >> Setup >> discovery >> processing >> verify_completion