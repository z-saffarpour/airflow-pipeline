"""
Replication MD store sync DAG factory.

Repairs missed SQL Server Replication data by querying the Replication_MD publisher
and syncing results to the target store database.

Ops — chunk pool sizing (one-time setup):
    airflow pools set replication_md_store_sync_pool 32 "Replication MD store chunk sync"

The pool slot count must be >= ``max_global_parallel_chunks`` in ``MasterDataSyncConfig``.
If the pool is smaller, pool limits apply before Airflow ``max_active_tis_per_dag``.
Light tasks (validation, discovery, create_chunks, report) use ``default_pool``.
Only ``sync_replication_md_store_chunk`` uses ``dag_config.pool`` (e.g. replication_md_store_sync_pool).
"""
import logging
from dataclasses import replace
from datetime import timedelta
from typing import Any, Dict, List

from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.models.param import Param # type: ignore 
from airflow.exceptions import AirflowFailException, AirflowException # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from pipeline.core.exceptions import (
    is_transient_sql_server_error,
    is_transient_sql_server_error_message,
    raise_sync_task_error,
)


from pipeline.config.AuditConfig import EventType, EventStatus
from pipeline.core.MSSQLToMSSQLQueryOrchestrator import MSSQLToMSSQLQueryOrchestrator
from pipeline.database.ConnectionFactory import ConnectionFactory
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.utils.validation import validate_mssql_conn

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION 
# ============================================================================

# HQ server connection (Windows Authentication) - stores metadata about all stores
CONNECTION_INFO_CONN_ID = "mssql_store_connectionInfo"

REPLICATION_MD_CONN_ID = "mssql_replication_md"

# Template connection for store credentials (SQL Authentication)
# Contains username/password that will be reused for all stores
STORE_DRIVER_TYPE = 'pyodbc' #'pymssql'
STORE_TEMPLATE_CONN_ID = "mssql_store_template"
STORE_TIMEOUT_SEC = 300 # 5 minutes
STORE_QUERY_TIMEOUT_SEC = 240 # 4 minutes
STORE_LOGIN_TIMEOUT_SEC = 60 # 1 minute
STORE_DURATION_THRESHOLD_SEC  = 1200 # 20 minutes

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def resolve_store_scoped_sync_config(
    sync_config: MasterDataSyncConfig,
    store_number: str,
) -> MasterDataSyncConfig:
    """Substitute ``{store_number}`` placeholders in source queries at runtime."""
    count_query = sync_config.source_query_count or ""
    if (
        "{store_number}" not in sync_config.source_query
        and "{store_number}" not in count_query
    ):
        return sync_config

    safe_store = str(store_number).strip().replace("'", "''")
    updates: Dict[str, Any] = {
        "source_query": sync_config.source_query.replace("{store_number}", safe_store),
    }
    if sync_config.source_query_count:
        updates["source_query_count"] = sync_config.source_query_count.replace(
            "{store_number}", safe_store
        )
    return replace(sync_config, **updates)

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
    template_conn_id: str
) -> str:
    """
    Create connection URI for a store server.
    
    Builds MSSQL connection string using credentials from template connection.
    Handles both named and default SQL Server instances.
    
    Args:
        server_ip: Store server IP address
        port: SQL Server port number
        database: Target database name
        template_conn_id: Airflow connection ID containing SQL credentials
        
    Returns:
        Connection URI string (mssql+pymssql://...)
    # mssql+pymssql://sa:MyP%40ssw0rd%21@192.168.1.100,1433/SalesDB?appname=MyApp&timeout=600&login_timeout=30&query_timeout=300
    # mssql+pyodbc://sa:MyP%40ssw0rd%21@192.168.1.100,1433/SalesDB?appname=Airflow-DataPipeline&timeout=600&login_timeout=30&query_timeout=300&driver=ODBC+Driver+17+for+SQL+Server

    """
    sql_user, sql_pass = get_sql_credentials(template_conn_id)
    
    from urllib.parse import quote_plus
    
    # URL-encode credentials
    encoded_user = quote_plus(sql_user)
    encoded_pass = quote_plus(sql_pass)
    encoded_appname = quote_plus("Airflow")
    
    # Build base URI based on driver type
    if STORE_DRIVER_TYPE == 'pyodbc':
        odbc_driver = get_odbc_driver(template_conn_id)
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
    
# ============================================================================
# TASK FACTORIES
# ============================================================================

def make_validate_mssql_connection_info_task():
    @task(
        task_id="validate_mssql_connection_info",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_mssql_connection(**context):
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
    
    return validate_mssql_connection

def make_validate_mssql_connection_replication_md_task():
    @task(
        task_id="validate_mssql_connection_replication_md",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_mssql_connection(**context):
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
            {'conn_id': REPLICATION_MD_CONN_ID, 'type': 'mssql'}
        )
        
        result = validate_mssql_conn(REPLICATION_MD_CONN_ID)
            
        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.SUCCESS,
            {'conn_id': REPLICATION_MD_CONN_ID, 'type': 'mssql'}
        )
        
        return result
    
    return validate_mssql_connection

def make_validate_store_number_task():
    @task(
        task_id="validate_store_number"
    )
    def validate_store_number(**context):
        
        audit = AuditLogger(
            dag_id=context['dag'].dag_id,
            run_id=context['run_id']
        )
        
        task_id = context['task_instance'].task_id
        
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        store_number = (
            conf.get("store_number")
            or conf.get("Store_number")
            or context.get("params", {}).get("store_number")
        )

        if not store_number:
            raise ValueError("store_number must be provided via trigger conf or DAG params")

        # if not store_number.startswith("oks"):
        #     raise ValueError("Parameter must start with 'oks'")
        
        # if len(store_number) <= 8:
        #     raise ValueError("Parameter must be longer than 8 characters")
        
        # numeric_part = store_number[3:]

        # if not numeric_part or not numeric_part.isdigit():
        #     raise ValueError("Value after 'oks' must be numeric only")
        
        audit.log(
            EventType.VALIDATION,
            task_id,
            EventStatus.SUCCESS,
            {'store_number': store_number}
        )
        
        return store_number
    
    return validate_store_number

def make_fetch_store_server_connection_task():
    @task
    def fetch_store_server_connection(store_number:str,**context) -> Dict[str,str]:
        """
        Retrieve active store servers from HQ and save to temp file.

        Queries retail.ConnectionInfo on the HQ server and writes
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

        query = f"""
                SELECT TOP (1) ServerDatabaseIP AS ServerIP, DatabaseInstanceName AS InstanceName, DatabaseName, '49159' AS Port, StoreNumber
                FROM retail.ConnectionInfo
                WHERE StoreNumber = '{store_number}' 
            """
            
        factory = ConnectionFactory(conn_id = CONNECTION_INFO_CONN_ID)
        rows  = factory.execute_query(query)

        if not rows:
            logger.error(
                "No connection info found for store_number=%s",
                store_number,
            )
            raise AirflowFailException(
                f"connection info not found for store_number={store_number}"
            )
            
        row = rows[0]
        server = {
                "ip": row["ServerIP"],
                "instance": row["InstanceName"],
                "port": row["Port"],
                "database": row["DatabaseName"],
                "storenumber": row["StoreNumber"]
            }
        
        audit.log(
            EventType.STORE_LIST_RETRIEVED,
            task_id,
            EventStatus.SUCCESS,
            {'store_count': len(server)}
        )
        
        logger.info(f"Retrieved {len(server)} servers")
        return server

    return fetch_store_server_connection

def make_validate_mssql_connection_task():
    @task(
        task_id="validate_mssql_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_mssql_connection(
        store_connection: Dict[str, str],
        **context,
    ) -> Dict[str, str]:
        """
        Validate store MSSQL connection before sync execution.

        Builds the dynamic store connection string and verifies connectivity
        before any query/sync work starts.
        """
        audit = AuditLogger(
            dag_id=context['dag'].dag_id,
            run_id=context['run_id']
        )
        task_id = context['task_instance'].task_id

        store_number = store_connection["storenumber"]
        server_ip = store_connection["ip"]
        port = store_connection["port"]
        database = store_connection["database"]

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.STARTED,
            {
                'store': store_number,
                'server_ip': server_ip,
                'database': database,
                'type': 'mssql',
            },
        )

        try:
            conn_uri = create_dynamic_connection(
                server_ip=server_ip,
                port=int(port),
                database=database,
                template_conn_id=STORE_TEMPLATE_CONN_ID,
            )
            factory = ConnectionFactory(conn_uri, is_connection_string=True)

            if not factory.test_connection():
                raise AirflowFailException(
                    f"Store MSSQL connection test failed for store_number={store_number}"
                )
        except AirflowFailException:
            audit.log(
                EventType.CONN_VALIDATED,
                task_id,
                EventStatus.FAILED,
                {
                    'store': store_number,
                    'server_ip': server_ip,
                    'database': database,
                    'type': 'mssql',
                },
            )
            raise
        except Exception as exc:
            audit.log(
                EventType.CONN_VALIDATED,
                task_id,
                EventStatus.FAILED,
                {
                    'store': store_number,
                    'server_ip': server_ip,
                    'database': database,
                    'type': 'mssql',
                },
                error=str(exc),
            )
            message = (
                f"Store MSSQL connection validation failed for store_number={store_number}: {exc}"
            )
            if is_transient_sql_server_error(exc):
                raise AirflowException(message) from exc
            raise AirflowFailException(message) from exc

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.SUCCESS,
            {
                'store': store_number,
                'server_ip': server_ip,
                'database': database,
                'type': 'mssql',
            },
        )

        logger.info(
            "Store MSSQL connection validated for store=%s server=%s database=%s",
            store_number,
            server_ip,
            database,
        )
        return store_connection

    return validate_mssql_connection

def make_sync_replication_md_store_task(dag_config: DAGConfig, sync_config: MasterDataSyncConfig):
    @task(
        retries=dag_config.sync_retries,
        retry_delay=dag_config.sync_retry_delay,
        execution_timeout=dag_config.execution_timeout,
    )
    def sync_replication_md_store(store_connection: Dict[str, str], **context) -> Dict[str, Any]:
        """
        Sync Replication_MD data for a single store connection.

        Args:
            store_connection: Store server connection details from discovery.

        Returns:
            Result dict with sync metrics for the store.
        """
        # Initialize audit logger
        audit = AuditLogger(
            dag_id=context['dag'].dag_id,
            run_id=context['run_id']
        )
        task_id = context['task_instance'].task_id
        
        exec_date = ExecutionDateExtractor.get_date_from_context(context, 0)
        #--===========================================
        server_ip = store_connection["ip"]
        port = store_connection["port"]
        database = store_connection["database"]
        store_number = store_connection["storenumber"]
        
        audit.log( EventType.PROCESSING_SERVER, task_id, EventStatus.STARTED, {'store': store_number, 'exec_date': exec_date} )
        
        try:
            # Create temporary connection
            conn_uri = create_dynamic_connection(
                server_ip = server_ip,
                port = port,
                database = database,
                template_conn_id = STORE_TEMPLATE_CONN_ID
            )
            
            resolved_sync_config = resolve_store_scoped_sync_config(
                sync_config, store_number
            )

            # Initialize orchestrator
            orchestrator = MSSQLToMSSQLQueryOrchestrator(source_conn_id = REPLICATION_MD_CONN_ID,
                                                        target_connection_string = conn_uri,
                                                        fail_on_error = True,
                                                        batch_size = sync_config.batch_size)
            
            # Execute and stream
            result = orchestrator.sync_data(resolved_sync_config, exec_date)
                
            #is_slow = result.duration_seconds > STORE_DURATION_THRESHOLD_SEC
            is_slow = False

            if not result.success:
                audit.log(EventType.DATA_TRANSFER_ORCHESTRATOR, task_id, EventStatus.FAILED, {'store': store_number, 'duration_seconds': result.duration_seconds}, error=result.error_message)
                logger.error(f"Transfer failed for store {store_number} after {result.duration_seconds:.2f}s: {result.error_message}")
                message = f"Sync failed for store {store_number}: {result.error_message}"
                if is_transient_sql_server_error_message(result.error_message or ""):
                    raise AirflowException(message)
                raise AirflowFailException(message)
            
            if is_slow:
                audit.log(EventType.DATA_TRANSFER_ORCHESTRATOR, task_id, EventStatus.WARNING, {'store': store_number, 'records': result.records_transferred, 'duration_seconds': result.duration_seconds},f"Store {store_number} transfer took {result.duration_seconds:.2f}s (>{STORE_DURATION_THRESHOLD_SEC}s threshold)")
                logger.warning(f"Store {store_number} transfer took {result.duration_seconds:.2f}s (>{STORE_DURATION_THRESHOLD_SEC}s threshold)")
                
            audit.log(EventType.PROCESSING_SERVER, task_id, EventStatus.SUCCESS, {'store': store_number, 'records': result.records_transferred, 'duration_seconds': result.duration_seconds})
            logger.info(f"Successfully processed store {store_number}: {result.records_transferred} rows in {result.duration_seconds:.2f}s")
            return {
                    "success": True,
                    "store": store_number, 
                    "records": result.records_transferred,
                    "inserted": result.inserted,
                    "updated": result.updated,
                    "deleted": result.deleted,
                    "duration_seconds": result.duration_seconds,
                    "is_slow": is_slow
                }
            
        except Exception as e:
            audit.log( EventType.PROCESSING_SERVER, task_id, EventStatus.FAILED, {'store': store_number}, error=str(e))
            logger.error(f"Failed processing store {store_number}: {str(e)}")
            raise_sync_task_error(
                f"Sync failed for store {store_number}: {str(e)}",
                e,
            )
    
    return sync_replication_md_store

def make_create_sync_chunks_task(sync_config: MasterDataSyncConfig):
    @task(task_id="create_sync_chunks")
    def create_sync_chunks(store_connection: Dict[str, str], **context) -> List[Dict[str, Any]]:
        """Plan key-range chunks for dynamic parallel Replication_MD store sync."""
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        store_number = store_connection["storenumber"]

        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.STARTED,
            {
                "store": store_number,
                "task_chunk_size": sync_config.task_chunk_size,
                "chunk_column": sync_config.chunk_column or sync_config.primary_keys[0],
            },
        )

        resolved_sync_config = resolve_store_scoped_sync_config(
            sync_config, store_number
        )

        orchestrator = MSSQLToMSSQLQueryOrchestrator(
            source_conn_id=REPLICATION_MD_CONN_ID,
            target_connection_string="",
            fail_on_error=True,
            batch_size=sync_config.batch_size,
        )
        chunks = orchestrator.plan_sync_chunks(resolved_sync_config)
        if not chunks:
            chunk_column = sync_config.chunk_column or sync_config.primary_keys[0]
            chunks = [{
                "chunk_no": 0,
                "min_key": None,
                "max_key": None,
                "row_count": 0,
                "chunk_column": chunk_column,
                "skip": True,
            }]

        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {
                "store": store_number,
                "chunk_count": len(chunks),
                "total_rows": sum(chunk.get("row_count", 0) for chunk in chunks),
            },
        )
        logger.info(
            "Created %d sync chunks for store=%s table=%s",
            len(chunks),
            store_number,
            sync_config.target_table,
        )
        return chunks

    return create_sync_chunks

def make_sync_replication_md_store_chunk_task(
    dag_config: DAGConfig,
    sync_config: MasterDataSyncConfig,
    chunk_pool: str,
):
    @task(
        task_id="sync_replication_md_store_chunk",
        retries=dag_config.sync_retries,
        retry_delay=dag_config.sync_retry_delay,
        execution_timeout=dag_config.execution_timeout,
        max_active_tis_per_dagrun=sync_config.max_parallel_chunks,
        max_active_tis_per_dag=sync_config.max_global_parallel_chunks,
        pool=chunk_pool,
    )
    def sync_replication_md_store_chunk(
        store_connection: Dict[str, str],
        chunk: Dict[str, Any],
        **context,
    ) -> Dict[str, Any]:
        """Sync one key-range chunk to the store database."""
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        exec_date = ExecutionDateExtractor.get_date_from_context(context, 0)

        server_ip = store_connection["ip"]
        port = store_connection["port"]
        database = store_connection["database"]
        store_number = store_connection["storenumber"]
        chunk_no = chunk["chunk_no"]

        audit.log(
            EventType.PROCESSING_SERVER,
            task_id,
            EventStatus.STARTED,
            {
                "store": store_number,
                "chunk_no": chunk_no,
                "min_key": chunk.get("min_key"),
                "max_key": chunk.get("max_key"),
                "row_count": chunk.get("row_count"),
            },
        )

        try:
            conn_uri = create_dynamic_connection(
                server_ip=server_ip,
                port=port,
                database=database,
                template_conn_id=STORE_TEMPLATE_CONN_ID,
            )
            resolved_sync_config = resolve_store_scoped_sync_config(
                sync_config, store_number
            )

            orchestrator = MSSQLToMSSQLQueryOrchestrator(
                source_conn_id=REPLICATION_MD_CONN_ID,
                target_connection_string=conn_uri,
                fail_on_error=True,
                batch_size=sync_config.batch_size,
            )
            result = orchestrator.sync_data_chunk(resolved_sync_config, chunk, exec_date)
            is_slow = result.duration_seconds > STORE_DURATION_THRESHOLD_SEC

            if not result.success:
                audit.log(
                    EventType.DATA_TRANSFER_ORCHESTRATOR,
                    task_id,
                    EventStatus.FAILED,
                    {
                        "store": store_number,
                        "chunk_no": chunk_no,
                        "duration_seconds": result.duration_seconds,
                    },
                    error=result.error_message,
                )
                message = (
                    f"Chunk sync failed for store={store_number} chunk={chunk_no}: "
                    f"{result.error_message}"
                )
                if is_transient_sql_server_error_message(result.error_message or ""):
                    raise AirflowException(message)
                raise AirflowFailException(message)

            status = EventStatus.WARNING if is_slow else EventStatus.SUCCESS
            audit.log(
                EventType.PROCESSING_SERVER,
                task_id,
                status,
                {
                    "store": store_number,
                    "chunk_no": chunk_no,
                    "records": result.records_transferred,
                    "duration_seconds": result.duration_seconds,
                },
            )
            return {
                "success": True,
                "store": store_number,
                "chunk_no": chunk_no,
                "records": result.records_transferred,
                "inserted": result.inserted,
                "updated": result.updated,
                "deleted": result.deleted,
                "duration_seconds": result.duration_seconds,
                "is_slow": is_slow,
            }
        except Exception as exc:
            audit.log(
                EventType.PROCESSING_SERVER,
                task_id,
                EventStatus.FAILED,
                {"store": store_number, "chunk_no": chunk_no},
                error=str(exc),
            )
            raise_sync_task_error(
                f"Chunk sync failed for store={store_number} chunk={chunk_no}: {exc}",
                exc,
            )

    return sync_replication_md_store_chunk

def _normalize_sync_results(results: Any) -> List[Dict[str, Any]]:
    """Normalize single or mapped task outputs into a list of result dicts."""
    if isinstance(results, dict):
        return [results]
    try:
        return list(results)
    except TypeError as exc:
        raise TypeError(
            f"Expected sync result dict or sequence, got {type(results).__name__}"
        ) from exc

def make_report_sync_metrics_task():
    @task()
    def report_sync_metrics(
        results: Any,
    ) -> Dict[str, Any]:
        """
        Aggregate and log sync metrics from execute_sync tasks.
        """
        result_rows = _normalize_sync_results(results)

        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        total_records = 0
        total_duration = 0

        logger.info("----- Store Sync Metrics -----")

        for r in result_rows:

            if not r or not r.get("success"):
                continue

            store = r.get("store")
            inserted = r.get("inserted", 0)
            updated = r.get("updated", 0)
            deleted = r.get("deleted", 0)
            records = r.get("records", 0)
            duration = r.get("duration_seconds", 0)

            total_inserted += inserted
            total_updated += updated
            total_deleted += deleted
            total_records += records
            total_duration += duration

            logger.info(
                f"Store {store} -> "
                f"inserted={inserted}, "
                f"updated={updated}, "
                f"deleted={deleted}, "
                f"records={records}, "
                f"duration={duration:.2f}s"
            )

        logger.info("----- Aggregated Sync Metrics -----")
        logger.info(
            f"TOTAL -> inserted={total_inserted}, "
            f"updated={total_updated}, "
            f"deleted={total_deleted}, "
            f"records={total_records}, "
            f"duration={total_duration:.2f}s"
        )
        logger.info("-----------------------------------")
        
        return {
            "inserted": total_inserted,
            "updated": total_updated,
            "deleted": total_deleted,
            "records": total_records,
            "duration_seconds": total_duration
        }

    return report_sync_metrics
        
# ============================================================================
# DAG DEFINITION
# ============================================================================

def create_dag(dag_config: DAGConfig, sync_config: MasterDataSyncConfig) -> DAG:

    default_args = {
        'owner': dag_config.owner,
        'depends_on_past': dag_config.depends_on_past,
        'email_on_failure': False,
        'email_on_retry': False,
        'email': ['Saffarpour.Zahra@okco.ir'],
        'retries': dag_config.retries,
        'retry_delay': dag_config.retry_delay,
        'execution_timeout': dag_config.execution_timeout,
        'pool': 'default_pool',
    }

    max_active_tasks = (
        max(
            dag_config.max_active_tasks,
            sync_config.max_parallel_chunks + 3,
        )
        if sync_config.use_dynamic_tasks
        else dag_config.max_active_tasks
    )

    with DAG(
        dag_id = dag_config.dag_id,
        description = "Sync Replication_MD publisher data to store MSSQL target for missed replication gaps",
        start_date = dag_config.start_date,
        schedule = dag_config.schedule,
        catchup = dag_config.catchup,
        max_active_runs = dag_config.max_active_runs,
        max_active_tasks = max_active_tasks,
        tags = dag_config.tags,
        default_args=default_args,
        params={
            'store_number': Param(
                default='OKS00000',
                type='string',
                description="Enter the store number for synchronization",
                title='Store Number'
            ),
        },
        doc_md=__doc__,
    ) as dag:

        # =========================
        # VALIDATION GROUP
        # =========================
        
        with TaskGroup(group_id="Validation") as validation:
            validate_mssql_connection_info = make_validate_mssql_connection_info_task()()
            validate_mssql_connection_replication_md = make_validate_mssql_connection_replication_md_task()()
            validate_store_number = make_validate_store_number_task()()

            # Internal dependencies
            validate_store_number >> [validate_mssql_connection_info , validate_mssql_connection_replication_md]

        # =========================
        # DISCOVERY GROUP
        # =========================
        
        with TaskGroup("discovery") as discovery:
            fetch_store_connection_task = make_fetch_store_server_connection_task()
            validate_mssql_connection_task = make_validate_mssql_connection_task()

            store_server_connection = fetch_store_connection_task(validate_store_number)
            validated_store_connection = validate_mssql_connection_task(store_server_connection)

            store_server_connection >> validated_store_connection
                    
        # =========================
        # PROCESSING GROUP
        # =========================
        
        with TaskGroup("processing") as processing:
            report_sync_metrics_task = make_report_sync_metrics_task()

            if sync_config.use_dynamic_tasks:
                create_sync_chunks_task = make_create_sync_chunks_task(sync_config=sync_config)
                sync_replication_md_store_chunk_task = make_sync_replication_md_store_chunk_task(
                    dag_config=dag_config,
                    sync_config=sync_config,
                    chunk_pool=dag_config.pool,
                )

                sync_chunks = create_sync_chunks_task(validated_store_connection)
                sync_results = sync_replication_md_store_chunk_task.partial(
                    store_connection=validated_store_connection,
                ).expand(chunk=sync_chunks)
                report_sync_metrics = report_sync_metrics_task(sync_results)

                sync_chunks >> sync_results >> report_sync_metrics
            else:
                sync_replication_md_store_task = make_sync_replication_md_store_task(
                    dag_config=dag_config,
                    sync_config=sync_config,
                )
                sync_replication_md_store = sync_replication_md_store_task(validated_store_connection)
                report_sync_metrics = report_sync_metrics_task(sync_replication_md_store)

                sync_replication_md_store >> report_sync_metrics

        # =========================
        # DEPENDENCIES
        # =========================
        validation >> discovery >> processing
        
    return dag