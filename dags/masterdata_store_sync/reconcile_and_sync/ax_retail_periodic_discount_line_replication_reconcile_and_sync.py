"""
masterdata_replication_reconcile_and_sync — Master-Data Replication Reconcile & Sync

Reconciles SQL Server transactional replication subscribers (stores) against the
publisher (HQ) for RetailPeriodicDiscountLine articles, detects row-count inconsistencies,
and triggers store master-data sync orchestrators for affected stores.

Pipeline Stages:
    1. Validate   : Replication_MD publisher + Connection_Info connectivity
    2. Generate   : Build subscriber reconcile script from publisher
    3. Discover   : Retrieve active subscriber servers from Connection_Info
    4. Chunk      : Split ~5000 servers into chunks to avoid XCom size limits
    5. Reconcile  : Execute reconcile script on each subscriber in parallel chunks
    6. Trigger    : Trigger sync orchestrators for stores with replication gaps
    7. Verify     : Aggregate reconcile and trigger results
    8. Cleanup    : Remove temporary shared files

Key Features:
    - Chunked processing  : Handles 5000+ subscribers without XCom payload limits
    - Parallel execution  : Up to 30 concurrent chunk tasks
    - In-memory execution : Reconcile script executed dynamically on subscribers
    - Idempotent triggers : Deterministic run IDs prevent duplicate orchestrator runs
    - Fault isolation     : Individual subscriber failures do not abort the DAG

Author: Senior Data Engineer
Version: 1.0
Created: 2026-06-04
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set,Tuple
from urllib.parse import quote_plus

from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowFailException  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore

from pipeline.config.AuditConfig import EventType, EventStatus
from pipeline.database.ConnectionFactory import ConnectionFactory
from pipeline.core.DagSyncTrigger import DagSyncTrigger
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.utils.validation import validate_mssql_conn

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

REPLICATION_MD_CONN_ID = "mssql_replication_md"
CONNECTION_INFO_CONN_ID = "mssql_store_connectionInfo"

# Template connection for store credentials (SQL Authentication)
# Contains username/password that will be reused for all stores
STORE_DRIVER_TYPE = 'pyodbc' #'pymssql'
STORE_TEMPLATE_CONN_ID = "mssql_store_template"
STORE_TIMEOUT_SEC = 300 # 5 minutes
STORE_QUERY_TIMEOUT_SEC = 150 # 2 minutes
STORE_LOGIN_TIMEOUT_SEC = 30 # 30 seconds
STORE_DURATION_THRESHOLD_SEC  = 1200 # 2 minutes

CHUNK_SIZE = 50
STORE_MAX_ACTIVE_TIS_PER_DAG = 60
AIRFLOW_SHARED_PATH = Variable.get("airflow_shared_path", default_var="/opt/airflow/shared")
SYNC_FAILED_DAG_RUN_RETRIES = int(
    Variable.get("sync_failed_dag_run_retries", default_var=1)
)
SYNC_FAILED_DAG_RUN_RETRY_DELAY = int(
    Variable.get("sync_failed_dag_run_retry_delay", default_var=60)
)
DAG_RETRIES = int(
    Variable.get("retries_ax_retail_periodic_discount_line_replication_reconcile_and_sync", default_var=0)
)
DAG_RETRY_DELAY = timedelta(
    minutes=int(
        Variable.get(
            "retry_delay_minutes_ax_retail_periodic_discount_line_replication_reconcile_and_sync",
            default_var=5,
        )
    )
)
DAG_EXECUTION_TIMEOUT = timedelta(
    hours=int(
        Variable.get(
            "execution_timeout_hours_ax_retail_periodic_discount_line_replication_reconcile_and_sync",
            default_var=8,
        )
    )
)

TABLE_SYNC_DAG_MAPPING  = """
{
  "ax.RetailPeriodicDiscountLine": {
    "dag_id": "ax_retail_periodic_discount_line_sync"
  }
}
"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_table_name(table_name: str) -> str:
    return str(table_name or "").strip().replace("[", "").replace("]", "").lower()

def _get_subscriber_servers() -> List[Dict[str, Any]]:
    """Retrieve active subscriber targets from Connection_Info."""
    query = """
    SELECT
        ServerDatabaseIP AS ServerIP,
        DatabaseInstanceName AS InstanceName,
        DatabaseName,
        '49159' AS Port,
        StoreNumber
    FROM retail.ActiveConnectionInfo
    """
    factory = ConnectionFactory(CONNECTION_INFO_CONN_ID)
    rows = factory.execute_query(query)
    servers = [
        {
            "ip": row["ServerIP"],
            "instance": row["InstanceName"],
            "port": row["Port"],
            "database": row["DatabaseName"],
            "storenumber": row["StoreNumber"],
        }
        for row in rows
    ]
    logger.info("Retrieved %d subscriber servers from Connection_Info", len(servers))
    return servers


def _shared_file_path(dag_id: str, run_id: str, suffix: str) -> str:
    return os.path.join(AIRFLOW_SHARED_PATH, f"{dag_id}_{run_id}_{suffix}")


def _reconcile_subscriber_chunk(
    servers: List[Dict[str, Any]],
    reconcile_script: str,
    audit: AuditLogger,
    task_id: str,
) -> List[Dict[str, Any]]:

    results: List[Dict[str, Any]] = []

    for server in servers:
        store_number = server["storenumber"]
        audit.log(
            EventType.REPLICATION_RECONCILE,
            task_id,
            EventStatus.STARTED,
            {
                "store": store_number,
                "server_ip": server["ip"],
                "database": server["database"],
            },
        )

        result = _reconcile_subscriber(server, reconcile_script)
        is_slow = result.get("duration_seconds", 0) > STORE_DURATION_THRESHOLD_SEC

        if not result.get("success"):
            audit.log(
                EventType.REPLICATION_RECONCILE,
                task_id,
                EventStatus.FAILED,
                {
                    "store": store_number,
                    "server_ip": server.get("server_ip"),
                    "duration_seconds": result.get("duration_seconds"),
                },
                error=result.get("error"),
            )
        elif result.get("has_inconsistency"):
            audit.log(
                EventType.REPLICATION_RECONCILE,
                task_id,
                EventStatus.WARNING,
                {
                    "store": store_number,
                    "server_ip": result.get("server_ip"),
                    "diff_count": result.get("diff_count"),
                    "duration_seconds": result.get("duration_seconds"),
                },
            )
        elif is_slow:
            audit.log(
                EventType.REPLICATION_RECONCILE,
                task_id,
                EventStatus.WARNING,
                {
                    "store": store_number,
                    "duration_seconds": result.get("duration_seconds"),
                },
                error=f"Reconcile took {result['duration_seconds']}s (>{STORE_DURATION_THRESHOLD_SEC}s threshold)",
            )
        else:
            audit.log(
                EventType.REPLICATION_RECONCILE,
                task_id,
                EventStatus.SUCCESS,
                {
                    "store": store_number,
                    "server_ip": result.get("server_ip"),
                    "duration_seconds": result.get("duration_seconds"),
                },
            )

        results.append(result)

    return results

def _create_dynamic_connection(
    server_ip: str,
    port: int,
    database: str,
) -> str:
    """Build a dynamic MSSQL connection URI for a subscriber store."""
    from airflow.hooks.base import BaseHook  # type: ignore

    conn = BaseHook.get_connection(STORE_TEMPLATE_CONN_ID)
    sql_user, sql_pass = conn.login, conn.password
    extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
    odbc_driver = extra.get("driver", "ODBC Driver 18 for SQL Server")

    encoded_user = quote_plus(sql_user)
    encoded_pass = quote_plus(sql_pass)
    encoded_appname = quote_plus("Airflow")

    if STORE_DRIVER_TYPE == "pyodbc":
        encoded_driver = quote_plus(odbc_driver)
        scheme = "mssql+pyodbc"
        driver_param = f"&driver={encoded_driver}"
    else:
        scheme = "mssql+pymssql"
        driver_param = ""

    return (
        f"{scheme}://{encoded_user}:{encoded_pass}@{server_ip},{port}/{database}"
        f"?appname={encoded_appname}"
        f"&timeout={STORE_TIMEOUT_SEC}"
        f"&login_timeout={STORE_LOGIN_TIMEOUT_SEC}"
        f"&query_timeout={STORE_QUERY_TIMEOUT_SEC}"
        f"{driver_param}"
    )


def _reconcile_subscriber(
    server: Dict[str, Any],
    reconcile_script: str,
) -> Dict[str, Any]:
    """
    Execute the generated reconcile script on a single subscriber database.

    Compares store row counts against HQ publication counts and returns
    inconsistency details when replication gaps exist.
    """
    store_number = server["storenumber"]
    start_time = time.time()

    try:
        conn_uri = _create_dynamic_connection(
            server_ip=server["ip"],
            port=int(server["port"]),
            database=server["database"],
        )
        factory = ConnectionFactory(conn_uri, is_connection_string=True)
        diff_rows = factory.execute_query(reconcile_script)
        duration_seconds = round(time.time() - start_time, 2)
        diff_tables = [
                          row["TableName"]
                          for row in diff_rows
                          if row.get("Diff", 0) > 1
                      ]

        if diff_rows:
            logger.warning(
                "Replication inconsistency detected for store=%s rows=%d duration=%.2fs",
                store_number,
                len(diff_rows),
                duration_seconds,
            )
            return {
                "success": True,
                "has_inconsistency": True,
                "store": store_number,
                "server_ip": server["ip"],
                "database": server["database"],
                "diff_count": len(diff_rows),
                "diff_rows": diff_rows,
                "diff_tables": diff_tables,
                "duration_seconds": duration_seconds,
            }

        logger.info(
            "Subscriber reconcile passed for store=%s duration=%.2fs",
            store_number,
            duration_seconds,
        )
        return {
            "success": True,
            "has_inconsistency": False,
            "store": store_number,
            "server_ip": server["ip"],
            "database": server["database"],
            "diff_count": 0,
            "duration_seconds": duration_seconds,
        }
    except Exception as exc:
        duration_seconds = round(time.time() - start_time, 2)
        logger.error(
            "Subscriber reconcile failed for store=%s: %s",
            store_number,
            exc,
            exc_info=True,
        )
        return {
            "success": False,
            "has_inconsistency": False,
            "store": store_number,
            "server_ip": server.get("ip"),
            "database": server.get("database"),
            "error": str(exc),
            "duration_seconds": duration_seconds,
        }

def _generate_subscriber_reconcile_script() -> Tuple[str, int]:
    """
    Execute publisher query and build the subscriber reconcile script.

    Returns:
        Tuple of (subscriber reconcile script, publication table count).
    """
    query = """
    DROP TABLE IF EXISTS #myPublication;
    SET NOCOUNT ON;
    SELECT DISTINCT myArticle.source_owner + '.' + myArticle.source_object AS TableName
    INTO #myPublication
    FROM distribution.dbo.MSarticles AS myArticle
    INNER JOIN distribution.dbo.MSpublications AS myPublication ON myPublication.publication_id = myArticle.publication_id
    WHERE myArticle.source_object LIKE '%RetailPeriodicDiscountLine%'

    DECLARE @mySubscriberScript NVARCHAR(MAX)
    DECLARE @myNewLine NVARCHAR(10)
    SET @mySubscriberScript  = 'DECLARE @myTable AS TABLE(SubscriberDB NVARCHAR(50), TableName NVARCHAR(50), SubscriberRowCount INT, PublicationRowCount INT)'
    SET @myNewLine = CHAR(13) + CHAR(10)

    DECLARE @myTable AS TABLE(TableName NVARCHAR(50), RowCNT INT)
    DECLARE @myTableName sysname;
    DECLARE @myPublisherDB sysname;
    DECLARE @myPublication sysname;
    DECLARE cr CURSOR FOR
    SELECT TableName
    FROM #myPublication;
    SET NOCOUNT ON;
    OPEN cr;
    FETCH NEXT FROM cr
    INTO @myTableName
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @myScript NVARCHAR(MAX);
        DECLARE @myRowCount INT

        SET @myScript = N'SELECT ''' + @myTableName + N'''AS TableName, COUNT(1) AS RowCNT FROM ' + @myTableName;

        INSERT @myTable (TableName, RowCNT)
        EXECUTE sp_executesql @myScript;

        SELECT @myRowCount = myTable.RowCNT
        FROM @myTable AS myTable
        WHERE TableName = @myTableName

        SET @mySubscriberScript = @mySubscriberScript + 'INSERT INTO @myTable(SubscriberDB,TableName, SubscriberRowCount, PublicationRowCount)' + @myNewLine
        SET @mySubscriberScript = @mySubscriberScript + 'SELECT DB_Name() AS SubscriberDB, ''' +  @myTableName +''' AS TableName, COUNT(1) AS SubscriberRowCount,' + CAST(@myRowCount AS NVARCHAR(10)) + ' AS PublicationRowCount'+ @myNewLine
        SET @mySubscriberScript = @mySubscriberScript + 'FROM ' + @myTableName + @myNewLine

        FETCH NEXT FROM cr
        INTO @myTableName
    END;
    CLOSE cr;
    DEALLOCATE cr;

    SET @mySubscriberScript = @mySubscriberScript + 'SELECT *,ABS(SubscriberRowCount - PublicationRowCount) AS Diff FROM @myTable' + @myNewLine
    SET @mySubscriberScript = @mySubscriberScript + 'WHERE PublicationRowCount - SubscriberRowCount > 0'

    SELECT
        @mySubscriberScript AS SubscriberScript,
        (SELECT COUNT(1) FROM #myPublication) AS PublicationTableCount
    """

    factory = ConnectionFactory(REPLICATION_MD_CONN_ID)
    rows = factory.execute_query(query)
    if not rows:
        raise ValueError("Publisher query returned no rows for subscriber reconcile script")

    first_row = rows[0]
    script = None
    publication_table_count = None
    if isinstance(first_row, dict):
        for key, value in first_row.items():
            key_lower = key.lower()
            if key_lower == "subscriberscript":
                script = value
            elif key_lower == "publicationtablecount":
                publication_table_count = value

    if script is None:
        raise ValueError(
            f"Publisher query result missing SubscriberScript column: {first_row!r}"
        )

    script = str(script).strip()
    if not script:
        raise ValueError("Publisher query returned an empty subscriber reconcile script")

    if publication_table_count is None:
        publication_table_count = script.count("INSERT INTO @myTable")
    else:
        publication_table_count = int(publication_table_count)

    logger.info(
        "Generated subscriber reconcile script (%d chars) for %d publication tables",
        len(script),
        publication_table_count,
    )
    return script, publication_table_count


# ============================================================================
# TASKS
# ============================================================================

@task
def validate_replication_md_connection_task(**context):
    """Validate Replication_MD publisher connectivity before DAG execution."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.STARTED,
        {"conn_id": REPLICATION_MD_CONN_ID, "type": "mssql", "server": "Replication_MD"},
    )

    result = validate_mssql_conn(REPLICATION_MD_CONN_ID)

    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {"conn_id": REPLICATION_MD_CONN_ID, "type": "mssql", "server": "Replication_MD"},
    )
    return result


@task
def validate_connection_info_task(**context):
    """Validate Connection_Info server connectivity before DAG execution."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.STARTED,
        {"conn_id": CONNECTION_INFO_CONN_ID, "type": "mssql", "server": "Connection_Info"},
    )

    result = validate_mssql_conn(CONNECTION_INFO_CONN_ID)

    audit.log(
        EventType.CONN_VALIDATED,
        task_id,
        EventStatus.SUCCESS,
        {"conn_id": CONNECTION_INFO_CONN_ID, "type": "mssql", "server": "Connection_Info"},
    )
    return result


@task
def generate_reconcile_script_task(**context) -> Dict[str, Any]:
    """
    Generate subscriber reconcile script from Replication_MD publisher (HQ).

    Builds the row-count comparison script on the publisher and returns only
    lightweight metadata (not the script content) via XCom.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(EventType.REPLICATION_SCRIPT_GENERATED, task_id, EventStatus.STARTED, {})

    script, publication_table_count = _generate_subscriber_reconcile_script()

    metadata = {
        "script_length": len(script),
        "publication_table_count": publication_table_count,
        "status": "generated",
    }

    audit.log(
        EventType.REPLICATION_SCRIPT_GENERATED,
        task_id,
        EventStatus.SUCCESS,
        metadata,
    )
    logger.info(
        "Generated reconcile script (%d chars, %d publication tables)",
        len(script),
        publication_table_count,
    )
    return metadata


@task
def get_subscriber_servers_task(**context) -> str:
    """
    Retrieve subscriber servers from Connection_Info and save to temp file.

    Returns only the file path to avoid XCom size limits.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    audit = AuditLogger(dag_id=dag_id, run_id=run_id)
    task_id = context["task_instance"].task_id

    audit.log(EventType.STORE_LIST_RETRIEVED, task_id, EventStatus.STARTED, {})

    servers = _get_subscriber_servers()

    server_file = _shared_file_path(dag_id, run_id, "servers.json")
    os.makedirs(AIRFLOW_SHARED_PATH, exist_ok=True)
    with open(server_file, "w", encoding="utf-8") as handle:
        json.dump(servers, handle)

    audit.log(
        EventType.STORE_LIST_RETRIEVED,
        task_id,
        EventStatus.SUCCESS,
        {"store_count": len(servers), "file": server_file},
    )
    logger.info("Retrieved %d subscriber servers, saved to %s", len(servers), server_file)
    return server_file


@task
def create_chunks_task(server_file: str) -> List[Dict[str, int]]:
    """Split subscriber server list into chunk index ranges."""
    with open(server_file, "r", encoding="utf-8") as handle:
        servers = json.load(handle)

    total = len(servers)
    chunks = []
    for index in range(0, total, CHUNK_SIZE):
        chunks.append(
            {
                "start_index": index,
                "end_index": min(index + CHUNK_SIZE, total),
            }
        )

    logger.info("Created %d chunks for %d subscriber servers", len(chunks), total)
    return chunks


@task(max_active_tis_per_dag=STORE_MAX_ACTIVE_TIS_PER_DAG)
def reconcile_subscribers_task(
    server_file: str,
    start_index: int,
    end_index: int,
    **context,
) -> List[Dict[str, Any]]:
    """
    Reconcile a chunk of subscribers using the generated script kept in memory.

    Regenerates the reconcile script from Replication_MD per chunk worker so the
    script is never written to disk or passed through XCom.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    try:
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.STARTED,
            {"start_index": start_index, "end_index": end_index},
        )

        reconcile_script, _ = _generate_subscriber_reconcile_script()

        with open(server_file, "r", encoding="utf-8") as handle:
            servers = json.load(handle)

        chunk_servers = servers[start_index:end_index]
        logger.info(
            "Reconciling chunk stores %d to %d (%d servers)",
            start_index,
            end_index - 1,
            len(chunk_servers),
        )

        results = _reconcile_subscriber_chunk(
            chunk_servers,
            reconcile_script,
            audit,
            task_id,
        )

        inconsistent = sum(1 for result in results if result.get("has_inconsistency"))
        failed = sum(1 for result in results if not result.get("success"))

        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.SUCCESS,
            {
                "total": len(results),
                "inconsistent": inconsistent,
                "failed": failed,
                "successful": len(results) - failed,
            },
        )
        return results
    
    except Exception as exc:
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.FAILED,
            {"start_index": start_index, "end_index": end_index},
            error=str(exc),
        )
        raise


@task
def trigger_sync_orchestrators_task(
    chunk_results: List[List[Dict[str, Any]]],
    **context,
) -> Dict[str, Any]:
    """
    Trigger sync orchestrators for stores with replication inconsistencies.

    Only stores with reconcile diffs trigger downstream orchestrators.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(EventType.SYNC_DAG_TRIGGERED, task_id, EventStatus.STARTED, {})

    all_results: List[Dict[str, Any]] = []
    for chunk in chunk_results:
        all_results.extend(chunk)

    store_dag_map: Dict[str, Set[str]] = {}
    
    table_sync_mapping = {
        _normalize_table_name(table): config
        for table, config in json.loads(TABLE_SYNC_DAG_MAPPING).items()
    }
    for result in all_results:
        if not result.get("success") or not result.get("has_inconsistency"):
            continue

        store_number = result["store"]

        for diff_row in result.get("diff_rows", []):
            table_name = diff_row.get("TableName")
            diff = diff_row.get("Diff", 0)

            if diff <= 1:
                continue

            table_name = diff_row.get("TableName")
            table_key = _normalize_table_name(table_name)
            mapping = table_sync_mapping.get(table_key)

            if not mapping:
                logger.warning(
                    "No sync DAG mapping found for table=%s store=%s",
                    table_name,
                    store_number,
                )
                continue

            store_dag_map.setdefault(store_number, set()).add(mapping["dag_id"])

    if not store_dag_map:
        summary = {
            "stores_triggered": 0,
            "store_numbers": [],
            "trigger_results": [],
            "status": "no_mapped_inconsistencies",
        }
        audit.log(EventType.SYNC_DAG_TRIGGERED, task_id, EventStatus.SUCCESS, summary)
        return summary


    trigger_results = []

    for store_number, dag_ids in store_dag_map.items():
        trigger = DagSyncTrigger(
            orchestrator_dag_ids=tuple(sorted(dag_ids)),
            parent_run_id=context["run_id"],
            logical_date=DagSyncTrigger.resolve_logical_date(context),
            wait_for_completion=True,
            poke_interval=60,
            execution_mode="serial",
            failed_dag_run_retries=SYNC_FAILED_DAG_RUN_RETRIES,
            failed_dag_run_retry_delay=SYNC_FAILED_DAG_RUN_RETRY_DELAY,
        )

        trigger_results.extend(
            trigger.trigger_stores({store_number}, emit_report=False)
        )

    if trigger_results:
        DagSyncTrigger.log_trigger_metrics(trigger_results)

    failed_triggers = [
        result
        for result in trigger_results
        if not result.get("success")
    ]
    if failed_triggers:
        raise AirflowFailException(
            f"Sync orchestrator trigger/wait failed: {failed_triggers}"
        )

    triggered_dag_names = sorted(
        {
            dag_id
            for result in trigger_results
            for dag_id in result.get("triggered_dags", [])
        }
    )

    summary = {
        "stores_triggered": len(store_dag_map),
        "store_numbers": sorted(store_dag_map.keys()),
        "store_dag_map": {
            store: sorted(dag_ids)
            for store, dag_ids in store_dag_map.items()
        },
        "trigger_results": trigger_results,
        "triggered_dag_names": triggered_dag_names,
        "status": "completed",
    }

    audit.log(EventType.SYNC_DAG_TRIGGERED, task_id, EventStatus.SUCCESS, summary)
    logger.info(
        "Triggered and completed sync orchestrators for %d stores: %s",
        len(store_dag_map),
        sorted(store_dag_map),
    )
    return summary


@task
def verify_reconcile_task(
    chunk_results: List[List[Dict[str, Any]]],
    trigger_summary: Dict[str, Any],
    **context,
) -> Dict[str, Any]:
    """Aggregate reconcile results and report inconsistencies and failures."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    all_results: List[Dict[str, Any]] = []
    for chunk in chunk_results:
        all_results.extend(chunk)

    inconsistent_stores = [
        result["store"]
        for result in all_results
        if result.get("success") and result.get("has_inconsistency")
    ]
    failed_stores = [result["store"] for result in all_results if not result.get("success")]
    slow_stores = [
        {"store": result["store"], "duration_seconds": result.get("duration_seconds")}
        for result in all_results
        if result.get("success")
        and result.get("duration_seconds", 0) > STORE_DURATION_THRESHOLD_SEC
    ]

    status = "completed"
    if failed_stores:
        status = "completed_with_errors"
    elif inconsistent_stores:
        status = "completed_with_inconsistencies"

    summary = {
        "total_servers": len(all_results),
        "inconsistent": len(inconsistent_stores),
        "failed": len(failed_stores),
        "successful": len(all_results) - len(failed_stores),
        "inconsistent_stores": inconsistent_stores,
        "failed_stores": failed_stores,
        "slow_stores": slow_stores,
        "trigger_summary": trigger_summary,
        "status": status,
    }

    audit.log(
        EventType.REPLICATION_RECONCILE_SUMMARY,
        task_id,
        EventStatus.SUCCESS if not failed_stores else EventStatus.FAILED,
        summary,
    )

    if failed_stores:
        logger.error("Reconcile failed for stores: %s", failed_stores)
    if inconsistent_stores:
        logger.warning("Replication inconsistencies detected for stores: %s", inconsistent_stores)

    return summary


@task(trigger_rule="all_done")
def cleanup_temp_files_task(server_file: str, **context) -> Dict[str, Any]:
    """Remove temporary shared files created during the DAG run."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    cleanup_results = []
    for file_path in (server_file,):
        try:
            audit.log(
                EventType.FILE_OPERATION,
                task_id,
                EventStatus.STARTED,
                {"file": file_path},
            )
            if os.path.exists(file_path):
                os.remove(file_path)
                cleanup_results.append({"file": file_path, "status": "deleted"})
                audit.log(
                    EventType.FILE_OPERATION,
                    task_id,
                    EventStatus.SUCCESS,
                    {"file": file_path, "action": "deleted"},
                )
            else:
                cleanup_results.append({"file": file_path, "status": "not_found"})
                audit.log(
                    EventType.FILE_OPERATION,
                    task_id,
                    EventStatus.SUCCESS,
                    {"file": file_path, "action": "not_found"},
                )
        except Exception as exc:
            cleanup_results.append({"file": file_path, "status": "failed", "error": str(exc)})
            audit.log(
                EventType.FILE_OPERATION,
                task_id,
                EventStatus.FAILED,
                {"file": file_path},
                error=str(exc),
            )

    return {"files": cleanup_results}


@task(trigger_rule="all_done")
def dag_completion_task(verification: Dict[str, Any], **context):
    """Log final DAG completion status to audit trail."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])

    failed = verification.get("failed", 0)
    audit.log(
        EventType.DAG_COMPLETED,
        "dag_completion",
        EventStatus.SUCCESS if failed == 0 else EventStatus.FAILED,
        verification,
    )


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    "owner": "Zahra Saffarpour",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["Saffarpour.Zahra@okco.ir"],
    "retries": DAG_RETRIES,
    "retry_delay": DAG_RETRY_DELAY,
    "execution_timeout": DAG_EXECUTION_TIMEOUT,
    "pool": "default_pool",
}

with DAG(
    dag_id="ax_retail_periodic_discount_line_replication_reconcile_and_sync",
    description="Reconcile master-data replication between HQ publisher and store subscribers, identify replication gaps, and trigger downstream synchronization workflows.",
    start_date=datetime(2026, 6, 4),
    schedule=None,# "0 6 * * *",  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=["mssql", "store", "master-data", "reconcile"],
    default_args=default_args,
) as dag:
    with TaskGroup(group_id="Validation") as validation:
        validate_replication_md = validate_replication_md_connection_task()
        validate_connection_info = validate_connection_info_task()
        [validate_replication_md, validate_connection_info]

    with TaskGroup(group_id="setup") as setup:
        script_metadata = generate_reconcile_script_task()

    with TaskGroup(group_id="discovery") as discovery:
        subscriber_server_file = get_subscriber_servers_task()
        subscriber_chunks = create_chunks_task(subscriber_server_file)
        subscriber_server_file >> subscriber_chunks

    with TaskGroup(group_id="reconcile") as reconcile:
        reconcile_results = reconcile_subscribers_task.partial(
            server_file=subscriber_server_file,
        ).expand_kwargs(subscriber_chunks)

    with TaskGroup(group_id="sync_and_verify") as sync_and_verify:
        trigger_summary = trigger_sync_orchestrators_task(reconcile_results)
        verification = verify_reconcile_task(reconcile_results, trigger_summary)
        cleanup = cleanup_temp_files_task(subscriber_server_file)
        completion = dag_completion_task(verification)
        trigger_summary >> verification >> cleanup >> completion

    validation >> setup >> discovery >> reconcile >> sync_and_verify
