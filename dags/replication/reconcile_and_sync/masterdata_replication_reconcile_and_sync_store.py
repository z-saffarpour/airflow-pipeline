"""
masterdata_replication_reconcile_and_sync_store — Single-Store Master-Data Replication Reconcile & Sync

Same pipeline as masterdata_replication_reconcile_and_sync, scoped to one store.
Accepts store_number via DAG params or trigger conf when the DAG is manually triggered.

Pipeline Stages:
    1. Validate   : Replication_MD publisher + Connection_Info connectivity + store_number
    2. Generate   : Build subscriber reconcile script from publisher
    3. Discover   : Retrieve subscriber server for the given store_number
    4. Reconcile  : Execute reconcile script on the subscriber
    5. Trigger    : Trigger sync orchestrators when replication gaps exist and wait for completion
    6. Verify     : Aggregate reconcile and trigger results
    7. Cleanup    : Log DAG completion

Author: Senior Data Engineer
Version: 1.0
Created: 2026-06-13
"""
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import quote_plus

from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowFailException  # type: ignore
from airflow.models import Variable  # type: ignore
from airflow.models.param import Param  # type: ignore
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

AIRFLOW_SHARED_PATH = Variable.get("airflow_shared_path", default_var="/opt/airflow/shared")
SYNC_FAILED_DAG_RUN_RETRIES = int(
    Variable.get("sync_failed_dag_run_retries", default_var=1)
)
SYNC_FAILED_DAG_RUN_RETRY_DELAY = int(
    Variable.get("sync_failed_dag_run_retry_delay", default_var=60)
)
DAG_RETRIES = int(
    Variable.get("retries_masterdata_replication_reconcile_and_sync_store", default_var=0)
)
DAG_RETRY_DELAY = timedelta(
    minutes=int(
        Variable.get("retry_delay_minutes_masterdata_replication_reconcile_and_sync_store", default_var=5)
    )
)
DAG_EXECUTION_TIMEOUT = timedelta(
    hours=int(
        Variable.get("execution_timeout_hours_masterdata_replication_reconcile_and_sync_store", default_var=8)
    )
)

TABLE_SYNC_DAG_MAPPING = """
{
  "ax.InventTableModule": {
    "dag_id": "ax_invent_table_module_sync"
  },
  "ax.PriceDiscGroup": {
    "dag_id": "ax_price_disc_group_sync"
  },
  "ax.PriceDiscTable": {
    "dag_id": "ax_price_disc_table_sync"
  },
  "ax.RetaillAbelchangeJournalTrans": {
    "dag_id": "ax_retail_abelchange_journal_trans_sync"
  },
  "ax.RetailAssortmentLookupChannelGroup": {
    "dag_id": "ax_retail_assortment_lookup_channel_group_sync"
  },
  "ax.RetailAssortmentLookup": {
    "dag_id": "ax_retail_assortment_lookup_sync"
  },
  "ax.RetailDiscountCode": {
    "dag_id": "ax_retail_discount_code_sync"
  },
  "ax.RetailDiscountLineMixAndMatch": {
    "dag_id": "ax_retail_discount_line_mix_and_match_sync"
  },
  "ax.RetailDiscountLineMultibuy": {
    "dag_id": "ax_retail_discount_line_multibuy_sync"
  },
  "ax.RetailDiscountLineOffer": {
    "dag_id": "ax_retail_discount_line_offer_sync"
  },
  "ax.RetailDiscountMixAndMatch": {
    "dag_id": "ax_retail_discount_mix_and_match_sync"
  },
  "ax.RetailDiscountMultibuy": {
    "dag_id": "ax_retail_discount_multibuy_sync"
  },
  "ax.RetailDiscountOffer": {
    "dag_id": "ax_retail_discount_offer_sync"
  },
  "ax.RetailDiscountPriceGroup": {
    "dag_id": "ax_retail_discount_price_group_sync"
  },
  "ax.RetailDiscountThreshold": {
    "dag_id": "ax_retail_discount_threshold_sync"
  },
  "ax.RetailDiscountThresholdTiers": {
    "dag_id": "ax_retail_discount_threshold_tiers_sync"
  },
  "ax.RetailDiscountValidationPeriod": {
    "dag_id": "ax_retail_discount_validation_period_sync"
  },
  "ax.RetailGroupMemberLine": {
    "dag_id": "ax_retail_group_member_line_sync"
  },
  "ax.RetailMixAndMatchLineGroups": {
    "dag_id": "ax_retail_mix_and_match_line_groups_sync"
  },
  "ax.RetailMultibuyDiscountLine": {
    "dag_id": "ax_retail_multibuy_discount_line_sync"
  },
  "ax.RetailPeriodicDiscountLine": {
    "dag_id": "ax_retail_periodic_discount_line_sync"
  },
  "ax.RetailPeriodicDiscount": {
    "dag_id": "ax_retail_periodic_discount_sync"
  }
}
"""

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_table_name(table_name: str) -> str:
    return str(table_name or "").strip().replace("[", "").replace("]", "").lower()


def _resolve_store_number(context: Dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    store_number = (
        conf.get("store_number")
        or conf.get("Store_number")
        or context.get("params", {}).get("store_number")
    )
    if not store_number:
        raise ValueError("store_number must be provided via trigger conf or DAG params")
    return str(store_number).strip()


def _get_subscriber_server(store_number: str) -> Dict[str, Any]:
    """Retrieve a single active subscriber target from Connection_Info."""
    query = f"""
    SELECT TOP (1)
        ServerDatabaseIP AS ServerIP,
        DatabaseInstanceName AS InstanceName,
        DatabaseName,
        '49159' AS Port,
        StoreNumber
    FROM retail.ConnectionInfo
    WHERE StoreNumber = '{store_number}'
    """
    factory = ConnectionFactory(CONNECTION_INFO_CONN_ID)
    rows = factory.execute_query(query)
    if not rows:
        raise AirflowFailException(
            f"No active connection info found for store_number={store_number}"
        )

    row = rows[0]
    server = {
        "ip": row["ServerIP"],
        "instance": row["InstanceName"],
        "port": row["Port"],
        "database": row["DatabaseName"],
        "storenumber": row["StoreNumber"],
    }
    logger.info("Retrieved subscriber server for store=%s", store_number)
    return server


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
                    "server_ip": result.get("server_ip"),
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
    --WHERE myArticle.source_object LIKE '%PRICEDISCTABLE%'

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
def validate_store_number_task(**context) -> str:
    """Validate store_number from trigger conf or DAG params."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(EventType.VALIDATION, task_id, EventStatus.STARTED, {})

    store_number = _resolve_store_number(context)

    audit.log(
        EventType.VALIDATION,
        task_id,
        EventStatus.SUCCESS,
        {"store_number": store_number},
    )
    return store_number


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
def get_subscriber_server_task(store_number: str, **context) -> Dict[str, Any]:
    """
    Retrieve subscriber servers from Connection_Info and save to temp file.

    Returns only the file path to avoid XCom size limits.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(
        EventType.STORE_LIST_RETRIEVED,
        task_id,
        EventStatus.STARTED,
        {"store_number": store_number},
    )

    server = _get_subscriber_server(store_number)

    audit.log(
        EventType.STORE_LIST_RETRIEVED,
        task_id,
        EventStatus.SUCCESS,
        {"store_number": store_number, "server_ip": server["ip"]},
    )
    return server


@task
def reconcile_subscriber_task(
    server: Dict[str, Any],
    **context,
) -> List[Dict[str, Any]]:
    """
    Reconcile a chunk of subscribers using the generated script kept in memory.

    Regenerates the reconcile script from Replication_MD per chunk worker so the
    script is never written to disk or passed through XCom.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id
    store_number = server["storenumber"]

    try:
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.STARTED,
            {"store_number": store_number},
        )

        reconcile_script, _ = _generate_subscriber_reconcile_script()
        results = _reconcile_subscriber_chunk(
            [server],
            reconcile_script,
            audit,
            task_id,
        )

        inconsistent = sum(1 for result in results if result.get("has_inconsistency"))
        failed = sum(1 for result in results if not result.get("success"))
        chunk_details = {
            "store_number": store_number,
            "total": len(results),
            "inconsistent": inconsistent,
            "failed": failed,
            "successful": len(results) - failed,
        }

        if failed:
            failed_details = [
                {"store": r.get("store"), "error": r.get("error")}
                for r in results
                if not r.get("success")
            ]
            audit.log(
                EventType.CHUNK_PROCESSING,
                task_id,
                EventStatus.FAILED,
                chunk_details,
                error=str(failed_details),
            )
            raise AirflowFailException(
                f"Subscriber reconcile failed for {failed} store(s): {failed_details}"
            )

        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.SUCCESS,
            chunk_details,
        )
        return results

    except Exception as exc:
        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.FAILED,
            {"store_number": store_number},
            error=str(exc),
        )
        raise


@task
def trigger_sync_orchestrators_task(
    reconcile_results: List[Dict[str, Any]],
    **context,
) -> Dict[str, Any]:
    """
    Trigger sync orchestrators for stores with replication inconsistencies.

    Waits for all triggered downstream DAG runs to finish before completing.
    """
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    audit.log(EventType.SYNC_DAG_TRIGGERED, task_id, EventStatus.STARTED, {})

    store_dag_map: Dict[str, Set[str]] = {}

    table_sync_mapping = {
        _normalize_table_name(table): config
        for table, config in json.loads(TABLE_SYNC_DAG_MAPPING).items()
    }
    for result in reconcile_results:
        if not result.get("success") or not result.get("has_inconsistency"):
            continue

        store_number = result["store"]

        for diff_row in result.get("diff_rows", []):
            table_name = diff_row.get("TableName")
            diff = diff_row.get("Diff", 0)

            if diff <= 1:
                continue

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
            execution_mode="parallel",
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
    reconcile_results: List[Dict[str, Any]],
    trigger_summary: Dict[str, Any],
    **context,
) -> Dict[str, Any]:
    """Aggregate reconcile results and report inconsistencies and failures."""
    audit = AuditLogger(dag_id=context["dag"].dag_id, run_id=context["run_id"])
    task_id = context["task_instance"].task_id

    inconsistent_stores = [
        result["store"]
        for result in reconcile_results
        if result.get("success") and result.get("has_inconsistency")
    ]
    failed_stores = [result["store"] for result in reconcile_results if not result.get("success")]
    slow_stores = [
        {"store": result["store"], "duration_seconds": result.get("duration_seconds")}
        for result in reconcile_results
        if result.get("success")
        and result.get("duration_seconds", 0) > STORE_DURATION_THRESHOLD_SEC
    ]

    status = "completed"
    if failed_stores:
        status = "completed_with_errors"
    elif inconsistent_stores:
        status = "completed_with_inconsistencies"

    summary = {
        "total_servers": len(reconcile_results),
        "inconsistent": len(inconsistent_stores),
        "failed": len(failed_stores),
        "successful": len(reconcile_results) - len(failed_stores),
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
    dag_id="masterdata_replication_reconcile_and_sync_store",
    description="Reconcile master-data replication for a single store (store_number param), identify replication gaps, and trigger downstream synchronization workflows.",
    start_date=datetime(2026, 6, 13),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_masterdata_replication_reconcile_and_sync_store", default_var=4)),
    tags=["mssql", "store", "master-data", "reconcile"],
    default_args=default_args,
    params={
        "store_number": Param(
            default="OKS00000",
            type="string",
            minLength=8,
            maxLength=8,
            description="Enter the store number for reconciliation and sync",
            title="Store Number",
        ),
    },
) as dag:
    with TaskGroup(group_id="Validation") as validation:
        validate_replication_md = validate_replication_md_connection_task()
        validate_connection_info = validate_connection_info_task()
        validate_store_number = validate_store_number_task()
        [validate_replication_md, validate_connection_info, validate_store_number]

    with TaskGroup(group_id="setup") as setup:
        script_metadata = generate_reconcile_script_task()

    with TaskGroup(group_id="discovery") as discovery:
        subscriber_server = get_subscriber_server_task(validate_store_number)

    with TaskGroup(group_id="reconcile") as reconcile:
        reconcile_results = reconcile_subscriber_task(subscriber_server)

    with TaskGroup(group_id="sync_and_verify") as sync_and_verify:
        trigger_summary = trigger_sync_orchestrators_task(reconcile_results)
        verification = verify_reconcile_task(reconcile_results, trigger_summary)
        completion = dag_completion_task(verification)
        trigger_summary >> verification >> completion

    validation >> setup >> discovery >> reconcile >> sync_and_verify
