"""
Kafka → MSSQL sync DAG factory.

Consumes JSON row messages from a Kafka topic and upserts into an MSSQL target
table, mirroring the MySQL/Mongo → MSSQL upsert pattern (MERGE/upsert,
optional parallel tasks per Kafka partition, optional scoped delete_missing).

Ops — partition pool sizing (one-time setup, only if use_dynamic_tasks=True):
    airflow pools set kafka_to_mssql_sync_pool 32 "Kafka to MSSQL partition sync"

The pool slot count must be >= ``max_global_parallel_chunks`` in ``KafkaSyncConfig``.
Only ``sync_kafka_to_mssql_chunk`` uses ``dag_config.pool``.
Light tasks (validation, create_chunks, report) use ``default_pool``.

Required Airflow connections:
    - kafka_*  : source (e.g. kafka_default)
    - mssql_*  : target (e.g. mssql_default)

Message contract:
    Value = JSON object (same as IdempotentKafkaProducer / MessageSerializer).
    Offsets commit after successful MERGE (at-least-once).
"""
import logging
from datetime import timedelta
from typing import Any, Dict, List

from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.exceptions import AirflowFailException, AirflowException  # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaSyncConfig import KafkaSyncConfig
from pipeline.config.AuditConfig import EventType, EventStatus
from pipeline.core.KafkaToMSSQLQueryOrchestrator import KafkaToMSSQLQueryOrchestrator
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor
from pipeline.core.exceptions import (
    is_transient_sql_server_error_message,
    raise_sync_task_error,
)
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.utils.validation import validate_mssql_conn, validate_kafka_conn

logger = logging.getLogger(__name__)

DURATION_THRESHOLD_SEC = 1200  # 20 minutes


# ============================================================================
# TASK FACTORIES
# ============================================================================

def make_validate_kafka_connection_task(conn_config: ConnectionConfig):
    @task(
        task_id="validate_kafka_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_kafka_connection(**context):
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        conn_id = conn_config.kafka_conn_id

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.STARTED,
            {"conn_id": conn_id, "type": "kafka"},
        )

        result = validate_kafka_conn(conn_id)

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.SUCCESS,
            {"conn_id": conn_id, "type": "kafka"},
        )
        return result

    return validate_kafka_connection


def make_validate_mssql_connection_task(conn_config: ConnectionConfig):
    @task(
        task_id="validate_mssql_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_mssql_connection(**context):
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        conn_id = conn_config.mssql_conn_id

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.STARTED,
            {"conn_id": conn_id, "type": "mssql"},
        )

        result = validate_mssql_conn(conn_id)

        audit.log(
            EventType.CONN_VALIDATED,
            task_id,
            EventStatus.SUCCESS,
            {"conn_id": conn_id, "type": "mssql"},
        )
        return result

    return validate_mssql_connection


def make_sync_kafka_to_mssql_task(
    dag_config: DAGConfig,
    sync_config: KafkaSyncConfig,
    conn_config: ConnectionConfig,
):
    @task(
        task_id="sync_kafka_to_mssql",
        retries=dag_config.sync_retries,
        retry_delay=dag_config.sync_retry_delay,
        execution_timeout=dag_config.execution_timeout,
        pool=dag_config.pool,
    )
    def sync_kafka_to_mssql(**context) -> Dict[str, Any]:
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        exec_date = ExecutionDateExtractor.get_date_from_context(context, 0)

        audit.log(
            EventType.KAFKA_CONSUME,
            task_id,
            EventStatus.STARTED,
            {
                "source": sync_config.source_name,
                "topic": sync_config.kafka_topic,
                "consumer_group": sync_config.consumer_group,
                "target": f"{sync_config.target_schema}.{sync_config.target_table}",
                "exec_date": exec_date,
            },
        )

        try:
            orchestrator = KafkaToMSSQLQueryOrchestrator(
                source_conn_id=conn_config.kafka_conn_id,
                target_conn_id=conn_config.mssql_conn_id,
                fail_on_error=True,
                batch_size=sync_config.batch_size,
            )
            result = orchestrator.sync_data(sync_config, exec_date)

            if not result.success:
                audit.log(
                    EventType.KAFKA_CONSUME,
                    task_id,
                    EventStatus.FAILED,
                    {"duration_seconds": result.duration_seconds},
                    error=result.error_message,
                )
                message = f"Kafka→MSSQL sync failed: {result.error_message}"
                if is_transient_sql_server_error_message(result.error_message or ""):
                    raise AirflowException(message)
                raise AirflowFailException(message)

            audit.log(
                EventType.KAFKA_CONSUME,
                task_id,
                EventStatus.SUCCESS,
                {
                    "records": result.records_transferred,
                    "inserted": result.inserted,
                    "updated": result.updated,
                    "deleted": result.deleted,
                    "duration_seconds": result.duration_seconds,
                },
            )
            logger.info(f"[kafka_to_mssql_sync_dag_factory.sync_kafka_to_mssql] Kafka→MSSQL sync completed: {result.records_transferred} rows in {result.duration_seconds:.2f}s")
            return {
                "success": True,
                "source": sync_config.source_name,
                "records": result.records_transferred,
                "inserted": result.inserted,
                "updated": result.updated,
                "deleted": result.deleted,
                "duration_seconds": result.duration_seconds,
            }
        except Exception as exc:
            audit.log(
                EventType.KAFKA_CONSUME,
                task_id,
                EventStatus.FAILED,
                {"source": sync_config.source_name},
                error=str(exc),
            )
            raise_sync_task_error(f"Kafka→MSSQL sync failed: {exc}", exc)

    return sync_kafka_to_mssql


def make_create_sync_chunks_task(
    sync_config: KafkaSyncConfig,
    conn_config: ConnectionConfig,
):
    @task(task_id="create_sync_chunks")
    def create_sync_chunks(**context) -> List[Dict[str, Any]]:
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id

        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.STARTED,
            {
                "topic": sync_config.kafka_topic,
                "consumer_group": sync_config.consumer_group,
            },
        )

        orchestrator = KafkaToMSSQLQueryOrchestrator(
            source_conn_id=conn_config.kafka_conn_id,
            target_conn_id=conn_config.mssql_conn_id,
            fail_on_error=True,
            batch_size=sync_config.batch_size,
        )
        chunks = orchestrator.plan_sync_chunks(sync_config)
        if not chunks:
            chunks = [{
                "chunk_no": 0,
                "partition_id": 0,
                "topic": sync_config.kafka_topic,
                "row_count": 0,
                "skip": True,
            }]

        audit.log(
            EventType.DATA_TRANSFER_ORCHESTRATOR,
            task_id,
            EventStatus.SUCCESS,
            {
                "chunk_count": len(chunks),
                "approx_lag": sum(chunk.get("row_count", 0) for chunk in chunks),
            },
        )
        logger.info(f"[kafka_to_mssql_sync_dag_factory.create_sync_chunks] Created {len(chunks)} partition sync chunks for topic={sync_config.kafka_topic} table={sync_config.target_table}")
        return chunks

    return create_sync_chunks


def make_sync_kafka_to_mssql_chunk_task(
    dag_config: DAGConfig,
    sync_config: KafkaSyncConfig,
    conn_config: ConnectionConfig,
    chunk_pool: str,
):
    @task(
        task_id="sync_kafka_to_mssql_chunk",
        retries=dag_config.sync_retries,
        retry_delay=dag_config.sync_retry_delay,
        execution_timeout=dag_config.execution_timeout,
        max_active_tis_per_dagrun=sync_config.max_parallel_chunks,
        max_active_tis_per_dag=sync_config.max_global_parallel_chunks,
        pool=chunk_pool,
    )
    def sync_kafka_to_mssql_chunk(
        chunk: Dict[str, Any],
        **context,
    ) -> Dict[str, Any]:
        audit = AuditLogger(
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
        )
        task_id = context["task_instance"].task_id
        exec_date = ExecutionDateExtractor.get_date_from_context(context, 0)
        chunk_no = chunk["chunk_no"]

        audit.log(
            EventType.CHUNK_PROCESSING,
            task_id,
            EventStatus.STARTED,
            {
                "chunk_no": chunk_no,
                "partition_id": chunk.get("partition_id"),
                "row_count": chunk.get("row_count"),
            },
        )

        try:
            orchestrator = KafkaToMSSQLQueryOrchestrator(
                source_conn_id=conn_config.kafka_conn_id,
                target_conn_id=conn_config.mssql_conn_id,
                fail_on_error=True,
                batch_size=sync_config.batch_size,
            )
            result = orchestrator.sync_data_chunk(sync_config, chunk, exec_date)
            is_slow = result.duration_seconds > DURATION_THRESHOLD_SEC

            if not result.success:
                audit.log(
                    EventType.CHUNK_PROCESSING,
                    task_id,
                    EventStatus.FAILED,
                    {"chunk_no": chunk_no, "duration_seconds": result.duration_seconds},
                    error=result.error_message,
                )
                message = (
                    f"Chunk sync failed for chunk={chunk_no}: {result.error_message}"
                )
                if is_transient_sql_server_error_message(result.error_message or ""):
                    raise AirflowException(message)
                raise AirflowFailException(message)

            status = EventStatus.WARNING if is_slow else EventStatus.SUCCESS
            audit.log(
                EventType.CHUNK_PROCESSING,
                task_id,
                status,
                {
                    "chunk_no": chunk_no,
                    "records": result.records_transferred,
                    "duration_seconds": result.duration_seconds,
                },
            )
            return {
                "success": True,
                "source": sync_config.source_name,
                "chunk_no": chunk_no,
                "partition_id": chunk.get("partition_id"),
                "records": result.records_transferred,
                "inserted": result.inserted,
                "updated": result.updated,
                "deleted": result.deleted,
                "duration_seconds": result.duration_seconds,
                "is_slow": is_slow,
            }
        except Exception as exc:
            audit.log(
                EventType.CHUNK_PROCESSING,
                task_id,
                EventStatus.FAILED,
                {"chunk_no": chunk_no},
                error=str(exc),
            )
            raise_sync_task_error(
                f"Chunk sync failed for chunk={chunk_no}: {exc}",
                exc,
            )

    return sync_kafka_to_mssql_chunk


def _normalize_sync_results(results: Any) -> List[Dict[str, Any]]:
    if isinstance(results, dict):
        return [results]
    try:
        return list(results)
    except TypeError as exc:
        raise TypeError(
            f"Expected sync result dict or sequence, got {type(results).__name__}"
        ) from exc


def make_report_sync_metrics_task():
    @task(task_id="report_sync_metrics")
    def report_sync_metrics(results: Any) -> Dict[str, Any]:
        result_rows = _normalize_sync_results(results)

        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        total_records = 0
        total_duration = 0.0

        logger.info("[kafka_to_mssql_sync_dag_factory.report_sync_metrics] ----- Kafka → MSSQL Sync Metrics -----")
        for row in result_rows:
            if not row or not row.get("success"):
                continue
            inserted = row.get("inserted", 0)
            updated = row.get("updated", 0)
            deleted = row.get("deleted", 0)
            records = row.get("records", 0)
            duration = row.get("duration_seconds", 0)

            total_inserted += inserted
            total_updated += updated
            total_deleted += deleted
            total_records += records
            total_duration += duration

            logger.info(f'[kafka_to_mssql_sync_dag_factory.report_sync_metrics] chunk/source={row.get("chunk_no", row.get("source"))} partition={row.get("partition_id")} -> inserted={inserted}, updated={updated}, deleted={deleted}, records={records}, duration={duration:.2f}s')

        logger.info(f"[kafka_to_mssql_sync_dag_factory.report_sync_metrics] TOTAL -> inserted={total_inserted}, updated={total_updated}, deleted={total_deleted}, records={total_records}, duration={total_duration:.2f}s")
        return {
            "inserted": total_inserted,
            "updated": total_updated,
            "deleted": total_deleted,
            "records": total_records,
            "duration_seconds": total_duration,
        }

    return report_sync_metrics


# ============================================================================
# DAG DEFINITION
# ============================================================================

def create_dag(
    dag_config: DAGConfig,
    sync_config: KafkaSyncConfig,
    conn_config: ConnectionConfig,
) -> DAG:
    """
    Create a Kafka → MSSQL sync DAG.

    Args:
        dag_config: Airflow DAG settings
        sync_config: Kafka topic + MSSQL upsert settings (KafkaSyncConfig)
        conn_config: Must set kafka_conn_id (source) and mssql_conn_id (target)
    """
    if not conn_config.kafka_conn_id:
        raise ValueError("conn_config.kafka_conn_id is required for Kafka→MSSQL sync")
    if not conn_config.mssql_conn_id:
        raise ValueError("conn_config.mssql_conn_id is required for Kafka→MSSQL sync")

    default_args = {
        "owner": dag_config.owner,
        "depends_on_past": dag_config.depends_on_past,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": dag_config.retries,
        "retry_delay": dag_config.retry_delay,
        "execution_timeout": dag_config.execution_timeout,
        "pool": "default_pool",
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
        dag_id=dag_config.dag_id,
        description=dag_config.description
        or "Sync Kafka topic data to MSSQL target (upsert)",
        start_date=dag_config.start_date,
        schedule=dag_config.schedule,
        catchup=dag_config.catchup,
        max_active_runs=dag_config.max_active_runs,
        max_active_tasks=max_active_tasks,
        tags=dag_config.tags,
        default_args=default_args,
        is_paused_upon_creation=dag_config.is_paused_upon_creation,
        doc_md=__doc__,
    ) as dag:

        with TaskGroup(group_id="validation") as validation:
            validate_kafka = make_validate_kafka_connection_task(conn_config)()
            validate_mssql = make_validate_mssql_connection_task(conn_config)()
            [validate_kafka, validate_mssql]

        with TaskGroup(group_id="processing") as processing:
            report_sync_metrics_task = make_report_sync_metrics_task()

            if sync_config.use_dynamic_tasks:
                create_sync_chunks_task = make_create_sync_chunks_task(
                    sync_config=sync_config,
                    conn_config=conn_config,
                )
                sync_chunk_task = make_sync_kafka_to_mssql_chunk_task(
                    dag_config=dag_config,
                    sync_config=sync_config,
                    conn_config=conn_config,
                    chunk_pool=dag_config.pool,
                )

                sync_chunks = create_sync_chunks_task()
                sync_results = sync_chunk_task.expand(chunk=sync_chunks)
                report = report_sync_metrics_task(sync_results)
                sync_chunks >> sync_results >> report
            else:
                sync_task = make_sync_kafka_to_mssql_task(
                    dag_config=dag_config,
                    sync_config=sync_config,
                    conn_config=conn_config,
                )
                sync_result = sync_task()
                report = report_sync_metrics_task(sync_result)
                sync_result >> report

        validation >> processing

    return dag
