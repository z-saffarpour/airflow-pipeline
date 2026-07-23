"""
Airflow DAG: SQL Server Query to Kafka Pipeline (TEMPLATE)
DAG Template: SQL Server Query → Kafka Sync
===========================================================
This is a TEMPLATE DAG for executing SQL query results from SQL Server to Kafka.
Do NOT run this DAG directly - create specific DAGs using create_query_sync_dag().

Features:
- Execute custom SQL query (SELECT/CTE)
- Stream fetchmany batching for memory control
- Idempotent Kafka producer
- Reusable template via create_query_sync_dag()
- Execution date token replacement in query/params

Usage:
    from core.sqlserver_kafka_query_sync import QuerySyncConfig, create_query_sync_dag

    dag = create_query_sync_dag(QuerySyncConfig(
        dag_id="fact_sales_sync",
        mssql_conn_id="mssql_prod",
        kafka_conn_id="kafka_prod",
        kafka_topic="fact.sales",
        query="SELECT * FROM dbo.FactSales WHERE updated_at > :watermark",
    ))
    
Author: Senior Data Engineer
Version: 4.0
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.exceptions import AirflowException # type: ignore

from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaTopicConfig import KafkaTopicConfig
from pipeline.config.ClickHouseConfig import ClickHouseConfig
from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.SyncConfig import SyncConfig
from pipeline.config.QueryConfiguration import QueryConfiguration

from pipeline.core.MSSQLDataTransferOrchestrator import MSSQLDataTransferOrchestrator
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor
from pipeline.kafka.KafkaTopicManager import KafkaTopicManager
from pipeline.utils.validation import validate_kafka_conn, validate_mssql_conn, validate_clickhouse_conn

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# TASK FACTORIES
# ============================================================================

def make_validate_mssql_task(conn_config: ConnectionConfig):
    """Factory: validate SQL Server connection task."""
    @task(
        task_id="validate_mssql_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_mssql():
        return validate_mssql_conn(conn_config.mssql_conn_id)

    return validate_mssql

def make_validate_kafka_task(conn_config: ConnectionConfig):
    """Factory: validate Kafka connection task."""
    @task(
        task_id="validate_kafka_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_kafka():
        if not conn_config.kafka_conn_id:
            logger.info("Kafka connection validation skipped (no kafka_conn_id provided)")
            return {"status": "skipped"}
        
        return validate_kafka_conn(conn_config.kafka_conn_id)

    return validate_kafka

def make_validate_clickhouse_task(conn_config: ConnectionConfig):
    """Factory: validate ClickHouse connection task."""
    @task(
        task_id="validate_clickhouse_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_clickhouse_connection():
        if not conn_config.clickhouse_conn_id:
            logger.info("ClickHouse connection validation skipped (no clickhouse_conn_id provided)")
            return {"status": "skipped"}
        
        return validate_clickhouse_conn(conn_config.clickhouse_conn_id)

    return validate_clickhouse_connection

def make_ensure_topic_task(conn_config: ConnectionConfig, sync_config: SyncConfig, kafka_topic_config: KafkaTopicConfig):
    """Factory: ensure Kafka topic exists (idempotent create)."""
    @task(
        task_id="ensure_kafka_topic",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=1),
    )
    def ensure_kafka_topic(**context):
        if not conn_config.kafka_conn_id or not sync_config.is_send_kafka:
            logger.info("Kafka topic creation skipped (no kafka_conn_id provided)")
            return {"status": "skipped"}
        
        topic_manager = KafkaTopicManager(conn_config.kafka_conn_id)
        topic_manager.ensure_topic_exists(
            topic_name=kafka_topic_config.name,
            num_partitions=kafka_topic_config.num_partitions,
            replication_factor=kafka_topic_config.replication_factor,
        )
        return {
            "topic": kafka_topic_config.name,
            "partitions": kafka_topic_config.num_partitions,
            "conn_id": conn_config.kafka_conn_id,
        }

    return ensure_kafka_topic

def make_transfer_task(dag_config: DAGConfig, sync_config: SyncConfig, conn_config: ConnectionConfig, query_config: QueryConfiguration ,kafka_topic_config: KafkaTopicConfig, clickhouse_config:ClickHouseConfig):
    """Factory: transfer_query_to_kafka."""   
    @task(
        task_id="transfer_query_to_kafka",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=6),
        pool=dag_config.pool,
    )
    def transfer_query_to_kafka(**context) -> Dict[str, Any]:
        execution_date_key = ExecutionDateExtractor.get_date_key_from_context(
            context, sync_config.date_offset
        )
        execution_ds = ExecutionDateExtractor.get_date_from_context(
            context, sync_config.date_offset
        )

        resolved_config = query_config.with_resolved_params(execution_date_key, execution_ds)

        logger.info(f"Starting query transfer for date: {execution_ds}")
        if kafka_topic_config:
            logger.info(f"Source: {resolved_config.source_name} -> Topic: {kafka_topic_config.name}")

        orchestrator = MSSQLDataTransferOrchestrator(
            mssql_conn_id=conn_config.mssql_conn_id,
            kafka_conn_id=conn_config.kafka_conn_id,
            clickhouse_conn_id = conn_config.clickhouse_conn_id,
            is_send_kafka=sync_config.is_send_kafka,
            is_send_clickhouse=sync_config.is_send_clickhouse,
            fail_on_error=sync_config.fail_on_error,
        )

        result = orchestrator.transfer_query_data(
            config=resolved_config,
            execution_date=execution_date_key,
            batch_size=sync_config.batch_size,
            kafka_topic=kafka_topic_config.name if kafka_topic_config else 'None',
            clickhouse_database=clickhouse_config.database if clickhouse_config else 'None',
            clickhouse_table_name=clickhouse_config.table_name if clickhouse_config else 'None'
        )

        logger.info(result.get_summary())

        if result.is_failure:
            raise AirflowException(f"Query transfer failed: {result.error_message}")

        return {
            "status": "success",
            "execution_date": execution_date_key,
            "source_name": resolved_config.source_name,
            "total_records": result.records_transferred,
            "batch_count": result.batch_count,
            "duration_seconds": result.duration_seconds,
        }

    return transfer_query_to_kafka

def make_verify_task(query_config: QueryConfiguration):
    @task(
        task_id="verify_query_transfer",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def verify_query_transfer(**context) -> Dict[str, Any]:
        transfer_result = context["ti"].xcom_pull(
            task_ids="processing.transfer_query_to_kafka"
        )
        transferred_records = transfer_result.get("total_records", 0) if transfer_result else 0

        logger.info(f"Verifying transfer: {transferred_records} records transferred")
        logger.info(f"Minimum expected: {query_config.min_expected_records}")

        if transferred_records < query_config.min_expected_records:
            raise AirflowException(
                f"Verification failed: transferred={transferred_records} < "
                f"min_expected={query_config.min_expected_records}"
            )

        status_message = f"Transfer verified: {transferred_records} records"
        if query_config.min_expected_records > 0:
            status_message += f" (min: {query_config.min_expected_records})"

        logger.info(status_message)

        return {
            "status": "success",
            "transferred_records": transferred_records,
            "min_expected_records": query_config.min_expected_records,
            "message": status_message,
            "timestamp": datetime.now().isoformat(),
        }

    return verify_query_transfer

# ============================================================================
# DAG FACTORY
# ============================================================================

def create_query_sync_dag(dag_config: DAGConfig, sync_config: SyncConfig, conn_config: ConnectionConfig, query_config: QueryConfiguration ,kafka_topic_config: KafkaTopicConfig, clickhouse_config:ClickHouseConfig) -> DAG:
    """
    Build and return a fully configured Airflow DAG from a QuerySyncConfig.

    Args:
        config: QuerySyncConfig instance describing the sync job.

    Returns:
        Airflow DAG object ready to be registered.

    Example:
        dag = create_query_sync_dag(QuerySyncConfig(
            dag_id="fact_sales_sync",
            mssql_conn_id="mssql_prod",
            kafka_conn_id="kafka_prod",
            kafka_topic="fact.sales",
            query="SELECT * FROM dbo.FactSales",
        ))
    """
    if not conn_config.mssql_conn_id:
        raise ValueError(
            "ConnectionConfig.mssql_conn_id is required for create_query_sync_dag"
        )
    if sync_config.is_send_kafka and not conn_config.kafka_conn_id:
        raise ValueError(
            "ConnectionConfig.kafka_conn_id is required for create_query_sync_dag "
            "when SyncConfig.is_send_kafka is True"
        )
    if sync_config.is_send_clickhouse and not conn_config.clickhouse_conn_id:
        raise ValueError(
            "ConnectionConfig.clickhouse_conn_id is required for create_query_sync_dag "
            "when SyncConfig.is_send_clickhouse is True"
        )

    default_args = {
        "owner": dag_config.owner,
        "depends_on_past": dag_config.depends_on_past,
        "email_on_failure": True,
        "email_on_retry": False,
        "email": ["Saffarpour.Zahra@okco.ir"],
        "retries": dag_config.retries,
        "retry_delay": dag_config.retry_delay, #timedelta(minutes=5),
        "execution_timeout": dag_config.execution_timeout, #timedelta(hours=8),
        "pool": dag_config.pool,
        #"priority_weight": 10,
    }

    with DAG(
        dag_id=dag_config.dag_id,
        description=dag_config.description or f"Query sync: {kafka_topic_config.name if kafka_topic_config else '' }",
        schedule=dag_config.schedule,
        start_date=dag_config.start_date,
        catchup=dag_config.catchup,
        default_args=default_args,
        is_paused_upon_creation=dag_config.is_paused_upon_creation,
        max_active_runs=dag_config.max_active_runs,
        max_active_tasks=dag_config.max_active_tasks,
        tags=dag_config.tags,
        doc_md=__doc__,
    ) as dag:

        with TaskGroup(group_id="validation") as validation:
            validate_mssql = make_validate_mssql_task(conn_config=conn_config)()
            if sync_config.is_send_kafka:
                validate_kafka = make_validate_kafka_task(conn_config=conn_config)()
            if sync_config.is_send_clickhouse:
                validate_clickhouse = make_validate_clickhouse_task(conn_config=conn_config)()
        
            # Internal dependencies
            validate_mssql
            if sync_config.is_send_kafka:
                validate_kafka
            if sync_config.is_send_clickhouse:
                validate_clickhouse
            
        with TaskGroup(group_id="setup") as setup:
            topic_setup = make_ensure_topic_task(conn_config=conn_config, kafka_topic_config= kafka_topic_config, sync_config= sync_config)()

            # Internal dependencies
            topic_setup
            
        with TaskGroup("processing") as processing:
            transfer = make_transfer_task(dag_config=dag_config, 
                                          kafka_topic_config= kafka_topic_config,
                                          clickhouse_config= clickhouse_config,
                                          conn_config= conn_config,
                                          query_config= query_config,
                                          sync_config= sync_config)()
        
            # Internal dependencies
            transfer 
            
        with TaskGroup(group_id="verify_and_completion") as verify_completion:
            verification = make_verify_task(query_config= query_config)()
        
            # Internal dependencies
            verification
            
        validation >> setup >> processing >> verify_completion

    return dag
