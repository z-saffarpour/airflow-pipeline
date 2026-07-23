"""
Airflow DAG: SQL Server Table to Kafka Pipeline (TEMPLATE)
==========================================================
This is a TEMPLATE DAG for full/incremental table sync from SQL Server to Kafka.
Do NOT run this DAG directly - create specific DAGs using create_table_sync_dag().

Features:
- Incremental loading based on execution_date
- Chunking/Batching for memory management
- Exactly-once semantics with idempotent producer
- Configurable date column (int YYYYMMDD or DATE/DATETIME)
- Record count verification

Usage:
    from core.sqlserver_kafka_table_sync import TableSyncConfig, create_table_sync_dag

    dag = create_table_sync_dag(TableSyncConfig(
        dag_id="dim_customer_sync",
        mssql_conn_id="mssql_prod",
        kafka_conn_id="kafka_prod",
        kafka_topic="dim.customer",
        schema="dbo",
        table="DimCustomer",
    ))
    
Author: Senior Data Engineer
Version: 3.0
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
from pipeline.config.TableConfiguration import TableConfiguration

from pipeline.core.MSSQLDataTransferOrchestrator import MSSQLDataTransferOrchestrator
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder
from pipeline.database.SafeMsSqlHook import SafeMsSqlHook
from pipeline.kafka.KafkaTopicManager import KafkaTopicManager
from pipeline.utils.validation import validate_kafka_conn, validate_mssql_conn, validate_clickhouse_conn
from pipeline.utils.kafka_utils import get_kafka_brokers

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
    def validate_mssql_connection():
        return validate_mssql_conn(conn_config.mssql_conn_id)

    return validate_mssql_connection

def make_validate_kafka_task(conn_config: ConnectionConfig):
    """Factory: validate Kafka connection task."""
    @task(
        task_id="validate_kafka_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_kafka_connection():
        if not conn_config.kafka_conn_id:
            logger.info("Kafka connection validation skipped (no kafka_conn_id provided)")
            return {"status": "skipped"}
        
        return validate_kafka_conn(conn_config.kafka_conn_id)

    return validate_kafka_connection

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
    def ensure_kafka_topic():
        if not conn_config.kafka_conn_id or not sync_config.is_send_kafka:
            logger.info("Kafka topic creation skipped (no kafka_conn_id provided)")
            return {"status": "skipped"}
        
        kafka_brokers = get_kafka_brokers(conn_config.kafka_conn_id)
        topic_manager = KafkaTopicManager(kafka_brokers)
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
 
def make_transfer_task(dag_config: DAGConfig, sync_config: SyncConfig, conn_config: ConnectionConfig, table_config: TableConfiguration ,kafka_topic_config: KafkaTopicConfig, clickhouse_config:ClickHouseConfig):
    """Factory: make_transfer_task."""
    @task(
        task_id="transfer_data_to_kafka",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=6),
        pool=dag_config.pool,
    )
    def transfer_data_to_kafka(**context) -> Dict[str, Any]:
        execution_date_key = ExecutionDateExtractor.get_date_key_from_context(
            context, sync_config.date_offset
        )
        execution_ds = ExecutionDateExtractor.get_date_from_context(
            context, sync_config.date_offset
        )
        if table_config.date_column_type == "int":
            execution_date = execution_date_key
        else:
            execution_date = execution_ds
        if not conn_config.kafka_conn_id or not sync_config.is_send_kafka:
            kafka_brokers = None
        else:
            kafka_brokers = get_kafka_brokers(conn_config.kafka_conn_id)

        logger.info(f"Starting data transfer for date: {execution_date}")
        
        if kafka_topic_config:
            logger.info(f"Table: {table_config.table_name} -> Topic: {kafka_topic_config.name}")

        orchestrator = MSSQLDataTransferOrchestrator(
            mssql_conn_id=conn_config.mssql_conn_id,
            kafka_bootstrap_servers=kafka_brokers,
            clickhouse_conn_id = conn_config.clickhouse_conn_id,
            is_send_kafka=sync_config.is_send_kafka,
            is_send_clickhouse=sync_config.is_send_clickhouse,
            fail_on_error=sync_config.fail_on_error,
        )

        result = orchestrator.transfer_table_data(
            config=table_config,
            execution_date=execution_date,
            batch_size=sync_config.batch_size,
            kafka_topic=kafka_topic_config.name if kafka_topic_config else 'None',
            clickhouse_database=clickhouse_config.database if clickhouse_config else 'None',
            clickhouse_table_name=clickhouse_config.table_name if clickhouse_config else 'None'
        )

        logger.info(result.get_summary())

        if result.is_failure:
            raise AirflowException(f"Data transfer failed: {result.error_message}")

        return {
            "status": "success",
            "execution_date": execution_date,
            "source_name": table_config.table_name,
            "total_records": result.records_transferred,
            "batch_count": result.batch_count,
            "duration_seconds": result.duration_seconds,
        }

    return transfer_data_to_kafka

def make_verify_task(sync_config: SyncConfig, conn_config: ConnectionConfig, table_config: TableConfiguration ):
    @task(
        task_id="verify_transfer",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def verify_transfer(**context) -> Dict[str, Any]:
        execution_date_key = ExecutionDateExtractor.get_date_key_from_context(
            context, sync_config.date_offset
        )
        execution_ds = ExecutionDateExtractor.get_date_from_context(
            context, sync_config.date_offset
        )
        if table_config.date_column_type == "int":
            execution_date = execution_date_key
        else:
            execution_date = execution_ds
        transfer_result = context["ti"].xcom_pull(
            task_ids="processing.transfer_data_to_kafka"
        )
        transferred_count = transfer_result.get("total_records", 0) if transfer_result else 0

        logger.info(f"Starting verification for date: {execution_date}")
        logger.info(f"Transferred records: {transferred_count}")

        try:
            hook = SafeMsSqlHook(mssql_conn_id=conn_config.mssql_conn_id)
            query, query_params = SQLQueryBuilder.build_count_query(
                table_name=table_config.table_name,
                date_column=table_config.date_column,
                date_key=execution_date if table_config.date_column else None,
                date_column_type=table_config.date_column_type,
            )

            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, query_params)
                    result = cursor.fetchone()
                    source_count = result[0] if result else 0

            logger.info(f"Source count: {source_count} | Transferred: {transferred_count}")

            if source_count == transferred_count:
                verification_status = "success"
                status_message = f"Record counts match: {source_count}"
            else:
                diff = abs(source_count - transferred_count)
                diff_pct = (diff / source_count * 100) if source_count > 0 else 0

                if diff_pct < 0.1:
                    verification_status = "success_with_warning"
                    status_message = f"Minor mismatch: {diff} records ({diff_pct:.2f}%)"
                    logger.warning(status_message)
                else:
                    raise AirflowException(
                        f"Verification failed: source={source_count}, "
                        f"transferred={transferred_count}, diff={diff} ({diff_pct:.2f}%)"
                    )

            return {
                "status": verification_status,
                "execution_date": execution_date,
                "source_count": source_count,
                "transferred_count": transferred_count,
                "difference": abs(source_count - transferred_count),
                "message": status_message,
                "timestamp": datetime.now().isoformat(),
            }

        except AirflowException:
            raise
        except Exception as e:
            logger.error("Verification error", exc_info=True)
            raise AirflowException(f"Verification error: {e}")

    return verify_transfer

# ============================================================================
# DAG FACTORY
# ============================================================================

def create_table_sync_dag(dag_config: DAGConfig, sync_config: SyncConfig, conn_config: ConnectionConfig, table_config: TableConfiguration ,kafka_topic_config: KafkaTopicConfig, clickhouse_config:ClickHouseConfig):
    """
    Build and return a fully configured Airflow DAG from a TableSyncConfig.

    Args:
        config: TableSyncConfig instance describing the sync job.

    Returns:
        Airflow DAG object ready to be registered.

    Example:
        dag = create_table_sync_dag(TableSyncConfig(
            dag_id="dim_customer_sync",
            mssql_conn_id="mssql_prod",
            kafka_conn_id="kafka_prod",
            kafka_topic="dim.customer",
            schema="dbo",
            table="DimCustomer",
        ))
    """
    if not conn_config.mssql_conn_id:
        raise ValueError(
            "ConnectionConfig.mssql_conn_id is required for create_table_sync_dag"
        )
    if sync_config.is_send_kafka and not conn_config.kafka_conn_id:
        raise ValueError(
            "ConnectionConfig.kafka_conn_id is required for create_table_sync_dag "
            "when SyncConfig.is_send_kafka is True"
        )
    if sync_config.is_send_clickhouse and not conn_config.clickhouse_conn_id:
        raise ValueError(
            "ConnectionConfig.clickhouse_conn_id is required for create_table_sync_dag "
            "when SyncConfig.is_send_clickhouse is True"
        )

    default_args = {
        "owner": dag_config.owner,
        "depends_on_past": dag_config.depends_on_past,
        "email_on_failure": True,
        "email_on_retry": False,
        "email": ["Saffarpour.Zahra@okco.ir"],
        "retries": dag_config.retries,
        "retry_delay": dag_config.retry_delay,
        "execution_timeout": dag_config.execution_timeout, #timedelta(hours=8),
        "pool": dag_config.pool,
        #"priority_weight": 10,
    }
 
    with DAG(
        dag_id=dag_config.dag_id,
        description=dag_config.description or f"Table sync: {table_config.table_name} → {kafka_topic_config.name if kafka_topic_config else ''}",
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
            topic_setup = make_ensure_topic_task(sync_config= sync_config, conn_config= conn_config, kafka_topic_config= kafka_topic_config)()

            # Internal dependencies
            topic_setup
            
        with TaskGroup("processing") as processing:
            transfer = make_transfer_task(dag_config= dag_config,
                                          clickhouse_config=clickhouse_config,
                                          conn_config= conn_config,
                                          kafka_topic_config= kafka_topic_config,
                                          sync_config= sync_config,
                                          table_config= table_config)()
        
            # Internal dependencies
            transfer 
            
        with TaskGroup(group_id="verify_and_completion") as verify_completion:
            verification = make_verify_task(table_config=table_config, conn_config=conn_config, sync_config= sync_config)()
        
            # Internal dependencies
            verification
            
        validation >> setup >> processing >> verify_completion

    return dag