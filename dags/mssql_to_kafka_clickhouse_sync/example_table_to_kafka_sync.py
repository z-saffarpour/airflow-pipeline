"""
Airflow DAG: MSSQL table → Kafka sync (example)
===============================================
Reads a full MSSQL table and produces batches to a Kafka topic
(idempotent producer).

Uses table_mssql_sync_dag_factory.create_table_sync_dag.

Required Airflow connections:
  - mssql_default   (source)
  - kafka_default       (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from pipeline.utils.parse_config import parse_int, parse_str

from pipeline.config import (
    DAGConfig,
    ConnectionConfig,
    KafkaTopicConfig,
    TableConfiguration,
    SyncConfig,
)

from template.table_mssql_sync_dag_factory import create_table_sync_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="example_table_to_kafka_sync",
    description="Example: sync MSSQL table to Kafka topic",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=parse_int("max_active_runs_example_table_kafka", 1),
    retries=parse_int("retries_example_table_kafka", 2),
    retry_delay=timedelta(
        minutes=parse_int("retry_delay_minutes_example_table_kafka", 5)
    ),
    execution_timeout=timedelta(
        hours=parse_int("execution_timeout_hours_example_table_kafka", 8)
    ),
    tags=["mssql", "kafka", "table", "example"],
    pool="mssql_to_kafka_clickhouse_sync_pool",
)

sync_config = SyncConfig(
    batch_size=parse_int("batch_size_example_table_kafka", 10000),
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id=parse_str("mssql_source_conn_id", "mssql_default"),
    kafka_conn_id=parse_str("kafka_target_conn_id", "kafka_default"),
)

# Replace table_name / columns / key with your real MSSQL source.
table_config = TableConfiguration(
    table_name="dbo.ExampleTable",
    date_column=None,
    date_column_type="int",
    primary_key_column="id",
    order_by_column="id",
    columns=("id", "code", "name", "updated_at"),
)

kafka_topic_config = KafkaTopicConfig(
    name=parse_str("kafka_topic_example_table", "dwh.table.curated.example_table"),
    num_partitions=parse_int("kafka_partitions_example_table", 3),
    replication_factor=parse_int("kafka_replication_example_table", 3),
)

clickhouse_config = None

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_table_sync_dag(
        dag_config=dag_config,
        sync_config=sync_config,
        conn_config=conn_config,
        table_config=table_config,
        kafka_topic_config=kafka_topic_config,
        clickhouse_config=clickhouse_config,
    )

dag
