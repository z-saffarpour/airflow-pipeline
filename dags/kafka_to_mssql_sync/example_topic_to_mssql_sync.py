"""
Airflow DAG: Kafka → MSSQL topic sync (example)
================================================
Consumes JSON messages from a Kafka topic and upserts into MSSQL
(MERGE).

Uses kafka_to_mssql_sync_dag_factory.

Required Airflow connections:
  - kafka_default       (source)
  - mssql_default   (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.KafkaSyncConfig import KafkaSyncConfig

from template.kafka_to_mssql_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="kafka_example_topic_to_mssql_sync",
    description="Example: sync Kafka topic to MSSQL (upsert)",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_kafka_example_topic", default_var=1)
    ),
    retries=int(Variable.get("retries_kafka_example_topic", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_kafka_example_topic", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_kafka_example_topic", default_var=8)
        )
    ),
    tags=["kafka", "mssql", "kafka-sync"],
    pool="kafka_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    kafka_conn_id=Variable.get("kafka_source_conn_id", default_var="kafka_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_default"),
)

# Replace topic / keys / target with your real Kafka source and MSSQL target.
# JSON field names (after exclude_columns) must match MSSQL destination columns.
sync_config = KafkaSyncConfig(
    source_name="kafka_example_topic",
    kafka_topic=Variable.get(
        "kafka_example_topic_name",
        default_var="dwh.table.curated.example",
    ),
    consumer_group=Variable.get(
        "kafka_example_consumer_group",
        default_var="mssql-sync-example-topic",
    ),
    primary_keys=("id",),
    target_schema="dbo",
    target_table="ExampleFromKafka",
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    # Strip producer metadata injected by IdempotentKafkaProducer
    exclude_columns=("version_id",),
    # Optional: keep only these columns from the JSON payload
    # value_columns=("id", "code", "name", "updated_at"),
    auto_offset_reset="earliest",
    max_idle_polls=int(
        Variable.get("max_idle_polls_kafka_example_topic", default_var=10)
    ),
    delete_missing=bool(
        int(Variable.get("delete_missing_kafka_example_topic", default_var=0))
    ),
    delete_scope_column="id",
    use_hash_change_detection=True,
    # For high-partition topics enable parallel consume:
    # use_dynamic_tasks=True,
    # max_parallel_chunks=8,
    batch_size=int(
        Variable.get("batch_size_kafka_example_topic", default_var=10000)
    ),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)

dag
