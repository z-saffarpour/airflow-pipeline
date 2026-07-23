"""
Airflow DAG: MSSQL query → Kafka sync (example)
===============================================
Runs a custom SELECT/CTE on MSSQL and produces batches to a Kafka topic
(idempotent producer).

Uses mssql_to_kafka_clickhouse_sync_dag_factory.create_query_sync_dag.

Required Airflow connections:
  - mssql_default   (source)
  - kafka_default       (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import (
    DAGConfig,
    ConnectionConfig,
    KafkaTopicConfig,
    QueryConfiguration,
    SyncConfig,
)

from template.mssql_to_kafka_clickhouse_sync_dag_factory import create_query_sync_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="example_query_to_kafka_sync",
    description="Example: sync MSSQL query results to Kafka topic",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_example_query_kafka", default_var=1)
    ),
    retries=int(Variable.get("retries_example_query_kafka", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_example_query_kafka", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_example_query_kafka", default_var=8)
        )
    ),
    tags=["mssql", "kafka", "query", "example"],
    pool="mssql_to_kafka_clickhouse_sync_pool",
)

sync_config = SyncConfig(
    batch_size=int(Variable.get("batch_size_example_query_kafka", default_var=10000)),
    date_offset=-1,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_default"),
    kafka_conn_id=Variable.get("kafka_target_conn_id", default_var="kafka_default"),
)

# Replace SELECT / COUNT / key / params with your real MSSQL query.
# Use %s placeholders with query_params (e.g. "{{ ds_nodash }}").
query_config = QueryConfiguration(
    source_name="adhoc_query_example_table",
    query="""
            SELECT id, code, name, updated_at, business_date
            FROM dbo.ExampleTable WITH (READPAST)
            WHERE business_date = %s
          """,
    query_params=["{{ ds_nodash }}"],
    key_column="id",
    count_query="""
            SELECT COUNT(1) AS CNT
            FROM dbo.ExampleTable WITH (READPAST)
            WHERE business_date = %s
          """,
)

kafka_topic_config = KafkaTopicConfig(
    name=Variable.get(
        "kafka_topic_example_query",
        default_var="dwh.query.curated.example_table",
    ),
    num_partitions=int(
        Variable.get("kafka_partitions_example_query", default_var=3)
    ),
    replication_factor=int(
        Variable.get("kafka_replication_example_query", default_var=3)
    ),
)

clickhouse_config = None

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_query_sync_dag(
        dag_config=dag_config,
        sync_config=sync_config,
        conn_config=conn_config,
        query_config=query_config,
        kafka_topic_config=kafka_topic_config,
        clickhouse_config=clickhouse_config,
    )

dag