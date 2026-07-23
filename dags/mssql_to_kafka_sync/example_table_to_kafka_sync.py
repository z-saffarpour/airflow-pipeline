"""
Airflow DAG: MSSQL → Kafka table sync (example)
===============================================
Reads from an MSSQL table/query and produces batches to a Kafka topic
(idempotent producer + version_id).

Uses mssql_to_kafka_sync_dag_factory.

Required Airflow connections:
  - mssql_default   (source)
  - kafka_default       (target)  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig
from pipeline.config.MSSQLToKafkaSyncConfig import MSSQLToKafkaSyncConfig

from template.mssql_to_kafka_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="mssql_example_table_to_kafka_sync",
    description="Example: sync MSSQL table to Kafka topic",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_mssql_example_table_kafka", default_var=1)
    ),
    retries=int(Variable.get("retries_mssql_example_table_kafka", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get(
                "retry_delay_minutes_mssql_example_table_kafka", default_var=5
            )
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get(
                "execution_timeout_hours_mssql_example_table_kafka", default_var=8
            )
        )
    ),
    tags=["mssql", "kafka", "sync", "mssql-to-kafka"],
    pool="mssql_to_kafka_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_default"),
    kafka_conn_id=Variable.get("kafka_target_conn_id", default_var="kafka_default"),
)

kafka_topic_config = KafkaTopicConfig(
    name=Variable.get(
        "kafka_topic_mssql_example_table",
        default_var="dwh.table.curated.example_table",
    ),
    num_partitions=int(
        Variable.get("kafka_partitions_mssql_example_table", default_var=3)
    ),
    replication_factor=int(
        Variable.get("kafka_replication_mssql_example_table", default_var=3)
    ),
)

# Replace SELECT / COUNT / key / topic with your real MSSQL source and Kafka topic.
sync_config = MSSQLToKafkaSyncConfig(
    source_name="mssql_example_table",
    source_query="""
            SELECT id, code, name, updated_at
            FROM dbo.ExampleTable
          """,
    source_query_count="""
            SELECT COUNT(1) AS CNT
            FROM dbo.ExampleTable
          """,
    key_column="id",
    primary_keys=("id",),
    # For large tables enable chunking (SQL Server NTILE):
    # use_dynamic_tasks=True,
    # chunk_column="id",
    # task_chunk_size=100_000,
    batch_size=int(
        Variable.get("batch_size_mssql_example_table_kafka", default_var=10000)
    ),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    kafka_topic_config=kafka_topic_config,
)

dag
