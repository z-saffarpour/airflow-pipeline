"""
Airflow DAG: Kafka topic health monitor (example)
=================================================
Checks cluster/topic health, consumer lag, and optional message sampling
for a Kafka pipeline.

Uses kafka_health_monitor_dag_factory.kafka_health_monitor_dag.

Required Airflow connections:
  - kafka_default  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig

from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="example_topic_health_monitor",
    description="Example: monitor Kafka topic / consumer-group health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_example_kafka_health", default_var=1)
    ),
    retries=int(Variable.get("retries_example_kafka_health", default_var=1)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_example_kafka_health", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_example_kafka_health", default_var=1)
        )
    ),
    tags=["monitoring", "health-check", "kafka", "example"],
    pool="kafka_health_monitor_pool",
)

# Replace topic / consumer_group / thresholds with your real pipeline.
health_config = KafkaHealthMonitorConfig(
    kafka_topic=Variable.get(
        "kafka_topic_example_health",
        default_var="dwh.table.curated.example_table",
    ),
    consumer_group=Variable.get(
        "kafka_consumer_group_example_health",
        default_var="clickhouse-consumer.dwh.table.curated.example_table",
    ),
    sample_count=int(
        Variable.get("sample_count_example_kafka_health", default_var=5)
    ),
    max_lag_records=int(
        Variable.get("max_lag_records_example_kafka_health", default_var=10000)
    ),
    max_lag_minutes=int(
        Variable.get("max_lag_minutes_example_kafka_health", default_var=15)
    ),
    expected_daily_records=int(
        Variable.get("expected_daily_records_example_kafka_health", default_var=0)
    ),
    include_message_sampling=True,
)

conn_config = ConnectionConfig(
    kafka_conn_id=Variable.get("kafka_conn_id", default_var="kafka_default"),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = kafka_health_monitor_dag(dag_config, conn_config, health_config)

dag