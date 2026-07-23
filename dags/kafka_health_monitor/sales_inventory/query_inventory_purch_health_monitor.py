"""
Airflow DAG: purchase quantity Pipeline Health Monitor
======================================================
Monitor purchase quantity Kafka pipeline health.

Related sync DAG(s): query_inventory_purch_sync
Topic: ax.query.raw.inventory.purch

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="query_inventory_purch_health_monitor",
    description="Monitor purchase quantity Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'sales_inventory', 'purch', 'ax'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="ax.query.raw.inventory.purch",
    consumer_group="clickhouse-consumer.ax.query.raw.inventory.purch",
    sample_count=5,
    max_lag_records=200000,
    max_lag_minutes=60,
    expected_daily_records=500000,
    include_message_sampling=True,
)

conn_config = ConnectionConfig(kafka_conn_id='kafka_default')

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
