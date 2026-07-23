"""
Airflow DAG: store retail sales Pipeline Health Monitor
=======================================================
Monitor store retail sales Kafka pipeline health.

Related sync DAG(s): query_inventory_sales_retail_sync / query_inventory_sales_retail_v01_sync
Topic: store.query.raw.inventory.sales_retail

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="query_inventory_sales_retail_health_monitor",
    description="Monitor store retail sales Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'sales_inventory', 'retail'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
    kafka_topic="store.query.raw.inventory.sales_retail",
    consumer_group="clickhouse-consumer.store.query.raw.inventory.sales_retail",
    sample_count=5,
    max_lag_records=500000,
    max_lag_minutes=60,
    expected_daily_records=2000000,
    include_message_sampling=True,
)

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
