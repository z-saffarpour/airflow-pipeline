"""
Airflow DAG: COM.DIM_Time Pipeline Health Monitor
=================================================
Monitor COM.DIM_Time Kafka pipeline health.

Related sync DAG(s): table_dwh_com_dim_time_sync
Topic: dwh.table.curated.com.dim_time

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
    dag_id="table_dwh_com_dim_time_health_monitor",
    description="Monitor COM.DIM_Time Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'DWH', 'dimension', 'COM'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="dwh.table.curated.com.dim_time",
    consumer_group="clickhouse-consumer.dwh.table.curated.com.dim_time",
    sample_count=5,
    max_lag_records=10000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)

conn_config = ConnectionConfig(kafka_conn_id='kafka_default')

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
