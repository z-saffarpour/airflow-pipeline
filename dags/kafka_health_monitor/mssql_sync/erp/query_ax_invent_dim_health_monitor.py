"""
Airflow DAG: dbo.INVENTDIM Pipeline Health Monitor
==================================================
Monitor dbo.INVENTDIM Kafka pipeline health.

Related sync DAG(s): query_ax_invent_dim_sync / query_ax_invent_dim_full_sync
Topic: ax.query.raw.dbo.inventdim

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
    dag_id="query_ax_invent_dim_health_monitor",
    description="Monitor dbo.INVENTDIM Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'ax', 'query'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="ax.query.raw.dbo.inventdim",
    consumer_group="clickhouse-consumer.ax.query.raw.dbo.inventdim",
    sample_count=5,
    max_lag_records=500000,
    max_lag_minutes=60,
    expected_daily_records=1000000,
    include_message_sampling=True,
)

conn_config = ConnectionConfig(kafka_conn_id='kafka_default')

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
