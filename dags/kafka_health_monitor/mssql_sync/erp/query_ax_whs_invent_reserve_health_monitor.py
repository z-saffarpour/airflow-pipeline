"""
Airflow DAG: dbo.WHSINVENTRESERVE Pipeline Health Monitor
=========================================================
Monitor dbo.WHSINVENTRESERVE Kafka pipeline health.

Related sync DAG(s): query_ax_whs_invent_reserve_full_sync
Topic: ax.query.raw.dbo.whsinventreserve

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="query_ax_whs_invent_reserve_health_monitor",
    description="Monitor dbo.WHSINVENTRESERVE Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'ax', 'query'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
    kafka_topic="ax.query.raw.dbo.whsinventreserve",
    consumer_group="clickhouse-consumer.ax.query.raw.dbo.whsinventreserve",
    sample_count=5,
    max_lag_records=500000,
    max_lag_minutes=60,
    expected_daily_records=500000,
    include_message_sampling=True,
)

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
