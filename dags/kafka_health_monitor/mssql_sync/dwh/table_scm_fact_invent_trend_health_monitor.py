"""
Airflow DAG: SCM.Fact_InventTrend Pipeline Health Monitor
=========================================================
Monitor SCM.Fact_InventTrend Kafka pipeline health.

Related sync DAG(s): table_dwh_scm_fact_invent_trend_sync
Topic: dwh.table.curated.scm.fact_inventtrend

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="table_dwh_scm_fact_invent_trend_health_monitor",
    description="Monitor SCM.Fact_InventTrend Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'DWH', 'fact', 'SCM'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
    kafka_topic="dwh.table.curated.scm.fact_inventtrend",
    consumer_group="clickhouse-consumer.dwh.table.curated.scm.fact_inventtrend",
    sample_count=5,
    max_lag_records=500000,
    max_lag_minutes=60,
    expected_daily_records=1000000,
    include_message_sampling=True,
)

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
