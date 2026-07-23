"""
Airflow DAG: RTL.DIM_SystemType Pipeline Health Monitor
=======================================================
Monitor RTL.DIM_SystemType Kafka pipeline health.

Related sync DAG(s): table_dwh_rtl_dim_system_type_sync
Topic: dwh.table.curated.rtl.dim_systemtype

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="table_dwh_rtl_dim_system_type_health_monitor",
    description="Monitor RTL.DIM_SystemType Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'DWH', 'dimension', 'RTL'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
    kafka_topic="dwh.table.curated.rtl.dim_systemtype",
    consumer_group="clickhouse-consumer.dwh.table.curated.rtl.dim_systemtype",
    sample_count=5,
    max_lag_records=10000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
