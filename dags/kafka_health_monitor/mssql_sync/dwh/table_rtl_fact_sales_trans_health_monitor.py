"""
Airflow DAG: RTL.Fact_SalesTrans (table sync) Pipeline Health Monitor
=====================================================================
Monitor RTL.Fact_SalesTrans (table sync) Kafka pipeline health.

Related sync DAG(s): table_dwh_rtl_fact_sales_trans_sync
Topic: dwh.table.curated.rtl.fact_salestrans

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
    dag_id="table_dwh_rtl_fact_sales_trans_health_monitor",
    description="Monitor RTL.Fact_SalesTrans (table sync) Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule='0 */4 * * *',
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=['monitoring', 'health-check', 'kafka', 'mssql', 'DWH', 'fact', 'RTL'],
)

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="dwh.table.curated.rtl.fact_salestrans",
    consumer_group="clickhouse-consumer.dwh.table.curated.rtl.fact_salestrans",
    sample_count=5,
    max_lag_records=500000,
    max_lag_minutes=60,
    expected_daily_records=5000000,
    include_message_sampling=True,
)

conn_config = ConnectionConfig(kafka_conn_id='kafka_default')

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
