"""
Airflow DAG: RTL.DIM_SaleIsReturnSale Pipeline Health Monitor
=============================================================
Monitor RTL.DIM_SaleIsReturnSale Kafka pipeline health.

Related sync DAG(s): table_dwh_rtl_dim_sale_is_return_sale_sync
Topic: dwh.table.curated.rtl.dim_saleisreturnsale

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id="table_dwh_rtl_dim_sale_is_return_sale_health_monitor",
    description="Monitor RTL.DIM_SaleIsReturnSale Kafka pipeline health",
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
    kafka_topic="dwh.table.curated.rtl.dim_saleisreturnsale",
    consumer_group="clickhouse-consumer.dwh.table.curated.rtl.dim_saleisreturnsale",
    sample_count=5,
    max_lag_records=10000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)

# Create DAG from config
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
