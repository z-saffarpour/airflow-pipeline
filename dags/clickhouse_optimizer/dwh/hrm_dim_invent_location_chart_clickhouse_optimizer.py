"""
Airflow DAG: HRM.DIM_InventLocationChart ClickHouse Optimization Pipeline
==========================================================
This DAG runs OPTIMIZE TABLE on ClickHouse for DIM_InventLocationChart table after data is 
transferred from Kafka.

Features:
- Full table optimization (no partition - dimension table)
- FINAL merge support
- Deduplication support
- Health check before/after optimization
- Configurable parameters

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig

from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id='hrm_dim_invent_location_chart_clickhouse_optimizer',
    description = 'Optimize HRM.DIM_InventLocationChart table in ClickHouse after Kafka data ingestion',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 3, 24),
    schedule = '0 2 * * *', # Every day at 2 AM (after data transfer at 11 PM)    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['clickhouse', 'optimization', 'DWH' , 'dimension', 'HRM'],
)

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name = 'cluster_2S_2R',
    database = 'HRM',
    table_name = 'Local_DIM_InventLocationChart',
    partition_column = None,
    partition_format = 'YYYYMM',
    final = True,
    deduplicate = True
)

# Connections
CLICKHOUSE_CONN= 'clickhouse_default'

# Create DAG from config
clickhouse_optimizer_dag(DAG_CONFIG, CLICKHOUSE_CONN, OPTIMIZE_CONFIG)