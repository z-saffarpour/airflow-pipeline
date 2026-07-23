"""
Airflow DAG: COM.Dim_Date ClickHouse Optimization Pipeline
==========================================================
This DAG runs OPTIMIZE TABLE on ClickHouse for Dim_Date table after data is 
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
    dag_id='ax_invent_dim_clickhouse_optimizer',
    description = 'Optimize COM.DIM_Date table in ClickHouse after Kafka data ingestion',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 3, 24),
    schedule = None, # Manual trigger only (dimension tables don't need daily sync)
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['clickhouse', 'optimization', 'DWH' , 'dimension', 'COM'],
)

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name = 'cluster_2S_2R',
    database = 'COM',
    table_name = 'Local_DIM_Date',
    partition_column = None,
    partition_format = 'YYYYMM',
    final = True,
    deduplicate = True
)

# Connections
CLICKHOUSE_CONN= 'clickhouse_default'

# Create DAG from config
clickhouse_optimizer_dag(DAG_CONFIG, CLICKHOUSE_CONN, OPTIMIZE_CONFIG)