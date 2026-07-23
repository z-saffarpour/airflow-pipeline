"""
Airflow DAG: RTL.Fact_SalesTrans ClickHouse Optimization Pipeline
=================================================================
This DAG runs OPTIMIZE TABLE on ClickHouse for Fact_SalesTrans table after data is 
transferred from Kafka.

Features:
- Partition-based optimization (by DateKey)
- FINAL merge support
- Deduplication support
- Health check before/after optimization
- Scheduled daily at 4 AM

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig

from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id='rtl_fact_sales_trans_clickhouse_optimizer',
    description = 'Optimize RTL.Fact_SalesTrans table in ClickHouse after Kafka data ingestion',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 4, 20),
    schedule = '0 8 * * *', # Every day at 7 AM (after data transfer at 6 AM)
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['clickhouse', 'optimization', 'DWH' , 'dimension', 'fact'],
)

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name = 'cluster_2S_2R',
    database = 'RTL',
    table_name = 'Local_Fact_SalesTrans',
    partition_column = 'COM_DIM_Date_TransRef',
    partition_format = 'YYYYMM',
    final = True,
    deduplicate = True
)

# Connections
CLICKHOUSE_CONN= 'clickhouse_default'

# Create DAG from config
clickhouse_optimizer_dag(DAG_CONFIG, CLICKHOUSE_CONN, OPTIMIZE_CONFIG)
