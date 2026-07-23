"""
Airflow DAG: inventory.onhand ClickHouse Optimization Pipeline
=================================================================
This DAG runs OPTIMIZE TABLE on ClickHouse for onhand table after data is 
transferred from Kafka.

Features:
- FINAL merge support
- Deduplication support
- Health check before/after optimization

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig

from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

# Configuration for this DAG
DAG_CONFIG = DAGConfig(
    dag_id='inventory_onhand_clickhouse_optimizer',
    description = 'Optimize inventory.onhand table in ClickHouse after Caclulated data',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 4, 20),
    schedule = None, # Every day at 7 AM (after data transfer at 6 AM)
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['clickhouse', 'optimization', 'inventory'],
)

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name = 'cluster_2S_2R',
    database = 'inventory',
    table_name = 'local_onhand',
    partition_column = None,
    partition_format = 'YYYYMM',
    final = True,
    deduplicate = True
)

# Connections
CLICKHOUSE_CONN= 'clickhouse_default'

# Create DAG from config
clickhouse_optimizer_dag(DAG_CONFIG, CLICKHOUSE_CONN, OPTIMIZE_CONFIG)
