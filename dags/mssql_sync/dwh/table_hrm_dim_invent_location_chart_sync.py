"""
Airflow DAG: HRM.DIM_InventLocationChart to Kafka (Config-based)
=================================================
This DAG reads dimension InventSite data from SQL Server and transfers it to Kafka.

Features:
- Full table sync (no date filtering - dimension table)
- Streaming with Generator to avoid loading full dataset in RAM
- Exactly-once semantics with idempotent producer
- Configurable parameters

Author: Senior Data Engineer
Version: 2.0
"""
from datetime import datetime

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, TableConfiguration, SyncConfig 

from template.table_mssql_sync_dag_factory import create_table_sync_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config= DAGConfig(
    dag_id = 'table_dwh_hrm_dim_invent_location_chart_sync',
    description = 'Sync HRM.DIM_InventLocationChart dimension table from SQL Server to Kafka',
    owner= "Zahra Saffarpour",
        
    # Schedule
    start_date = datetime(2026, 4, 15),
    schedule = '4 23 * * *',
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['mssql', 'kafka', 'table', 'DWH' , 'dimension', 'HRM'],
)

sync_config = SyncConfig(
    batch_size = 10000,
    is_send_kafka=True,
    is_send_clickhouse=False, 
)

conn_config= ConnectionConfig(
    mssql_conn_id = 'mssql_dwh_primary',   
    kafka_conn_id = 'kafka_default',   
)

table_config = TableConfiguration (
    table_name = 'HRM.DIM_InventLocationChart',
    date_column = None,
    date_column_type = 'int',
    primary_key_column = 'ID',
    order_by_column= 'ID',
    columns=[
        'ID',
        'COM_DIM_InventLocationRef',
        'BKPersonelID',
        'Position',
        'PersonelFirstName',
        'PersonelLastName',
        'PersonelFullName',
        'ModirPersonelID',
        'LevelPersonelID',
        'LevelSalesID',
        'SysIsValid'
    ],
)

kafka_topic_config= KafkaTopicConfig(
    name = 'dwh.table.curated.hrm.dim_inventlocationchart',
    num_partitions = 3,
    replication_factor = 3,
)

clickhouse_config = None

# ============================================================================
# Create DAG from config
# ============================================================================

create_table_sync_dag(
    dag_config = dag_config,
    sync_config = sync_config,
    conn_config = conn_config,
    table_config = table_config,
    kafka_topic_config = kafka_topic_config,
    clickhouse_config = clickhouse_config
)
