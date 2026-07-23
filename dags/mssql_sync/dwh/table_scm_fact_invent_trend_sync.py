"""
Airflow DAG: SCM.Fact_InventTrend to Kafka (Config-based)
=========================================================
This DAG reads daily inventory trend data from SQL Server and transfers it to Kafka.

Features:
- Incremental loading based on execution_date
- Daily schedule for Fact table sync
- Streaming with Generator to avoid loading full dataset in RAM
- Exactly-once semantics with idempotent producer
- Backfill support

Author: Senior Data Engineer
Version: 4.0
"""
from datetime import datetime
from airflow.models import Variable # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, TableConfiguration, SyncConfig 

from template.table_mssql_sync_dag_factory import create_table_sync_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config= DAGConfig(
    dag_id='table_dwh_scm_fact_invent_trend_sync',
    description='Daily sync SCM.Fact_InventTrend from SQL Server to Kafka',
    owner= "Zahra Saffarpour",
        
    # Schedule
    start_date=datetime(2026, 3, 24),
    schedule='0 1 * * *',
    catchup=True,
    max_active_runs=3,
    
    # Tags
    tags=['mssql', 'kafka', 'table', 'DWH' ,'fact', 'SCM'],
)

sync_config = SyncConfig(
    batch_size = int(Variable.get("batch_size_dwh_scm_inventtrend", default_var = 50000)),
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config= ConnectionConfig(
    mssql_conn_id = 'mssql_dwh_primary',   
    kafka_conn_id = 'kafka_default',   
)

table_config = TableConfiguration (
    table_name='SCM.Fact_InventTrend',
    date_column='COM_DIM_Date_PhysicalRef',
    date_column_type='int',
    primary_key_column='ID',
    order_by_column= 'ID',
    columns=[
        'ID',
        'COM_DIM_Date_PhysicalRef',
        'COM_DIM_ItemRef',
        'SCM_DIM_ItemCoverageGroupRef',
        'SCM_DIM_ItemCoverageCodeRef',
        'COM_DIM_InventSiteRef',
        'COM_DIM_InventLocationRef',
        'SCM_DIM_InventStatusRef',
        'SCM_DIM_WMSLocationRef',
        'CumulativeAvailablePhysical',
        'CumulativeOrdered',
        'CumulativeOnordered',
        'CumulativePhysicalInventory',
        'CumulativePhysicalReserved',
        'CumulativeTotalAvailable',
        'PhysicalInventoryAmount',
        'PhysicalInventoryGrossWeight',
        'PhysicalInventoryMultipleQty',
        'AvailablePhysicalAmount',
        'AvailablePhysicalGrossWeight',
        'AvailablePhysicalMultipleQty'
    ],

)

kafka_topic_config= KafkaTopicConfig(
    # Kafka config 
    name ='dwh.table.curated.scm.fact_inventtrend',
    num_partitions = 12,
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