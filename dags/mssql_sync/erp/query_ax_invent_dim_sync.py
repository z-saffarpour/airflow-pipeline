"""
Airflow DAG: dbo.INVENTDIM Query to Kafka
===============================================
This DAG executes a business query on dbo.INVENTDIM and sends results to Kafka.
Uses sqlserver_kafka_query_sync template tasks.

Author: Senior Data Engineer
Version: 3.0
"""
from datetime import datetime

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, QueryConfiguration, SyncConfig 
from template.mssql_to_kafka_clickhouse_sync_dag_factory import create_query_sync_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config = DAGConfig(
    dag_id = 'query_ax_invent_dim_sync',
    description = 'Hourly business query sync from dbo.INVENTDIM to Kafka',
    owner= "Zahra Saffarpour",
        
    # Schedule
    start_date = datetime(2026, 4, 5),
    schedule = '1 * * * *',
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['mssql', 'kafka', 'query', 'ax','incremental'],
)

sync_config = SyncConfig(
    batch_size = 10000,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id = 'mssql_erp_primary',
    kafka_conn_id = 'kafka_default',
)

query_config = QueryConfiguration(
    source_name = 'adhoc_query_dbo.INVENTDIM',
    query = """
            SELECT INVENTDIMID, WMSLOCATIONID, INVENTLOCATIONID, INVENTSITEID, LICENSEPLATEID, INVENTSTATUSID, MODIFIEDDATETIME, CREATEDDATETIME, RECID
            FROM dbo.INVENTDIM WITH(READPAST)
            WHERE DATAAREAID = 'OKCS'
                AND MODIFIEDDATETIME >= DATEADD(HOUR,-1*3,GETUTCDATE())
          """,
    key_column = 'RECID',
    count_query= """
            SELECT COUNT(1) AS CNT 
            FROM dbo.INVENTDIM WITH(READPAST) 
            WHERE DATAAREAID = 'OKCS'
                AND MODIFIEDDATETIME >= DATEADD(HOUR,-1*3,GETUTCDATE())
          """,
    query_params= None,          
)

kafka_topic_config = KafkaTopicConfig(
    name = 'ax.query.raw.dbo.inventdim',
    num_partitions = 6,
    replication_factor = 3,
)

clickhouse_config = None

# ============================================================================
# Create DAG from config
# ============================================================================

create_query_sync_dag(
    dag_config = dag_config,
    sync_config = sync_config,
    conn_config = conn_config,
    query_config = query_config,
    kafka_topic_config = kafka_topic_config,
    clickhouse_config = clickhouse_config
)