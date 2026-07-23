"""
Airflow DAG: dbo.WHSINVENTRESERVE Query to Kafka
===============================================
This DAG executes a business query on dbo.WHSINVENTRESERVE and sends results to Kafka.
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
    dag_id = 'query_ax_whs_invent_reserve_full_sync',
    description = 'Hourly business query sync from dbo.WHSINVENTRESERVE to Kafka',
    owner= "Zahra Saffarpour",
 
    # Schedule
    start_date = datetime(2026, 4, 5),
    schedule = '9 * * * *',
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['mssql', 'kafka', 'query', 'ax','full'],
)

sync_config = SyncConfig(
    batch_size = 50000,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id = 'mssql_erp_primary',
    kafka_conn_id = 'kafka_default',
)

query_config = QueryConfiguration(
    source_name = 'adhoc_query_dbo.WHSINVENTRESERVE',
    query = """
            SELECT ITEMID, RESERVPHYSICAL, RESERVORDERED, AVAILPHYSICAL, AVAILORDERED, INVENTDIMID, HIERARCHYLEVEL, RECID 
            FROM dbo.WHSINVENTRESERVE WITH(READPAST)
            WHERE DATAAREAID = 'OKCS'
                AND PARTITION = 5637144576
                AND HIERARCHYLEVEL = 3
          """,
    key_column = 'RECID',
    count_query= """
            SELECT COUNT(1) AS CNT 
            FROM dbo.WHSINVENTRESERVE WITH(READPAST) 
            WHERE DATAAREAID = 'OKCS'
                AND PARTITION = 5637144576
                AND HIERARCHYLEVEL = 3
          """,
    query_params= None,
)

kafka_topic_config = KafkaTopicConfig(
    name = 'ax.query.raw.dbo.whsinventreserve',
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