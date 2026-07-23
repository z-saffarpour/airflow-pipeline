"""
Airflow DAG: dbo.INVENTTRANS Query to Kafka
===============================================
This DAG executes a business query on dbo.INVENTTRANS and sends results to Kafka.
Uses sqlserver_kafka_query_sync template tasks.

Author: Senior Data Engineer
Version: 3.0
"""
from datetime import datetime

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, QueryConfiguration, SyncConfig 
from template.query_mssql_sync_dag_factory import create_query_sync_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config = DAGConfig(
    dag_id = 'query_ax_invent_trans_full_sync',
    description = 'Daily business query sync from dbo.INVENTTRANS to Kafka',
    owner= "Zahra Saffarpour",
        
    # Schedule
    start_date = datetime(2026, 4, 5),
    schedule = '0 1 * * *',
    catchup = False, # No backfill for dimension tables
    max_active_runs = 1,
    
    # Tags
    tags = ['mssql', 'kafka', 'query', 'ax','full'],
)

sync_config = SyncConfig(
    batch_size = 100000,
    date_offset = -12,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id = 'mssql_erp_primary',
    kafka_conn_id = 'kafka_default',
)

query_config = QueryConfiguration(
    source_name = 'adhoc_query_dbo.INVENTTRANS',
    query = """
            SELECT
            ITEMID,INVENTDIMID,STATUSISSUE,DATEPHYSICAL,QTY,COSTAMOUNTPOSTED,INVOICEID,VOUCHER,DATEFINANCIAL,COSTAMOUNTPHYSICAL,STATUSRECEIPT,VOUCHERPHYSICAL,
            COSTAMOUNTADJUSTMENT,QTYSETTLED,COSTAMOUNTSETTLED,INVOICERETURNED,PACKINGSLIPID,INVENTTRANSORIGIN,PROJID,
            DATESTATUS,CURRENCYCODE,MODIFIEDDATETIME,CREATEDDATETIME,PARTITION,RECVERSION,RECID,DATAAREAID
            FROM MicrosoftDynamicsAX.dbo.INVENTTRANS WITH (READPAST)
            WHERE DATAAREAID = N'OKCS' AND PARTITION = 5637144576 AND DATEPHYSICAL >= %s
            ORDER BY RECID
          """,
    query_params = ["{{ ds }}"],
    key_column = 'RECID',
)

kafka_topic_config = KafkaTopicConfig(
    name ='ax.query.raw.dbo.inventtrans',
    num_partitions = 12,
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
