"""
Airflow DAG: RTL.Fact_SalesTrans to Kafka (Config-based)
========================================================
This DAG reads daily sales transaction data from SQL Server and transfers it to Kafka.

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
    dag_id = 'table_dwh_rtl_fact_sales_trans_sync',
    description = 'Daily sync RTL.Fact_SalesTrans from SQL Server to Kafka',
    owner= "Zahra Saffarpour",
        
    # Schedule  
    start_date = datetime(2026, 4, 21),
    schedule = '35 6 * * *',
    catchup = True,
    max_active_runs = 5,
    
    # Tags 
    tags = ['mssql', 'kafka', 'table', 'DWH' , 'fact', 'RTL'],
)

sync_config = SyncConfig(
    batch_size = int(Variable.get("batch_size_dwh_rtl_salestrans", default_var = 50000)),
    date_offset = -1,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config= ConnectionConfig(
    mssql_conn_id = 'mssql_dwh_primary',   
    kafka_conn_id = 'kafka_default',   
)

table_config = TableConfiguration (
    table_name = 'RTL.Fact_SalesTrans',
    date_column = 'COM_DIM_Date_TransRef',
    date_column_type = 'int',
    primary_key_column = 'ID',
    order_by_column= 'ID',
    columns = [
        'ID',
        'COM_DIM_Date_TransRef',
        'COM_DIM_Time_TransRef',
        'COM_DIM_ItemRef',
        'COM_DIM_InventLocationRef',
        'COM_DIM_InventSiteRef',
        'RTL_DIM_CostAmountRef',
        'RTL_DIM_OrganizationDiscTypeRef',
        'RTL_DIM_SaleIsReturnSaleRef',
        'RTL_DIM_SalesTypeRef',
        'RTL_DIM_SystemTypeRef',
        'QTY',
        'Price',
        'GrossAmount',
        'NetAmount',
        'NetAmountINCLTax',
        'TaxAmount',
        'DiscAmount',
        'GeneralDiscAmount',
        'ExclusiveDiscAmount',
        'CouponDiscAmount',
        'WeightQTY',
        'MultipleQTY',
        'InventLocationDays',
        'TransactionSerial',
        'BKTransactionId',
        'BKLineNum',
        'ReceiptId',
    ],

)

kafka_topic_config= KafkaTopicConfig(
    name = 'dwh.table.curated.rtl.fact_salestrans',
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
