"""
Airflow DAG: RTL.Fact_SalesTrans Query to Kafka
===============================================
This DAG executes a business query on RTL.Fact_SalesTrans and sends results to Kafka.
Uses sqlserver_kafka_query_sync template tasks.

Author: Senior Data Engineer
Version: 1.0
"""
from datetime import datetime
from airflow.models import Variable # type: ignore

from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, QueryConfiguration, SyncConfig 

from template.query_mssql_sync_dag_factory import create_query_sync_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config= DAGConfig(
    dag_id = 'query_dwh_rtl_fact_sales_trans_sync',
    description = 'Daily sync RTL.Fact_SalesTrans from SQL Server to Kafka',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 4, 21),
    schedule = '35 6 * * *',
    catchup = True, # No backfill for dimension tables
    max_active_runs = 5,
    # Tags
    tags = ['mssql', 'kafka', 'query', 'DWH', 'fact', 'RTL'],
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

query_config = QueryConfiguration (
    source_name = 'adhoc_query_RTL.Fact_SalesTrans',
    query = """
            SELECT mySalesTrans.ID,
                mySalesTrans.COM_DIM_Date_TransRef,
                mySalesTrans.COM_DIM_Time_TransRef,
                mySalesTrans.COM_DIM_ItemRef,
                mySalesTrans.COM_DIM_InventLocationRef,
                mySalesTrans.COM_DIM_InventSiteRef,
                mySalesTrans.RTL_DIM_CostAmountRef,
                mySalesTrans.RTL_DIM_OrganizationDiscTypeRef,
                mySalesTrans.RTL_DIM_SaleIsReturnSaleRef,
                mySalesTrans.RTL_DIM_SalesTypeRef,
                mySalesTrans.RTL_DIM_SystemTypeRef,
                mySalesTrans.QTY,
                mySalesTrans.Price,
                mySalesTrans.GrossAmount,
                mySalesTrans.NetAmount,
                mySalesTrans.NetAmountINCLTax,
                mySalesTrans.TaxAmount,
                mySalesTrans.DiscAmount,
                mySalesTrans.GeneralDiscAmount,
                mySalesTrans.ExclusiveDiscAmount,
                mySalesTrans.CouponDiscAmount,
                mySalesTrans.WeightQTY,
                mySalesTrans.MultipleQTY,
                mySalesTrans.InventLocationDays,
                mySalesTrans.TransactionSerial,
                mySalesTrans.BKTransactionId,
                mySalesTrans.BKLineNum,
                mySalesTrans.ReceiptId,
                myDate.GregorianDate AS TransDate,
                myDate.PersianYearInt,
                myDate.PersianYearMonthInt,
                myDate.PersianInt,
                CONVERT( BIGINT, SUBSTRING( HASHBYTES( 'SHA2_512', BKTransactionId ), 1, 8 )) AS BKTransactionIdHash
            FROM RTL.Fact_SalesTrans AS mySalesTrans WITH(READPAST)
            INNER JOIN COM.DIM_Date AS myDate WITH(READPAST) ON myDate.ID = mySalesTrans.COM_DIM_Date_TransRef
            WHERE mySalesTrans.COM_DIM_Date_TransRef = %s
          """,
    query_params = ["{{ ds_nodash }}"],
    key_column = 'ID',
    count_query= """
            SELECT COUNT(1) AS CNT 
            FROM RTL.Fact_SalesTrans AS mySalesTrans WITH(READPAST)
            INNER JOIN COM.DIM_Date AS myDate WITH(READPAST) ON myDate.ID = mySalesTrans.COM_DIM_Date_TransRef
            WHERE mySalesTrans.COM_DIM_Date_TransRef = %s
          """,
)

kafka_topic_config= KafkaTopicConfig(
    name = 'dwh.query.curated.rtl.fact_salestrans',
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