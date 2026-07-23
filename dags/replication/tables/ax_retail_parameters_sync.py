"""
Airflow DAG: ax.RetailParameters Query to Store
===============================================
This DAG executes a business query on ax.RetailParameters and sends results to Store.
Uses query_mssql_replication_md_store_sync_dag_factory template tasks.

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable # type: ignore

from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.query_mssql_replication_md_store_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config = DAGConfig(
    dag_id = 'ax_retail_parameters_sync',
    description = 'masterdata query sync from ax.RetailParameters to Store',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 7, 14),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs=int(Variable.get("max_active_runs_retail_parameters", default_var=4)),
    retries=int(Variable.get("retries_retail_parameters", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_retail_parameters", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_retail_parameters", default_var=8))),
    # Tags
    tags = ["mssql","store", "master-data"],
    pool = "replication_md_store_sync_pool"
)

sync_config =  MasterDataSyncConfig(
    source_name = 'adhoc_ax_RetailParameters',
    source_query= """
            SELECT RECID, APPLYDISCOUNTSTOPRICEOVERRIDES, AUTOMATICRETURNOFLOYALTYPAYMENT_RU,
                   AWARDPOINTSFORPARTIALREDEMPTION_RU, CANCELLATIONCHARGE, CANCELLATIONCHARGECODE,
                   DEFAULTCUSTOMERPOSTING, DEFAULTORDERTYPE, DISCOUNTALGORITHMMODE, DISCOUNTOFFERACTIVITYTYPE,
                   EANLICENSENO, ELECTRONICDELIVERYMODECODE, EXPIRATIONDATE, GIFTCARDITEM, ITEMLABELLASTACTION,
                   ITEMLABELSFORNEGATIVESTOCK, ITEMSALESSTATISTICSON, [KEY], MAXBESTDEALSTEPCOUNT,
                   MINIMUMDEPOSITFORSALESORDER, MINIMUMPASSWORDLENGTH, MIXANDMATCHACTIVITYTYPE,
                   MULTIBUYACTIVITYTYPE, PAYMENTSTATISTICS, PICKUPDELIVERYMODECODE, POSTCUSTDISC, POSTINFOCODEDISC,
                   POSTLINEDISC, POSTPERIODICDISC, POSTTOTALDISC, PROCESSGIFTCARDSASPREPAYMENTS_RU,
                   PROCESSRETURNSASINORIGINALSALESHIFT_RU, PROPOSEREFUNDPAYMENTAMOUNT_RU, RECEIPTOPTION,
                   REFUNDSHIPPINGAMTWITHOUTAPPR, REFUNDSHIPPINGCHARGES, REQUIRENUMERICCHAR, REQUIRESPECIALCHAR,
                   REQUIREUPPERCASE, SHELFLABELLASTACTION, SHIPPINGCHARGECODE, STAFFSTATISTICS, TAXONGIFTCARDS,
                   TERMINALSTATISTICS, USEADVANCEINVOICE_W, USEGIFTCARDPOLICIES, DATAAREAID, [AllowedRefundDays],
                   [gigCompanyRegisterNumber], [gigICRMMasterServiceUri], [gigICRMLoginTokenUri],
                   [gigOKALACheckerUri], [gigOKALALoginUri], [gigOKALASubmitUri],
                   [gigOKALAUpdateOkalaPricesFromAXUri], [gigTaraMasterServiceUri], [gigMehrBankPaymentUri],
                   [gigMehrBankQueryUri], [gigMehrBankTransactionRequestUri], [gigMehrBankSignKey],
                   [gigOKALAReturnCheckerUri], [gigOKALAReturnSubmitUri], [gigApplyDiscOnJetPrint],
                   [gigTransferSuspendedTranNo], [gigOKALAGetOpenReturnOrderApiUri],
                   [gigOKALAGetOrderSummaryForCheckerV2Uri], [gigBarcodeDiscount], [gigMobileEFTUri],
                   [gigTaraQueryPaymentInfoServiceUri]
            FROM ax.RETAILPARAMETERS WITH (READPAST);
          """,
    source_query_count= """
            SELECT COUNT(1) AS CNT
            FROM ax.RETAILPARAMETERS WITH (READPAST);
          """,
    primary_keys=('KEY', 'DATAAREAID',),

    use_hash_change_detection=True,
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=int(Variable.get("task_chunk_size_retail_parameters", default_var=100000)),
    max_parallel_chunks=int(Variable.get("max_parallel_chunks_retail_parameters", default_var=8)),
    max_global_parallel_chunks=int(Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)),

    target_schema = 'ax',
    target_table = 'RetailParameters',
    staging_schema = Variable.get("mssql_staging_schema", default_var = "crt"),
    delete_missing=bool(int(Variable.get("delete_missing_retail_parameters", default_var=0))),
    delete_scope_column='RECID',
    batch_size = int(Variable.get("batch_size_retail_parameters", default_var = 30000))
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config = dag_config,
    sync_config = sync_config
)

dag