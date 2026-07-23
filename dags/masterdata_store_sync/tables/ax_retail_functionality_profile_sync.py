"""
Airflow DAG: ax.RetailFunctionalityProfile Query to Store
===============================================
This DAG executes a business query on ax.RetailFunctionalityProfile and sends results to Store.
Uses mssql_masterdata_to_mssql_store_sync_dag_factory template tasks.

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable # type: ignore

from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig

from template.mssql_masterdata_to_mssql_store_sync_dag_factory import create_dag

# ============================================================================
# CONFIGURATION 
# ============================================================================

dag_config = DAGConfig(
    dag_id = 'ax_retail_functionality_profile_sync',
    description = 'masterdata query sync from ax.RetailFunctionalityProfile to Store',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 7, 14),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs=int(Variable.get("max_active_runs_retail_functionality_profile", default_var=4)),
    retries=int(Variable.get("retries_retail_functionality_profile", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_retail_functionality_profile", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_retail_functionality_profile", default_var=8))),
    # Tags
    tags = ["mssql","store", "master-data"],
    pool = "replication_md_store_sync_pool"
)

sync_config =  MasterDataSyncConfig(
    source_name = 'adhoc_ax_RetailFunctionalityProfile',
    source_query= """
            SELECT RECID, ADDTAXONPRICES, AGGREGATEITEMS, AGGREGATEITEMSFORPRINTING, AGGREGATEPAYMENTS,
                   ALWAYSEXPANDTRANSACTIONSCREENLINEDETAILS, AMOUNTDECIMALPLACES, AMOUNTROUNDINGTO,
                   APPLYDISCOUNTONUNITPRICES, AUDITENABLED, BACKUPTRAININGTRANSACTIONS, BLOCKEDCLOSEDACCOUNT,
                   BLOCKRETURNDURINGTRANSACTIONUPLOAD, CANCELREASON, CENTRALTABLESERVER, CENTRALTABLESERVERPORT,
                   CONTINGENCYREASON, CREATEASYNCCUSTOMERS, CREATEOFFLINECUSTOMERORDERS, CURRENCYSYMBOL,
                   CUSTOMERSEARCHDEFAULT, CUSTOMERSEARCHMODE, DAYSCUSTOMERHISTORY, DAYSTRANSACTIONSEXISTS,
                   DECIMALNOTREQUIREDFORMINORCURRENCYUNIT, DEFAULTSTARTAMOUNT, DISCOUNTATTOTAL,
                   DISPLAYSECONDARYTOTALCURRENCY, ENDOFTRANSACTION, EODBANKTOTALSVERIFICATION, INCLUDEKITCOMPONENTS,
                   ITEMNOTONFILE, LIMITSTAFFLISTTOSTORE, LINEITEMTAXCHANGE, MANUALCALCULATEDISCOUNTS, MARKDOWN,
                   MARKUP, MAXIMUMPRICE, MAXIMUMQTY, MAXIMUMSTORETRANSLOG, MAXTRANSACTIONSEARCHRESULTS,
                   MINIMUMPASSWORDLENGTH, MULTIBLEITEMSYMBOL, MUSTKEYINPRICEIFZERO, [NAME], NEGATIVEADJUSTMENT,
                   NEGATIVESALESLINE, NOSALE, NOTAXUSED, OVERRIDEPRICE, PRICEDECIMALPLACES, PRICEROUNDINGTO,
                   PRINTTENDERDETAILSONXREPORT, PRINTXREPORTONTERMINAL, PROFILEID, REFUNDSALE, REQUESTPRESALESID,
                   REQUIREAMOUNTDECLARATION, RETAILRECEIPTPROFILE, SALESPERSON, SALESPERSONMODE,
                   SECONDARYTOTALCURRENCY, SERIALNUMBER, SHOWSTAFFLISTATLOGON, SKIPTAXONRECEIPT, STAFFBARCODELOGON,
                   STAFFBARCODELOGONREQUIRESPASSWORD, STAFFCARDLOGON, STAFFCARDLOGONREQUIRESPASSWORD,
                   STARTOFTRANSACTION, SUPPORTEDCOUNTRYREGIONISOCODE, TAENABLEREGISTRATION,
                   TAXREGISTRATIONNUMBERONRECEIPT, TENDERDECLARATION, TRANSACTIONDELETEREMINDER,
                   TRANSACTIONTAXCHANGE, TSCENTRALTABLESERVER, TSCUSTOMER, TSDATAENTRIES, TSFLOATINGCASHIER,
                   TSINVENTORYLOOKUP, TSRESENDDELAY, TSSENDTRANSACTIONS, TSSENDVOIDTRANSACTIONS, TSSTAFF,
                   TSSUSPENDRETRIEVETRANSACTIONS, TSTRANSACTIONRESENDLIMIT, TSUPDATEREPLICATIONCOUNTER,
                   VOIDISPRESSED, VOIDPAYMENT, VOIDTRANSACTION, MODIFIEDDATETIME
            FROM ax.RETAILFUNCTIONALITYPROFILE WITH (READPAST);
          """,
    source_query_count= """
            SELECT COUNT(1) AS CNT
            FROM ax.RETAILFUNCTIONALITYPROFILE WITH (READPAST);
          """,
    primary_keys=('PROFILEID',),

    use_hash_change_detection=True,
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=int(Variable.get("task_chunk_size_retail_functionality_profile", default_var=100000)),
    max_parallel_chunks=int(Variable.get("max_parallel_chunks_retail_functionality_profile", default_var=8)),
    max_global_parallel_chunks=int(Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)),

    target_schema = 'ax',
    target_table = 'RetailFunctionalityProfile',
    staging_schema = Variable.get("mssql_staging_schema", default_var = "crt"),
    delete_missing=bool(int(Variable.get("delete_missing_retail_functionality_profile", default_var=0))),
    delete_scope_column='RECID',
    batch_size = int(Variable.get("batch_size_retail_functionality_profile", default_var = 30000))
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config = dag_config,
    sync_config = sync_config
)

dag