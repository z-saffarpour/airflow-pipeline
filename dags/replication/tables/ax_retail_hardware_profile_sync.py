"""
Airflow DAG: ax.RetailHardwareProfile Query to Store
===============================================
This DAG executes a business query on ax.RetailHardwareProfile and sends results to Store.
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
    dag_id = 'ax_retail_hardware_profile_sync',
    description = 'masterdata query sync from ax.RetailHardwareProfile to Store',
    owner= "Zahra Saffarpour",

    # Schedule
    start_date = datetime(2026, 7, 14),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs=int(Variable.get("max_active_runs_retail_hardware_profile", default_var=4)),
    retries=int(Variable.get("retries_retail_hardware_profile", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_retail_hardware_profile", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_retail_hardware_profile", default_var=8))),
    # Tags
    tags = ["mssql","store", "master-data"],
    pool = "replication_md_store_sync_pool"
)

sync_config =  MasterDataSyncConfig(
    source_name = 'adhoc_ax_RetailHardwareProfile',
    source_query= """
            SELECT RECID, CAPTUREEXTRADATA, CASHCHANGER, CASHCHANGERINITSETTINGS, CASHCHANGERPORTSETTINGS, CCTV,
                   CCTVCAMERA, CCTVHOSTNAME, CCTVPORT, DANFEPRINTERDEVICENAME, DELAYFORLINKEDITEMS,
                   DISPLAYBALANCETEXT, DISPLAYBINCONVERSION, DISPLAYCHARACTERSET, DISPLAYCLOSEDLINE1,
                   DISPLAYCLOSEDLINE2, DISPLAYDESCRIPTION, DISPLAYDEVICE, DISPLAYDEVICENAME, DISPLAYLINKEDITEM,
                   DISPLAYTERMINALCLOSED, DISPLAYTOTALTEXT, DOCINSERTREMOVALTIMEOUT, DRAWER, DRAWER2,
                   DRAWER2DESCRIPTION, DRAWER2DEVICENAME, DRAWER2DEVICEPOOL, DRAWER2MAKE, DRAWER2MODEL,
                   DRAWER2USECASHDRAWERPOOL, DRAWERDESCRIPTION, DRAWERDEVICENAME, DRAWERDEVICEPOOL, DRAWERMAKE,
                   DRAWERMODEL, DRAWERUSECASHDRAWERPOOL, DUALDISPLAY, DUALDISPLAYBROWSERURL,
                   DUALDISPLAYIMAGEINTERVAL, DUALDISPLAYIMAGEPATH, DUALDISPLAYRECEIPTPERCENTAGE, DUALDISPLAYTYPE,
                   EFRSATCONTRACTNAME, EFRSATLIBRARYFOLDER, EFRSATLIBRARYNAME, EFT, EFTCOMPANYID, EFTCONFIGURATION,
                   EFTCONNECTORNAME, EFTCONNECTORPROPERTIES, EFTDATA, EFTDESCRIPTION, EFTMAXIMUMCARDPAYMENTS,
                   EFTMERCHANTID, EFTPASSWORD, EFTSERVERNAME, EFTSERVERPORT, EFTUSERID, ENDTRACK1, ENDTRACK2,
                   FISCALPRINTER, FISCALPRINTERCONFIGID, FISCALPRINTERDESCRIPTION, FISCALPRINTERDEVICENAME,
                   FISCALREGISTER, FISCALREGISTERCONFIGID, FISCALREGISTERDESCRIPTION, FISCALREGISTERDEVICENAME,
                   FORMXPOS, FORMYPOS, HARDTOTAL, HARDTOTALDESCRIPTION, HARDTOTALDEVICENAME, KEYBOARDMAPPINGID,
                   KEYLOCK, KEYLOCKDESCRIPTION, KEYLOCKDEVICENAME, LOGO, LOGOALIGNMENT, LOGOBITMAP,
                   MANUALINPUTALLOWED, MAXINVOICELINES, MICR, MICRDESCRIPTION, MICRDRIVERNAME, MSR, MSRDESCRIPTION,
                   MSRDEVICENAME, MSRMAKE, MSRMODEL, MULTIPLECONNECTORS, [NAME], PHARMACY, PHARMACYHOST,
                   PHARMACYPORT, PINPAD, PINPADDESCRIPTION, PINPADDEVICENAME, PINPADMAKE, PINPADMODEL,
                   PRINTBINARYCONVERSION, PRINTER, PRINTER2, PRINTER2BINARYCONVERSION, PRINTER2CHARACTERSET,
                   PRINTER2DESCRIPTION, PRINTER2DEVICENAME, PRINTER2DOCINSERTREMOVALTIMEOUT, PRINTER2LOGO,
                   PRINTER2LOGOALIGNMENT, PRINTER2LOGOBITMAP, PRINTER2MAKE, PRINTER2MODEL, PRINTER2RECEIPTPROFILEID,
                   PRINTERCHARACTERSET, PRINTERDESCRIPTION, PRINTERDEVICENAME, PRINTERMAKE, PRINTERMODEL,
                   PRINTERRECEIPTPROFILEID, PROFILEID, RFIDDESCRIPTION, RFIDDEVICENAME, RFIDSCANNERTYPE, SCALE,
                   SCALEDESCRIPTION, SCALEDEVICENAME, SCANNER, SCANNER2, SCANNER2DESCRIPTION, SCANNER2DEVICENAME,
                   SCANNERDESCRIPTION, SCANNERDEVICENAME, SCREENKEYBOARD, SEPARATOR1, SHOWPICTURE, SIGCAP,
                   SIGCAPDESCRIPTION, SIGCAPDEVICENAME, SIGCAPFORMNAME, SIGCAPMAKE, SIGCAPMODEL, STARTTRACK1,
                   STARTTRACK2AFTER, TIMEOUTINSEC, MODIFIEDDATETIME, EFTTESTMODE, EFT1BANKNAME, EFT2BANKNAME,
                   EFT3BANKNAME, EFT4BANKNAME, EFT1ISACTIVE, EFT2ISACTIVE, EFT3ISACTIVE, EFT4ISACTIVE
            FROM ax.RETAILHARDWAREPROFILE WITH (READPAST);
          """,
    source_query_count= """
            SELECT COUNT(1) AS CNT
            FROM ax.RETAILHARDWAREPROFILE WITH (READPAST);
          """,
    primary_keys=('PROFILEID',),

    use_hash_change_detection=True,
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=int(Variable.get("task_chunk_size_retail_hardware_profile", default_var=100000)),
    max_parallel_chunks=int(Variable.get("max_parallel_chunks_retail_hardware_profile", default_var=8)),
    max_global_parallel_chunks=int(Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)),

    target_schema = 'ax',
    target_table = 'RetailHardwareProfile',
    staging_schema = Variable.get("mssql_staging_schema", default_var = "crt"),
    delete_missing=bool(int(Variable.get("delete_missing_retail_hardware_profile", default_var=0))),
    delete_scope_column='RECID',
    batch_size = int(Variable.get("batch_size_retail_hardware_profile", default_var = 30000))
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = create_dag(
    dag_config = dag_config,
    sync_config = sync_config
)

dag