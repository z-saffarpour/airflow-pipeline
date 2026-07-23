# راهنمای ایجاد DAG برای Sync داده از MSSQL به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از یک **SQL Server** به **SQL Server** دیگر را توضیح می‌دهد. داده با یک query از منبع خوانده می‌شود و با **staging MERGE upsert** در جدول مقصد نوشته می‌شود.

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

> **تفاوت با Replication MD → Store:** مسیر `mssql_masterdata_to_mssql_store_sync_dag_factory` مقصد فروشگاه را به‌صورت پویا از `ConnectionInfo` می‌سازد. این راهنما برای دو connection از پیش‌تعریف‌شده است (مثلاً DWH → staging DB یا سرور A → سرور B).

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MSSQL (Source)      │                      │  MSSQL (Target)     │
│  mssql_conn_id       │                      │  mssql_target_conn_id│
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToMSSQLQueryOrchestrator                  │
│                  → MSSQLServerWriter                              │
└───────────────────────────────────────────────────────────────────┘
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_mssql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MSSQLToMSSQLQueryOrchestrator.py` | خواندن MSSQL + upsert به MSSQL |
| **Reader / Writer** | `pipeline/database/MSSQLDataReader.py`، `MSSQLServerWriter.py` | stream و MERGE upsert |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_mssql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش | فیلد `ConnectionConfig` |
|------------------------|-----|-------------------------|
| `mssql_dwh_primary` | منبع | `mssql_conn_id` |
| `mssql_target_default` | مقصد | `mssql_target_conn_id` |

> نام connectionها از طریق Variable قابل تنظیم است؛ الزامی نیست دقیقاً همین IDها باشند.

### Pool

```bash
airflow pools set mssql_to_mssql_sync_pool 32 "MSSQL to MSSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در مقصد |
| `mssql_source_conn_id` | `mssql_dwh_primary` | Airflow conn منبع |
| `mssql_target_conn_id` | `mssql_target_default` | Airflow conn مقصد |
| `max_global_parallel_chunks_mssql_to_mssql` | `32` | سقف chunk همزمان |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/mssql_to_mssql_sync/<source_or_table>_to_mssql_sync.py`
- **`dag_id`:** `mssql_<name>_to_mssql_sync` (snake_case)

مثال: جدول `Products` → فایل `products_to_mssql_sync.py` و `dag_id = 'mssql_products_to_mssql_sync'`

### گام ۲ — کپی از DAG نمونه

الگو: `dags/mssql_to_mssql_sync/example_table_to_mssql_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید (بخش ۵).

### گام ۳ — پیکربندی `DAGConfig` و `ConnectionConfig`

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_mssql_sync',
    description='sync MSSQL dbo.Products to target dbo.Products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mssql_products_mssql", default_var=1)),
    retries=int(Variable.get("retries_mssql_products_mssql", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mssql_products_mssql", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mssql_products_mssql", default_var=8))),
    tags=["mssql", "replication", "mssql-to-mssql"],
    pool="mssql_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_dwh_primary"),
    mssql_target_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_target_default"),
)
```

> **نکته:** `mssql_conn_id` و `mssql_target_conn_id` هر دو **الزامی** هستند.

### گام ۴ — پیکربندی `MasterDataSyncConfig`

```python
sync_config = MasterDataSyncConfig(
    source_name="mssql_products",
    source_query="""
            SELECT id, code, name, updated_at
            FROM dbo.Products
          """,
    source_query_count="""
            SELECT COUNT(1) AS CNT
            FROM dbo.Products
          """,
    primary_keys=("id",),
    target_schema="dbo",
    target_table="Products",
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=False,
    delete_scope_column="id",
    use_hash_change_detection=True,
    batch_size=10000,
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

> - `source_query` باید **دیالکت T-SQL** باشد.
> - نام و نوع ستون‌های SELECT باید با جدول مقصد هم‌خوان باشند.
> - `primary_keys` کلید upsert در مقصد است (باید PRIMARY/UNIQUE روی مقصد باشد).
> - جدول مقصد و schema مربوط به `staging_schema` باید از قبل در مقصد وجود داشته باشند.

---

## ۴. جریان Taskها

```
validation
  ├─ validate_mssql_source_connection
  └─ validate_mssql_target_connection
        │
        ▼
processing
  ├─ (بدون chunk) sync_mssql_to_mssql → report_sync_metrics
  └─ (با chunk)   create_sync_chunks → sync_mssql_to_mssql_chunk.expand → report_sync_metrics
```

---

## ۵. Chunking برای جداول بزرگ

```python
sync_config = MasterDataSyncConfig(
    ...
    use_dynamic_tasks=True,
    chunk_column="id",
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=32,
)
```

Chunkها با `NTILE` روی SQL Server منبع ساخته می‌شوند.

---

## ۶. `delete_missing` (حذف ردیف‌های غایب در محدوده)

وقتی `delete_missing=True`:

1. یک جدول موقت keys در `staging_schema` ساخته می‌شود
2. کلیدهای هر batch در staging جمع می‌شوند
3. ردیف‌های مقصد در محدودهٔ `delete_scope_column` بین `min_key` و `max_key` که در staging نیستند حذف می‌شوند

همیشه `delete_scope_column` (یا `chunk_column`) را مشخص کنید.

---

## ۷. تفاوت با مسیرهای مشابه

| مسیر | Factory | مقصد | اتصال مقصد |
|------|---------|------|------------|
| **MSSQL → MSSQL (این راهنما)** | `mssql_to_mssql_sync_dag_factory` | SQL Server | ثابت (`mssql_target_conn_id`) |
| **Replication MD → Store** | `mssql_masterdata_to_mssql_store_sync_dag_factory` | SQL Server فروشگاه | پویا از `ConnectionInfo` |
| **MSSQL → MySQL** | `mssql_to_mysql_sync_dag_factory` | MySQL | ثابت (`mysql_conn_id`) |
| **MSSQL → Kafka** | `table_mssql_sync_dag_factory` / query factory | Kafka (± CH) | ثابت |

هر دو مسیر MSSQL→MSSQL از همان `MSSQLToMSSQLQueryOrchestrator` و `MSSQLServerWriter` استفاده می‌کنند؛ تفاوت در نحوهٔ resolve اتصال مقصد است (`target_is_connection_string`).

---

## ۸. عیب‌یابی سریع

| مشکل | بررسی |
|------|--------|
| DAG ظاهر نمی‌شود | import path، `template.mssql_to_mssql_sync_dag_factory`، نصب `pipeline` |
| `mssql_target_conn_id is required` | هر دو conn را در `ConnectionConfig` ست کنید |
| خطای MERGE / schema | وجود جدول مقصد و `staging_schema` در سرور مقصد |
| Chunkها گیر کرده‌اند | اندازه pool و `max_global_parallel_chunks` |
| حذف ناخواسته | `delete_missing` و `delete_scope_column` را محدود کنید |

---

## فایل‌های مرتبط

| مسیر | نقش |
|------|-----|
| `dags/template/mssql_to_mssql_sync_dag_factory.py` | factory |
| `dags/mssql_to_mssql_sync/example_table_to_mssql_sync.py` | نمونه |
| `pipeline/core/MSSQLToMSSQLQueryOrchestrator.py` | orchestrator |
| `pipeline/database/MSSQLServerWriter.py` | writer (MERGE) |
| `pipeline/database/MSSQLDataReader.py` | reader |
| `pipeline/config/MasterDataSyncConfig.py` | تنظیمات sync |
| `pipeline/config/ConnectionConfig.py` | `mssql_conn_id` + `mssql_target_conn_id` |
| `docs/MASTERDATA_STORE_SYNC_GUIDE.md` | مسیر پویا Publisher → Store |
