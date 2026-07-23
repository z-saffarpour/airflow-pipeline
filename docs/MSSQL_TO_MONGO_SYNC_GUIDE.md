# راهنمای ایجاد DAG برای Sync داده از MSSQL به MongoDB

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی (replication-style)** از **SQL Server** به **MongoDB** را توضیح می‌دهد. داده با یک query از MSSQL خوانده می‌شود و با **bulk upsert** در collection مقصد MongoDB نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MSSQL (Source)      │                      │  MongoDB (Target)   │
│  mssql_* conn        │                      │  mongo_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (bulk_write ReplaceOne)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToMongoDBQueryOrchestrator                │
│                  → MongoDBServerWriter                            │
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_mongo_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_mongo_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MSSQLToMongoDBQueryOrchestrator.py` | خواندن MSSQL + upsert به MongoDB |
| **Reader / Writer** | `pipeline/database/MSSQLDataReader.py`، `MongoDBServerWriter.py` | stream و bulk upsert |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_mongo_sync/`** کافی است.

> **نکته:** مسیر معکوس **MongoDB → MSSQL** از قبل آماده است (`mongo_to_mssql_sync_dag_factory` + `docs/MONGO_TO_MSSQL_SYNC_GUIDE.md`).

### قرارداد نام‌گذاری مقصد

| فیلد `MasterDataSyncConfig` | معنای MongoDB |
|-----------------------------|---------------|
| `target_schema` | نام **database** |
| `target_table` | نام **collection** |
| `staging_schema` | database موقت برای keys staging (پیش‌فرض: همان `target_schema`) |
| `primary_keys` | فیلدهای کلید؛ اگر شامل `id` باشد به‌صورت پیش‌فرض به `_id` نگاشت می‌شود |

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_dwh_primary` | منبع — SQL Server |
| `mongo_target_default` | مقصد — MongoDB (قابل تغییر) |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است.

برای MongoDB می‌توانید URI کامل را در `extra.uri` بگذارید، یا host/port/login/password (+ `authSource` در extra) تنظیم کنید. بستهٔ لازم: `pymongo>=4.6.0` (در `requirements.txt` موجود است).

### Pool

```bash
airflow pools set mssql_to_mongo_sync_pool 32 "MSSQL to MongoDB chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mongo_staging_database` | `staging` | database موقت keys staging در MongoDB |
| `mssql_source_conn_id` | `mssql_dwh_primary` | Airflow conn منبع |
| `mongo_target_conn_id` | `mongo_target_default` | Airflow conn مقصد |
| `max_global_parallel_chunks_mssql_to_mongo` | `32` | سقف chunk همزمان |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/mssql_to_mongo_sync/<source_or_table>_to_mongo_sync.py`
- **`dag_id`:** `mssql_<name>_to_mongo_sync` (snake_case)

مثال: جدول MSSQL `Products` → فایل `products_to_mongo_sync.py` و `dag_id = 'mssql_products_to_mongo_sync'`

### گام ۲ — کپی از DAG نمونه

الگو:

- **جدول کوچک / بدون chunk:** `dags/mssql_to_mongo_sync/example_table_to_mongo_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید (بخش ۵).

### گام ۳ — پیکربندی `DAGConfig` و `ConnectionConfig`

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_mongo_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_mongo_sync',
    description='sync MSSQL dbo.Products to MongoDB app.products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,          # یا مثلاً '0 2 * * *' برای nightly
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mssql_products_mongo", default_var=1)),
    retries=int(Variable.get("retries_mssql_products_mongo", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mssql_products_mongo", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mssql_products_mongo", default_var=8))),
    tags=["mssql", "mongo", "mongodb", "replication", "mssql-to-mongo"],
    pool="mssql_to_mongo_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_dwh_primary"),
    mongo_conn_id=Variable.get("mongo_target_conn_id", default_var="mongo_target_default"),
)
```

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
    target_schema="app",              # MongoDB database
    target_table="products",          # MongoDB collection
    staging_schema=Variable.get("mongo_staging_database", default_var="staging"),
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

### نگاشت `id` → `_id`

به‌صورت پیش‌فرض، فیلد منبع با نام `id` در MongoDB به‌عنوان `_id` ذخیره می‌شود (معکوسِ `rename_id_to` در مسیر Mongo→MSSQL). اگر کلید شما نام دیگری دارد و نباید به `_id` تبدیل شود، در سازندهٔ orchestrator مقدار `id_as_mongo_id=None` بگذارید (یا factory را برای پاس‌دادن این پارامتر گسترش دهید).

---

## ۴. جریان Taskها

```
validation
  ├─ validate_mssql_connection
  └─ validate_mongo_connection
        │
        ▼
processing
  ├─ (بدون chunk) sync_mssql_to_mongo → report_sync_metrics
  └─ (با chunk)   create_sync_chunks → sync_mssql_to_mongo_chunk.expand → report_sync_metrics
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

Chunkها با `NTILE` روی SQL Server ساخته می‌شوند (همان الگوی MSSQL→MySQL).

---

## ۶. `delete_missing` (حذف اسناد غایب در محدوده)

وقتی `delete_missing=True`:

1. یک collection موقت keys در `staging_schema` ساخته می‌شود
2. کلیدهای هر batch در staging جمع می‌شوند
3. اسناد مقصد در محدودهٔ `delete_scope_column` بین `min_key` و `max_key` که در staging نیستند حذف می‌شوند

همیشه `delete_scope_column` (یا `chunk_column`) را مشخص کنید.

---

## ۷. عیب‌یابی سریع

| مشکل | بررسی |
|------|--------|
| DAG ظاهر نمی‌شود | import path، `template.mssql_to_mongo_sync_dag_factory`، نصب `pipeline` |
| خطای Mongo ping | URI / authSource / TLS در Airflow Connection |
| Duplicate `_id` | یکتا بودن `primary_keys` و نگاشت `id`→`_id` |
| Chunkها گیر کرده‌اند | اندازه pool و `max_global_parallel_chunks` |

---

## فایل‌های مرتبط

| مسیر | نقش |
|------|-----|
| `dags/template/mssql_to_mongo_sync_dag_factory.py` | factory |
| `dags/mssql_to_mongo_sync/example_table_to_mongo_sync.py` | نمونه |
| `pipeline/core/MSSQLToMongoDBQueryOrchestrator.py` | orchestrator |
| `pipeline/database/MongoDBServerWriter.py` | writer |
| `pipeline/database/MongoDBConnectionFactory.py` | اتصال |
| `docs/MONGO_TO_MSSQL_SYNC_GUIDE.md` | مسیر معکوس |
