# راهنمای ایجاد DAG برای Sync داده از MongoDB به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **MongoDB** به **SQL Server** را توضیح می‌دهد. داده از یک collection (یا aggregation) خوانده می‌شود و با **upsert / MERGE** در جدول مقصد MSSQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MongoDB (Source)    │                      │  MSSQL (Target)     │
│  mongo_* conn        │                      │  mssql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  find / aggregate + stream batches           │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MongoDBDataReader → MongoDBToMSSQLQueryOrchestrator → MSSQLWriter│
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Collection Sync** | `dags/mongo_to_mssql_sync/` | یک DAG به‌ازای هر collection |
| **Factory** | `dags/template/mongo_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MongoDBToMSSQLQueryOrchestrator.py` | خواندن Mongo + upsert به MSSQL |
| **Reader / Writer** | `pipeline/database/MongoDBDataReader.py`، `MSSQLServerWriter.py` | stream و MERGE |
| **Config** | `pipeline/config/MongoSyncConfig.py` | پارامترهای collection + upsert |

برای افزودن collection جدید، معمولاً **فقط یک فایل در `dags/mongo_to_mssql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mongo_source_default` | منبع — MongoDB |
| `mssql_default` | مقصد — SQL Server |

**تنظیم Connection MongoDB:**

| فیلد | مقدار نمونه |
|------|-------------|
| Conn Type | `Generic` یا `MongoDB` |
| Host | `mongo.example.com` |
| Port | `27017` |
| Login / Password | در صورت نیاز |
| Schema | نام database پیش‌فرض |

**Extra JSON (نمونه):**

```json
{
  "authSource": "admin",
  "database": "myapp"
}
```

یا URI کامل:

```json
{
  "uri": "mongodb://user:pass@host:27017/?authSource=admin"
}
```

وابستگی: `pymongo` (در `requirements.txt`).

### Pool

```bash
airflow pools set mongo_to_mssql_sync_pool 32 "MongoDB to MSSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در MSSQL |
| `mongo_source_conn_id` | `mongo_source_default` | Airflow conn منبع |
| `mssql_target_conn_id` | `mssql_default` | Airflow conn مقصد |
| `mongo_source_database` | (خالی) | override نام database |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/mongo_to_mssql_sync/<name>_to_mssql_sync.py`
- **`dag_id`:** `mongo_<name>_to_mssql_sync`

مثال: collection `products` → `products_to_mssql_sync.py` و `dag_id = 'mongo_products_to_mssql_sync'`

### گام ۲ — کپی از DAG نمونه

`dags/mongo_to_mssql_sync/example_collection_to_mssql_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MongoSyncConfig import MongoSyncConfig
from template.mongo_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mongo_products_to_mssql_sync',
    description='sync MongoDB products to MSSQL dbo.Products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    tags=["mongo", "mssql", "mongo-sync"],
    pool="mongo_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    mongo_conn_id=Variable.get("mongo_source_conn_id", default_var="mongo_source_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_default"),
)

sync_config = MongoSyncConfig(
    source_name='mongo_products',
    collection='products',
    database='myapp',
    filter_query={"status": "active"},
    projection={"_id": 1, "sku": 1, "name": 1, "updated_at": 1},
    rename_id_to='id',
    primary_keys=('id',),
    target_schema='dbo',
    target_table='Products',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    use_hash_change_detection=True,
    batch_size=10000,
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

> **نکته‌ها:**
> - نام فیلدهای projection (پس از `rename_id_to`) باید با ستون‌های جدول MSSQL یکی باشد.
> - `_id` به‌صورت پیش‌فرض به `id` تغییر نام می‌دهد (`rename_id_to="id"`).
> - nested document/array به‌صورت JSON string ذخیره می‌شود (`serialize_nested=True`).
> - `primary_keys` کلید MERGE در **MSSQL** است.

---

## ۴. پارامترهای مهم `MongoSyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `collection` | بله | نام collection در MongoDB |
| `database` | خیر | override database از Connection |
| `filter_query` | خیر | فیلتر `find` (dict) |
| `projection` | خیر | projection فیلدها |
| `sort` | خیر | مثلاً `(("updated_at", 1),)` |
| `aggregation_pipeline` | خیر | به‌جای find؛ با `use_dynamic_tasks` سازگار نیست |
| `rename_id_to` | خیر | پیش‌فرض `"id"`؛ برای نگه‌داشتن `_id` مقدار `None` بگذارید |
| `primary_keys` | بله | کلید merge در MSSQL |
| `target_schema` / `target_table` | بله | مقصد در SQL Server |
| `use_dynamic_tasks` | خیر | chunk موازی با `$bucketAuto` |
| `chunk_column` | اگر dynamic | معمولاً `"id"` (maps به `_id`) |
| `delete_missing` | خیر | حذف scoped ردیف‌های اضافی در مقصد |
| `batch_size` | خیر | اندازه batch |

---

## ۵. انتخاب حالت Sync

### حالت ساده

```
validation → sync_mongo_to_mssql → report_sync_metrics
```

### حالت Chunk موازی (collection بزرگ)

```python
sync_config = MongoSyncConfig(
    # ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
)
```

```
validation → create_sync_chunks → sync_mongo_to_mssql_chunk (×N) → report_sync_metrics
```

Chunking با `$bucketAuto` انجام می‌شود (MongoDB 3.4+).

### فیلتر افزایشی

```python
filter_query={"updated_at": {"$gte": "2026-07-01T00:00:00Z"}}
```

---

## ۶. چک‌لیست قبل از Production

- [ ] Connectionهای Mongo و MSSQL در Airflow تعریف و تست شده‌اند
- [ ] `pymongo` روی worker نصب است
- [ ] projection فقط فیلدهای موجود در جدول مقصد را دارد
- [ ] `primary_keys` با schema MSSQL یکی است
- [ ] برای collection بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده
- [ ] pool `mongo_to_mssql_sync_pool` ایجاد شده
- [ ] اگر `delete_missing=True` است، `delete_scope_column` درست است

---

## ۷. تفاوت با MySQL → MSSQL

| مورد | MySQL → MSSQL | MongoDB → MSSQL |
|------|---------------|-----------------|
| منبع | MySQL query | Mongo collection / aggregation |
| Config | `MasterDataSyncConfig` | `MongoSyncConfig` |
| Factory | `mysql_to_mssql_sync_dag_factory` | `mongo_to_mssql_sync_dag_factory` |
| Orchestrator | `MySQLToMSSQLQueryOrchestrator` | `MongoDBToMSSQLQueryOrchestrator` |
| Chunk plan | NTILE (MySQL 8+) | `$bucketAuto` |
| Writer مقصد | `MSSQLServerWriter` | همان |

---

## ۸. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mongo_to_mssql_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MongoDBToMSSQLQueryOrchestrator.py` | منطق sync |
| `pipeline/database/MongoDBDataReader.py` | stream از Mongo |
| `pipeline/database/MongoDBConnectionFactory.py` | اتصال Mongo |
| `pipeline/config/MongoSyncConfig.py` | تنظیمات sync |
| `dags/mongo_to_mssql_sync/example_collection_to_mssql_sync.py` | نمونه کامل |
| `docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md` | الگوی مشابه MySQL |
