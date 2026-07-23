# راهنمای ایجاد DAG برای Sync داده از PostgreSQL به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **PostgreSQL** به **SQL Server** را توضیح می‌دهد. داده با یک query از PostgreSQL خوانده می‌شود و با **upsert / MERGE** در جدول مقصد MSSQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  PostgreSQL (Source) │                      │  MSSQL (Target)     │
│  postgres_* conn     │                      │  mssql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  PostgreSQLDataReader → PostgreSQLToMSSQLQueryOrchestrator        │
│                       → MSSQLServerWriter                         │
└───────────────────────────────────────────────────────────────────┘
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/postgresql_to_mssql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/postgresql_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/PostgreSQLToMSSQLQueryOrchestrator.py` | خواندن PostgreSQL + upsert به MSSQL |
| **Reader / Writer** | `pipeline/database/PostgreSQLDataReader.py`، `MSSQLServerWriter.py` | stream و MERGE |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/postgresql_to_mssql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `postgres_source_default` | منبع — PostgreSQL |
| `mssql_dwh_primary` | مقصد — SQL Server (قابل تغییر) |

Provider لازم روی ایمیج Airflow: `apache-airflow-providers-postgres` (+ `psycopg2`).

### Pool

```bash
airflow pools set postgresql_to_mssql_sync_pool 32 "PostgreSQL to MSSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در MSSQL |
| `postgres_source_conn_id` | `postgres_source_default` | Airflow conn منبع |
| `mssql_target_conn_id` | `mssql_dwh_primary` | Airflow conn مقصد |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/postgresql_to_mssql_sync/<source_or_table>_to_mssql_sync.py`
- **`dag_id`:** `postgresql_<name>_to_mssql_sync`

### گام ۲ — کپی از DAG نمونه

`dags/postgresql_to_mssql_sync/example_table_to_mssql_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.postgresql_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='postgresql_products_to_mssql_sync',
    description='sync PostgreSQL products to MSSQL dbo.Products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    tags=["postgresql", "mssql", "replication", "postgresql-to-mssql"],
    pool="postgresql_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    postgres_conn_id=Variable.get("postgres_source_conn_id", default_var="postgres_source_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_dwh_primary"),
)

sync_config = MasterDataSyncConfig(
    source_name='postgresql_products',
    source_query="""
        SELECT id, sku, name, updated_at
        FROM public.products
    """,
    source_query_count="""
        SELECT COUNT(1) AS CNT
        FROM public.products
    """,
    primary_keys=('id',),
    target_schema='dbo',
    target_table='Products',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    use_hash_change_detection=True,
    delete_missing=False,
    batch_size=10000,
)

dag = create_dag(dag_config=dag_config, sync_config=sync_config, conn_config=conn_config)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت PostgreSQL** باشد (نه T-SQL).
> - شناسه‌های رزرو‌شده را با double-quote بگیرید؛ مثلاً `"name"`، `"user"`.
> - `primary_keys` کلید MERGE در **MSSQL** است.
> - `postgres_conn_id` و `mssql_conn_id` هر دو **الزامی** هستند.

---

## ۴. انتخاب حالت Sync

### حالت ساده

```
validation → sync_postgresql_to_mssql → report_sync_metrics
```

### حالت Chunk موازی (جداول بزرگ)

```python
use_dynamic_tasks=True,
chunk_column='id',
task_chunk_size=100_000,
max_parallel_chunks=8,
```

```
validation → create_sync_chunks → sync_postgresql_to_mssql_chunk (×N) → report_sync_metrics
```

Chunking از `NTILE` در PostgreSQL استفاده می‌کند.

---

## ۵. delete_missing

با `delete_missing=True` ردیف‌هایی که در منبع نیستند ولی در محدودهٔ `delete_scope_column` در مقصد هستند حذف می‌شوند (scoped delete، نه truncate کامل جدول).

---

## ۶. عیب‌یابی سریع

| مشکل | بررسی |
|------|--------|
| خطای connection | `postgres_*` و `mssql_*` در Airflow Connections |
| خطای identifier | quoting با `"..."` در PostgreSQL |
| deadlock / timeout روی مقصد | کاهش `batch_size` یا `max_parallel_chunks` |
| pool slots پر | افزایش slotهای `postgresql_to_mssql_sync_pool` |
