# راهنمای ایجاد DAG برای Sync داده از MSSQL به PostgreSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **SQL Server** به **PostgreSQL** را توضیح می‌دهد. داده با یک query از MSSQL خوانده می‌شود و با **staging upsert** در جدول مقصد PostgreSQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌──────────────────────┐
│  MSSQL (Source)      │                      │  PostgreSQL (Target) │
│  mssql_* conn        │                      │  postgres_* conn     │
└──────────┬───────────┘                      └──────────▲───────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging UPDATE+INSERT)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToPostgreSQLQueryOrchestrator → PG Writer │
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_postgresql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_postgresql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MSSQLToPostgreSQLQueryOrchestrator.py` | خواندن MSSQL + upsert به PostgreSQL |
| **Reader / Writer** | `pipeline/database/MSSQLDataReader.py`، `PostgreSQLServerWriter.py` | stream و staging upsert |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_postgresql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_dwh_primary` | منبع — SQL Server |
| `postgres_target_default` | مقصد — PostgreSQL (قابل تغییر) |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است.

Provider لازم روی ایمیج Airflow: `apache-airflow-providers-postgres` (+ `psycopg2`).

### Pool

```bash
airflow pools set mssql_to_postgresql_sync_pool 32 "MSSQL to PostgreSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `postgres_staging_schema` | `staging` | schema موقت staging در PostgreSQL |
| `mssql_source_conn_id` | `mssql_dwh_primary` | Airflow conn منبع |
| `postgres_target_conn_id` | `postgres_target_default` | Airflow conn مقصد |

> در PostgreSQL، `target_schema` و `staging_schema` نام **schema** واقعی هستند (مثلاً `public`، `staging`). schema مربوط به staging باید از قبل وجود داشته باشد.

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب نام فایل و `dag_id`

- **فایل:** `dags/mssql_to_postgresql_sync/<source_or_table>_to_postgresql_sync.py`
- **`dag_id`:** `mssql_<name>_to_postgresql_sync` (snake_case)

### گام ۲ — کپی از DAG نمونه

- `dags/mssql_to_postgresql_sync/example_table_to_postgresql_sync.py`

برای جداول بزرگ `use_dynamic_tasks=True` را فعال کنید.

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_postgresql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_postgresql_sync',
    description='sync MSSQL dbo.Products to PostgreSQL public.products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    retries=2,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=8),
    tags=["mssql", "postgresql", "replication", "mssql-to-postgresql"],
    pool="mssql_to_postgresql_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_dwh_primary"),
    postgres_conn_id=Variable.get("postgres_target_conn_id", default_var="postgres_target_default"),
)

sync_config = MasterDataSyncConfig(
    source_name="mssql_products",
    source_query="SELECT id, code, name, updated_at FROM dbo.Products",
    source_query_count="SELECT COUNT(1) AS CNT FROM dbo.Products",
    primary_keys=("id",),
    target_schema="public",
    target_table="products",
    staging_schema="staging",
    delete_missing=False,
    use_hash_change_detection=True,
    batch_size=10000,
)

dag = create_dag(dag_config, sync_config, conn_config)
```

فایل را در `dags/mssql_to_postgresql_sync/` ذخیره کنید.

---

## ۴. جریان Taskها

بدون chunk:

```
validation → sync_mssql_to_postgresql → report_sync_metrics
```

با chunk (`use_dynamic_tasks=True`):

```
validation → create_sync_chunks → sync_mssql_to_postgresql_chunk (×N) → report_sync_metrics
```

---

## ۵. Chunking برای جداول بزرگ

```python
sync_config = MasterDataSyncConfig(
    # ...
    use_dynamic_tasks=True,
    chunk_column="id",
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=32,
)
```

Chunking روی **منبع MSSQL** با `NTILE` انجام می‌شود؛ دیالکت مقصد فقط در writer است.

---

## ۶. delete_missing (حذف یتیم‌ها)

اگر `delete_missing=True` باشد، ردیف‌هایی در محدودهٔ `delete_scope_column` که در منبع نیستند از مقصد حذف می‌شوند. حتماً `delete_scope_column` یا `chunk_column` را تنظیم کنید.

---

## ۷. نکات PostgreSQL

| موضوع | توضیح |
|--------|--------|
| Quoting | شناسه‌ها با `"double quotes"` |
| Staging | `CREATE TABLE ... (LIKE target INCLUDING DEFAULTS)` |
| Upsert | `UPDATE ... FROM staging` + `INSERT ... WHERE NOT EXISTS` |
| Hash | `md5(concat_ws(...))` (بدون نیاز به pgcrypto) |
| Deadlock | SQLSTATE `40P01` با retry |
| Schema | جدول مقصد و schemaی `staging` باید از قبل ساخته شده باشند |

---

## ۸. چک‌لیست قبل از Run

- [ ] Connectionهای `mssql_*` و `postgres_*` در Airflow تعریف شده‌اند
- [ ] pool `mssql_to_postgresql_sync_pool` ایجاد شده
- [ ] جدول مقصد در PostgreSQL وجود دارد و PK با `primary_keys` هم‌خوان است
- [ ] schema مربوط به `staging_schema` ساخته شده
- [ ] ستون‌های SELECT منبع با ستون‌های جدول مقصد هم‌نام هستند

---

## ۹. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mssql_to_postgresql_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MSSQLToPostgreSQLQueryOrchestrator.py` | منطق خواندن MSSQL / نوشتن PostgreSQL |
| `pipeline/database/MSSQLDataReader.py` | stream از MSSQL |
| `pipeline/database/PostgreSQLServerWriter.py` | upsert / delete_missing در مقصد |
| `pipeline/database/PostgreSQLConnectionFactory.py` | اتصال PostgreSQL |
| `pipeline/config/MasterDataSyncConfig.py` | تعریف پارامترهای sync |
| `pipeline/config/ConnectionConfig.py` | `mssql_conn_id` + `postgres_conn_id` |
| `dags/mssql_to_postgresql_sync/example_table_to_postgresql_sync.py` | نمونه کامل |
| `docs/MSSQL_TO_MYSQL_SYNC_GUIDE.md` | الگوی مشابه (MSSQL → MySQL) |
