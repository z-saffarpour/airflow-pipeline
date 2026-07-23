# راهنمای ایجاد DAG برای Sync داده از ClickHouse به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **ClickHouse** به **SQL Server** را توضیح می‌دهد. داده با یک query از ClickHouse خوانده می‌شود و با **upsert / MERGE** در جدول مقصد MSSQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  ClickHouse (Source) │                      │  MSSQL (Target)     │
│  clickhouse_* conn   │                      │  mssql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  ClickHouseDataReader → ClickHouseToMSSQLQueryOrchestrator        │
│                       → MSSQLServerWriter                         │
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/clickhouse_to_mssql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/clickhouse_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/ClickHouseToMSSQLQueryOrchestrator.py` | خواندن ClickHouse + upsert به MSSQL |
| **Reader / Writer** | `pipeline/database/ClickHouseDataReader.py`، `MSSQLServerWriter.py` | stream و MERGE |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/clickhouse_to_mssql_sync/`** کافی است.

> **نکته:** مسیر **Kafka → MSSQL** از قبل آماده است (`kafka_to_mssql_sync_dag_factory` + `docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md`). این راهنما فقط ClickHouse→MSSQL است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `clickhouse_default` | منبع — ClickHouse |
| `mssql_default` | مقصد — SQL Server (قابل تغییر) |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است.

بستهٔ لازم: `clickhouse-driver` (در `requirements.txt` پروژه موجود است).

### Pool

```bash
airflow pools set clickhouse_to_mssql_sync_pool 32 "ClickHouse to MSSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در MSSQL |
| `clickhouse_source_conn_id` | `clickhouse_default` | Airflow conn منبع |
| `mssql_target_conn_id` | `mssql_default` | Airflow conn مقصد |
| `max_global_parallel_chunks_clickhouse_to_mssql` | `32` | سقف chunk همزمان |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/clickhouse_to_mssql_sync/<source_or_table>_to_mssql_sync.py`
- **`dag_id`:** `clickhouse_<name>_to_mssql_sync` (snake_case)

مثال: جدول `products` → `products_to_mssql_sync.py` و `dag_id = 'clickhouse_products_to_mssql_sync'`

### گام ۲ — کپی از DAG نمونه

- `dags/clickhouse_to_mssql_sync/example_table_to_mssql_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.clickhouse_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='clickhouse_products_to_mssql_sync',
    description='sync ClickHouse products to MSSQL dbo.Products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_clickhouse_products", default_var=1)),
    retries=int(Variable.get("retries_clickhouse_products", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_clickhouse_products", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_clickhouse_products", default_var=8))),
    tags=["clickhouse", "mssql", "clickhouse-sync"],
    pool="clickhouse_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    clickhouse_conn_id=Variable.get("clickhouse_source_conn_id", default_var="clickhouse_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_default"),
)

sync_config = MasterDataSyncConfig(
    source_name='clickhouse_products',
    source_query="""
        SELECT id, sku, name, updated_at
        FROM products
    """,
    source_query_count="""
        SELECT count() AS CNT
        FROM products
    """,
    primary_keys=('id',),
    target_schema='dbo',
    target_table='Products',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    use_hash_change_detection=True,
    delete_missing=bool(int(Variable.get("delete_missing_clickhouse_products", default_var=0))),
    delete_scope_column='id',
    batch_size=int(Variable.get("batch_size_clickhouse_products", default_var=10000)),
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت ClickHouse** باشد (نه T-SQL).
> - برای شمارش از `count()` استفاده کنید.
> - شناسه‌ها را با backtick بگیرید؛ مثلاً `` `name` ``.
> - نام و نوع ستون‌های SELECT باید با جدول مقصد MSSQL هم‌خوان باشند.
> - `primary_keys` کلید MERGE در **MSSQL** است.

---

## ۴. پارامترهای مهم `MasterDataSyncConfig`

همان dataclass مشترک مسیرهای upsert به MSSQL:

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از ClickHouse |
| `source_query_count` | بله | شمارش ردیف‌ها |
| `primary_keys` | بله | کلید merge در MSSQL |
| `target_schema` / `target_table` | بله | مقصد در SQL Server |
| `use_dynamic_tasks` | خیر | sync موازی با chunk |
| `chunk_column` | اگر dynamic | معمولاً PK |
| `task_chunk_size` | اگر dynamic | ردیف در هر chunk |
| `delete_missing` | خیر | حذف scoped در مقصد |
| `batch_size` | خیر | اندازه batch |

---

## ۵. انتخاب حالت Sync

### حالت ساده

```
validation → sync_clickhouse_to_mssql → report_sync_metrics
```

### حالت Chunk موازی (جدول بزرگ)

نیاز به window function `ntile` در ClickHouse:

```python
sync_config = MasterDataSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=int(
        Variable.get("max_global_parallel_chunks_clickhouse_to_mssql", default_var=32)
    ),
)
```

```
validation → create_sync_chunks → sync_clickhouse_to_mssql_chunk (×N) → report_sync_metrics
```

### فیلتر افزایشی (اختیاری)

```sql
SELECT id, sku, name, updated_at
FROM products
WHERE updated_at >= today() - 1
```

همان شرط را در `source_query_count` تکرار کنید.

---

## ۶. جریان اجرای DAG

### Validation
- تست اتصال ClickHouse
- تست اتصال MSSQL

### Processing
- stream/batch از ClickHouse
- staging + MERGE در MSSQL
- در صورت `delete_missing`: حذف scoped
- گزارش متریک

---

## ۷. چک‌لیست قبل از Production

- [ ] Connectionهای ClickHouse و MSSQL تست شده‌اند
- [ ] `source_query` دیالکت ClickHouse است
- [ ] `primary_keys` با schema واقعی MSSQL یکی است
- [ ] جدول مقصد و `staging_schema` در MSSQL وجود دارند
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` تنظیم شده
- [ ] pool `clickhouse_to_mssql_sync_pool` ایجاد شده
- [ ] در test متریک‌های inserted/updated/deleted بررسی شده

---

## ۸. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `clickhouse_conn_id is required` | `ConnectionConfig` ناقص | هر دو conn را ست کنید |
| ClickHouse validation failed | conn / فایروال / driver | Connection UI و `clickhouse-driver` را چک کنید |
| Syntax error / unknown function | query به سبک T-SQL یا MySQL نوشته شده | به دیالکت ClickHouse تبدیل کنید |
| `ntile` error | نسخهٔ قدیمی ClickHouse | بدون `use_dynamic_tasks` sync کنید یا CH را ارتقا دهید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool | pool را بزرگ‌تر کنید |

---

## ۹. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/clickhouse_to_mssql_sync_dag_factory.py` | Factory |
| `pipeline/core/ClickHouseToMSSQLQueryOrchestrator.py` | منطق sync |
| `pipeline/database/ClickHouseDataReader.py` | stream از ClickHouse |
| `pipeline/database/ClickHouseConnectionFactory.py` | اتصال |
| `pipeline/database/MSSQLServerWriter.py` | upsert مقصد |
| `dags/clickhouse_to_mssql_sync/example_table_to_mssql_sync.py` | نمونه |
| `docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md` | مسیر مرتبط Kafka→MSSQL |
| `docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md` | الگوی مشابه |
