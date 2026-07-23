# راهنمای ایجاد DAG برای Sync داده از MSSQL به ClickHouse

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **SQL Server** به **ClickHouse** را توضیح می‌دهد. داده با یک query از MSSQL خوانده می‌شود و با **bulk INSERT** (سازگار با ReplacingMergeTree و `version_id`) در جدول مقصد ClickHouse نوشته می‌شود.

> **تفاوت با مسیر Kafka±ClickHouse:** مسیر `mssql_to_kafka_clickhouse_sync` می‌تواند همزمان به Kafka و ClickHouse بنویسد. این راهنما برای **sync اختصاصی MSSQL → ClickHouse** بدون Kafka است (مشابه الگوی MSSQL→MySQL / MSSQL→MSSQL).

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MSSQL (Source)      │                      │  ClickHouse (Target)│
│  mssql_* conn        │                      │  clickhouse_* conn  │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (bulk INSERT)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToClickHouseQueryOrchestrator             │
│                  → ClickHouseWriter                               │
└───────────────────────────────────────────────────────────────────┘
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_clickhouse_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_clickhouse_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MSSQLToClickHouseQueryOrchestrator.py` | خواندن MSSQL + INSERT به ClickHouse |
| **Reader / Writer** | `pipeline/database/MSSQLDataReader.py`، `ClickHouseWriter.py` | stream و bulk INSERT |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_clickhouse_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_dwh_primary` | منبع — SQL Server |
| `clickhouse_default` | مقصد — ClickHouse |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است.

وابستگی: `clickhouse-driver` (در `requirements.txt` موجود است).

### Pool

```bash
airflow pools set mssql_to_clickhouse_sync_pool 32 "MSSQL to ClickHouse chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_source_conn_id` | `mssql_dwh_primary` | Airflow conn منبع |
| `clickhouse_target_conn_id` | `clickhouse_default` | Airflow conn مقصد |
| `max_global_parallel_chunks_mssql_to_ch` | `32` | سقف chunk همزمان (در صورت تعریف) |

> در ClickHouse، `target_schema` همان **نام database** است.

### جدول مقصد

جدول ClickHouse باید از قبل وجود داشته باشد. برای رفتار upsert، موتورهایی مثل **ReplacingMergeTree** با ستون `version_id` توصیه می‌شود؛ orchestrator در هر run یک `version_id` عددی به ردیف‌ها اضافه می‌کند (اگر در SELECT نباشد).

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/mssql_to_clickhouse_sync/<source_or_table>_to_clickhouse_sync.py`
- **`dag_id`:** `mssql_<name>_to_clickhouse_sync` (snake_case)

مثال: جدول MSSQL `Products` → فایل `products_to_clickhouse_sync.py` و `dag_id = 'mssql_products_to_clickhouse_sync'`

### گام ۲ — کپی از DAG نمونه

الگو: `dags/mssql_to_clickhouse_sync/example_table_to_clickhouse_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید.

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_clickhouse_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_clickhouse_sync',
    description='sync MSSQL dbo.Products to ClickHouse default.products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mssql_products_ch", default_var=1)),
    retries=int(Variable.get("retries_mssql_products_ch", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mssql_products_ch", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mssql_products_ch", default_var=8))),
    tags=["mssql", "clickhouse", "sync", "mssql-to-clickhouse"],
    pool="mssql_to_clickhouse_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_dwh_primary"),
    clickhouse_conn_id=Variable.get("clickhouse_target_conn_id", default_var="clickhouse_default"),
)

sync_config = MasterDataSyncConfig(
    source_name='mssql_products',
    source_query="""
        SELECT id, sku, name, updated_at
        FROM dbo.Products
    """,
    source_query_count="""
        SELECT COUNT(1) AS CNT
        FROM dbo.Products
    """,
    primary_keys=('id',),
    target_schema='default',       # ClickHouse database
    target_table='products',
    use_hash_change_detection=False,
    delete_missing=False,
    batch_size=int(Variable.get("batch_size_mssql_products_ch", default_var=10000)),
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت T-SQL** باشد.
> - نام ستون‌های SELECT باید با جدول مقصد ClickHouse هم‌خوان باشند.
> - `delete_missing` و `use_hash_change_detection` در این مسیر اعمال نمی‌شوند (در صورت فعال بودن فقط warning/log می‌خورند).

---

## ۴. پارامترهای مهم `MasterDataSyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از MSSQL؛ ستون‌ها با مقصد CH هم‌خوان |
| `source_query_count` | بله | شمارش ردیف‌ها روی همان منبع/فیلتر |
| `primary_keys` | توصیه‌شده | برای chunking (اگر `chunk_column` نباشد) |
| `target_schema` / `target_table` | بله | database و table در ClickHouse |
| `use_dynamic_tasks` | خیر | فعال‌سازی sync موازی بر اساس chunk |
| `chunk_column` | اگر dynamic | معمولاً همان PK |
| `task_chunk_size` | اگر dynamic | تعداد ردیف در هر chunk |
| `max_parallel_chunks` | اگر dynamic | chunk همزمان در یک DAG run |
| `max_global_parallel_chunks` | اگر dynamic | سقف chunk همزمان در کل DAGها |
| `batch_size` | خیر | اندازه batch خواندن/نوشتن |

---

## ۵. انتخاب حالت Sync

### حالت ساده (جدول کوچک)

`use_dynamic_tasks` را تنظیم نکنید (پیش‌فرض `False`).

```
validation → sync_mssql_to_clickhouse → report_sync_metrics
```

### حالت Chunk موازی (جدول بزرگ)

```python
sync_config = MasterDataSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=32,
)
```

```
validation → create_sync_chunks → sync_mssql_to_clickhouse_chunk (×N) → report_sync_metrics
```

### فیلتر افزایشی (اختیاری)

```sql
SELECT id, sku, name, updated_at
FROM dbo.Products
WHERE updated_at >= DATEADD(day, -1, CAST(GETDATE() AS date))
```

همان شرط را در `source_query_count` تکرار کنید.

---

## ۶. جریان اجرای DAG

### Validation
- تست اتصال MSSQL (`conn_config.mssql_conn_id`)
- تست اتصال ClickHouse (`conn_config.clickhouse_conn_id`)

### Processing
- خواندن query از MSSQL به‌صورت stream/batch
- bulk INSERT به جدول مقصد ClickHouse (+ `version_id`)
- گزارش متریک (inserted / records)

---

## ۷. تفاوت با مسیر Kafka ± ClickHouse

| مورد | MSSQL → Kafka ± CH | MSSQL → ClickHouse (این راهنما) |
|------|--------------------|----------------------------------|
| Factory | `mssql_to_kafka_clickhouse_sync_dag_factory` | `mssql_to_clickhouse_sync_dag_factory` |
| Config | `SyncConfig` + `QueryConfiguration` / `TableConfiguration` | `MasterDataSyncConfig` |
| Kafka | اختیاری (`is_send_kafka`) | ندارد |
| Chunk موازی NTILE | ندارد | دارد (`use_dynamic_tasks`) |
| Orchestrator | `MSSQLDataTransferOrchestrator` | `MSSQLToClickHouseQueryOrchestrator` |

اگر فقط sink به ClickHouse می‌خواهید و chunking/audit مشابه مسیرهای replication لازم است، از **همین factory** استفاده کنید.

---

## ۸. چک‌لیست قبل از Production

- [ ] Connectionهای MSSQL و ClickHouse در Airflow تعریف و تست شده‌اند
- [ ] `source_query` دیالکت T-SQL است و ستون‌ها با مقصد CH هم‌خوان‌اند
- [ ] جدول مقصد در ClickHouse وجود دارد (ترجیحاً ReplacingMergeTree + `version_id`)
- [ ] `source_query_count` روی همان منبع / همان فیلتر است
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده
- [ ] pool `mssql_to_clickhouse_sync_pool` در Airflow ایجاد شده
- [ ] DAG در محیط test اجرا و متریک inserted بررسی شده

---

## ۹. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `mssql_conn_id is required` | `ConnectionConfig` ناقص | هر دو `mssql_conn_id` و `clickhouse_conn_id` را ست کنید |
| ClickHouse validation failed | conn اشتباه / فایروال | Connection UI و دسترسی شبکه را چک کنید |
| Column mismatch / INSERT failed | نام/نوع ستون اشتباه | SELECT و schema جدول CH را هم‌تراز کنید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool slots | pool را بزرگ‌تر کنید یا chunk را کم کنید |
| Duplicateهای موقت در CH | هنوز OPTIMIZE نشده | ReplacingMergeTree + OPTIMIZE یا `FINAL` در خواندن |

---

## ۱۰. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mssql_to_clickhouse_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MSSQLToClickHouseQueryOrchestrator.py` | منطق خواندن MSSQL / نوشتن CH |
| `pipeline/database/MSSQLDataReader.py` | stream از MSSQL |
| `pipeline/database/ClickHouseWriter.py` | bulk INSERT |
| `pipeline/config/MasterDataSyncConfig.py` | پارامترهای sync |
| `pipeline/config/ConnectionConfig.py` | `mssql_conn_id` + `clickhouse_conn_id` |
| `dags/mssql_to_clickhouse_sync/example_table_to_clickhouse_sync.py` | نمونه کامل |
| `docs/MSSQL_TO_MYSQL_SYNC_GUIDE.md` | الگوی مشابه (MSSQL → MySQL) |
| `docs/CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md` | مسیر معکوس |
