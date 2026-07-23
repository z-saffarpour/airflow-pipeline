# راهنمای ایجاد DAG برای Sync داده از MSSQL به Kafka

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **SQL Server** به **Apache Kafka** را توضیح می‌دهد. داده با یک query از MSSQL خوانده می‌شود و با **idempotent producer** (به‌همراه `version_id`) به topic مقصد ارسال می‌شود.

> **تفاوت با مسیر Kafka±ClickHouse:** مسیر `mssql_to_kafka_clickhouse_sync` می‌تواند همزمان به Kafka و ClickHouse بنویسد و از `SyncConfig` / `TableConfiguration` استفاده می‌کند. این راهنما برای **sync اختصاصی MSSQL → Kafka** است (مشابه الگوی MSSQL→ClickHouse / MSSQL→MySQL) با AuditLogger و chunk موازی NTILE.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MSSQL (Source)      │                      │  Kafka (Target)     │
│  mssql_* conn        │                      │  kafka_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  produce batch
           ▼                                             │  (idempotent)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToKafkaQueryOrchestrator                  │
│                  → IdempotentKafkaProducer                        │
└───────────────────────────────────────────────────────────────────┘
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_kafka_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_kafka_sync_dag_factory.py` | ساخت TaskGroupهای validation / setup / processing |
| **Orchestrator** | `pipeline/core/MSSQLToKafkaQueryOrchestrator.py` | خواندن MSSQL + produce به Kafka |
| **Reader / Producer** | `MSSQLDataReader.py`، `IdempotentKafkaProducer.py` | stream و produce |
| **Config** | `MSSQLToKafkaSyncConfig` + `KafkaTopicConfig` | query، key، topic |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_kafka_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_default` | منبع — SQL Server |
| `kafka_default` | مقصد — Kafka brokers |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است.

### Pool

```bash
airflow pools set mssql_to_kafka_sync_pool 32 "MSSQL to Kafka chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_source_conn_id` | `mssql_default` | Airflow conn منبع |
| `kafka_target_conn_id` | `kafka_default` | Airflow conn مقصد |

### Topic مقصد

اگر topic از قبل وجود نداشته باشد، task `ensure_kafka_topic` آن را با `num_partitions` و `replication_factor` از `KafkaTopicConfig` می‌سازد.

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/mssql_to_kafka_sync/<source_or_table>_to_kafka_sync.py`
- **`dag_id`:** `mssql_<name>_to_kafka_sync` (snake_case)

مثال: جدول MSSQL `Products` → فایل `products_to_kafka_sync.py` و `dag_id = 'mssql_products_to_kafka_sync'`

### گام ۲ — کپی از DAG نمونه

الگو: `dags/mssql_to_kafka_sync/example_table_to_kafka_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید.

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig
from pipeline.config.MSSQLToKafkaSyncConfig import MSSQLToKafkaSyncConfig
from template.mssql_to_kafka_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_kafka_sync',
    description='sync MSSQL dbo.Products to Kafka topic',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mssql_products_kafka", default_var=1)),
    retries=int(Variable.get("retries_mssql_products_kafka", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mssql_products_kafka", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mssql_products_kafka", default_var=8))),
    tags=["mssql", "kafka", "sync", "mssql-to-kafka"],
    pool="mssql_to_kafka_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_default"),
    kafka_conn_id=Variable.get("kafka_target_conn_id", default_var="kafka_default"),
)

kafka_topic_config = KafkaTopicConfig(
    name="dwh.table.curated.products",
    num_partitions=3,
    replication_factor=3,
)

sync_config = MSSQLToKafkaSyncConfig(
    source_name='mssql_products',
    source_query="""
        SELECT id, sku, name, updated_at
        FROM dbo.Products
    """,
    source_query_count="""
        SELECT COUNT(1) AS CNT
        FROM dbo.Products
    """,
    key_column='id',
    primary_keys=('id',),
    batch_size=int(Variable.get("batch_size_mssql_products_kafka", default_var=10000)),
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    kafka_topic_config=kafka_topic_config,
)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت T-SQL** باشد.
> - `key_column` باید در SELECT باشد (کلید پیام Kafka).
> - هر ردیف یک `version_id` عددی از `execution_date` می‌گیرد.

---

## ۴. پارامترهای مهم `MSSQLToKafkaSyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از MSSQL |
| `source_query_count` | بله | شمارش ردیف‌ها روی همان منبع/فیلتر |
| `key_column` | بله | ستون کلید پیام Kafka |
| `primary_keys` | توصیه‌شده | برای chunking (اگر `chunk_column` نباشد) |
| `use_dynamic_tasks` | خیر | فعال‌سازی sync موازی بر اساس chunk |
| `chunk_column` | اگر dynamic | معمولاً همان PK / key |
| `task_chunk_size` | اگر dynamic | تعداد ردیف در هر chunk |
| `max_parallel_chunks` | اگر dynamic | chunk همزمان در یک DAG run |
| `max_global_parallel_chunks` | اگر dynamic | سقف chunk همزمان در کل DAGها |
| `batch_size` | خیر | اندازه batch خواندن/نوشتن |
| `date_offset` | خیر | جابه‌جایی `execution_date` |

---

## ۵. انتخاب حالت Sync

### حالت ساده (جدول کوچک)

`use_dynamic_tasks` را تنظیم نکنید (پیش‌فرض `False`).

```
validation → ensure_kafka_topic → sync_mssql_to_kafka → report_sync_metrics
```

### حالت Chunk موازی (جدول بزرگ)

```python
sync_config = MSSQLToKafkaSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=32,
)
```

```
validation → ensure_kafka_topic → create_sync_chunks → sync_mssql_to_kafka_chunk (×N) → report_sync_metrics
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
- تست اتصال Kafka (`conn_config.kafka_conn_id`)

### Setup
- ایجاد topic در صورت نبود (`KafkaTopicManager.ensure_topic_exists`)

### Processing
- خواندن query از MSSQL به‌صورت stream/batch
- produce به topic مقصد (+ `version_id`)
- گزارش متریک (produced / records)

---

## ۷. تفاوت با مسیر Kafka ± ClickHouse

| مورد | MSSQL → Kafka ± CH | MSSQL → Kafka (این راهنما) |
|------|--------------------|----------------------------|
| Factory | `mssql_to_kafka_clickhouse_sync_dag_factory` / `table_mssql_sync_dag_factory` | `mssql_to_kafka_sync_dag_factory` |
| Config | `SyncConfig` + `QueryConfiguration` / `TableConfiguration` | `MSSQLToKafkaSyncConfig` + `KafkaTopicConfig` |
| ClickHouse | اختیاری (`is_send_clickhouse`) | ندارد |
| Chunk موازی NTILE | ندارد | دارد (`use_dynamic_tasks`) |
| AuditLogger | محدود | کامل (validation / chunk / transfer) |
| Orchestrator | `MSSQLDataTransferOrchestrator` | `MSSQLToKafkaQueryOrchestrator` |

اگر فقط sink به Kafka می‌خواهید و chunking/audit مشابه مسیرهای Gen-2 لازم است، از **همین factory** استفاده کنید.

---

## ۸. چک‌لیست قبل از Production

- [ ] Connectionهای MSSQL و Kafka در Airflow تعریف و تست شده‌اند
- [ ] `source_query` دیالکت T-SQL است و `key_column` در SELECT هست
- [ ] `KafkaTopicConfig` (نام / partitions / replication) درست است
- [ ] `source_query_count` روی همان منبع / همان فیلتر است
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده
- [ ] pool `mssql_to_kafka_sync_pool` در Airflow ایجاد شده
- [ ] DAG در محیط test اجرا و متریک produced بررسی شده

---

## ۹. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `mssql_conn_id is required` | `ConnectionConfig` ناقص | هر دو `mssql_conn_id` و `kafka_conn_id` را ست کنید |
| Kafka validation failed | conn اشتباه / فایروال | Connection UI و bootstrap_servers را چک کنید |
| Topic create failed | ACL / replication | دسترسی AdminClient و replication_factor را بررسی کنید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool slots | pool را بزرگ‌تر کنید یا chunk را کم کنید |
| پیام بدون key | `key_column` در ردیف نیست | نام ستون را با SELECT هم‌تراز کنید |

---

## ۱۰. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mssql_to_kafka_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MSSQLToKafkaQueryOrchestrator.py` | منطق خواندن MSSQL / produce Kafka |
| `pipeline/database/MSSQLDataReader.py` | stream از MSSQL |
| `pipeline/kafka/IdempotentKafkaProducer.py` | produce idempotent |
| `pipeline/kafka/KafkaTopicManager.py` | ایجاد topic |
| `pipeline/config/MSSQLToKafkaSyncConfig.py` | پارامترهای sync |
| `pipeline/config/KafkaTopicConfig.py` | نام / partitions topic |
| `pipeline/config/ConnectionConfig.py` | `mssql_conn_id` + `kafka_conn_id` |
| `dags/mssql_to_kafka_sync/example_table_to_kafka_sync.py` | نمونه کامل |
| `docs/MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md` | الگوی مشابه (MSSQL → ClickHouse) |
| `docs/MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md` | مسیر Gen-1 Kafka ± CH |
| `docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md` | مسیر معکوس |
