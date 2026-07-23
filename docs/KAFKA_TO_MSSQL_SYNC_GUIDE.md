# راهنمای ایجاد DAG برای Sync داده از Kafka به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **Kafka** به **SQL Server** را توضیح می‌دهد. پیام‌های JSON از یک topic خوانده می‌شوند و با **upsert / MERGE** در جدول مقصد MSSQL نوشته می‌شوند.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  Kafka (Source)      │                      │  MSSQL (Target)     │
│  kafka_* conn        │                      │  mssql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  consume JSON batches + commit offsets      │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  KafkaDataConsumer → KafkaToMSSQLQueryOrchestrator → MSSQLWriter  │
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

**Semantics:** at-least-once — آفست Kafka فقط بعد از MERGE موفق در MSSQL commit می‌شود.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Topic Sync** | `dags/kafka_to_mssql_sync/` | یک DAG به‌ازای هر topic |
| **Factory** | `dags/template/kafka_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/KafkaToMSSQLQueryOrchestrator.py` | consume Kafka + upsert به MSSQL |
| **Consumer / Writer** | `pipeline/kafka/KafkaDataConsumer.py`، `MSSQLServerWriter.py` | stream و MERGE |
| **Config** | `pipeline/config/KafkaSyncConfig.py` | پارامترهای topic + upsert |

برای افزودن topic جدید، معمولاً **فقط یک فایل در `dags/kafka_to_mssql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `kafka_default` | منبع — Kafka cluster |
| `mssql_default` | مقصد — SQL Server |

**تنظیم Connection Kafka:** همان تنظیمات موجود برای produce (bootstrap / SASL / SSL).

وابستگی: `confluent-kafka` (در `requirements.txt`).

### Pool

```bash
airflow pools set kafka_to_mssql_sync_pool 32 "Kafka to MSSQL partition sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد (فقط اگر `use_dynamic_tasks=True`).

### Variableهای مشترک

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در MSSQL |
| `kafka_source_conn_id` | `kafka_default` | Airflow conn منبع |
| `mssql_target_conn_id` | `mssql_default` | Airflow conn مقصد |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — نام فایل و `dag_id`

- **فایل:** `dags/kafka_to_mssql_sync/<name>_to_mssql_sync.py`
- **`dag_id`:** `kafka_<name>_to_mssql_sync`

مثال: topic `dwh.table.curated.dim_date` → `dim_date_to_mssql_sync.py` و `dag_id = 'kafka_dim_date_to_mssql_sync'`

### گام ۲ — کپی از DAG نمونه

`dags/kafka_to_mssql_sync/example_topic_to_mssql_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.KafkaSyncConfig import KafkaSyncConfig
from template.kafka_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='kafka_dim_date_to_mssql_sync',
    description='sync Kafka dim_date topic to MSSQL dbo.DimDate',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule='*/15 * * * *',  # یا None
    catchup=False,
    tags=["kafka", "mssql", "kafka-sync"],
    pool="kafka_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    kafka_conn_id=Variable.get("kafka_source_conn_id", default_var="kafka_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_default"),
)

sync_config = KafkaSyncConfig(
    source_name='kafka_dim_date',
    kafka_topic='dwh.table.curated.dim_date',
    consumer_group='mssql-sync-dim-date',
    primary_keys=('DateKey',),
    target_schema='dbo',
    target_table='DimDate',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    exclude_columns=('version_id',),
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
> - مقدار پیام باید JSON object باشد (همان فرمت `MessageSerializer.serialize_row`).
> - `exclude_columns` به‌صورت پیش‌فرض `version_id` را حذف می‌کند (فیلد تزریق‌شده توسط producer).
> - `primary_keys` کلید MERGE در **MSSQL** است.
> - `consumer_group` باید برای هر pipeline یکتا باشد تا آفست‌ها قاطی نشوند.

---

## ۴. پارامترهای مهم `KafkaSyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `kafka_topic` | بله | نام topic |
| `consumer_group` | بله | Kafka consumer group (offset store) |
| `primary_keys` | بله | کلید merge در MSSQL |
| `target_schema` / `target_table` | بله | مقصد در SQL Server |
| `auto_offset_reset` | خیر | `earliest` (پیش‌فرض) یا `latest` — فقط وقتی group جدید است |
| `max_idle_polls` | خیر | توقف وقتی N poll خالی متوالی رخ دهد |
| `max_messages_per_run` | خیر | سقف پیام در هر DAG run (اختیاری) |
| `value_columns` | خیر | فقط این ستون‌ها از JSON نگه داشته شوند |
| `exclude_columns` | خیر | پیش‌فرض `("version_id",)` |
| `use_dynamic_tasks` | خیر | یک task به‌ازای هر partition |
| `delete_missing` | خیر | معمولاً برای streaming خاموش؛ برای snapshot کامل با احتیاط |
| `batch_size` | خیر | اندازه batch |

---

## ۵. انتخاب حالت Sync

### حالت ساده (پیشنهادی)

```
validation → sync_kafka_to_mssql → report_sync_metrics
```

Consumer همهٔ partitionها را با `subscribe` می‌گیرد و تا idle ادامه می‌دهد.

### حالت موازی per-partition

```python
sync_config = KafkaSyncConfig(
    # ...
    use_dynamic_tasks=True,
    max_parallel_chunks=8,
)
```

```
validation → create_sync_chunks → sync_kafka_to_mssql_chunk (×N) → report_sync_metrics
```

هر chunk یک partition را با `assign` مصرف می‌کند (همان `consumer_group`).

---

## ۶. قرارداد پیام (سازگار با MSSQL→Kafka)

Producer فعلی (`IdempotentKafkaProducer.send_batch_to_kafka`) پیام را چنین می‌فرستد:

- **Value:** JSON row (+ `version_id`)
- **Key:** از `key_column`
- **Headers:** `source_table`, `execution_date`, `timestamp`, `batch_number`

Consumer فقط **value** را deserialize می‌کند و `version_id` را حذف می‌کند.

---

## ۷. چک‌لیست قبل از Production

- [ ] Connectionهای Kafka و MSSQL در Airflow تعریف و تست شده‌اند
- [ ] Topic وجود دارد و پیام‌ها JSON object هستند
- [ ] ستون‌های JSON با جدول مقصد MSSQL هم‌نام‌اند
- [ ] `primary_keys` با schema MSSQL یکی است
- [ ] `consumer_group` یکتا و پایدار است
- [ ] برای topic پر-partition، در صورت نیاز `use_dynamic_tasks=True`
- [ ] pool `kafka_to_mssql_sync_pool` ایجاد شده
- [ ] `delete_missing` فقط وقتی معنی دارد که run یک snapshot کامل باشد

---

## ۸. تفاوت با Mongo / MySQL → MSSQL

| مورد | MySQL/Mongo → MSSQL | Kafka → MSSQL |
|------|---------------------|---------------|
| منبع | Query / Collection | Kafka topic |
| Config | `MasterDataSyncConfig` / `MongoSyncConfig` | `KafkaSyncConfig` |
| Factory | `mysql_` / `mongo_to_mssql_sync_dag_factory` | `kafka_to_mssql_sync_dag_factory` |
| Orchestrator | MySQL/Mongo orchestrator | `KafkaToMSSQLQueryOrchestrator` |
| پیشرفت sync | کل query/collection | Kafka consumer group offsets |
| Chunk plan | NTILE / `$bucketAuto` | Kafka partitions |
| Writer مقصد | `MSSQLServerWriter` | همان |

---

## ۹. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/kafka_to_mssql_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/KafkaToMSSQLQueryOrchestrator.py` | منطق sync |
| `pipeline/kafka/KafkaDataConsumer.py` | stream از Kafka |
| `pipeline/config/KafkaSyncConfig.py` | تنظیمات sync |
| `dags/kafka_to_mssql_sync/example_topic_to_mssql_sync.py` | نمونه کامل |
| `docs/MONGO_TO_MSSQL_SYNC_GUIDE.md` | الگوی مشابه Mongo |
| `docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md` | الگوی مشابه MySQL |
