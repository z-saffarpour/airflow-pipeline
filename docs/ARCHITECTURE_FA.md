# معماری سیستم (Architecture)

مستند معماری پلتفرم **SQL Server Data Pipeline** مبتنی بر Apache Airflow.

---

## ۱. نمای کلی

پلتفرم مجموعه‌ای از DAGهای Airflow و بستهٔ مشترک `pipeline/` است که چند جریان داده را پوشش می‌دهد:

| جریان | منبع | مقصد |
|-------|------|------|
| Table / Query Sync | SQL Server (DWH / ERP) | Kafka (± ClickHouse) |
| MSSQL → Kafka / ClickHouse | SQL Server | Kafka یا ClickHouse |
| MySQL ↔ MSSQL | MySQL / SQL Server | SQL Server / MySQL |
| MSSQL ↔ PostgreSQL | SQL Server / PostgreSQL | PostgreSQL / SQL Server |
| MSSQL ↔ MongoDB | SQL Server / MongoDB | MongoDB / SQL Server |
| Kafka / ClickHouse → MSSQL | Kafka یا ClickHouse | SQL Server |
| MSSQL → MSSQL (fixed) | SQL Server | SQL Server |
| Sales & Inventory | فروشگاه‌ها + ERP AX | Kafka |
| Master Data → Store | Publisher (`mssql_replication_md`) | دیتابیس فروشگاه (Subscriber) |
| ClickHouse Optimize | ClickHouse | ClickHouse |
| Health Monitor | Kafka / SQL Server | گزارش سلامت |

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  SQL Server     │────►│  Airflow DAG         │────►│  Kafka Topics   │
│  MySQL / Mongo  │◄───►│  + pipeline/         │────►│  ClickHouse     │
│  Kafka / CH     │     │  Orchestrators       │────►│  Store DBs      │
└─────────────────┘     └──────────────────────┘     └─────────────────┘
```

---

## ۲. لایه‌بندی نرم‌افزاری

معماری از اصول SOLID پیروی می‌کند: اینترفیس‌های ABC، پیکربندی immutable، و جداسازی orchestration از I/O.

```
dags/                  ← تعریف و ثبت DAG (thin layer)
  └── template/        ← factoryهای قابل‌استفاده مجدد
pipeline/
  ├── interfaces/      ← قراردادها (ABC)
  ├── config/          ← dataclassهای پیکربندی
  ├── database/        ← خواندن/نوشتن SQL Server، MySQL، MongoDB و ClickHouse
  ├── kafka/           ← producer، consumer و مدیریت topic
  ├── core/            ← orchestratorها و نتایج
  ├── utils/           ← validation، retry، audit
  └── compat/          ← سازگاری نسخه‌های Airflow
```

### جریان وابستگی

```
DAG / Factory
    │
    ├─► MSSQLDataTransferOrchestrator
    │      ├─ MSSQLDataReader  ← ConnectionFactory / SafeMsSqlHook
    │      ├─ IdempotentKafkaProducer
    │      └─ ClickHouseWriter (اختیاری)
    │
    ├─► MSSQLToKafkaQueryOrchestrator / MSSQLToClickHouseQueryOrchestrator
    │
    ├─► MSSQLToMSSQLQueryOrchestrator
    │      ├─ MSSQLDataReader (منبع)
    │      └─ MSSQLServerWriter (مقصد)  [upsert + delete_missing]
    │
    ├─► MySQLToMSSQLQueryOrchestrator / MSSQLToMySQLQueryOrchestrator
    ├─► MongoDBToMSSQLQueryOrchestrator / MSSQLToMongoDBQueryOrchestrator
    ├─► KafkaToMSSQLQueryOrchestrator / ClickHouseToMSSQLQueryOrchestrator
    │
    ├─► ClickHouseOptimizationOrchestrator
    │      └─ ClickHouseTableOptimizer
    │
    └─► kafka_health_monitor_dag (factory)
           ├─ validate_kafka_conn / AdminClient
           └─ KafkaTopicManager (lag / stats / sample)
```

---

## ۳. جریان‌های داده

### ۳.۱ SQL Server → Kafka (Table / Query Sync)

**Factoryها:**

- `dags/template/table_mssql_sync_dag_factory.py` → `create_table_sync_dag`
- `dags/template/mssql_to_kafka_clickhouse_sync_dag_factory.py` → `create_query_sync_dag`

**گراف Task:**

```
validation → setup (ensure topic) → processing (transfer) → verify_and_completion
```

| مسیر | پیکربندی | رفتار |
|------|----------|--------|
| Table | `TableConfiguration` | Incremental بر اساس `date_column` / `execution_date`؛ keyset pagination |
| Query | `QueryConfiguration` | اجرای query/CTE با توکن‌های `{{ ds }}` و `{{ ds_nodash }}` |

پرچم‌های `SyncConfig`: `is_send_kafka`، `is_send_clickhouse`، `fail_on_error`، `batch_size`، `date_offset`.

**Exactly-once در سمت Producer:** `enable.idempotence=true`، `acks=all` در `IdempotentKafkaProducer`.

نمونه‌ها:

- `dags/mssql_to_kafka_clickhouse_sync/example_table_to_kafka_sync.py`
- `dags/mssql_to_kafka_clickhouse_sync/example_query_to_kafka_sync.py`

جزئیات عملیاتی: [MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md)

### ۳.۲ Sales & Inventory → Kafka

این جریان با DAGهای سفارشی در `dags/sales_inventory/` پیاده می‌شود. الگوی کلی:

1. اعتبارسنجی اتصالات HQ / ERP / Kafka  
2. ایجاد topicها  
3. کشف لیست فروشگاه‌ها از `mssql_store_connectionInfo`  
4. تقسیم سرورها به chunk (محدودیت XCom)  
5. پردازش موازی با اتصال پویا به هر فروشگاه  
6. تجمیع نتیجه و پاکسازی فایل‌های موقت  

منابع نمونه: Retail POS، EC، Sales Order، On-hand Inventory.

### ۳.۳ Master Data → Store (MSSQL → MSSQL پویا)

**Factory:** `mssql_masterdata_to_mssql_store_sync_dag_factory.create_dag`

**اتصالات:**

| Connection | نقش |
|------------|-----|
| `mssql_replication_md` | Publisher |
| `mssql_store_connectionInfo` | اطلاعات فروشگاه‌ها |
| `mssql_store_template` | الگوی احراز هویت SQL برای Subscriber |

**الگو:**

- `use_dynamic_tasks=True` → `plan_sync_chunks` سپس `sync_data_chunk.expand(...)`
- در غیر این صورت → `sync_data` یک‌مرحله‌ای
- در هر دو مسیر، `{store_number}` داخل `source_query` / `source_query_count` در runtime با `resolve_store_scoped_sync_config` جایگزین می‌شود

**Upsert:** `MSSQLServerWriter.upsert_batch` با staging اختیاری، `use_hash_change_detection`، و `unique_keys`.

**`delete_missing`:** کلیدهای منبع در staging جمع می‌شوند؛ سپس ردیف‌هایی که داخل بازهٔ `[min, max]` ستون `delete_scope_column` (یا `chunk_column`) هستند ولی در staging نیستند حذف می‌شوند.

نمونه: `dags/masterdata_store_sync/example_table_to_store_sync.py`

| لایه | مسیر |
|------|------|
| Sample | `dags/masterdata_store_sync/example_table_to_store_sync.py` |
| Factory | `dags/template/mssql_masterdata_to_mssql_store_sync_dag_factory.py` |

جزئیات عملیاتی: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)

### ۳.۴ مسیرهای Sync با Connection ثابت (upsert)

منبع و مقصد از **Connectionهای ازپیش‌تعریف‌شدهٔ Airflow** خوانده می‌شوند (برخلاف مسیر ۳.۳ که مقصد فروشگاه را پویا resolve می‌کند).

| مسیر | Factory | Orchestrator | راهنما |
|------|---------|--------------|--------|
| MySQL → MSSQL | `mysql_to_mssql_sync_dag_factory` | `MySQLToMSSQLQueryOrchestrator` | [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md) |
| MSSQL → MySQL | `mssql_to_mysql_sync_dag_factory` | `MSSQLToMySQLQueryOrchestrator` | [MSSQL_TO_MYSQL_SYNC_GUIDE.md](MSSQL_TO_MYSQL_SYNC_GUIDE.md) |
| MSSQL → PostgreSQL | `mssql_to_postgresql_sync_dag_factory` | `MSSQLToPostgreSQLQueryOrchestrator` | [MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md](MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md) |
| PostgreSQL → MSSQL | `postgresql_to_mssql_sync_dag_factory` | `PostgreSQLToMSSQLQueryOrchestrator` | [POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md](POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md) |
| MSSQL → MSSQL | `mssql_to_mssql_sync_dag_factory` | `MSSQLToMSSQLQueryOrchestrator` | [MSSQL_TO_MSSQL_SYNC_GUIDE.md](MSSQL_TO_MSSQL_SYNC_GUIDE.md) |
| MSSQL → MongoDB | `mssql_to_mongo_sync_dag_factory` | `MSSQLToMongoDBQueryOrchestrator` | [MSSQL_TO_MONGO_SYNC_GUIDE.md](MSSQL_TO_MONGO_SYNC_GUIDE.md) |
| MongoDB → MSSQL | `mongo_to_mssql_sync_dag_factory` | `MongoDBToMSSQLQueryOrchestrator` | [MONGO_TO_MSSQL_SYNC_GUIDE.md](MONGO_TO_MSSQL_SYNC_GUIDE.md) |
| Kafka → MSSQL | `kafka_to_mssql_sync_dag_factory` | `KafkaToMSSQLQueryOrchestrator` | [KAFKA_TO_MSSQL_SYNC_GUIDE.md](KAFKA_TO_MSSQL_SYNC_GUIDE.md) |
| ClickHouse → MSSQL | `clickhouse_to_mssql_sync_dag_factory` | `ClickHouseToMSSQLQueryOrchestrator` | [CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md) |
| MSSQL → ClickHouse | `mssql_to_clickhouse_sync_dag_factory` | `MSSQLToClickHouseQueryOrchestrator` | [MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md) |
| MSSQL → Kafka (Gen-2) | `mssql_to_kafka_sync_dag_factory` | `MSSQLToKafkaQueryOrchestrator` | [MSSQL_TO_KAFKA_SYNC_GUIDE.md](MSSQL_TO_KAFKA_SYNC_GUIDE.md) |

الگوی مشترک Task:

```
validation → sync (± chunks) → report
```

تنظیمات اغلب روی `MasterDataSyncConfig` / `MongoSyncConfig` / `KafkaSyncConfig` / `MSSQLToKafkaSyncConfig` سوار می‌شوند؛ نوشتن به MSSQL با `MSSQLServerWriter.upsert_batch` (همان الگوی MERGE/staging).

### ۳.۴ بهینه‌سازی ClickHouse

**Factory:** `clickhouse_optimizer_dag_factory.clickhouse_optimizer_dag`

امضا: `clickhouse_optimizer_dag(dag_config, conn_config, optimize_config)` — `conn_config.clickhouse_conn_id` الزامی است.

```
validate → health_before → OPTIMIZE (partition / FINAL / deduplicate) → health_after
```

تنظیمات: `ClickHouseOptimizationConfig` (`partition_column`، `partition_format`، `final`، `deduplicate`).

نمونه: `dags/clickhouse_optimizer/example_table_clickhouse_optimizer.py`

جزئیات عملیاتی: [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md)

### ۳.۵ Kafka Health Monitor

**Factory:** `kafka_health_monitor_dag_factory.kafka_health_monitor_dag`

امضا: `kafka_health_monitor_dag(dag_config, conn_config, health_config)` — `conn_config.kafka_conn_id` الزامی است.

**هدف:** مانیتور topicهایی که توسط pipelineهای Kafka نوشته می‌شوند (یک مانیتور به‌ازای هر topic یکتا).

```
health_checks (موازی):
  validate_kafka_health ∥ check_consumer_lag ∥ check_topic_stats
        → sample_recent_messages (اختیاری)
        → generate_health_report
```

تنظیمات: `KafkaHealthMonitorConfig` (`kafka_topic`، `consumer_group`، `max_lag_records`، …) + `ConnectionConfig` برای Kafka.

نمونه: `dags/kafka_health_monitor/example_topic_health_monitor.py`

جزئیات عملیاتی: [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md)

---

## ۴. پیکربندی‌های کلیدی

| کلاس | فایل | فیلدهای مهم |
|------|------|-------------|
| `DAGConfig` | `pipeline/config/DAGConfig.py` | `dag_id`, `schedule`, `pool`, `retries`, `max_active_runs`, `tags` |
| `SyncConfig` | `SyncConfig.py` | `is_send_kafka`, `is_send_clickhouse`, `batch_size`, `fail_on_error` |
| `TableConfiguration` | `TableConfiguration.py` | `table_name`, `primary_key_column`, `order_by_column`, `date_column`, `columns` |
| `QueryConfiguration` | `QueryConfiguration.py` | `query`, `key_column`, `query_params`, `min_expected_records` |
| `MasterDataSyncConfig` | `MasterDataSyncConfig.py` | `source_query`, `target_schema/table`, `primary_keys`, `chunk_column`, `delete_missing`, `delete_scope_column` |
| `MongoSyncConfig` | `MongoSyncConfig.py` | collection / aggregation + upsert به MSSQL |
| `KafkaSyncConfig` | `KafkaSyncConfig.py` | topic / consumer + upsert به MSSQL |
| `MSSQLToKafkaSyncConfig` | `MSSQLToKafkaSyncConfig.py` | query + topic برای مسیر Gen-2 |
| `ConnectionConfig` | `ConnectionConfig.py` | `mssql_conn_id`, `kafka_conn_id`, `clickhouse_conn_id`, … |
| `KafkaTopicConfig` | `KafkaTopicConfig.py` | `name`, `num_partitions`, `replication_factor` |
| `KafkaHealthMonitorConfig` | `KafkaHealthMonitorConfig.py` | `kafka_topic`, `consumer_group`, `max_lag_records`, `include_message_sampling` |
| `ClickHouseConfig` | `ClickHouseConfig.py` | `database`, `table_name` |
| `ClickHouseOptimizationConfig` | `ClickHouseOptimizationConfig.py` | `partition_column`, `final`, `deduplicate` |

الگوی استفاده در DAGهای واقعی: ساخت dataclassها در فایل DAG و فراخوانی factory در زمان import (ثبت DAG).

---

## ۵. لایه دیتابیس و احراز هویت

### SafeMsSqlHook

زیرکلاس Airflow `MsSqlHook`:

- پیش‌فرض: **pymssql**
- **pyodbc / Kerberos** وقتی extras شامل مواردی مثل `auth_mode`، `driver`، `trusted_connection`، `odbc_connect` باشد

نمونه Extra برای Windows Integrated:

```json
{
  "auth_mode": "kerberos",
  "driver": "ODBC Driver 18 for SQL Server",
  "trusted_connection": true,
  "encrypt": true,
  "trustservercertificate": false
}
```

### ConnectionFactory

- با `conn_id` Airflow → `SafeMsSqlHook`
- با connection string (`mssql+pymssql://` یا `mssql+pyodbc://`) برای اتصال پویا به فروشگاه‌ها

### ClickHouseConnectionFactory

- با `conn_id` Airflow → `clickhouse_driver.Client`
- `get_connection()` context manager و `test_connection()`

### KafkaConnectionFactory

- با `conn_id` Airflow → bootstrap servers + config (SASL/SSL از `extra`)
- `get_bootstrap_servers()`، `get_client_config()`، `list_topics()` / `get_cluster_info()`، `test_connection()`
- `get_admin_client()` فقط برای موارد خاص (low-level)
- `kafka_utils.get_kafka_brokers` و `build_kafka_admin_client` روی همین factory سوار شده‌اند

### KafkaTopicManager / Producer / Orchestrator

- `KafkaTopicManager(conn_id)` و `IdempotentKafkaProducer(conn_id=...)` از `KafkaConnectionFactory` استفاده می‌کنند
- `MSSQLDataTransferOrchestrator` پارامتر `kafka_conn_id` می‌گیرد (نه bootstrap string)

### SQLQueryBuilder

ساخت کوئری امن با اعتبارسنجی identifier، count، min/max و **keyset pagination** (به‌جای OFFSET سنگین).

---

## ۶. مدیریت خطا و پایداری

```
PipelineException
├── DatabaseException
│   ├── SQLServerConnectionError
│   ├── SQLServerQueryError → SQLServerDeadlockError
│   └── DataReadError
├── KafkaException
├── ConfigurationException
├── ValidationException
└── DataTransferException
```

- خطاهای **transient** (SQLSTATEهایی مثل `08S01`، `HYT00`، `08001` و پیام‌های timeout/connection) → قابل retry (`AirflowException`)
- خطاهای غیرموقت → `AirflowFailException` از طریق `raise_sync_task_error`
- Deadlock: کد `1205` / SQLSTATE `40001`
- Retry سطح کد: `pipeline/utils/retry_helper.py` (exponential backoff + jitter)
- Retry سطح Airflow: `DAGConfig.retries` و decoratorهای `@task`

---

## ۷. Observability

### AuditLogger

رویدادهای ساخت‌یافته (JSONL) زیر مسیر لاگ audit (پیش‌فرض `/opt/airflow/logs/audit`):

- `EventType`: مانند `conn_validated`، `kafka_produce`، `chunk_processing`، `replication_reconcile`
- `EventStatus`: `started`، `success`، `failed`، `retry`، `warning`

در Sales/Inventory، Replication و برخی پردازش‌های ClickHouse استفاده می‌شود.

### Validation

`validate_mssql_conn` / `validate_mysql_conn` / `validate_mongo_conn` / `validate_kafka_conn` / `validate_clickhouse_conn` نتیجهٔ ساخت‌یافته برای XCom برمی‌گردانند.

### متریک‌ها

`TransferResult`، `TransferMetrics`، `OptimizationResult` برای گزارش تعداد رکورد، مدت‌زمان و وضعیت انتقال.

مانیتورهای Kafka در task `generate_health_report` وضعیت `healthy` / `warning` / `error`، lag کل و لیست `alerts` را به XCom می‌فرستند.

---

## ۸. استقرار Docker

ایمیج‌ها و composeهای WinAuth در `docker/` قرار دارند. نسخهٔ harden برای production توصیه می‌شود.

جزئیات: [../docker/README.md](../docker/README.md)


---

## ۹. نقشهٔ اسناد مرتبط

| سند | موضوع |
|-----|--------|
| [../PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md) | درخت پوشه‌ها و نقش هر مسیر |
| [QUICKSTART.md](QUICKSTART.md) | راه‌اندازی سریع |
| [QUICK_START_IMPROVEMENTS.md](QUICK_START_IMPROVEMENTS.md) | استفاده از بهبودهای reliability |
| [MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md) | SQL Server → Kafka (± ClickHouse) |
| [MSSQL_TO_KAFKA_SYNC_GUIDE.md](MSSQL_TO_KAFKA_SYNC_GUIDE.md) | MSSQL → Kafka (Gen-2) |
| [MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md) | MSSQL → ClickHouse |
| [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md) | MySQL → MSSQL |
| [MSSQL_TO_MYSQL_SYNC_GUIDE.md](MSSQL_TO_MYSQL_SYNC_GUIDE.md) | MSSQL → MySQL |
| [MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md](MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md) | MSSQL → PostgreSQL |
| [POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md](POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md) | PostgreSQL → MSSQL |
| [MSSQL_TO_MSSQL_SYNC_GUIDE.md](MSSQL_TO_MSSQL_SYNC_GUIDE.md) | MSSQL → MSSQL (conn ثابت) |
| [MSSQL_TO_MONGO_SYNC_GUIDE.md](MSSQL_TO_MONGO_SYNC_GUIDE.md) | MSSQL → MongoDB |
| [MONGO_TO_MSSQL_SYNC_GUIDE.md](MONGO_TO_MSSQL_SYNC_GUIDE.md) | MongoDB → MSSQL |
| [KAFKA_TO_MSSQL_SYNC_GUIDE.md](KAFKA_TO_MSSQL_SYNC_GUIDE.md) | Kafka → MSSQL |
| [CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md) | ClickHouse → MSSQL |
| [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md) | Master Data → Store |
| [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md) | مانیتور سلامت Kafka |
| [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md) | بهینه‌سازی ClickHouse |

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
