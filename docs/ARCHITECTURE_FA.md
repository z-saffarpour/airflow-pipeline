# معماری سیستم (Architecture)

مستند معماری پلتفرم **SQL Server Data Pipeline** مبتنی بر Apache Airflow.

---

## ۱. نمای کلی

پلتفرم مجموعه‌ای از DAGهای Airflow و بستهٔ مشترک `pipeline/` است که چند جریان داده را پوشش می‌دهد:

| جریان | منبع | مقصد |
|-------|------|------|
| Table / Query Sync | SQL Server (DWH / ERP) | Kafka (± ClickHouse) |
| Sales & Inventory | فروشگاه‌ها + ERP AX | Kafka |
| Replication MD Repair | Publisher (`mssql_replication_md`) | دیتابیس فروشگاه (Subscriber) |
| ClickHouse Optimize | ClickHouse | ClickHouse |
| Health Monitor | Kafka / SQL Server | گزارش سلامت |

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  SQL Server     │────►│  Airflow DAG         │────►│  Kafka Topics   │
│  ERP / DWH /    │     │  + pipeline/         │     │  ClickHouse     │
│  Store / MD     │◄────│  Orchestrators       │────►│  Store DBs      │
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
  ├── database/        ← خواندن/نوشتن SQL Server و ClickHouse
  ├── kafka/           ← producer و مدیریت topic
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
    ├─► MSSQLToMSSQLQueryOrchestrator
    │      ├─ MSSQLDataReader (Publisher)
    │      └─ MSSQLServerWriter (Store)  [upsert + delete_missing]
    │
    └─► ClickHouseOptimizationOrchestrator
           └─ ClickHouseTableOptimizer
```

---

## ۳. جریان‌های داده

### ۳.۱ SQL Server → Kafka (Table / Query Sync)

**Factoryها:**

- `dags/template/table_mssql_sync_dag_factory.py` → `create_table_sync_dag`
- `dags/template/query_mssql_sync_dag_factory.py` → `create_query_sync_dag`

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

### ۳.۲ Sales & Inventory → Kafka

DAGهای سفارشی در `dags/sales_inventory/` (نه فقط template ساده):

1. اعتبارسنجی اتصالات HQ / ERP / Kafka  
2. ایجاد topicها  
3. کشف لیست فروشگاه‌ها از `mssql_store_connectionInfo`  
4. تقسیم سرورها به chunk (محدودیت XCom)  
5. پردازش موازی با اتصال پویا به هر فروشگاه  
6. تجمیع نتیجه و پاکسازی فایل‌های موقت  

منابع نمونه: Retail POS، EC، Sales Order، On-hand Inventory.

### ۳.۳ Replication MD → Store (MSSQL → MSSQL)

**Factory:** `query_mssql_replication_md_store_sync_dag_factory.create_dag`

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

پوشش فعلی: حدود **۲۰۰+** فایل در `dags/replication/tables/` برای جداول AX/Retail master data.

لایه‌های DAG:

| لایه | مسیر |
|------|------|
| Table Sync | `dags/replication/tables/` |
| Orchestrator | `dags/replication/orchestrator/` |
| Reconcile & Sync | `dags/replication/reconcile_and_sync/` (+ `DagSyncTrigger`) |

جزئیات عملیاتی: [REPLICATION_MD_STORE_SYNC_GUIDE.md](REPLICATION_MD_STORE_SYNC_GUIDE.md)

### ۳.۴ بهینه‌سازی ClickHouse

**Factory:** `clickhouse_optimizer_dag_factory.clickhouse_optimizer_dag`

```
validate → health_before → OPTIMIZE (partition / FINAL / deduplicate) → health_after
```

تنظیمات: `ClickHouseOptimizationConfig` (`partition_column`، `partition_format`، `final`، `deduplicate`).

---

## ۴. پیکربندی‌های کلیدی

| کلاس | فایل | فیلدهای مهم |
|------|------|-------------|
| `DAGConfig` | `pipeline/config/DAGConfig.py` | `dag_id`, `schedule`, `pool`, `retries`, `max_active_runs`, `tags` |
| `SyncConfig` | `SyncConfig.py` | `is_send_kafka`, `is_send_clickhouse`, `batch_size`, `fail_on_error` |
| `TableConfiguration` | `TableConfiguration.py` | `table_name`, `primary_key_column`, `order_by_column`, `date_column`, `columns` |
| `QueryConfiguration` | `QueryConfiguration.py` | `query`, `key_column`, `query_params`, `min_expected_records` |
| `MasterDataSyncConfig` | `MasterDataSyncConfig.py` | `source_query`, `target_schema/table`, `primary_keys`, `chunk_column`, `delete_missing`, `delete_scope_column` |
| `ConnectionConfig` | `ConnectionConfig.py` | `mssql_conn_id`, `kafka_conn_id`, `clickhouse_conn_id` |
| `KafkaTopicConfig` | `KafkaTopicConfig.py` | `name`, `num_partitions`, `replication_factor` |
| `ClickHouseConfig` | `ClickHouseConfig.py` | `database`, `table_name` |

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

`validate_mssql_conn` / `validate_kafka_conn` / `validate_clickhouse_conn` نتیجهٔ ساخت‌یافته برای XCom برمی‌گردانند.

### متریک‌ها

`TransferResult`، `TransferMetrics`، `OptimizationResult` برای گزارش تعداد رکورد، مدت‌زمان و وضعیت انتقال.

---

## ۸. استقرار Docker

ایمیج‌ها و composeهای WinAuth در `docker/` قرار دارند. نسخهٔ harden برای production توصیه می‌شود.

جزئیات: [../docker/README.md](../docker/README.md)

> آرشیوهای `.tar` ایمیج داخل Git نیستند (`images/` در `.gitignore`).

---

## ۹. نقشهٔ اسناد مرتبط

| سند | موضوع |
|-----|--------|
| [../PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md) | درخت پوشه‌ها و نقش هر مسیر |
| [QUICKSTART.md](QUICKSTART.md) | راه‌اندازی سریع |
| [QUICK_START_IMPROVEMENTS.md](QUICK_START_IMPROVEMENTS.md) | استفاده از بهبودهای reliability |
| [REPLICATION_MD_STORE_SYNC_GUIDE.md](REPLICATION_MD_STORE_SYNC_GUIDE.md) | ساخت DAG Replication MD |

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
