# راهنمای سریع بهبودها (Quick Start Improvements)

چگونه از قابلیت‌های reliability، validation و error-handling پروژه در کد و عملیات استفاده کنید.

برای نمای معماری کامل → [ARCHITECTURE_FA.md](ARCHITECTURE_FA.md)  
برای راه‌اندازی اولیه → [QUICKSTART.md](QUICKSTART.md)

---

## ۱. سلسله‌مراتب Exception

ماژول: `pipeline/core/exceptions.py`

```python
from pipeline.core.exceptions import (
    PipelineException,
    SQLServerConnectionError,
    SQLServerQueryError,
    SQLServerDeadlockError,
    KafkaProducerError,
    InvalidConfigurationError,
    TransferVerificationError,
    is_transient_sql_server_error,
    is_sql_server_deadlock,
    raise_sync_task_error,
)
```

| نوع | کاربرد |
|-----|--------|
| `SQLServerConnectionError` | شکست اتصال |
| `SQLServerQueryError` / `SQLServerDeadlockError` | خطای کوئری / deadlock 1205 |
| `Kafka*` | مشکلات broker / produce / topic |
| `InvalidConfigurationError` | تنظیمات ناقص یا نامعتبر |
| `TransferVerificationError` | عدم تطابق شمارش یا verify |

در taskهای Replication از `raise_sync_task_error` استفاده کنید تا:

- خطای **transient** → `AirflowException` (قابل retry)
- خطای **دائمی** → `AirflowFailException` (بدون retry بیهوده)

```python
try:
    orchestrator.sync_data_chunk(...)
except Exception as exc:
    raise_sync_task_error("chunk sync failed", exc)
```

### تشخیص خطای موقت

SQLSTATEهای شناخته‌شده: `08S01`، `HYT00`، `HYT01`، `08001`  
الگوی پیام: connection / timeout / communication link / TCP provider / login timeout / broken pipe

```python
if is_transient_sql_server_error(exc):
    # قابل retry
    ...
```

تست مرتبط: `tests/test_transient_sql_server_error.py`

---

## ۲. Retry با Exponential Backoff + Jitter

ماژول: `pipeline/utils/retry_helper.py`

```python
from pipeline.utils.retry_helper import retry_with_backoff, RetryContext

@retry_with_backoff(
    max_attempts=3,
    base_delay=2.0,
    max_delay=60.0,
    exceptions=(ConnectionError, TimeoutError),
)
def fetch_batch():
    ...
```

یا با context:

```python
with RetryContext(max_attempts=3, base_delay=1.0) as retry:
    while retry.should_continue():
        try:
            do_work()
            break
        except TransientError as exc:
            retry.record_failure(exc)
```

در کنار این، retry سطح Airflow را از `DAGConfig.retries` / `sync_retries` تنظیم کنید.

---

## ۳. Validation اتصالات

ماژول: `pipeline/utils/validation.py`

```python
from pipeline.utils.validation import (
    validate_mssql_conn,
    validate_kafka_conn,
    validate_clickhouse_conn,
)

result = validate_kafka_conn("kafka_default")
# {"status": "ok", "conn_id": "kafka_default", "timestamp": "...", "details": {...}}
```

نتیجه برای XCom مناسب است و در ابتدای اکثر DAGهای template فراخوانی می‌شود.

---

## ۴. SafeMsSqlHook و Windows Authentication

ماژول: `pipeline/database/SafeMsSqlHook.py`

- پیش‌فرض: **pymssql**
- فعال‌سازی **pyodbc**: وجود extras مانند `auth_mode`، `driver`، `trusted_connection`

نمونه Extra امن برای production:

```json
{
  "auth_mode": "kerberos",
  "driver": "ODBC Driver 18 for SQL Server",
  "trusted_connection": true,
  "encrypt": true,
  "trustservercertificate": false,
  "login_timeout": 15,
  "timeout": 30
}
```

برای فروشگاه‌ها معمولاً URI پویا از روی `mssql_store_template` ساخته می‌شود (`mssql+pyodbc://...`).

---

## ۵. Audit Logging ساخت‌یافته

ماژول: `pipeline/utils/AuditLogger.py` + `pipeline/config/AuditConfig.py`

```python
from pipeline.utils.AuditLogger import AuditLogger
from pipeline.config.AuditConfig import EventType, EventStatus

audit = AuditLogger(dag_id=dag_id, run_id=run_id)
audit.log(
    task_id="sync_chunk",
    event_type=EventType.chunk_processing,
    status=EventStatus.success,
    details={"store_number": store_number, "rows": 1200},
)
```

فایل‌ها به‌صورت JSONL در مسیر audit (پیش‌فرض `/opt/airflow/logs/audit`) نوشته می‌شوند.

در Sales/Inventory به‌طور گسترده استفاده شده است؛ در table/query templateهای ساده کمتر.

---

## ۶. `delete_missing` با Scope امن

در `MasterDataSyncConfig`:

| فیلد | نقش |
|------|-----|
| `delete_missing` | فعال‌سازی حذف ردیف‌های موجود در مقصد که در منبع chunk نیستند |
| `delete_scope_column` | ستونی که بازهٔ حذف را محدود می‌کند (مثلاً `RECID`) |
| `chunk_column` | جایگزین scope اگر `delete_scope_column` خالی باشد |

حذف فقط داخل `[min_key, max_key]` همان chunk انجام می‌شود تا کل جدول پاک نشود.

اغلب از Airflow Variable خوانده می‌شود، مثلاً:

```python
delete_missing = Variable.get("delete_missing_retail_discount_code", default_var="false").lower() == "true"
```

---

## ۷. فیلتر فروشگاهی با `{store_number}`

در Factoryی Master Data → Store، اگر `source_query` یا `source_query_count` شامل `{store_number}` باشد، قبل از `plan_sync_chunks` / `sync_data` / `sync_data_chunk` جایگزین می‌شود:

```python
# داخل factory (خلاصه رفتار)
resolved = resolve_store_scoped_sync_config(sync_config, store_number)
orchestrator.sync_data(resolved, exec_date)
```

نمونه query:

```sql
FROM ax.INVENTDIM WITH (READPAST)
WHERE INVENTLOCATIONID = ''
   OR INVENTLOCATIONID = '{store_number}'
```

مقدار فروشگاه برای SQL escape می‌شود (`'` → `''`). نقطهٔ شروع در ریپو: `dags/masterdata_store_sync/example_table_to_store_sync.py`.

---

## ۸. Chunk موازی و Pool

برای جداول بزرگ با `use_dynamic_tasks=True`:

```python
MasterDataSyncConfig(
    use_dynamic_tasks=True,
    task_chunk_size=...,
    chunk_column="RECID",
    max_parallel_chunks=...,
    max_global_parallel_chunks=32,
    ...
)
```

و در Airflow یک pool متناسب با سقف موازی‌سازی بسازید؛ برای Master Data → Store:

```bash
airflow pools set replication_md_store_sync_pool 32 "Master data store chunk sync"
```

`DAGConfig.pool` را روی همین pool بگذارید تا فقط taskهای chunk محدود شوند.

---

## ۹. سازگاری نسخه‌های Airflow

ماژول: `pipeline/compat/airflow_compat.py`

برای خواندن Connection بدون وابستگی سخت به SDK نسخه خاص، از helperهای این ماژول استفاده کنید (fallback Airflow 3 → 2.x → `BaseHook`).

---

## ۱۰. چک‌لیست عملیاتی کوتاه

1. Connections و Extra مربوط به WinAuth را یک‌بار validate کنید.
2. Poolهای chunk را با Variable سقف موازی‌سازی هم‌تراز کنید.
3. برای جداول حساس، ابتدا `delete_missing=false` را در محیط تست اجرا کنید.
4. اگر query فروشگاهی است، trigger را با `store_number` درست بزنید و جایگزینی `{store_number}` را در لاگ/نتیجه چک کنید.
5. لاگ‌های audit و task log را برای `AirflowFailException` در برابر retryهای مکرر مقایسه کنید.
6. پس از تغییر exception/transient logic، `pytest tests/test_transient_sql_server_error.py -v` را اجرا کنید.

---

## ارجاع کد

| موضوع | مسیر |
|-------|------|
| Exceptions | `pipeline/core/exceptions.py` |
| Retry | `pipeline/utils/retry_helper.py` |
| Validation | `pipeline/utils/validation.py` |
| Hook امن | `pipeline/database/SafeMsSqlHook.py` |
| Audit | `pipeline/utils/AuditLogger.py` |
| MD Orchestrator | `pipeline/core/MSSQLToMSSQLQueryOrchestrator.py` |
| Master Data factory | `dags/template/mssql_masterdata_to_mssql_store_sync_dag_factory.py` |
| Store-scoped resolve | `resolve_store_scoped_sync_config` در همان factory |

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
