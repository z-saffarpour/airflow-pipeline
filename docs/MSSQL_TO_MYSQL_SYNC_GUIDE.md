# راهنمای ایجاد DAG برای Sync داده از MSSQL به MySQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **SQL Server** به **MySQL** را توضیح می‌دهد. داده با یک query از MSSQL خوانده می‌شود و با **staging upsert** در جدول مقصد MySQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MSSQL (Source)      │                      │  MySQL (Target)     │
│  mssql_* conn        │                      │  mysql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging UPDATE+INSERT)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MSSQLDataReader → MSSQLToMySQLQueryOrchestrator → MySQLWriter    │
└───────────────────────────────────────────────────────────────────┘
```

منبع و مقصد هر دو **Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mssql_to_mysql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mssql_to_mysql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MSSQLToMySQLQueryOrchestrator.py` | خواندن MSSQL + upsert به MySQL |
| **Reader / Writer** | `pipeline/database/MSSQLDataReader.py`، `MySQLServerWriter.py` | stream و staging upsert |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mssql_to_mysql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_default` | منبع — SQL Server |
| `mysql_target_default` | مقصد — MySQL (قابل تغییر) |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است؛ الزامی نیست دقیقاً همین IDها باشند.

Provider لازم روی ایمیج Airflow: `apache-airflow-providers-mysql` (+ `mysqlclient`).

### Pool

فقط taskهای chunk (و در حالت ساده، task اصلی sync) از pool اختصاصی استفاده می‌کنند. یک‌بار در Airflow اجرا کنید:

```bash
airflow pools set mssql_to_mysql_sync_pool 32 "MSSQL to MySQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mysql_staging_schema` | `staging` | database موقت staging در MySQL |
| `mssql_source_conn_id` | `mssql_default` | Airflow conn منبع (در نمونه DAG) |
| `mysql_target_conn_id` | `mysql_target_default` | Airflow conn مقصد (در نمونه DAG) |
| `max_global_parallel_chunks_mssql_to_mysql` | `32` | سقف chunk همزمان (اگر Variable تعریف کنید) |

> در MySQL، `target_schema` و `staging_schema` همان **نام database** هستند.

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب نام فایل و `dag_id`

قرارداد نام‌گذاری:

- **فایل:** `dags/mssql_to_mysql_sync/<source_or_table>_to_mysql_sync.py`
- **`dag_id`:** `mssql_<name>_to_mysql_sync` (snake_case)

مثال: جدول MSSQL `Products` → فایل `products_to_mysql_sync.py` و `dag_id = 'mssql_products_to_mysql_sync'`

### گام ۲ — کپی از DAG نمونه

الگو:

- **جدول کوچک / بدون chunk:** `dags/mssql_to_mysql_sync/example_table_to_mysql_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید (بخش ۵).

### گام ۳ — پیکربندی `DAGConfig` و `ConnectionConfig`

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_mysql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mssql_products_to_mysql_sync',
    description='sync MSSQL dbo.Products to MySQL app.products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,          # یا مثلاً '0 2 * * *' برای nightly
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mssql_products_mysql", default_var=1)),
    retries=int(Variable.get("retries_mssql_products_mysql", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mssql_products_mysql", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mssql_products_mysql", default_var=8))),
    tags=["mssql", "mysql", "mssql-to-mysql"],
    pool="mssql_to_mysql_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_default"),
    mysql_conn_id=Variable.get("mysql_target_conn_id", default_var="mysql_target_default"),
)
```

> **نکته:** `mssql_conn_id` و `mysql_conn_id` هر دو **الزامی** هستند؛ بدون آن‌ها factory خطا می‌دهد.

### گام ۴ — پیکربندی `MasterDataSyncConfig`

```python
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
    target_schema='app',           # MySQL database name
    target_table='products',
    staging_schema=Variable.get("mysql_staging_schema", default_var="staging"),
    use_hash_change_detection=True,
    delete_missing=bool(int(Variable.get("delete_missing_mssql_products_mysql", default_var=0))),
    delete_scope_column='id',
    batch_size=int(Variable.get("batch_size_mssql_products_mysql", default_var=10000)),
)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت T-SQL** باشد (نه MySQL).
> - نام و نوع ستون‌های SELECT باید با جدول مقصد MySQL هم‌خوان باشند.
> - `primary_keys` کلید upsert در **MySQL** است (باید UNIQUE/PRIMARY روی مقصد باشد).
> - جدول مقصد و در صورت نیاز database مربوط به `staging_schema` باید از قبل در MySQL وجود داشته باشند.

### گام ۵ — ساخت DAG

```python
dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

### گام ۶ — ایجاد DAG در Airflow

فایل را در `dags/mssql_to_mysql_sync/` ذخیره کنید. Airflow پس از parse، DAG را در UI نمایش می‌دهد.

---

## ۴. پارامترهای مهم `MasterDataSyncConfig`

همان dataclass مشترک مسیرهای upsert استفاده می‌شود:

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از MSSQL؛ ستون‌ها باید با مقصد MySQL هم‌خوان باشند |
| `source_query_count` | بله | شمارش ردیف‌ها روی همان منبع/فیلتر |
| `primary_keys` | بله | کلید upsert در MySQL |
| `target_schema` / `target_table` | بله | مقصد در MySQL (`target_schema` = نام database) |
| `staging_schema` | خیر | database موقت؛ پیش‌فرض از Variable |
| `unique_keys` | خیر | کلیدهای جایگزین برای resolve تداخل |
| `resolve_unique_key_conflicts` | خیر | پیش‌فرض `True` |
| `use_hash_change_detection` | خیر | فقط ردیف‌های تغییرکرده sync می‌شوند |
| `use_dynamic_tasks` | خیر | فعال‌سازی sync موازی بر اساس chunk |
| `chunk_column` | اگر dynamic | معمولاً همان PK (مثلاً `id`) |
| `task_chunk_size` | اگر dynamic | تعداد ردیف در هر chunk |
| `max_parallel_chunks` | اگر dynamic | chunk همزمان در یک DAG run |
| `max_global_parallel_chunks` | اگر dynamic | سقف chunk همزمان در کل DAGها |
| `delete_missing` | خیر | حذف ردیف‌های اضافی در مقصد (scoped) |
| `delete_scope_column` | اگر delete_missing | ستون محدودکنندهٔ scope حذف |
| `batch_size` | خیر | اندازه batch خواندن/نوشتن |

---

## ۵. انتخاب حالت Sync

### حالت ساده (جدول کوچک)

`use_dynamic_tasks` را تنظیم نکنید (پیش‌فرض `False`).

```
validation → sync_mssql_to_mysql → report_sync_metrics
```

مثال: `example_table_to_mysql_sync.py`

### حالت Chunk موازی (جدول بزرگ)

برای جداول با میلیون‌ها ردیف از `NTILE` روی منبع SQL Server استفاده می‌شود.

```python
sync_config = MasterDataSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=int(
        Variable.get("max_global_parallel_chunks_mssql_to_mysql", default_var=32)
    ),
)
```

```
validation → create_sync_chunks → sync_mssql_to_mysql_chunk (×N) → report_sync_metrics
```

### فیلتر افزایشی (اختیاری)

CDC واقعی وجود ندارد؛ برای sync نیمه‌افزایشی فیلتر زمانی را داخل `source_query` بگذارید:

```sql
SELECT id, sku, name, updated_at
FROM dbo.Products
WHERE updated_at >= DATEADD(day, -1, CAST(GETDATE() AS date))
```

و همان شرط را در `source_query_count` تکرار کنید. برای حذف orphanها در همان بازه، `delete_missing=True` و `delete_scope_column` مناسب را تنظیم کنید.

---

## ۶. جریان اجرای DAG (خودکار توسط Factory)

Factory در `dags/template/mssql_to_mysql_sync_dag_factory.py` این مراحل را می‌سازد:

### Validation
- تست اتصال MSSQL (`conn_config.mssql_conn_id`)
- تست اتصال MySQL (`conn_config.mysql_conn_id`)

### Processing
- خواندن query از MSSQL به‌صورت stream/batch
- نوشتن در staging و upsert به جدول مقصد MySQL
- در صورت `delete_missing`: حذف scoped ردیف‌های اضافی در مقصد
- گزارش متریک (inserted / updated / deleted)

---

## ۷. نحوهٔ اجرا

### اجرای دستی

در Airflow UI → DAG → **Trigger DAG**.

برای اجرای زمان‌بندی‌شده، در `DAGConfig` مقدار `schedule` را تنظیم کنید (مثلاً cron).

### اجرا از Orchestrator (اختیاری)

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_products = TriggerDagRunOperator(
    task_id='mssql_products',
    trigger_dag_id='mssql_products_to_mysql_sync',
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'],
)
```

---

## ۸. قالب کامل (Template)

```python
"""
Airflow DAG: MSSQL dbo.Products → MySQL app.products
Uses mssql_to_mysql_sync_dag_factory template.
"""
from datetime import datetime, timedelta
from airflow.models import Variable

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mssql_to_mysql_sync_dag_factory import create_dag

TABLE_MSSQL = "Products"
TABLE_MYSQL = "products"
DAG_SUFFIX = "mssql_products"

dag_config = DAGConfig(
    dag_id=f"{DAG_SUFFIX}_to_mysql_sync",
    description=f"sync MSSQL dbo.{TABLE_MSSQL} to MySQL app.{TABLE_MYSQL}",
    owner="Your Name",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get(f"max_active_runs_{DAG_SUFFIX}_mysql", default_var=1)),
    retries=int(Variable.get(f"retries_{DAG_SUFFIX}_mysql", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get(f"retry_delay_minutes_{DAG_SUFFIX}_mysql", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get(f"execution_timeout_hours_{DAG_SUFFIX}_mysql", default_var=8))),
    tags=["mssql", "mysql", "mssql-to-mysql"],
    pool="mssql_to_mysql_sync_pool",
)

conn_config = ConnectionConfig(
    mssql_conn_id=Variable.get("mssql_source_conn_id", default_var="mssql_default"),
    mysql_conn_id=Variable.get("mysql_target_conn_id", default_var="mysql_target_default"),
)

sync_config = MasterDataSyncConfig(
    source_name=DAG_SUFFIX,
    source_query=f"""
        SELECT /* تمام ستون‌های موردنیاز */
        FROM dbo.{TABLE_MSSQL}
    """,
    source_query_count=f"""
        SELECT COUNT(1) AS CNT
        FROM dbo.{TABLE_MSSQL}
    """,
    primary_keys=("id",),
    target_schema="app",
    target_table=TABLE_MYSQL,
    staging_schema=Variable.get("mysql_staging_schema", default_var="staging"),
    delete_missing=bool(int(Variable.get(f"delete_missing_{DAG_SUFFIX}_mysql", default_var=0))),
    delete_scope_column="id",
    use_hash_change_detection=True,
    batch_size=int(Variable.get(f"batch_size_{DAG_SUFFIX}_mysql", default_var=10000)),
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

---

## ۹. چک‌لیست قبل از Production

- [ ] Connectionهای MSSQL و MySQL در Airflow تعریف و تست شده‌اند
- [ ] `source_query` دیالکت T-SQL است و فقط ستون‌های موجود در مقصد را SELECT می‌کند
- [ ] `primary_keys` با schema واقعی جدول MySQL یکی است
- [ ] `source_query_count` روی همان منبع / همان فیلتر اجرا می‌شود
- [ ] جدول مقصد و در صورت نیاز database مربوط به `staging_schema` در MySQL وجود دارند
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده
- [ ] pool `mssql_to_mysql_sync_pool` در Airflow ایجاد شده
- [ ] DAG در محیط test اجرا و متریک‌های inserted/updated/deleted بررسی شده
- [ ] Variableهای اختصاصی DAG در Airflow تعریف شده (یا defaultها کافی‌اند)
- [ ] اگر `delete_missing=True` است، `delete_scope_column` و بازهٔ داده درست است

---

## ۱۰. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `mssql_conn_id is required` | `ConnectionConfig` ناقص | هر دو `mssql_conn_id` و `mysql_conn_id` را ست کنید |
| MySQL validation failed | conn اشتباه / فایروال / provider | Connection UI و نصب `providers-mysql` را چک کنید |
| Syntax error near `` ` `` یا `DATE_SUB` | query به سبک MySQL نوشته شده | query را به دیالکت T-SQL تبدیل کنید |
| Duplicate key / unique conflict | کلید upsert یا unique اشتباه | `primary_keys` / `unique_keys` را بررسی کنید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool slots | pool را بزرگ‌تر کنید یا chunk را کم کنید |
| Sync کند است | `batch_size` کوچک یا chunk زیاد | `batch_size` و `task_chunk_size` را tune کنید |
| هیچ ردیفی update نمی‌شود | hash detection یا کلید اشتباه | `primary_keys` و محتوای SELECT را بررسی کنید |
| حذف ناخواسته در مقصد | `delete_missing` با scope اشتباه | scope و فیلتر `source_query` را محدود کنید |

---

## ۱۱. تفاوت با MySQL → MSSQL

| مورد | MySQL → MSSQL | MSSQL → MySQL |
|------|---------------|---------------|
| منبع | MySQL | MSSQL |
| مقصد | MSSQL | MySQL |
| Factory | `mysql_to_mssql_sync_dag_factory` | `mssql_to_mysql_sync_dag_factory` |
| Orchestrator | `MySQLToMSSQLQueryOrchestrator` | `MSSQLToMySQLQueryOrchestrator` |
| دیالکت source query | MySQL | T-SQL |
| Writer مقصد | `MSSQLServerWriter` (MERGE) | `MySQLServerWriter` (staging UPDATE+INSERT) |
| Config sync | `MasterDataSyncConfig` | همان `MasterDataSyncConfig` |
| Chunking | NTILE روی MySQL 8+ | NTILE روی SQL Server |

---

## ۱۲. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mssql_to_mysql_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MSSQLToMySQLQueryOrchestrator.py` | منطق خواندن MSSQL / نوشتن MySQL |
| `pipeline/database/MSSQLDataReader.py` | stream از MSSQL |
| `pipeline/database/MySQLServerWriter.py` | upsert / delete_missing در مقصد |
| `pipeline/database/MySQLConnectionFactory.py` | اتصال MySQL |
| `pipeline/config/MasterDataSyncConfig.py` | تعریف پارامترهای sync |
| `pipeline/config/ConnectionConfig.py` | `mssql_conn_id` + `mysql_conn_id` |
| `pipeline/config/DAGConfig.py` | تعریف پارامترهای DAG |
| `dags/mssql_to_mysql_sync/example_table_to_mysql_sync.py` | نمونه کامل |
| `docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md` | الگوی معکوس (MySQL → MSSQL) |
