# راهنمای ایجاد DAG برای Sync داده از MySQL به MSSQL

این راهنما نحوهٔ افزودن یک DAG جدید برای **همگام‌سازی** از **MySQL** به **SQL Server** را توضیح می‌دهد. داده با یک query از MySQL خوانده می‌شود و با **upsert / MERGE** (همان الگوی Replication MD) در جدول مقصد MSSQL نوشته می‌شود.

---

## ۱. معماری کلی

```
┌──────────────────────┐                      ┌─────────────────────┐
│  MySQL (Source)      │                      │  MSSQL (Target)     │
│  mysql_* conn        │                      │  mssql_* conn       │
└──────────┬───────────┘                      └──────────▲──────────┘
           │                                             │
           │  SELECT / stream batches                    │  upsert_batch
           ▼                                             │  (staging MERGE)
┌────────────────────────────────────────────────────────┴──────────┐
│  DAG Sync (این راهنما)                                            │
│  MySQLDataReader → MySQLToMSSQLQueryOrchestrator → MSSQLWriter    │
└───────────────────────────────────────────────────────────────────┘
```

برخلاف Replication MD (که مقصد فروشگاه را از `ConnectionInfo` کشف می‌کند)، اینجا **منبع و مقصد هر دو Connection ثابت Airflow** هستند.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table / Query Sync** | `dags/mysql_to_mssql_sync/` | یک DAG به‌ازای هر جدول/query |
| **Factory** | `dags/template/mysql_to_mssql_sync_dag_factory.py` | ساخت TaskGroupهای validation / processing |
| **Orchestrator** | `pipeline/core/MySQLToMSSQLQueryOrchestrator.py` | خواندن MySQL + upsert به MSSQL |
| **Reader / Writer** | `pipeline/database/MySQLDataReader.py`، `MSSQLServerWriter.py` | stream و MERGE |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `dags/mysql_to_mssql_sync/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mysql_source_default` | منبع — MySQL |
| `mssql_dwh_primary` | مقصد — SQL Server (قابل تغییر) |

> نام connectionها از طریق `ConnectionConfig` یا Variable قابل تنظیم است؛ الزامی نیست دقیقاً همین IDها باشند.

Provider لازم روی ایمیج Airflow: `apache-airflow-providers-mysql` (+ `mysqlclient`).

### Pool

فقط taskهای chunk (و در حالت ساده، task اصلی sync) از pool اختصاصی استفاده می‌کنند. یک‌بار در Airflow اجرا کنید:

```bash
airflow pools set mysql_to_mssql_sync_pool 32 "MySQL to MSSQL chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در MSSQL |
| `mysql_source_conn_id` | `mysql_source_default` | Airflow conn منبع (در نمونه DAG) |
| `mssql_target_conn_id` | `mssql_dwh_primary` | Airflow conn مقصد (در نمونه DAG) |
| `max_global_parallel_chunks_mysql_to_mssql` | `32` | سقف chunk همزمان (اگر Variable تعریف کنید) |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب نام فایل و `dag_id`

قرارداد نام‌گذاری:

- **فایل:** `dags/mysql_to_mssql_sync/<source_or_table>_to_mssql_sync.py`
- **`dag_id`:** `mysql_<name>_to_mssql_sync` (snake_case)

مثال: جدول MySQL `products` → فایل `products_to_mssql_sync.py` و `dag_id = 'mysql_products_to_mssql_sync'`

### گام ۲ — کپی از DAG نمونه

الگو:

- **جدول کوچک / بدون chunk:** `dags/mysql_to_mssql_sync/example_table_to_mssql_sync.py`

برای جداول بزرگ همان فایل را کپی کنید و `use_dynamic_tasks=True` را فعال کنید (بخش ۵).

### گام ۳ — پیکربندی `DAGConfig` و `ConnectionConfig`

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mysql_to_mssql_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='mysql_products_to_mssql_sync',
    description='sync MySQL products to MSSQL dbo.Products',
    owner='نام شما',
    start_date=datetime(2026, 7, 23),
    schedule=None,          # یا مثلاً '0 2 * * *' برای nightly
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_mysql_products", default_var=1)),
    retries=int(Variable.get("retries_mysql_products", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_mysql_products", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_mysql_products", default_var=8))),
    tags=["mysql", "mssql", "replication", "mysql-sync"],
    pool="mysql_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    mysql_conn_id=Variable.get("mysql_source_conn_id", default_var="mysql_source_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_dwh_primary"),
)
```

> **نکته:** `mysql_conn_id` و `mssql_conn_id` هر دو **الزامی** هستند؛ بدون آن‌ها factory خطا می‌دهد.

### گام ۴ — پیکربندی `MasterDataSyncConfig`

```python
sync_config = MasterDataSyncConfig(
    source_name='mysql_products',
    source_query="""
        SELECT id, sku, name, updated_at
        FROM products
    """,
    source_query_count="""
        SELECT COUNT(1) AS CNT
        FROM products
    """,
    primary_keys=('id',),
    target_schema='dbo',
    target_table='Products',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    use_hash_change_detection=True,
    delete_missing=bool(int(Variable.get("delete_missing_mysql_products", default_var=0))),
    delete_scope_column='id',
    batch_size=int(Variable.get("batch_size_mysql_products", default_var=10000)),
)
```

> **نکته‌ها:**
> - `source_query` باید **دیالکت MySQL** باشد (نه T-SQL / `WITH (READPAST)` / براکت `[col]`).
> - نام ستون‌های رزرو‌شده MySQL را با backtick بگیرید؛ مثلاً `` `name` ``، `` `status` ``.
> - نام و نوع ستون‌های SELECT باید با جدول مقصد MSSQL هم‌خوان باشند.
> - `primary_keys` کلید MERGE در **MSSQL** است.

### گام ۵ — ساخت DAG

```python
dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

### گام ۶ — ایجاد DAG در Airflow

فایل را در `dags/mysql_to_mssql_sync/` ذخیره کنید. Airflow پس از parse، DAG را در UI نمایش می‌دهد.

---

## ۴. پارامترهای مهم `MasterDataSyncConfig`

همان dataclass مشترک با Replication MD استفاده می‌شود:

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از MySQL؛ ستون‌ها باید با مقصد MSSQL هم‌خوان باشند |
| `source_query_count` | بله | شمارش ردیف‌ها روی همان منبع/فیلتر |
| `primary_keys` | بله | کلید merge در MSSQL |
| `target_schema` / `target_table` | بله | مقصد در SQL Server |
| `staging_schema` | خیر | schema موقت؛ پیش‌فرض از Variable |
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
validation → sync_mysql_to_mssql → report_sync_metrics
```

مثال: `example_table_to_mssql_sync.py`

### حالت Chunk موازی (جدول بزرگ)

برای جداول با میلیون‌ها ردیف. **نیاز به MySQL 8+** دارد (`NTILE` window function).

```python
sync_config = MasterDataSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='id',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=int(
        Variable.get("max_global_parallel_chunks_mysql_to_mssql", default_var=32)
    ),
)
```

```
validation → create_sync_chunks → sync_mysql_to_mssql_chunk (×N) → report_sync_metrics
```

### فیلتر افزایشی (اختیاری)

CDC واقعی وجود ندارد؛ برای sync نیمه‌افزایشی فیلتر زمانی را داخل `source_query` بگذارید:

```sql
SELECT id, sku, name, updated_at
FROM products
WHERE updated_at >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
```

و همان شرط را در `source_query_count` تکرار کنید. برای حذف orphanها در همان بازه، `delete_missing=True` و `delete_scope_column` مناسب را تنظیم کنید.

---

## ۶. جریان اجرای DAG (خودکار توسط Factory)

Factory در `dags/template/mysql_to_mssql_sync_dag_factory.py` این مراحل را می‌سازد:

### Validation
- تست اتصال MySQL (`conn_config.mysql_conn_id`)
- تست اتصال MSSQL (`conn_config.mssql_conn_id`)

### Processing
- خواندن query از MySQL به‌صورت stream/batch
- نوشتن در staging و MERGE به جدول مقصد MSSQL
- در صورت `delete_missing`: حذف scoped ردیف‌های اضافی در مقصد
- گزارش متریک (inserted / updated / deleted)

---

## ۷. نحوهٔ اجرا

### اجرای دستی

در Airflow UI → DAG → **Trigger DAG**.

برای اجرای زمان‌بندی‌شده، در `DAGConfig` مقدار `schedule` را تنظیم کنید (مثلاً cron).

### اجرا از Orchestrator (اختیاری)

اگر چند جدول وابسته دارید، می‌توانید با `TriggerDagRunOperator` چند DAG را زنجیره کنید:

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_products = TriggerDagRunOperator(
    task_id='mysql_products',
    trigger_dag_id='mysql_products_to_mssql_sync',
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
Airflow DAG: MySQL products → MSSQL dbo.Products
Uses mysql_to_mssql_sync_dag_factory template.
"""
from datetime import datetime, timedelta
from airflow.models import Variable

from pipeline.config import DAGConfig, ConnectionConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.mysql_to_mssql_sync_dag_factory import create_dag

TABLE_MYSQL = "products"
TABLE_MSSQL = "Products"
DAG_SUFFIX = "mysql_products"

dag_config = DAGConfig(
    dag_id=f"{DAG_SUFFIX}_to_mssql_sync",
    description=f"sync MySQL {TABLE_MYSQL} to MSSQL dbo.{TABLE_MSSQL}",
    owner="Your Name",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get(f"max_active_runs_{DAG_SUFFIX}", default_var=1)),
    retries=int(Variable.get(f"retries_{DAG_SUFFIX}", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get(f"retry_delay_minutes_{DAG_SUFFIX}", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get(f"execution_timeout_hours_{DAG_SUFFIX}", default_var=8))),
    tags=["mysql", "mssql", "replication", "mysql-sync"],
    pool="mysql_to_mssql_sync_pool",
)

conn_config = ConnectionConfig(
    mysql_conn_id=Variable.get("mysql_source_conn_id", default_var="mysql_source_default"),
    mssql_conn_id=Variable.get("mssql_target_conn_id", default_var="mssql_dwh_primary"),
)

sync_config = MasterDataSyncConfig(
    source_name=DAG_SUFFIX,
    source_query=f"""
        SELECT /* تمام ستون‌های موردنیاز */
        FROM {TABLE_MYSQL}
    """,
    source_query_count=f"""
        SELECT COUNT(1) AS CNT
        FROM {TABLE_MYSQL}
    """,
    primary_keys=("id",),
    target_schema="dbo",
    target_table=TABLE_MSSQL,
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=bool(int(Variable.get(f"delete_missing_{DAG_SUFFIX}", default_var=0))),
    delete_scope_column="id",
    use_hash_change_detection=True,
    batch_size=int(Variable.get(f"batch_size_{DAG_SUFFIX}", default_var=10000)),
)

dag = create_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
)
```

---

## ۹. چک‌لیست قبل از Production

- [ ] Connectionهای MySQL و MSSQL در Airflow تعریف و تست شده‌اند
- [ ] `source_query` دیالکت MySQL است و فقط ستون‌های موجود در مقصد را SELECT می‌کند
- [ ] نام ستون‌های رزرو‌شده MySQL با backtick نوشته شده‌اند
- [ ] `primary_keys` با schema واقعی جدول MSSQL یکی است
- [ ] `source_query_count` روی همان منبع / همان فیلتر اجرا می‌شود
- [ ] جدول مقصد و در صورت نیاز `staging_schema` در MSSQL وجود دارند
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده (MySQL 8+)
- [ ] pool `mysql_to_mssql_sync_pool` در Airflow ایجاد شده
- [ ] DAG در محیط test اجرا و متریک‌های inserted/updated/deleted بررسی شده
- [ ] Variableهای اختصاصی DAG در Airflow تعریف شده (یا defaultها کافی‌اند)
- [ ] اگر `delete_missing=True` است، `delete_scope_column` و بازهٔ داده درست است

---

## ۱۰. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `mysql_conn_id is required` | `ConnectionConfig` ناقص | هر دو `mysql_conn_id` و `mssql_conn_id` را ست کنید |
| MySQL validation failed | conn اشتباه / فایروال / provider | Connection UI و نصب `providers-mysql` را چک کنید |
| Syntax error near `[` یا `WITH (READPAST)` | query به سبک T-SQL نوشته شده | query را به دیالکت MySQL تبدیل کنید |
| `NTILE` / window function error | MySQL کمتر از 8 | یا نسخه را ارتقا دهید یا بدون `use_dynamic_tasks` sync کنید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool slots | pool را بزرگ‌تر کنید یا chunk را کم کنید |
| Sync کند است | `batch_size` کوچک یا chunk زیاد | `batch_size` و `task_chunk_size` را tune کنید |
| هیچ ردیفی update نمی‌شود | hash detection یا کلید اشتباه | `primary_keys` و محتوای SELECT را بررسی کنید |
| حذف ناخواسته در مقصد | `delete_missing` با scope اشتباه | scope و فیلتر `source_query` را محدود کنید |

---

## ۱۱. تفاوت با Replication MD

| مورد | Replication MD | MySQL → MSSQL |
|------|----------------|---------------|
| منبع | MSSQL Publisher | MySQL |
| مقصد | فروشگاه پویا (از ConnectionInfo) | MSSQL با conn ثابت |
| Factory | `mssql_masterdata_to_mssql_store_sync_dag_factory` | `mysql_to_mssql_sync_dag_factory` |
| Orchestrator | `MSSQLToMSSQLQueryOrchestrator` | `MySQLToMSSQLQueryOrchestrator` |
| پارامتر trigger | معمولاً `store_number` | نیاز نیست |
| دیالکت source query | T-SQL | MySQL |
| Writer مقصد | همان `MSSQLServerWriter` | همان `MSSQLServerWriter` |
| Config sync | `MasterDataSyncConfig` | همان `MasterDataSyncConfig` |

---

## ۱۲. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/mysql_to_mssql_sync_dag_factory.py` | Factory اصلی |
| `pipeline/core/MySQLToMSSQLQueryOrchestrator.py` | منطق خواندن MySQL / نوشتن MSSQL |
| `pipeline/database/MySQLDataReader.py` | stream از MySQL |
| `pipeline/database/MySQLConnectionFactory.py` | اتصال MySQL |
| `pipeline/database/MSSQLServerWriter.py` | upsert / delete_missing در مقصد |
| `pipeline/config/MasterDataSyncConfig.py` | تعریف پارامترهای sync |
| `pipeline/config/ConnectionConfig.py` | `mysql_conn_id` + `mssql_conn_id` |
| `pipeline/config/DAGConfig.py` | تعریف پارامترهای DAG |
| `dags/mysql_to_mssql_sync/example_table_to_mssql_sync.py` | نمونه کامل |
| `docs/MASTERDATA_STORE_SYNC_GUIDE.md` | الگوی مشابه MSSQL→MSSQL |
