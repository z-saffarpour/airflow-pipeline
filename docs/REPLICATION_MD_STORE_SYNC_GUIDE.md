# راهنمای ایجاد DAG جدید برای Sync داده از Publisher به Subscriber

این راهنما نحوهٔ افزودن یک DAG جدید در پروژه **Replication MD** را توضیح می‌دهد. این DAGها وقتی SQL Server Replication دادهٔ یک فروشگاه (Subscriber) را از دست داده یا عقب افتاده، داده را از **Publisher** (`mssql_replication_md`) می‌خوانند و در دیتابیس همان فروشگاه می‌نویسند.

---

## ۱. معماری کلی

```
┌──────────────────────┐     SQL Server        ┌─────────────────────┐
│  Publisher (HQ)      │ ─── Replication ───►  │  Subscriber (Store) │
│  mssql_replication_md│                       │  دیتابیس فروشگاه   │
└──────────┬───────────┘                       └──────────┬──────────┘
           │                                              │
           │         اگر داده miss شد:                   │
           └──────────────► DAG Sync (این راهنما) ───────┘
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Table Sync** | `dags/replication/tables/` | sync یک جدول برای یک فروشگاه |
| **Orchestrator** | `dags/replication/orchestrator/` | اجرای چند DAG sync به‌صورت زنجیره‌ای برای یک فروشگاه |
| **Reconcile & Sync** | `dags/replication/reconcile_and_sync/` | تشخیص gap در replication و trigger خودکار orchestratorها |

برای افزودن جدول جدید، معمولاً **فقط یک فایل در `tables/`** کافی است. در صورت نیاز، orchestrator و mapping reconcile را هم به‌روز می‌کنید.

در حال حاضر حدود **۲۰۰+** DAG در `dags/replication/tables/` پوشش جداول master data خرده‌فروشی/AX را فراهم می‌کند (موجودی، قیمت، تخفیف، کانال، لجستیک، مالیات، سازمان و …).

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID | نقش |
|---------------|-----|
| `mssql_replication_md` | Publisher — منبع داده (HQ) |
| `mssql_store_connectionInfo` | اطلاعات اتصال فروشگاه‌ها (`retail.ConnectionInfo`) |
| `mssql_store_template` | الگوی احراز هویت SQL برای اتصال به Subscriber |

### Pool

فقط taskهای chunk از pool اختصاصی استفاده می‌کنند. یک‌بار در Airflow اجرا کنید:

```bash
airflow pools set replication_md_store_sync_pool 32 "Replication MD store chunk sync"
```

تعداد slot باید **بزرگ‌تر یا مساوی** `max_global_parallel_chunks` باشد.

### Variableهای مشترک (اختیاری ولی توصیه‌شده)

| Variable | پیش‌فرض | توضیح |
|----------|---------|-------|
| `mssql_staging_schema` | `crt` | schema موقت staging در Subscriber |
| `max_global_parallel_chunks_replication_md_store` | `32` | سقف chunk همزمان در کل DAGها |

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب نام فایل و `dag_id`

قرارداد نام‌گذاری:

- **فایل:** `dags/replication/tables/ax_<table_name>_sync.py`
- **`dag_id`:** `ax_<table_name>_sync` (با snake_case)

مثال: جدول `ax.MyNewTable` → فایل `ax_my_new_table_sync.py` و `dag_id = 'ax_my_new_table_sync'`

### گام ۲ — کپی از یک DAG مشابه

بهترین الگوها:

- **جدول کوچک / بدون chunk:** `ax_price_disc_group_sync.py`
- **جدول بزرگ / با chunk موازی:** `ax_invent_table_sync.py` یا `ax_retail_periodic_discount_line_sync.py`
- **فیلتر فروشگاهی (`{store_number}`):** `ax_invent_dim_sync.py` یا `ax_pos_register_connected_efts_sync.py`

### گام ۳ — پیکربندی `DAGConfig`

```python
from datetime import datetime, timedelta
from airflow.models import Variable
from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.query_mssql_replication_md_store_sync_dag_factory import create_dag

dag_config = DAGConfig(
    dag_id='ax_my_new_table_sync',
    description='masterdata query sync from ax.MyNewTable to Store',
    owner='نام شما',
    start_date=datetime(2026, 5, 12),
    schedule=None,          # معمولاً None — فقط با trigger اجرا می‌شود
    catchup=False,
    max_active_runs=int(Variable.get("max_active_runs_my_new_table", default_var=4)),
    retries=int(Variable.get("retries_my_new_table", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get("retry_delay_minutes_my_new_table", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get("execution_timeout_hours_my_new_table", default_var=8))),
    tags=["mssql", "store", "master-data", "replication-md"],
    pool="replication_md_store_sync_pool",
)
```

> **نکته:** `schedule=None` یعنی DAG به‌صورت دستی یا از orchestrator/reconcile trigger می‌شود، نه زمان‌بندی خودکار.

### گام ۴ — پیکربندی `MasterDataSyncConfig`

```python
sync_config = MasterDataSyncConfig(
    source_name='adhoc_ax_MyNewTable',
    source_query="""
        SELECT Col1, Col2, RECID, DATAAREAID
        FROM ax.MyNewTable WITH (READPAST);
    """,
    source_query_count="""
        SELECT COUNT(1) AS CNT
        FROM ax.MyNewTable WITH (READPAST);
    """,
    primary_keys=('RECID',),           # کلید اصلی در Subscriber
    target_schema='ax',
    target_table='MyNewTable',
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    batch_size=int(Variable.get("batch_size_my_new_table", default_var=30000)),
)
```

> **نکته‌ها:**
> - روی `FROM` ترجیحاً `WITH (READPAST)` بگذارید تا قفل‌های همزمان کمتر مزاحم شوند.
> - نام ستون‌های رزرو شده SQL را براکت کنید؛ مثلاً `[MODULE]`، `[NAME]`، `[STATUS]`.
> - پیش‌فرض رایج `batch_size` در DAGهای جدید حدود `30000` است (از Variable قابل تنظیم).

### گام ۵ — ساخت DAG

```python
dag = create_dag(dag_config=dag_config, sync_config=sync_config)
```

### گام ۶ — ایجاد DAG در Airflow

فایل را در `dags/replication/tables/` ذخیره کنید. Airflow پس از parse، DAG را در UI نمایش می‌دهد.

---

## ۴. پارامترهای مهم `MasterDataSyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام یکتا برای audit/log |
| `source_query` | بله | SELECT از Publisher؛ ستون‌ها باید با Subscriber هم‌خوان باشند |
| `source_query_count` | بله | شمارش ردیف‌ها (ترجیحاً با `READPAST`) |
| `primary_keys` | بله | کلید merge در Subscriber |
| `target_schema` / `target_table` | بله | مقصد در دیتابیس فروشگاه |
| `staging_schema` | خیر | schema موقت؛ پیش‌فرض از Variable |
| `unique_keys` | خیر | کلیدهای جایگزین برای resolve تداخل |
| `resolve_unique_key_conflicts` | خیر | پیش‌فرض `True` |
| `use_hash_change_detection` | خیر | فقط ردیف‌های تغییرکرده sync می‌شوند |
| `use_dynamic_tasks` | خیر | فعال‌سازی sync موازی بر اساس chunk |
| `chunk_column` | اگر dynamic | معمولاً `RECID` |
| `task_chunk_size` | اگر dynamic | تعداد ردیف در هر chunk |
| `max_parallel_chunks` | اگر dynamic | chunk همزمان در یک DAG run |
| `max_global_parallel_chunks` | اگر dynamic | سقف chunk همزمان در کل DAGها |
| `delete_missing` | خیر | حذف ردیف‌های اضافی در Subscriber |
| `delete_scope_column` | اگر delete_missing | ستون محدودکنندهٔ scope حذف |
| `batch_size` | خیر | اندازه batch در INSERT/UPDATE |

---

## ۵. انتخاب حالت Sync

### حالت ساده (جدول کوچک)

برای جداول با حجم کم، `use_dynamic_tasks` را تنظیم نکنید (پیش‌فرض `False`).

```
Validation → Discovery → sync_replication_md_store → report
```

مثال: `ax_price_disc_group_sync.py`

### حالت Chunk موازی (جدول بزرگ)

برای جداول با میلیون‌ها ردیف:

```python
sync_config = MasterDataSyncConfig(
    # ... سایر تنظیمات ...
    use_dynamic_tasks=True,
    chunk_column='RECID',
    task_chunk_size=100_000,
    max_parallel_chunks=8,
    max_global_parallel_chunks=int(
        Variable.get("max_global_parallel_chunks_replication_md_store", default_var=32)
    ),
)
```

```
Validation → Discovery → create_sync_chunks → sync_replication_md_store_chunk (×N) → report
```

مثال: `ax_retail_periodic_discount_line_sync.py`، `ax_invent_table_sync.py`

### فیلتر فروشگاهی با `{store_number}`

اگر query باید فقط دادهٔ همان فروشگاه را از Publisher بخواند، در `source_query` / `source_query_count` از placeholder استفاده کنید:

```sql
WHERE INVENTLOCATIONID = ''
   OR INVENTLOCATIONID = '{store_number}'
```

یا:

```sql
WHERE RETAILTERMINALID LIKE '{store_number}%'
```

Factory در زمان اجرا تابع `resolve_store_scoped_sync_config` را صدا می‌زند و `{store_number}` را با مقدار واقعی (escape شده برای SQL) جایگزین می‌کند — هم در مسیر `sync_data` و هم در `plan_sync_chunks` / `sync_data_chunk`.

مثال‌ها: `ax_invent_dim_sync.py`، `ax_pos_register_connected_efts_sync.py`

---

## ۶. جریان اجرای DAG (خودکار توسط Factory)

Factory در `dags/template/query_mssql_replication_md_store_sync_dag_factory.py` این مراحل را می‌سازد:

### Validation
- اعتبارسنجی `store_number` (از trigger conf یا params)
- تست اتصال `mssql_store_connectionInfo`
- تست اتصال `mssql_replication_md` (Publisher)

### Discovery
- خواندن IP/Port/Database فروشگاه از `retail.ConnectionInfo`
- تست اتصال مستقیم به Subscriber

### Processing
- خواندن query از Publisher
- نوشتن در staging و merge به جدول مقصد در Subscriber
- گزارش متریک (inserted / updated / deleted)

---

## ۷. نحوهٔ اجرا

### اجرای دستی برای یک فروشگاه

در Airflow UI → DAG → **Trigger DAG w/ config**:

```json
{
  "store_number": "OKS00123"
}
```

یا از params DAG (پیش‌فرض `OKS00000`).

### اجرا از Orchestrator

اگر چند جدول وابسته دارید، یک orchestrator در `dags/replication/orchestrator/` بسازید:

```python
trigger_my_table = TriggerDagRunOperator(
    task_id='ax_my_new_table',
    trigger_dag_id='ax_my_new_table_sync',
    conf={"store_number": "{{ params.store_number }}"},
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'],
)
```

الگو: `ax_retail_assortment_lookup_sync_orchestrator.py`

### Trigger خودکار از Reconcile

اگر می‌خواهید DAG جدید پس از تشخیص gap در replication به‌صورت خودکار اجرا شود، mapping را در فایل reconcile مربوطه اضافه کنید:

**فایل:** `dags/replication/reconcile_and_sync/masterdata_replication_reconcile_and_sync.py`

```python
TABLE_SYNC_DAG_MAPPING = """
{
  ...
  "ax.MyNewTable": {
    "dag_id": "ax_my_new_table_sync"
  }
}
"""
```

همچنین `dag_id` orchestrator مربوطه را در `orchestrator_dag_ids` همان فایل reconcile ثبت کنید (در صورت استفاده از orchestrator).

---

## ۸. قالب کامل (Template)

```python
"""
Airflow DAG: ax.MyNewTable Query to Store
Uses query_mssql_replication_md_store_sync_dag_factory template.
"""
from datetime import datetime, timedelta
from airflow.models import Variable

from pipeline.config import DAGConfig
from pipeline.config.MasterDataSyncConfig import MasterDataSyncConfig
from template.query_mssql_replication_md_store_sync_dag_factory import create_dag

TABLE = "MyNewTable"
DAG_SUFFIX = "my_new_table"

dag_config = DAGConfig(
    dag_id=f"ax_{DAG_SUFFIX}_sync",
    description=f"masterdata query sync from ax.{TABLE} to Store",
    owner="Your Name",
    start_date=datetime(2026, 5, 12),
    schedule=None,
    catchup=False,
    max_active_runs=int(Variable.get(f"max_active_runs_{DAG_SUFFIX}", default_var=4)),
    retries=int(Variable.get(f"retries_{DAG_SUFFIX}", default_var=2)),
    retry_delay=timedelta(minutes=int(Variable.get(f"retry_delay_minutes_{DAG_SUFFIX}", default_var=5))),
    execution_timeout=timedelta(hours=int(Variable.get(f"execution_timeout_hours_{DAG_SUFFIX}", default_var=8))),
    tags=["mssql", "store", "master-data", "replication-md"],
    pool="replication_md_store_sync_pool",
)

sync_config = MasterDataSyncConfig(
    source_name=f"adhoc_ax_{TABLE}",
    source_query=f"""
        SELECT /* تمام ستون‌های موردنیاز */
        FROM ax.{TABLE};
    """,
    source_query_count=f"""
        SELECT COUNT(1) AS CNT
        FROM ax.{TABLE} WITH (READPAST);
    """,
    primary_keys=("RECID",),
    target_schema="ax",
    target_table=TABLE,
    staging_schema=Variable.get("mssql_staging_schema", default_var="crt"),
    delete_missing=bool(int(Variable.get(f"delete_missing_{DAG_SUFFIX}", default_var=0))),
    delete_scope_column="RECID",
    batch_size=int(Variable.get(f"batch_size_{DAG_SUFFIX}", default_var=10000)),
)

dag = create_dag(dag_config=dag_config, sync_config=sync_config)
```

---

## ۹. چک‌لیست قبل از Production

- [ ] `source_query` فقط ستون‌های موجود در Subscriber را SELECT می‌کند
- [ ] ستون‌های رزرو شده با براکت نوشته شده‌اند (`[NAME]` و …)
- [ ] `WITH (READPAST)` روی کوئری‌های سنگین اعمال شده
- [ ] `primary_keys` با schema واقعی Subscriber یکی است
- [ ] `source_query_count` روی همان منبع / همان فیلتر اجرا می‌شود
- [ ] اگر فیلتر فروشگاهی لازم است، `{store_number}` در query آمده
- [ ] برای جدول بزرگ، `use_dynamic_tasks=True` و `chunk_column` تنظیم شده
- [ ] pool `replication_md_store_sync_pool` در Airflow ایجاد شده
- [ ] DAG با `store_number` واقعی در محیط test اجرا و متریک‌ها بررسی شده
- [ ] (اختیاری) mapping در reconcile و orchestrator به‌روز شده
- [ ] Variableهای اختصاصی DAG در Airflow تعریف شده (یا defaultها کافی‌اند)

---

## ۱۰. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `store_number must be provided` | trigger بدون conf | `{"store_number": "OKS00xxx"}` بفرستید |
| `connection info not found` | فروشگاه در `ConnectionInfo` نیست | رکورد فروشگاه را بررسی کنید |
| Pool slot تمام شد | `max_global_parallel_chunks` > pool slots | pool را بزرگ‌تر کنید یا chunk را کم کنید |
| Sync کند است | `batch_size` کوچک یا chunk زیاد | `batch_size` و `task_chunk_size` را tune کنید |
| DAG در reconcile trigger نمی‌شود | mapping نیست یا diff ≤ 1 | `TABLE_SYNC_DAG_MAPPING` را بررسی کنید |
| فیلتر فروشگاه اعمال نشده | `{store_number}` در query نیست یا اشتباه نوشته شده | placeholder را دقیقاً `{store_number}` بگذارید |
| نام DAG قدیمی در mapping | مثلاً Abelchange به‌جای LabelChange | mapping و نام فایل را هم‌نام کنید |

> **تغییر نام مهم:** `ax_retail_abelchange_journal_trans_sync` به `ax_retail_label_change_journal_trans_sync` تغییر کرده و در reconcile به جدول `ax.RetailLabelChangeJournalTrans` map می‌شود.

---

## ۱۱. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/query_mssql_replication_md_store_sync_dag_factory.py` | Factory اصلی (+ `resolve_store_scoped_sync_config`) |
| `pipeline/config/MasterDataSyncConfig.py` | تعریف پارامترهای sync |
| `pipeline/config/DAGConfig.py` | تعریف پارامترهای DAG |
| `pipeline/core/MSSQLToMSSQLQueryOrchestrator.py` | منطق خواندن/نوشتن |
| `dags/replication/tables/ax_price_disc_group_sync.py` | نمونه ساده |
| `dags/replication/tables/ax_invent_table_sync.py` | نمونه chunk موازی |
| `dags/replication/tables/ax_invent_dim_sync.py` | نمونه فیلتر `{store_number}` |
| `dags/replication/orchestrator/` | نمونه orchestrator |
| `dags/replication/reconcile_and_sync/` | reconcile و trigger خودکار |
