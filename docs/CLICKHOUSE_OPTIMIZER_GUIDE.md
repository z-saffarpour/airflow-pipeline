# راهنمای ایجاد DAG جدید برای بهینه‌سازی جداول ClickHouse

این راهنما نحوهٔ افزودن یک DAG جدید در پروژه **ClickHouse Optimizer** را توضیح می‌دهد. این DAGها پس از ورود داده (معمولاً از Kafka / processing)، دستور `OPTIMIZE TABLE` را روی جدول MergeTree اجرا می‌کنند تا partها ادغام شوند، (اختیاری) `FINAL` و `DEDUPLICATE` اعمال شود، و سلامت جدول قبل/بعد بررسی گردد.

---

## ۱. معماری کلی

```
┌──────────────────┐     ingest      ┌─────────────────┐
│  Sync / Process  │ ───────────────► │  ClickHouse     │
│  (Kafka sink /   │                  │  Local_* table  │
│   sales_inventory)│                 └────────┬────────┘
└──────────────────┘                           │
                                               │ OPTIMIZE
                                               ▼
                                    ┌──────────────────────┐
                                    │  Optimizer DAG       │
                                    │  (این راهنما)        │
                                    └──────────┬───────────┘
                                               │
                                               ▼
                         health_before → OPTIMIZE → health_after
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Factory** | `dags/template/clickhouse_optimizer_dag_factory.py` | ساخت DAG و taskها از روی config |
| **Config** | `pipeline/config/ClickHouseOptimizationConfig.py` + `ConnectionConfig` | database، table، partition، FINAL/DEDUPLICATE |
| **Orchestrator** | `pipeline/core/ClickHouseOptimizationOrchestrator.py` | هماهنگی health + optimize |
| **Optimizer** | `pipeline/database/ClickHouseTableOptimizer.py` | اجرای `OPTIMIZE TABLE` و خواندن `system.parts` |
| **DAGهای نازک** | `dags/clickhouse_optimizer/` | فقط config + فراخوانی factory |

دسته‌بندی فعلی:

```
dags/clickhouse_optimizer/
└── example_table_clickhouse_optimizer.py
```

برای افزودن جدول جدید: از نمونه کپی کنید و فایل را در مسیر دلخواه قرار دهید.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID | نقش |
|---------------|-----|
| `clickhouse_default` | اتصال native به ClickHouse (همان sink داده‌ها) |

نمونه Extra برای ClickHouse (بسته به نحوهٔ تعریف Connection در محیط شما):

```json
{
  "host": "clickhouse-host",
  "port": 9000,
  "user": "...",
  "password": "...",
  "database": "default"
}
```

> `ConnectionConfig.clickhouse_conn_id` الزامی است؛ بدون آن factory خطا می‌دهد.

### Pool

Optimizerها از pool پیش‌فرض `DAGConfig` استفاده می‌کنند (`data_sync_pool`):

```bash
airflow pools set data_sync_pool 5 "SQL to Kafka transfers / ClickHouse optimize"
```

### Cluster

بیشتر DAGهای فعلی روی cluster با نام `cluster_2S_2R` اجرا می‌شوند (`ON CLUSTER`). اگر جدول replicated نیست یا cluster دیگری دارید، `cluster_name` را مطابق محیط تنظیم کنید (یا `None` بگذارید تا بدون `ON CLUSTER` اجرا شود).

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب پوشه، نام فایل و `dag_id`

قرارداد نام‌گذاری پیشنهادی:

| نوع جدول | مسیر پیشنهادی | الگوی فایل | الگوی `dag_id` |
|----------|---------------|------------|----------------|
| DWH | `dags/clickhouse_optimizer/dwh/` | `<name>_clickhouse_optimizer.py` | `<name>_clickhouse_optimizer` |
| ERP / AX | `dags/clickhouse_optimizer/erp/` | `ax_<name>_clickhouse_optimizer.py` | `ax_<name>_clickhouse_optimizer` |

مثال: جدول `COM.Local_DIM_Item` → فایل `com_dim_item_clickhouse_optimizer.py` و `dag_id = 'com_dim_item_clickhouse_optimizer'`

### گام ۲ — کپی از DAG نمونه

از نمونه کپی کنید:

- **`dags/clickhouse_optimizer/example_table_clickhouse_optimizer.py`**

سپس `database`، `table_name`، partition و schedule را مطابق جدول خودتان تنظیم کنید.

### گام ۳ — پیکربندی `DAGConfig`

```python
from datetime import datetime
from pipeline.config.DAGConfig import DAGConfig

DAG_CONFIG = DAGConfig(
    dag_id='com_my_table_clickhouse_optimizer',
    description='Optimize COM.Local_MyTable in ClickHouse after Kafka data ingestion',
    owner='نام شما',
    start_date=datetime(2026, 3, 24),
    schedule=None,          # dimensionها معمولاً دستی؛ factها اغلب cron روزانه
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse', 'optimization', 'DWH', 'dimension', 'COM'],
)
```

> **نکته:** `schedule=None` یعنی فقط با trigger دستی (یا از DAG بالادستی) اجرا می‌شود. برای factهای روزانه معمولاً cron بعد از ingest می‌گذارند؛ مثلاً `'0 8 * * *'`.

### گام ۴ — پیکربندی `ClickHouseOptimizationConfig`

```python
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name='cluster_2S_2R',
    database='COM',
    table_name='Local_MyTable',
    partition_column=None,       # None = کل جدول؛ یا نام ستون partition
    partition_format='YYYYMM',   # فقط وقتی partition_column ست باشد معنا دارد
    final=True,
    deduplicate=True,
)
```

### گام ۵ — اتصال و ساخت DAG

```python
from pipeline.config.ConnectionConfig import ConnectionConfig
from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

conn_config = ConnectionConfig(clickhouse_conn_id='clickhouse_default')
clickhouse_optimizer_dag(DAG_CONFIG, conn_config, OPTIMIZE_CONFIG)
```

### گام ۶ — ایجاد DAG در Airflow

فایل را در پوشهٔ مناسب ذخیره کنید. Airflow پس از parse، DAG را در UI نمایش می‌دهد.

---

## ۴. پارامترهای مهم `ClickHouseOptimizationConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `database` | بله | نام دیتابیس ClickHouse (مثلاً `COM`, `RTL`, `inventory`) |
| `table_name` | بله | نام جدول local (معمولاً با پیشوند `Local_`) |
| `cluster_name` | خیر | اگر ست شود: `OPTIMIZE ... ON CLUSTER <name>` |
| `partition_column` | خیر | اگر `None` باشد، کل جدول optimize می‌شود |
| `partition_format` | خیر | فرمت تبدیل `execution_date` به مقدار partition؛ پیش‌فرض `YYYYMMDD` |
| `final` | خیر | پیش‌فرض `True` — ادغام کامل (`FINAL`) |
| `deduplicate` | خیر | پیش‌فرض `True` — حذف ردیف‌های تکراری بر اساس order key |
| `optimize_timeout_seconds` | خیر | پیش‌فرض `3600` (برای کنترل سمت config؛ timeout واقعی task در factory هم هست) |

### فرمت‌های `partition_format`

| فرمت | مثال خروجی از `20260415` | کاربرد |
|------|--------------------------|--------|
| `YYYYMMDD` | `20260415` | partition روزانه میلادی |
| `YYYYMM` | `202604` | partition ماهانه میلادی |
| `YYYY` | `2026` | partition سالانه میلادی |
| `PERSIAN_YYYYMMDD` | کلید شمسی روز | partition روزانه جلالی |
| `PERSIAN_YYYYMM` | مثلاً `140501` | partition ماهانه جلالی |
| `PERSIAN_YYYY` | سال شمسی | partition سالانه جلالی |
| `PERSIAN_YYYY/MM/DD` | مثلاً `1405/01/01` | partition رشته‌ای با `/` |

Aliasها: `JALALI_*` و `SHAMSI_*` معادل `PERSIAN_*` هستند.

`execution_date` از context Airflow به‌صورت میلادی `YYYYMMDD` (`ds_nodash`) گرفته می‌شود و در صورت نیاز به شمسی تبدیل می‌گردد.

---

## ۵. انتخاب حالت Optimize

### حالت کل جدول (بدون partition)

برای dimensionها و جداول کوچک/بدون partition:

```python
OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name='cluster_2S_2R',
    database='COM',
    table_name='Local_DIM_Date',
    partition_column=None,
    final=True,
    deduplicate=True,
)
```

دستور تقریبی:

```sql
OPTIMIZE TABLE COM.Local_DIM_Date ON CLUSTER cluster_2S_2R FINAL DEDUPLICATE
```

نقطهٔ شروع در ریپو: `example_table_clickhouse_optimizer.py`

### حالت partition میلادی

```python
OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name='cluster_2S_2R',
    database='RTL',
    table_name='Local_Fact_SalesTrans',
    partition_column='COM_DIM_Date_TransRef',
    partition_format='YYYYMM',
    final=True,
    deduplicate=True,
)
```

`partition_format='YYYYMM'` — از همان `example_table_clickhouse_optimizer.py` با تنظیم `partition_column` شروع کنید.

### حالت partition شمسی (Jalali)

```python
OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name='cluster_2S_2R',
    database='RTL',
    table_name='Local_Fact_SalesTrans_V01',
    partition_column='PersianYearMonthInt',
    partition_format='PERSIAN_YYYYMM',
    final=True,
    deduplicate=True,
)
```

`partition_format='PERSIAN_YYYYMM'` (یا aliasهای `JALALI_*` / `SHAMSI_*`).

> مقدار partition عددی بدون کوتیشن و مقدار رشته‌ای (مثل `1405/05/01`) با کوتیشن در SQL ساخته می‌شود.

---

## ۶. جریان اجرای DAG (خودکار توسط Factory)

Factory در `dags/template/clickhouse_optimizer_dag_factory.py` این مراحل را می‌سازد:

```
validate_clickhouse_connection
        ↓
health_checks.check_table_health_before
        ↓
optimization.run_optimization
        ↓
check_table_health_after
```

### Validation
- تست اتصال `clickhouse_conn_id` با `validate_clickhouse_conn`

### Health before
- خواندن آمار از `system.parts` (تعداد part، ردیف، حجم دیسک)
- وضعیت: `healthy` / `warning` / `critical` بر اساس fragmentation و level-0 parts

### Optimization
- محاسبهٔ partition از `execution_date` (اگر `partition_column` ست باشد)
- اجرای `OPTIMIZE TABLE ... [ON CLUSTER] [PARTITION ...] [FINAL] [DEDUPLICATE]`
- گزارش `parts_before` / `parts_after` / مدت زمان

### Health after
- مقایسهٔ تعداد part قبل و بعد
- لاگ کاهش fragmentation

آستانه‌های health در `ClickHouseTableOptimizer.check_table_health`:

| شرط | معنی |
|-----|------|
| `parts_count > 300` | هشدار fragmentation بالا |
| `level_0_parts > 50` | هشدار partهای ادغام‌نشده زیاد |

---

## ۷. نحوهٔ اجرا

### اجرای دستی

پس از نصب pipeline:

```bash
airflow dags trigger example_table_clickhouse_optimizer
```

در Airflow UI → DAG → **Trigger DAG**:

- برای جدول بدون partition معمولاً همان کافی است.
- برای جدول partition‌دار، `logical_date` / execution date تعیین می‌کند کدام partition optimize شود (از طریق `ExecutionDateExtractor` و `partition_format`).

### زمان‌بندی پیشنهادی

| نوع جدول | پیشنهاد |
|----------|---------|
| Dimension | `schedule=None` — trigger بعد از sync کامل |
| Fact روزانه | cron بعد از اتمام ingest (مثلاً `0 8 * * *`) |
| Inventory محاسبه شده | `schedule=None` یا trigger از DAG پردازش |

### اجرا از DAG بالادستی (اختیاری)

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_optimize = TriggerDagRunOperator(
    task_id='trigger_com_dim_item_optimize',
    trigger_dag_id='com_dim_item_clickhouse_optimizer',
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
Airflow DAG: COM.Local_MyTable ClickHouse Optimization Pipeline
Uses clickhouse_optimizer_dag_factory template.
"""
from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig
from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

DAG_CONFIG = DAGConfig(
    dag_id='com_my_table_clickhouse_optimizer',
    description='Optimize COM.Local_MyTable table in ClickHouse after Kafka data ingestion',
    owner='Your Name',
    start_date=datetime(2026, 3, 24),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse', 'optimization', 'DWH', 'COM'],
)

OPTIMIZE_CONFIG = ClickHouseOptimizationConfig(
    cluster_name='cluster_2S_2R',
    database='COM',
    table_name='Local_MyTable',
    partition_column=None,          # یا مثلاً 'DateKey'
    partition_format='YYYYMM',      # یا PERSIAN_YYYYMM برای جلالی
    final=True,
    deduplicate=True,
)

conn_config = ConnectionConfig(clickhouse_conn_id='clickhouse_default')
clickhouse_optimizer_dag(DAG_CONFIG, conn_config, OPTIMIZE_CONFIG)
```

---

## ۹. چک‌لیست قبل از Production

- [ ] `database` و `table_name` دقیقاً با جدول ClickHouse یکی است (معمولاً `Local_*`)
- [ ] اگر جدول Replicated است، `cluster_name` درست است
- [ ] اگر جدول partition دارد، `partition_column` و `partition_format` با تعریف `PARTITION BY` جدول هم‌خوان است
- [ ] برای partition شمسی از `PERSIAN_*` (یا alias جلالی) استفاده شده، نه `YYYYMM` میلادی
- [ ] `final` / `deduplicate` متناسب با engine و حجم جدول انتخاب شده (FINAL روی جدول خیلی بزرگ می‌تواند سنگین باشد)
- [ ] `schedule` بعد از اتمام ingest تنظیم شده (یا `None` + trigger دستی)
- [ ] `max_active_runs=1` برای جلوگیری از overlap روی همان جدول
- [ ] Connection `clickhouse_default` در Airflow تست شده
- [ ] یک بار در محیط test اجرا و کاهش `parts_count` در لاگ/XCom بررسی شده
- [ ] pool `data_sync_pool` ظرفیت کافی دارد

---

## ۱۰. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `clickhouse_conn_id is required` | `ConnectionConfig` بدون ClickHouse | `clickhouse_conn_id='clickhouse_default'` بگذارید |
| Validation fail | Connection اشتباه یا شبکه | host/port/user و native port (۹۰۰۰) را چک کنید |
| Partition اشتباه optimize شد | `partition_format` با schema جدول یکی نیست | میلادی vs شمسی را با `SHOW CREATE TABLE` مقایسه کنید |
| `OPTIMIZE` خیلی طول می‌کشد | `FINAL` روی حجم بالا / کل جدول | فقط partition همان روز/ماه را هدف بگیرید؛ یا `final=False` موقت |
| Parts کم نشد | merge در پس‌زمینه هنوز در جریان است / threshold پایین | بعد از چند دقیقه `system.parts` را دوباره ببینید؛ health را چک کنید |
| خطای identifier | نام database/table نامعتبر | فقط حروف، عدد و `_`؛ با حرف/underscore شروع شود |
| DAG در UI نیست | مسیر فایل یا خطای parse | لاگ scheduler و importهای `template` / `pipeline` را بررسی کنید |
| Overlap روی همان جدول | چند run همزمان | `max_active_runs=1` و schedule را اصلاح کنید |

---

## ۱۱. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/clickhouse_optimizer_dag_factory.py` | Factory اصلی |
| `pipeline/config/ClickHouseOptimizationConfig.py` | پارامترهای optimize + فرمت partition |
| `pipeline/config/DAGConfig.py` | پارامترهای DAG |
| `pipeline/config/ConnectionConfig.py` | `clickhouse_conn_id` |
| `pipeline/core/ClickHouseOptimizationOrchestrator.py` | منطق health + optimize |
| `pipeline/database/ClickHouseTableOptimizer.py` | اجرای SQL و خواندن `system.parts` |
| `pipeline/core/OptimizationResult.py` | نتیجهٔ immutable |
| `dags/clickhouse_optimizer/example_table_clickhouse_optimizer.py` | نمونه Optimizer |
