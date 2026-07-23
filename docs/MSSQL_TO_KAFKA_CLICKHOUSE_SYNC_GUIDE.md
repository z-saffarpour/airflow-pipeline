# راهنمای ایجاد DAG برای Sync از SQL Server به Kafka (± ClickHouse)

این راهنما نحوهٔ افزودن یک DAG جدید در پروژه **mssql_to_kafka_clickhouse_sync** را توضیح می‌دهد. این DAGها داده را از **SQL Server** (DWH یا ERP) می‌خوانند و به **Kafka** می‌فرستند؛ در صورت نیاز می‌توان همزمان به **ClickHouse** هم نوشت.

---

## ۱. معماری کلی

```
┌──────────────────────┐     stream batches      ┌─────────────────────┐
│  SQL Server          │ ──────────────────────► │  Kafka Topic        │
│  mssql_dwh_primary   │                         │  (idempotent prod.) │
│  یا mssql_erp_primary│                         └──────────┬──────────┘
└──────────┬───────────┘                                    │
           │                                                │ (اختیاری)
           │         اگر is_send_clickhouse=True:           ▼
           └──────────────────────────────────► ┌─────────────────────┐
                                                │  ClickHouse         │
                                                │  database.table     │
                                                └─────────────────────┘
```

برخلاف Replication MD (که مقصد فروشگاه را از `ConnectionInfo` کشف می‌کند)، اینجا **منبع و مقصدها Connection ثابت Airflow** هستند و داده به‌صورت streaming / batch به Kafka (و اختیاری ClickHouse) منتقل می‌شود.

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **DWH Table Sync** | `dags/mssql_to_kafka_clickhouse_sync/dwh/table_*.py` | sync یک جدول DWH با `create_table_sync_dag` |
| **DWH Query Sync** | `dags/mssql_to_kafka_clickhouse_sync/dwh/query_*.py` | sync نتیجهٔ query سفارشی DWH |
| **ERP Query Sync** | `dags/mssql_to_kafka_clickhouse_sync/erp/query_*.py` | sync queryهای AX/ERP |
| **Orchestrator** | `dags/mssql_to_kafka_clickhouse_sync/erp/*_orchestrator.py` | اجرای چند DAG وابسته به‌صورت زنجیره‌ای |
| **Factory (Table)** | `dags/template/table_mssql_sync_dag_factory.py` | `create_table_sync_dag` |
| **Factory (Query)** | `dags/template/mssql_to_kafka_clickhouse_sync_dag_factory.py` | `create_query_sync_dag` |

برای افزودن جدول یا query جدید، معمولاً **فقط یک فایل در `dwh/` یا `erp/`** کافی است.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID (نمونه) | نقش |
|------------------------|-----|
| `mssql_dwh_primary` | منبع — SQL Server DWH |
| `mssql_erp_primary` | منبع — SQL Server ERP / AX |
| `kafka_default` | مقصد Kafka |
| `clickhouse_default` | مقصد ClickHouse (فقط اگر `is_send_clickhouse=True`) |

> نام connectionها از طریق `ConnectionConfig` قابل تغییر است؛ الزامی نیست دقیقاً همین IDها باشند.

### Pool

در صورت استفاده از pool اختصاصی روی `DAGConfig.pool`، یک‌بار در Airflow ایجاد کنید (نام را با تنظیمات خودتان هم‌تراز کنید). بسیاری از DAGهای فعلی از `default_pool` استفاده می‌کنند.

### Variableهای رایج (اختیاری)

| Variable | مثال / پیش‌فرض | توضیح |
|----------|----------------|--------|
| `batch_size_dwh_rtl_salestrans` | `50000` | اندازه batch برای fact فروش |
| Variableهای `batch_size_*` | وابسته به DAG | override اندازه batch بدون تغییر کد |

---

## ۳. دو الگوی ساخت DAG

| الگو | Factory | Config اصلی | کاربرد |
|------|---------|-------------|--------|
| **Table Sync** | `create_table_sync_dag` | `TableConfiguration` | خواندن مستقیم جدول؛ incremental با `date_column` یا full |
| **Query Sync** | `create_query_sync_dag` | `QueryConfiguration` | SELECT/CTE سفارشی؛ join، فیلتر، watermark |

هر دو مسیر گراف یکسانی می‌سازند:

```
validation → setup (ensure topic) → processing (transfer) → verify_and_completion
```

---

## ۴. مراحل ایجاد Table Sync

### گام ۱ — انتخاب نام فایل و `dag_id`

قرارداد نام‌گذاری:

- **فایل:** `dags/mssql_to_kafka_clickhouse_sync/dwh/table_<domain>_<entity>_sync.py`
- **`dag_id`:** `table_dwh_<domain>_<entity>_sync` (snake_case)

مثال: `RTL.Fact_SalesTrans` → `table_rtl_fact_sales_trans_sync.py` و `dag_id = 'table_dwh_rtl_fact_sales_trans_sync'`

### گام ۲ — کپی از یک DAG مشابه

بهترین الگوها:

- **Fact incremental (با تاریخ):** `table_rtl_fact_sales_trans_sync.py`
- **Dimension full (بدون فیلتر تاریخ):** `table_com_dim_item_sync.py`
- **با تعریف ClickHouse:** `table_rtl_dim_system_type_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime
from airflow.models import Variable

from pipeline.config import (
    DAGConfig, ConnectionConfig, KafkaTopicConfig,
    TableConfiguration, SyncConfig,
)
from template.table_mssql_sync_dag_factory import create_table_sync_dag

dag_config = DAGConfig(
    dag_id='table_dwh_rtl_my_new_table_sync',
    description='Daily sync RTL.MyNewTable from SQL Server to Kafka',
    owner='نام شما',
    start_date=datetime(2026, 4, 21),
    schedule='35 6 * * *',   # یا None برای trigger دستی
    catchup=True,            # برای fact؛ برای dim معمولاً False
    max_active_runs=5,
    tags=['mssql', 'kafka', 'table', 'DWH', 'fact', 'RTL'],
)

sync_config = SyncConfig(
    batch_size=int(Variable.get("batch_size_my_new_table", default_var=50000)),
    date_offset=-1,          # اجرا برای روز قبل (اختیاری)
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id='mssql_dwh_primary',
    kafka_conn_id='kafka_default',
)

table_config = TableConfiguration(
    table_name='RTL.MyNewTable',
    date_column='COM_DIM_Date_TransRef',  # None = full sync
    date_column_type='int',               # 'int' | 'date' | 'datetime'
    primary_key_column='ID',
    order_by_column='ID',
    columns=['ID', 'COM_DIM_Date_TransRef', '...'],
)

kafka_topic_config = KafkaTopicConfig(
    name='dwh.table.curated.rtl.my_new_table',
    num_partitions=12,
    replication_factor=3,
)

clickhouse_config = None

create_table_sync_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    table_config=table_config,
    kafka_topic_config=kafka_topic_config,
    clickhouse_config=clickhouse_config,
)
```

### گام ۴ — ایجاد در Airflow

فایل را در `dags/mssql_to_kafka_clickhouse_sync/dwh/` ذخیره کنید. Airflow پس از parse، DAG را در UI نمایش می‌دهد.

---

## ۵. مراحل ایجاد Query Sync

### گام ۱ — نام فایل و `dag_id`

| منبع | فایل | `dag_id` |
|------|------|----------|
| DWH | `dwh/query_<domain>_<entity>_sync.py` | `query_dwh_<domain>_<entity>_sync` |
| ERP | `erp/query_ax_<entity>_sync.py` | `query_ax_<entity>_sync` |
| ERP full | `erp/query_ax_<entity>_full_sync.py` | `query_ax_<entity>_full_sync` |

### گام ۲ — کپی از نمونه

- **Query با تاریخ (`{{ ds_nodash }}`):** `query_rtl_fact_sales_trans_sync.py`
- **Incremental بدون توکن (مثلاً `MODIFIEDDATETIME`):** `query_ax_invent_sum_sync.py`
- **Full load:** `query_ax_invent_sum_full_sync.py`

### گام ۳ — پیکربندی

```python
from datetime import datetime
from airflow.models import Variable

from pipeline.config import (
    DAGConfig, ConnectionConfig, KafkaTopicConfig,
    QueryConfiguration, SyncConfig,
)
from template.mssql_to_kafka_clickhouse_sync_dag_factory import create_query_sync_dag

dag_config = DAGConfig(
    dag_id='query_dwh_rtl_my_new_query_sync',
    description='Daily query sync from RTL.MyNewTable to Kafka',
    owner='نام شما',
    start_date=datetime(2026, 4, 21),
    schedule='35 6 * * *',
    catchup=True,
    max_active_runs=5,
    tags=['mssql', 'kafka', 'query', 'DWH', 'fact', 'RTL'],
)

sync_config = SyncConfig(
    batch_size=int(Variable.get("batch_size_my_new_query", default_var=50000)),
    date_offset=-1,
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id='mssql_dwh_primary',
    kafka_conn_id='kafka_default',
)

query_config = QueryConfiguration(
    source_name='adhoc_query_RTL.MyNewTable',
    query="""
        SELECT Col1, Col2, ID
        FROM RTL.MyNewTable WITH (READPAST)
        WHERE DateKey = %s
    """,
    query_params=["{{ ds_nodash }}"],
    key_column='ID',
    count_query="""
        SELECT COUNT(1) AS CNT
        FROM RTL.MyNewTable WITH (READPAST)
        WHERE DateKey = %s
    """,
    min_expected_records=0,
)

kafka_topic_config = KafkaTopicConfig(
    name='dwh.query.curated.rtl.my_new_table',
    num_partitions=12,
    replication_factor=3,
)

clickhouse_config = None

create_query_sync_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    query_config=query_config,
    kafka_topic_config=kafka_topic_config,
    clickhouse_config=clickhouse_config,
)
```

### توکن‌های تاریخ در Query

| توکن | مقدار در runtime |
|------|------------------|
| `{{ ds }}` | `YYYY-MM-DD` |
| `{{ ds_nodash }}` | `YYYYMMDD` |

این توکن‌ها هم در متن query و هم در `query_params` جایگزین می‌شوند. با `SyncConfig.date_offset` می‌توانید تاریخ اجرا را جابه‌جا کنید (مثلاً `-1` = روز قبل).

> **نکته:** روی `FROM` ترجیحاً `WITH (READPAST)` بگذارید. فقط `SELECT` / `WITH` / `EXEC` مجاز است.

---

## ۶. پارامترهای مهم Configها

### `SyncConfig`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `is_send_kafka` | خیر | پیش‌فرض `True` — ارسال به Kafka |
| `is_send_clickhouse` | خیر | پیش‌فرض `False` — ارسال به ClickHouse |
| `fail_on_error` | خیر | پیش‌فرض `True` |
| `batch_size` | خیر | اندازه fetch/produce؛ باید `> 0` |
| `date_offset` | خیر | جابه‌جایی روز نسبت به `execution_date` |

### `TableConfiguration`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `table_name` | بله | مثلاً `RTL.Fact_SalesTrans` |
| `primary_key_column` | بله | کلید پیام Kafka / pagination |
| `order_by_column` | خیر | مرتب‌سازی خواندن |
| `columns` | خیر | لیست ستون‌ها؛ `None` = همه |
| `date_column` | خیر | اگر `None` باشد full sync |
| `date_column_type` | اگر date | `'int'` / `'date'` / `'datetime'` |

### `QueryConfiguration`

| پارامتر | الزامی | توضیح |
|---------|--------|-------|
| `source_name` | بله | نام منطقی برای audit/header |
| `query` | بله | SELECT/CTE |
| `count_query` | توصیه‌شده | برای شمارش و verification |
| `query_params` | خیر | پارامترهای `%s`؛ می‌تواند توکن تاریخ داشته باشد |
| `key_column` | خیر | کلید پیام Kafka |
| `min_expected_records` | خیر | حداقل ردیف برای پاس شدن verify |

### `KafkaTopicConfig` / `ClickHouseConfig`

| پارامتر | توضیح |
|---------|-------|
| `KafkaTopicConfig.name` | نام topic |
| `num_partitions` / `replication_factor` | باید `> 0` |
| `ClickHouseConfig.database` / `table_name` | مقصد در ClickHouse |

---

## ۷. انتخاب حالت Sync

### Fact incremental (جدول با ستون تاریخ)

```python
table_config = TableConfiguration(
    table_name='RTL.Fact_SalesTrans',
    date_column='COM_DIM_Date_TransRef',
    date_column_type='int',
    primary_key_column='ID',
    order_by_column='ID',
    columns=[...],
)
sync_config = SyncConfig(date_offset=-1, batch_size=50000, is_send_kafka=True)
```

مثال: `table_rtl_fact_sales_trans_sync.py`

### Dimension / full table

```python
table_config = TableConfiguration(
    table_name='COM.DIM_Item',
    date_column=None,
    primary_key_column='ID',
    order_by_column='ID',
    columns=[...],
)
# schedule روزانه یا None؛ catchup=False
```

مثال: `table_com_dim_item_sync.py`

### Query incremental با watermark زمانی

بدون توکن تاریخ — فیلتر داخل SQL:

```sql
WHERE MODIFIEDDATETIME >= DATEADD(HOUR, -3, GETUTCDATE())
```

مثال: `query_ax_invent_sum_sync.py`

### ارسال همزمان به ClickHouse

```python
sync_config = SyncConfig(
    is_send_kafka=True,
    is_send_clickhouse=True,   # باید True شود تا transfer واقعاً بنویسد
)

conn_config = ConnectionConfig(
    mssql_conn_id='mssql_dwh_primary',
    kafka_conn_id='kafka_default',
    clickhouse_conn_id='clickhouse_default',
)

clickhouse_config = ClickHouseConfig(
    database='RTL',
    table_name='DIM_SystemType',
)
```

> حتی اگر `ClickHouseConfig` تعریف شود، بدون `is_send_clickhouse=True` داده به ClickHouse نمی‌رود. نمونهٔ تعریف config (بدون فعال‌سازی ارسال): `table_rtl_dim_system_type_sync.py`.

---

## ۸. جریان اجرای DAG (خودکار توسط Factory)

### Validation
- تست اتصال SQL Server (`mssql_conn_id`)
- در صورت `is_send_kafka`: تست Kafka
- در صورت `is_send_clickhouse`: تست ClickHouse

### Setup
- ایجاد topic در صورت نبود (`ensure_kafka_topic`) — idempotent

### Processing
- Table: ساخت query با `SQLQueryBuilder` + keyset pagination / فیلتر تاریخ
- Query: resolve توکن‌های `{{ ds }}` / `{{ ds_nodash }}` سپس اجرای query
- ارسال streaming به Kafka با `IdempotentKafkaProducer`؛ اختیاری نوشتن ClickHouse

### Verify
- مقایسه تعداد ردیف منتقل‌شده با `min_expected_records` (در Query Sync)

---

## ۹. نحوهٔ اجرا

### زمان‌بندی خودکار

بسیاری از DAGهای DWH با cron روزانه اجرا می‌شوند (مثلاً `35 6 * * *`). DAGهای ERP اغلب hourly یا `schedule=None` هستند.

### اجرای دستی

```bash
airflow dags trigger table_dwh_rtl_fact_sales_trans_sync
airflow dags trigger query_ax_invent_sum_sync
```

از UI هم می‌توانید Unpause و Trigger کنید. برای backfill factها، `catchup=True` و بازهٔ تاریخ را در نظر بگیرید.

### اجرا از Orchestrator

اگر چند query وابسته دارید (مثلاً on-hand)، یک orchestrator با `TriggerDagRunOperator` بسازید:

```python
trigger_invent_sum = TriggerDagRunOperator(
    task_id='invent_sum_sync',
    trigger_dag_id='query_ax_invent_sum_sync',
    reset_dag_run=True,
    wait_for_completion=True,
    poke_interval=60,
    allowed_states=['success'],
    failed_states=['failed'],
)
```

الگو: `dags/mssql_to_kafka_clickhouse_sync/erp/query_ax_onhand_sync_orchestrator.py`

---

## ۱۰. قالب کامل Table Sync

```python
"""
Airflow DAG: RTL.MyNewTable to Kafka
Uses table_mssql_sync_dag_factory template.
"""
from datetime import datetime
from airflow.models import Variable

from pipeline.config import (
    DAGConfig, ConnectionConfig, KafkaTopicConfig,
    TableConfiguration, SyncConfig,
)
from template.table_mssql_sync_dag_factory import create_table_sync_dag

dag_config = DAGConfig(
    dag_id='table_dwh_rtl_my_new_table_sync',
    description='Sync RTL.MyNewTable from SQL Server to Kafka',
    owner='Your Name',
    start_date=datetime(2026, 4, 21),
    schedule='0 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['mssql', 'kafka', 'table', 'DWH'],
)

sync_config = SyncConfig(
    batch_size=int(Variable.get("batch_size_my_new_table", default_var=10000)),
    is_send_kafka=True,
    is_send_clickhouse=False,
)

conn_config = ConnectionConfig(
    mssql_conn_id='mssql_dwh_primary',
    kafka_conn_id='kafka_default',
)

table_config = TableConfiguration(
    table_name='RTL.MyNewTable',
    date_column=None,
    primary_key_column='ID',
    order_by_column='ID',
    columns=['ID', 'Name'],
)

kafka_topic_config = KafkaTopicConfig(
    name='dwh.table.curated.rtl.my_new_table',
    num_partitions=3,
    replication_factor=3,
)

create_table_sync_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    table_config=table_config,
    kafka_topic_config=kafka_topic_config,
    clickhouse_config=None,
)
```

---

## ۱۱. چک‌لیست قبل از Production

- [ ] `mssql_conn_id` / `kafka_conn_id` در Airflow تعریف و تست شده‌اند
- [ ] نام topic با قرارداد نام‌گذاری پروژه هم‌خوان است (`dwh.table...` / `ax.query...`)
- [ ] `num_partitions` و `replication_factor` با cluster واقعی سازگارند
- [ ] برای Fact، `date_column` و `date_column_type` درست تنظیم شده‌اند
- [ ] برای Query، `count_query` همان فیلتر `query` را دارد
- [ ] توکن‌های تاریخ (`{{ ds }}` / `{{ ds_nodash }}`) با نوع ستون تاریخ هم‌خوان‌اند
- [ ] `key_column` / `primary_key_column` برای idempotency انتخاب شده
- [ ] `WITH (READPAST)` روی کوئری‌های سنگین اعمال شده
- [ ] اگر ClickHouse لازم است: `is_send_clickhouse=True` + `clickhouse_conn_id` + `ClickHouseConfig`
- [ ] `batch_size` برای حجم داده tune شده (Variable یا مقدار ثابت)
- [ ] DAG یک‌بار در test اجرا و متریک transfer بررسی شده
- [ ] (اختیاری) orchestrator و health monitor برای topic جدید اضافه شده

---

## ۱۲. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| DAG ظاهر نمی‌شود | خطای parse / import | لاگ parser و مسیر `template.*` را چک کنید |
| `mssql_conn_id is required` | `ConnectionConfig` ناقص | `mssql_conn_id` را ست کنید |
| `kafka_conn_id is required when is_send_kafka` | Kafka روشن بدون conn | `kafka_conn_id` بدهید یا `is_send_kafka=False` |
| Verification failed | ردیف کمتر از حداقل | `min_expected_records` یا فیلتر تاریخ را بررسی کنید |
| Duplicate در Kafka | کلید پیام نامناسب | `key_column` / `primary_key_column` را اصلاح کنید |
| حافظه بالا / OOM | `batch_size` خیلی بزرگ | batch را کم کنید؛ stream را حفظ کنید |
| تاریخ اشتباه در query | توکن یا `date_offset` | `{{ ds_nodash }}` و offset را با ستون مقصد هم‌تراز کنید |
| ClickHouse خالی است | flag خاموش است | `is_send_clickhouse=True` و conn/config را ست کنید |
| Topic ساخته نمی‌شود | ACL / auth Kafka | دسترسی AdminClient و `ensure_kafka_topic` را بررسی کنید |

---

## ۱۳. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/table_mssql_sync_dag_factory.py` | Factory Table Sync |
| `dags/template/mssql_to_kafka_clickhouse_sync_dag_factory.py` | Factory Query Sync |
| `pipeline/config/SyncConfig.py` | پرچم‌های Kafka/ClickHouse و batch |
| `pipeline/config/TableConfiguration.py` | تنظیمات جدول |
| `pipeline/config/QueryConfiguration.py` | تنظیمات query + توکن تاریخ |
| `pipeline/config/KafkaTopicConfig.py` | نام و پارتیشن topic |
| `pipeline/config/ClickHouseConfig.py` | مقصد ClickHouse |
| `pipeline/core/MSSQLDataTransferOrchestrator.py` | منطق انتقال |
| `dags/mssql_to_kafka_clickhouse_sync/dwh/table_rtl_fact_sales_trans_sync.py` | نمونه Fact incremental |
| `dags/mssql_to_kafka_clickhouse_sync/dwh/table_com_dim_item_sync.py` | نمونه Dimension full |
| `dags/mssql_to_kafka_clickhouse_sync/dwh/query_rtl_fact_sales_trans_sync.py` | نمونه Query با تاریخ |
| `dags/mssql_to_kafka_clickhouse_sync/erp/query_ax_invent_sum_sync.py` | نمونه ERP incremental |
| `dags/mssql_to_kafka_clickhouse_sync/erp/query_ax_onhand_sync_orchestrator.py` | نمونه Orchestrator |

### اسناد مرتبط

| سند | موضوع |
|-----|--------|
| [QUICKSTART.md](QUICKSTART.md) | راه‌اندازی سریع |
| [ARCHITECTURE_FA.md](ARCHITECTURE_FA.md) | معماری جریان Table/Query Sync |
| [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md) | الگوی مشابه MSSQL→Store |
| [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md) | مانیتور lag برای topic جدید |
| [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md) | بهینه‌سازی جداول ClickHouse |
