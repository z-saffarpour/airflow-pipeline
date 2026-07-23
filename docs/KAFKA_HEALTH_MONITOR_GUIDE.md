# راهنمای ایجاد DAG برای Kafka Health Monitor

این راهنما نحوهٔ افزودن یک DAG مانیتورینگ سلامت برای topicهای Kafka را توضیح می‌دهد. این DAGها پس از sync داده به Kafka، وضعیت **cluster**، **consumer lag**، **آمار topic** و (اختیاری) **نمونه پیام** را بررسی و گزارش می‌کنند.

---

## ۱. معماری کلی

```
┌──────────────────┐     produce      ┌─────────────────┐
│  Sync DAG        │ ───────────────► │  Kafka Topic    │
│  (mssql_sync /   │                  │                 │
│   sales_inventory)│                 └────────┬────────┘
└──────────────────┘                           │
                                               │ monitor
                                               ▼
                                    ┌──────────────────────┐
                                    │  Health Monitor DAG  │
                                    │  (این راهنما)        │
                                    └──────────┬───────────┘
                                               │
                                               ▼
                                    گزارش: healthy / warning / error
```

### لایه‌های پروژه

| لایه | مسیر | نقش |
|------|------|-----|
| **Factory** | `dags/template/kafka_health_monitor_dag_factory.py` | ساخت DAG و taskها از روی config |
| **Config** | `pipeline/config/KafkaHealthMonitorConfig.py` + `ConnectionConfig` | topic، consumer group، thresholdها؛ اتصال Kafka از `ConnectionConfig` |
| **DAGهای نازک** | `dags/kafka_health_monitor/` | فقط config + فراخوانی factory |

دسته‌بندی مانیتورها:

```
dags/kafka_health_monitor/
└── example_topic_health_monitor.py
```

برای افزودن مانیتور جدید: از نمونه کپی کنید و فایل را در مسیر دلخواه قرار دهید.

---

## ۲. پیش‌نیازها

### اتصالات Airflow (Connection)

| Connection ID | نقش |
|---------------|-----|
| `kafka_default` | Kafka brokers (همان اتصال sync DAGها) |

نمونه Extra برای Kafka:

```json
{
  "bootstrap_servers": "broker1:9092,broker2:9092",
  "client_id": "airflow-monitor",
  "security_protocol": "SASL_SSL",
  "sasl_mechanism": "PLAIN",
  "sasl_username": "...",
  "sasl_password": "..."
}
```

> فیلدهای امنیتی اختیاری‌اند؛ اگر در Connection نباشند، اتصال plain استفاده می‌شود.

### Pool

مانیتورها از pool پیش‌فرض `DAGConfig` استفاده می‌کنند (`data_sync_pool`):

```bash
airflow pools set data_sync_pool 5 "SQL to Kafka transfers / health monitors"
```

### Consumer Group

برای چک کردن lag باید `consumer_group` واقعی downstream (مثلاً ClickHouse / sink) را بگذارید.

قرارداد فعلی در پروژه:

```text
clickhouse-consumer.{topic_name}
```

مثال: topic `dwh.table.curated.com.dim_date` →  
`clickhouse-consumer.dwh.table.curated.com.dim_date`

اگر نام group در محیط شما فرق دارد، فقط فیلد `consumer_group` در config را عوض کنید.

---

## ۳. مراحل ایجاد DAG جدید

### گام ۱ — انتخاب پوشه، نام فایل و `dag_id`

قرارداد نام‌گذاری پیشنهادی:

| نوع sync | مسیر پیشنهادی مانیتور | الگوی فایل | الگوی `dag_id` |
|----------|----------------------|------------|----------------|
| DWH table/query | `kafka_health_monitor/mssql_sync/dwh/` | همان نام sync با `_health_monitor` | `{sync_dag_id بدون _sync}_health_monitor` یا معادل |
| ERP query | `kafka_health_monitor/mssql_sync/erp/` | `query_ax_<name>_health_monitor.py` | `query_ax_<name>_health_monitor` |
| Sales / Inventory | `kafka_health_monitor/sales_inventory/` | `query_inventory_<name>_health_monitor.py` | `query_inventory_<name>_health_monitor` |

> **یک مانیتور به‌ازای هر topic یکتا.** اگر چند DAG روی یک topic می‌نویسند، فقط یک health monitor بسازید.

### گام ۲ — کپی از DAG نمونه

از نمونه کپی کنید:

- **`dags/kafka_health_monitor/example_topic_health_monitor.py`**

سپس `kafka_topic`، `consumer_group` و آستانه‌ها را مطابق pipeline خودتان تنظیم کنید.

### گام ۳ — پیکربندی `DAGConfig`

```python
from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

DAG_CONFIG = DAGConfig(
    dag_id="table_dwh_com_dim_item_health_monitor",
    description="Monitor COM.DIM_Item Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule="0 */4 * * *",   # یا None برای dimensionهای نادر
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=["monitoring", "health-check", "kafka", "mssql", "DWH", "dimension", "COM"],
)
```

پیشنهاد زمان‌بندی:

| نوع داده | `schedule` پیشنهادی |
|----------|---------------------|
| Dimension استاتیک / نادر | `None` (دستی، بعد از sync) |
| Dimension روزانه / Fact / ERP / Inventory | `0 */4 * * *` (هر ۴ ساعت) |

### گام ۴ — پیکربندی `ConnectionConfig` و `KafkaHealthMonitorConfig`

```python
conn_config = ConnectionConfig(kafka_conn_id="kafka_default")

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="dwh.table.curated.com.dim_item",
    consumer_group="clickhouse-consumer.dwh.table.curated.com.dim_item",
    sample_count=5,
    max_lag_records=50000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)
```

| فیلد | توضیح |
|------|--------|
| `conn_config.kafka_conn_id` | Connection Airflow برای Kafka (از `ConnectionConfig`) |
| `kafka_topic` | نام topic (باید با `KafkaTopicConfig.name` در sync یکی باشد) |
| `consumer_group` | گروه مصرف‌کننده برای چک lag؛ خالی = skip lag |
| `sample_count` | تعداد پیام نمونه از partition 0 |
| `max_lag_records` | آستانه هشدار lag (تعداد پیام) |
| `max_lag_minutes` | آستانه زمانی (در گزارش thresholds ثبت می‌شود) |
| `expected_daily_records` | حجم روزانه مورد انتظار (برای گزارش) |
| `include_message_sampling` | اگر `False`، task نمونه‌گیری حذف می‌شود |

آستانه‌های پیشنهادی:

| نوع | `max_lag_records` | `max_lag_minutes` | `expected_daily_records` |
|-----|-------------------|-------------------|--------------------------|
| Dimension کوچک | `10000` | `15` | `0` |
| Dimension بزرگ‌تر | `50000` | `15` | `0` |
| Fact / Query پرترافیک | `500000`–`1000000` | `60` | متناسب با حجم واقعی |
| Sales / Inventory | `200000`–`500000` | `60` | متناسب با حجم واقعی |

### گام ۵ — ساخت DAG

```python
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

### گام ۶ — ایجاد در Airflow

فایل را در مسیر صحیح زیر `dags/kafka_health_monitor/` ذخیره کنید. پس از parse، DAG در UI ظاهر می‌شود.

---

## ۴. جریان اجرای DAG (خودکار توسط Factory)

```
┌─────────────────────────────────────────┐
│  TaskGroup: health_checks (موازی)       │
│  ├─ validate_kafka_health               │
│  ├─ check_consumer_lag                  │
│  └─ check_topic_stats                   │
└──────────────────┬──────────────────────┘
                   │
                   ▼
         sample_recent_messages   (اگر include_message_sampling=True)
                   │
                   ▼
         generate_health_report
```

خروجی نهایی (`generate_health_report`) شامل:

- `overall_status`: `healthy` | `warning` | `error` | `unknown`
- `alerts` و `alert_count`
- `thresholds`
- جزئیات هر چک در `checks`
- خلاصه در `summary` (broker_count، total_lag، topic، consumer_group)

---

## ۵. نمونه

| فایل | dag_id |
|------|--------|
| `dags/kafka_health_monitor/example_topic_health_monitor.py` | `example_topic_health_monitor` |

برای هر topic یکتای Kafka یک مانیتور بسازید.

---

## ۶. قالب کامل (Template)

```python
"""
Airflow DAG: COM.DIM_Item Pipeline Health Monitor
=================================================
Monitor COM.DIM_Item Kafka pipeline health.

Related sync DAG(s): table_dwh_com_dim_item_sync
Topic: dwh.table.curated.com.dim_item

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

DAG_CONFIG = DAGConfig(
    dag_id="table_dwh_com_dim_item_health_monitor",
    description="Monitor COM.DIM_Item Kafka pipeline health",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 1, 1),
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    retries=1,
    tags=["monitoring", "health-check", "kafka", "mssql", "DWH", "dimension", "COM"],
)

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_topic="dwh.table.curated.com.dim_item",
    consumer_group="clickhouse-consumer.dwh.table.curated.com.dim_item",
    sample_count=5,
    max_lag_records=50000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)

kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

---

## ۷. چک‌لیست قبل از Production

- [ ] Topic دقیقاً با `KafkaTopicConfig.name` / `KAFKA_TOPIC` در sync یکی است
- [ ] مانیتور برای topic یکتا ساخته شده (مثلاً هم‌دسته با sync)
- [ ] برای topic مشترک، فقط **یک** مانیتور ساخته شده
- [ ] `consumer_group` با group واقعی sink هماهنگ است
- [ ] آستانه `max_lag_records` با حجم topic متناسب است
- [ ] `schedule` برای dimension استاتیک `None` و برای pipeline فعال تنظیم شده
- [ ] Connection `kafka_default` در محیط test/prod درست است
- [ ] یک بار دستی trigger شده و `overall_status` در XCom/لاگ بررسی شده
- [ ] Orchestrator یا ClickHouse-only به‌اشتباه مانیتور نشده

---

## ۸. عیب‌یابی رایج

| مشکل | علت احتمالی | راه‌حل |
|------|-------------|--------|
| `Kafka validation failed` | Connection اشتباه / شبکه / SASL | `kafka_default` و Extra را چک کنید |
| `No Kafka brokers available` | bootstrap خالی یا broker down | `bootstrap_servers` و وضعیت cluster |
| Lag همیشه `skipped` | `consumer_group` یا topic خالی | هر دو را در `HEALTH_CONFIG` پر کنید |
| Lag همیشه `error` / صفر اشتباه | نام group با sink یکی نیست | group واقعی ClickHouse/consumer را بگذارید |
| Topic stats error | topic هنوز ساخته نشده | اول sync را اجرا کنید تا topic ایجاد شود |
| Sample خالی | partition خالی یا offset latest بدون پیام | بعد از produce دوباره امتحان کنید |
| DAG ظاهر نمی‌شود | مسیر اشتباه / خطای parse | لاگ scheduler و import factory را ببینید |
| هشدارهای کاذب lag | آستانه خیلی کم | `max_lag_records` را بالا ببرید |

---

## ۹. فایل‌های مرجع

| فایل | کاربرد |
|------|--------|
| `dags/template/kafka_health_monitor_dag_factory.py` | Factory اصلی |
| `pipeline/config/KafkaHealthMonitorConfig.py` | پارامترهای مانیتور |
| `pipeline/config/DAGConfig.py` | پارامترهای DAG |
| `pipeline/kafka/KafkaConnectionFactory.py` | resolve broker، list_topics، cluster info |
| `pipeline/utils/kafka_utils.py` | thin wrapper سازگاری با کد قدیمی |
| `pipeline/utils/validation.py` | `validate_kafka_conn` |
| `pipeline/kafka/KafkaTopicManager.py` | lag / stats / sample از روی `conn_id` |
| `dags/kafka_health_monitor/example_topic_health_monitor.py` | نمونه Health Monitor |

---

## ۱۰. اجرای نمونه

```bash
airflow dags trigger example_topic_health_monitor
```

در Airflow UI → DAG → آخرین run → task `generate_health_report` → XCom / Log را برای `overall_status` و `alerts` ببینید.

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
