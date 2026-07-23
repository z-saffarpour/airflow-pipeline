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
| **Config** | `pipeline/config/KafkaHealthMonitorConfig.py` | topic، consumer group، thresholdها |
| **DAGهای نازک** | `dags/kafka_health_monitor/` | فقط config + فراخوانی factory |

دسته‌بندی مانیتورها **هم‌تراز** با DAGهای تولیدکننده به Kafka است:

```
dags/kafka_health_monitor/
├── mssql_sync/
│   ├── dwh/              # مانیتور topicهای DWH (۱۴)
│   └── erp/              # مانیتور topicهای AX ERP (۴)
└── sales_inventory/      # مانیتور topicهای فروش و موجودی (۶)
```

در حال حاضر حدود **۲۴** مانیتور (یک مانیتور به‌ازای هر topic یکتا) پوشش داده شده است.

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

| نوع sync | مسیر مانیتور | الگوی فایل | الگوی `dag_id` |
|----------|--------------|------------|----------------|
| DWH table/query | `kafka_health_monitor/mssql_sync/dwh/` | همان نام sync با `_health_monitor` | `{sync_dag_id بدون _sync}_health_monitor` یا معادل |
| ERP query | `kafka_health_monitor/mssql_sync/erp/` | `query_ax_<name>_health_monitor.py` | `query_ax_<name>_health_monitor` |
| Sales / Inventory | `kafka_health_monitor/sales_inventory/` | `query_inventory_<name>_health_monitor.py` | `query_inventory_<name>_health_monitor` |

مثال‌ها:

| Sync DAG | Topic | Health Monitor |
|----------|-------|----------------|
| `table_dwh_com_dim_date_sync` | `dwh.table.curated.com.dim_date` | `table_dwh_com_dim_date_health_monitor` |
| `query_ax_invent_dim_sync` (+ full) | `ax.query.raw.dbo.inventdim` | `query_ax_invent_dim_health_monitor` |
| `query_inventory_purch_sync` | `ax.query.raw.inventory.purch` | `query_inventory_purch_health_monitor` |

> **یک مانیتور به‌ازای هر topic یکتا.** اگر چند DAG روی یک topic می‌نویسند (مثل inventdim incr + full)، فقط یک health monitor بسازید.

### گام ۲ — کپی از یک DAG مشابه

بهترین الگوها:

- **Dimension استاتیک (manual):** `mssql_sync/dwh/table_com_dim_date_health_monitor.py`
- **Fact پرترافیک:** `mssql_sync/dwh/table_rtl_fact_sales_trans_health_monitor.py`
- **ERP query:** `mssql_sync/erp/query_ax_invent_dim_health_monitor.py`
- **Sales / Inventory:** `sales_inventory/query_inventory_purch_health_monitor.py`

### گام ۳ — پیکربندی `DAGConfig`

```python
from datetime import datetime

from pipeline.config.DAGConfig import DAGConfig
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

### گام ۴ — پیکربندی `KafkaHealthMonitorConfig`

```python
HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
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
| `kafka_conn_id` | Connection Airflow برای Kafka |
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
kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
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

## ۵. فهرست مانیتورهای فعلی

### ۵.۱ `mssql_sync/dwh` (۱۴)

| dag_id | Topic | Related sync |
|--------|-------|--------------|
| `table_dwh_com_dim_date_health_monitor` | `dwh.table.curated.com.dim_date` | `table_dwh_com_dim_date_sync` |
| `table_dwh_com_dim_item_health_monitor` | `dwh.table.curated.com.dim_item` | `table_dwh_com_dim_item_sync` |
| `table_dwh_com_dim_time_health_monitor` | `dwh.table.curated.com.dim_time` | `table_dwh_com_dim_time_sync` |
| `table_dwh_com_dim_invent_location_health_monitor` | `dwh.table.curated.com.dim_inventlocation` | `table_dwh_com_dim_invent_location_sync` |
| `table_dwh_com_dim_invent_site_health_monitor` | `dwh.table.curated.com.dim_inventsite` | `table_dwh_com_dim_invent_site_sync` |
| `table_dwh_hrm_dim_invent_location_chart_health_monitor` | `dwh.table.curated.hrm.dim_inventlocationchart` | `table_dwh_hrm_dim_invent_location_chart_sync` |
| `table_dwh_rtl_dim_cost_amount_health_monitor` | `dwh.table.curated.rtl.dim_costamount` | `table_dwh_rtl_dim_cost_amount_sync` |
| `table_dwh_rtl_dim_organization_disc_type_health_monitor` | `dwh.table.curated.rtl.dim_organizationdisctype` | `table_dwh_rtl_dim_organization_disc_type_sync` |
| `table_dwh_rtl_dim_sale_is_return_sale_health_monitor` | `dwh.table.curated.rtl.dim_saleisreturnsale` | `table_dwh_rtl_dim_sale_is_return_sale_sync` |
| `table_dwh_rtl_dim_sales_type_health_monitor` | `dwh.table.curated.rtl.dim_salestype` | `table_dwh_rtl_dim_sales_type_sync` |
| `table_dwh_rtl_dim_system_type_health_monitor` | `dwh.table.curated.rtl.dim_systemtype` | `table_dwh_rtl_dim_system_type_sync` |
| `table_dwh_rtl_fact_sales_trans_health_monitor` | `dwh.table.curated.rtl.fact_salestrans` | `table_dwh_rtl_fact_sales_trans_sync` |
| `query_dwh_rtl_fact_sales_trans_health_monitor` | `dwh.query.curated.rtl.fact_salestrans` | `query_dwh_rtl_fact_sales_trans_sync` |
| `table_dwh_scm_fact_invent_trend_health_monitor` | `dwh.table.curated.scm.fact_inventtrend` | `table_dwh_scm_fact_invent_trend_sync` |

### ۵.۲ `mssql_sync/erp` (۴)

| dag_id | Topic | Related sync |
|--------|-------|--------------|
| `query_ax_invent_dim_health_monitor` | `ax.query.raw.dbo.inventdim` | invent_dim incr + full |
| `query_ax_invent_sum_health_monitor` | `ax.query.raw.dbo.inventsum` | invent_sum incr + full |
| `query_ax_whs_invent_reserve_health_monitor` | `ax.query.raw.dbo.whsinventreserve` | `query_ax_whs_invent_reserve_full_sync` |
| `query_ax_invent_trans_health_monitor` | `ax.query.raw.dbo.inventtrans` | `query_ax_invent_trans_full_sync` |

### ۵.۳ `sales_inventory` (۶)

| dag_id | Topic | Related sync |
|--------|-------|--------------|
| `query_inventory_sales_retail_health_monitor` | `store.query.raw.inventory.sales_retail` | retail sync + v01 |
| `query_inventory_sales_retail_hq_health_monitor` | `ax.query.raw.inventory.sales_retail` | `query_inventory_sales_retail_hq_sync` |
| `query_inventory_sales_online_health_monitor` | `ax.query.raw.inventory.sales_online` | `query_inventory_sales_online_sync` |
| `query_inventory_sales_order_health_monitor` | `ax.query.raw.inventory.sales_order` | `query_inventory_sales_order_sync` |
| `query_inventory_onhand_lite_health_monitor` | `ax.query.raw.inventory.onhand_lite` | `query_inventory_onhand_lite_sync` |
| `query_inventory_purch_health_monitor` | `ax.query.raw.inventory.purch` | `query_inventory_purch_sync` |

**بدون مانیتور (عمدی):**

- Orchestratorها (`query_ax_onhand_sync_orchestrator`, `query_inventory_and_sales_sync_orchestrator`) — خودشان به Kafka نمی‌نویسند
- DAGهای فقط ClickHouse در `sales_inventory`
- DAG monolith `query_inventory_and_sales_sync` — topicهایش با مانیتورهای split پوشش داده شده‌اند

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

HEALTH_CONFIG = KafkaHealthMonitorConfig(
    kafka_conn_id="kafka_default",
    kafka_topic="dwh.table.curated.com.dim_item",
    consumer_group="clickhouse-consumer.dwh.table.curated.com.dim_item",
    sample_count=5,
    max_lag_records=50000,
    max_lag_minutes=15,
    expected_daily_records=0,
    include_message_sampling=True,
)

kafka_health_monitor_dag(DAG_CONFIG, HEALTH_CONFIG)
```

---

## ۷. چک‌لیست قبل از Production

- [ ] Topic دقیقاً با `KafkaTopicConfig.name` / `KAFKA_TOPIC` در sync یکی است
- [ ] مانیتور در همان دستهٔ sync قرار گرفته (`mssql_sync/dwh` | `erp` | `sales_inventory`)
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
| `pipeline/utils/kafka_utils.py` | resolve broker و AdminClient |
| `pipeline/utils/validation.py` | `validate_kafka_conn` |
| `pipeline/kafka/KafkaTopicManager.py` | lag / stats / sample |
| `dags/kafka_health_monitor/mssql_sync/dwh/table_com_dim_date_health_monitor.py` | نمونه dimension |
| `dags/kafka_health_monitor/mssql_sync/dwh/table_rtl_fact_sales_trans_health_monitor.py` | نمونه fact |
| `dags/kafka_health_monitor/mssql_sync/erp/query_ax_invent_dim_health_monitor.py` | نمونه ERP |
| `dags/kafka_health_monitor/sales_inventory/query_inventory_purch_health_monitor.py` | نمونه sales/inventory |

---

## ۱۰. اجرای نمونه

```bash
# Dimension (manual)
airflow dags trigger table_dwh_com_dim_date_health_monitor

# Fact (scheduled every 4h; can also trigger manually)
airflow dags trigger table_dwh_rtl_fact_sales_trans_health_monitor

# ERP
airflow dags trigger query_ax_invent_sum_health_monitor

# Sales / Inventory
airflow dags trigger query_inventory_onhand_lite_health_monitor
```

در Airflow UI → DAG → آخرین run → task `generate_health_report` → XCom / Log را برای `overall_status` و `alerts` ببینید.

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
