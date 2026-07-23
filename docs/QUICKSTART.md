# راهنمای شروع سریع (Quickstart)

راه‌اندازی حداقلی برای اجرای pipeline روی Apache Airflow.

---

## پیش‌نیازها

- Python ≥ 3.8
- Apache Airflow ≥ 2.8
- دسترسی شبکه به SQL Server و (در صورت نیاز) Kafka / ClickHouse
- برای WinAuth: ODBC Driver 18 + تنظیمات Kerberos مناسب محیط

---

## ۱. کلون و نصب

```bash

python -m venv .venv
# Windows:
.venv\Scripts\activate
راهنمای کامل: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)
# source .venv/bin/activate

pip install -r requirements.txt
pip install -e .
```

---

## ۲. قرار دادن کد در Airflow

```bash
cp -r dags/ "$AIRFLOW_HOME/dags/"
cp -r pipeline/ "$AIRFLOW_HOME/dags/"
```

یا اگر پکیج را با `pip install -e .` نصب کرده‌اید، فقط پوشه `dags/` را کپی کنید و مطمئن شوید `PYTHONPATH` پکیج `pipeline` را می‌بیند.

پس از کپی، scheduler را یک‌بار restart کنید تا DAGها بارگذاری شوند.

---

## ۳. Connections

### SQL Server (SQL Auth)

```bash
airflow connections add 'mssql_default' \
  --conn-type 'mssql' \
  --conn-host 'your-sql-server' \
  --conn-schema 'your_database' \
  --conn-login 'username' \
  --conn-password 'password' \
  --conn-port 1433
```

> برای مسیر پیش‌فرض `pymssql`، Extra را از کلیدهای مخصوص ODBC خالی نگه دارید.


Extra نمونه:

```json
{
  "auth_mode": "kerberos",
  "driver": "ODBC Driver 18 for SQL Server",
  "trusted_connection": true,
  "encrypt": true,
  "trustservercertificate": false
}
```

### Kafka
```bash
airflow connections add 'kafka_default' \
  --conn-type 'http' \
  --conn-host 'kafka-broker' \
  --conn-port 9092 \
  --conn-extra '{"bootstrap_servers": "kafka-broker:9092", "client_id": "airflow"}'
```
### Connections رایج پروژه

| Connection ID | کاربرد |
|---------------|--------|
| اتصالات DWH / ERP | `mssql_sync` |
| `kafka_default` | تولید پیام Kafka |
| `mssql_replication_md` | Publisher Replication MD |
| `mssql_store_connectionInfo` | لیست/اطلاعات فروشگاه‌ها |
| `mssql_store_template` | یوزر/پسورد الگوی دسترسی به فروشگاه |
| ClickHouse conn | optimizer و sink اختیاری |

---
## ۴. Poolها

```bash


بسته به DAG، Variableهای زیر ممکن است لازم باشد:

| Variable | مثال / پیش‌فرض | توضیح |
|----------|----------------|--------|
| `mssql_staging_schema` | `crt` | schema staging در Subscriber |
| `max_global_parallel_chunks_replication_md_store` | `32` | سقف chunk همزمان |
| Variableهای `delete_missing_*` | `true`/`false` | حذف رکوردهای گم‌شده در scope |

تنظیمات batch/Kafka معمولاً در dataclassهای هر DAG تعریف می‌شوند؛ در صورت نیاز از Admin → Variables هم می‌توانید override کنید.

---

## ۶. اولین اجرا

```bash
# لیست DAGها
airflow dags list | findstr /I sync

# نمونه table sync
airflow dags trigger table_dwh_rtl_fact_sales_trans_sync

# نمونه sales & inventory
airflow dags trigger query_inventory_and_sales_sync
```

از UI هم می‌توانید DAG را Unpause و Trigger کنید.

---

## ۷. ساخت DAG جدید (خلاصه)

### Table → Kafka

1. از یک فایل مشابه در `dags/mssql_to_kafka_clickhouse_sync/dwh/` کپی بگیرید.
2. `DAGConfig`، `TableConfiguration`، `KafkaTopicConfig` را تنظیم کنید.
3. `create_table_sync_dag(...)` را فراخوانی کنید.

### Query → Kafka

از `create_query_sync_dag` و `QueryConfiguration` استفاده کنید. توکن‌های تاریخ:

- `{{ ds }}` → `YYYY-MM-DD`
- `{{ ds_nodash }}` → `YYYYMMDD`

### Replication MD → Store

راهنمای کامل: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)

پوشش فعلی حدود ۲۰۰+ جدول در `dags/masterdata_store_sync/tables/` است. برای فیلتر فروشگاهی، در query از `{store_number}` استفاده کنید.

### MySQL → MSSQL Sync

راهنمای کامل: [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/mysql_to_mssql_sync/example_table_to_mssql_sync.py` با `create_dag` از `mysql_to_mssql_sync_dag_factory`.

### Kafka Health Monitor

راهنمای کامل: [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md)

برای هر topic یکتای Kafka یک مانیتور در `dags/kafka_health_monitor/` (هم‌دسته با sync) بسازید:

```python
from pipeline.config.ConnectionConfig import ConnectionConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

### ClickHouse Optimizer

راهنمای کامل: [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md)

برای هر جدول ClickHouse یک DAG نازک در `dags/clickhouse_optimizer/` (یا `sales_inventory/`) بسازید:

```python
from pipeline.config.ConnectionConfig import ConnectionConfig
from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

conn_config = ConnectionConfig(clickhouse_conn_id="clickhouse_default")
clickhouse_optimizer_dag(DAG_CONFIG, conn_config, OPTIMIZE_CONFIG)
```

---

## ۸. Docker (اختیاری)

برای محیط WinAuth آماده:

```bash
cd docker/airflow-data-platform-winauth_3.2.1_harden
cp .env.example .env
# مقادیر را ویرایش کنید
docker compose -f docker-compose.winauth.yml up -d
```

جزئیات: [../docker/README.md](../docker/README.md)

---

## ۹. تست

```bash
pytest tests/ -v
```

---

## عیب‌یابی سریع

| مشکل | اقدام |
|------|--------|
| DAG ظاهر نمی‌شود | مسیر import، `__init__.py`، لاگ parser، نصب `pipeline` |
| خطای اتصال SQL | host/port/firewall؛ برای Kerberos extras و ticket را چک کنید |
| HTTP 500 هنگام `git push` | فایل‌های بزرگ (`images/*.tar`) را commit نکنید |
| Chunkهای Replication گیر کرده‌اند | اندازه pool و `max_global_parallel_chunks` را هم‌تراز کنید |
| Duplicate در Kafka | تنظیمات idempotent producer و key ستون را بررسی کنید |
| Health monitor lag اشتباه | `consumer_group` را با group واقعی sink هم‌تراز کنید |

---

## گام بعدی

- معماری: [ARCHITECTURE_FA.md](ARCHITECTURE_FA.md)
- ساختار پوشه‌ها: [../PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md)
- Replication MD: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)
- MySQL → MSSQL: [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md)
- Kafka Health Monitor: [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md)
- ClickHouse Optimizer: [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md)
- بهبودهای reliability: [QUICK_START_IMPROVEMENTS.md](QUICK_START_IMPROVEMENTS.md)

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
