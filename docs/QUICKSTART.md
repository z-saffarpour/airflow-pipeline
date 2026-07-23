# راهنمای شروع سریع (Quickstart)

راه‌اندازی حداقلی برای اجرای pipeline روی Apache Airflow.

---

## پیش‌نیازها

- Python ≥ 3.8
- Apache Airflow ≥ 2.8
- دسترسی شبکه به SQL Server و (در صورت نیاز) Kafka / ClickHouse / MySQL / MongoDB
- برای WinAuth: ODBC Driver 18 + تنظیمات Kerberos مناسب محیط

---

## ۱. کلون و نصب

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
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

### SQL Server (Windows Auth / Kerberos)

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
| اتصالات DWH / ERP | `mssql_to_kafka_clickhouse_sync` و مسیرهای مرتبط |
| `kafka_default` | تولید/مصرف پیام Kafka |
| `mssql_replication_md` | Publisher Replication MD |
| `mssql_store_connectionInfo` | لیست/اطلاعات فروشگاه‌ها |
| `mssql_store_template` | یوزر/پسورد الگوی دسترسی به فروشگاه |
| ClickHouse conn | optimizer و sink / sync |
| MySQL / Mongo conn | مسیرهای MySQL↔MSSQL و Mongo↔MSSQL |

---

## ۴. Poolها

```bash
airflow pools set data_sync_pool 5 "SQL to Kafka transfers"
airflow pools set replication_md_store_sync_pool 32 "Replication MD store chunk sync"
```

تعداد slot در `replication_md_store_sync_pool` باید ≥ `max_global_parallel_chunks` باشد.

---

## ۵. Variables

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

### Table / Query → Kafka (± ClickHouse)

راهنمای کامل: [MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md)

1. از یک فایل مشابه در `dags/mssql_to_kafka_clickhouse_sync/dwh/` یا `erp/` کپی بگیرید.
2. برای Table: `DAGConfig`، `TableConfiguration`، `KafkaTopicConfig` و `create_table_sync_dag(...)`.
3. برای Query: `QueryConfiguration` و `create_query_sync_dag(...)`؛ توکن‌های `{{ ds }}` / `{{ ds_nodash }}`.

### Replication MD → Store

راهنمای کامل: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)

پوشش فعلی حدود ۲۰۰+ جدول در `dags/masterdata_store_sync/tables/` است. برای فیلتر فروشگاهی، در query از `{store_number}` استفاده کنید.

### MySQL → MSSQL Sync

راهنمای کامل: [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/mysql_to_mssql_sync/example_table_to_mssql_sync.py` با `create_dag` از `mysql_to_mssql_sync_dag_factory`.

### MSSQL → MySQL Sync

راهنمای کامل: [MSSQL_TO_MYSQL_SYNC_GUIDE.md](MSSQL_TO_MYSQL_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_mysql_sync/example_table_to_mysql_sync.py` با `create_dag` از `mssql_to_mysql_sync_dag_factory`.

### MSSQL → PostgreSQL Sync

راهنمای کامل: [MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md](MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_postgresql_sync/example_table_to_postgresql_sync.py` با `create_dag` از `mssql_to_postgresql_sync_dag_factory`.

### MSSQL → MSSQL Sync (fixed connections)

راهنمای کامل: [MSSQL_TO_MSSQL_SYNC_GUIDE.md](MSSQL_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_mssql_sync/example_table_to_mssql_sync.py` با `create_dag` از `mssql_to_mssql_sync_dag_factory`.

برای مسیر پویا Publisher → Store فروشگاه، از [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md) استفاده کنید.

### MSSQL → ClickHouse Sync

راهنمای کامل: [MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_clickhouse_sync/example_table_to_clickhouse_sync.py` با `create_dag` از `mssql_to_clickhouse_sync_dag_factory`.

### MSSQL → Kafka Sync (Gen-2)

راهنمای کامل: [MSSQL_TO_KAFKA_SYNC_GUIDE.md](MSSQL_TO_KAFKA_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_kafka_sync/example_table_to_kafka_sync.py` با `create_dag` از `mssql_to_kafka_sync_dag_factory`.

### MSSQL → MongoDB Sync

راهنمای کامل: [MSSQL_TO_MONGO_SYNC_GUIDE.md](MSSQL_TO_MONGO_SYNC_GUIDE.md)

نمونه: `dags/mssql_to_mongo_sync/example_table_to_mongo_sync.py` با `create_dag` از `mssql_to_mongo_sync_dag_factory`.

### MongoDB → MSSQL Sync

راهنمای کامل: [MONGO_TO_MSSQL_SYNC_GUIDE.md](MONGO_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/mongo_to_mssql_sync/example_collection_to_mssql_sync.py` با `create_dag` از `mongo_to_mssql_sync_dag_factory`.

### Kafka → MSSQL Sync

راهنمای کامل: [KAFKA_TO_MSSQL_SYNC_GUIDE.md](KAFKA_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/kafka_to_mssql_sync/example_topic_to_mssql_sync.py` با `create_dag` از `kafka_to_mssql_sync_dag_factory`.

### ClickHouse → MSSQL Sync

راهنمای کامل: [CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md)

نمونه: `dags/clickhouse_to_mssql_sync/example_table_to_mssql_sync.py` با `create_dag` از `clickhouse_to_mssql_sync_dag_factory`.

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
- SQL Server → Kafka: [MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md)
- Replication MD: [MASTERDATA_STORE_SYNC_GUIDE.md](MASTERDATA_STORE_SYNC_GUIDE.md)
- MySQL → MSSQL: [MYSQL_TO_MSSQL_SYNC_GUIDE.md](MYSQL_TO_MSSQL_SYNC_GUIDE.md)
- MSSQL → MySQL: [MSSQL_TO_MYSQL_SYNC_GUIDE.md](MSSQL_TO_MYSQL_SYNC_GUIDE.md)
- MSSQL → PostgreSQL: [MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md](MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md)
- MSSQL → MSSQL: [MSSQL_TO_MSSQL_SYNC_GUIDE.md](MSSQL_TO_MSSQL_SYNC_GUIDE.md)
- MSSQL → ClickHouse: [MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md](MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md)
- MSSQL → Kafka (Gen-2): [MSSQL_TO_KAFKA_SYNC_GUIDE.md](MSSQL_TO_KAFKA_SYNC_GUIDE.md)
- MSSQL → MongoDB: [MSSQL_TO_MONGO_SYNC_GUIDE.md](MSSQL_TO_MONGO_SYNC_GUIDE.md)
- MongoDB → MSSQL: [MONGO_TO_MSSQL_SYNC_GUIDE.md](MONGO_TO_MSSQL_SYNC_GUIDE.md)
- Kafka → MSSQL: [KAFKA_TO_MSSQL_SYNC_GUIDE.md](KAFKA_TO_MSSQL_SYNC_GUIDE.md)
- ClickHouse → MSSQL: [CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md)
- Kafka Health Monitor: [KAFKA_HEALTH_MONITOR_GUIDE.md](KAFKA_HEALTH_MONITOR_GUIDE.md)
- ClickHouse Optimizer: [CLICKHOUSE_OPTIMIZER_GUIDE.md](CLICKHOUSE_OPTIMIZER_GUIDE.md)
- بهبودهای reliability: [QUICK_START_IMPROVEMENTS.md](QUICK_START_IMPROVEMENTS.md)

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
