# ساختار پروژه (Project Structure)

توضیح درخت پوشه‌ها و نقش هر بخش در ریپازیتوری فعلی.

---

## درخت سطح بالا

```
sqlserver-kafka-pipeline/
├── dags/                 # تعریف DAGهای Airflow
├── pipeline/             # هستهٔ مشترک (پکیج Python)
├── docker/               # Dockerfile و docker-compose با WinAuth
├── docs/                 # مستندات
├── tests/                # تست‌های واحد
├── images/               # آرشیو Docker (در Git نیست)
├── scripts/              # اسکریپت‌های کمکی (در صورت وجود)
├── requirements.txt
├── setup.py
├── LICENSE
├── .gitignore
├── README.md
└── PROJECT_STRUCTURE.md  # این فایل
```

---

## `dags/` — تعریف‌های Airflow

```
dags/
├── template/                      # Factoryهای قابل‌استفاده مجدد
│   ├── table_mssql_sync_dag_factory.py
│   ├── mssql_to_kafka_clickhouse_sync_dag_factory.py
│   ├── mssql_masterdata_to_mssql_store_sync_dag_factory.py
│   ├── mysql_to_mssql_sync_dag_factory.py
│   ├── mssql_to_mysql_sync_dag_factory.py
│   ├── mssql_to_mssql_sync_dag_factory.py
│   ├── mssql_to_mongo_sync_dag_factory.py
│   ├── mongo_to_mssql_sync_dag_factory.py
│   ├── kafka_to_mssql_sync_dag_factory.py
│   ├── clickhouse_to_mssql_sync_dag_factory.py
│   ├── clickhouse_optimizer_dag_factory.py
│   └── kafka_health_monitor_dag_factory.py
│
├── mssql_to_kafka_clickhouse_sync/ # SQL Server → Kafka (± ClickHouse)
│   ├── dwh/                       # جداول و کوئری‌های DWH
│   └── erp/                       # کوئری‌ها و orchestratorهای ERP/AX
│
├── mysql_to_mssql_sync/           # MySQL → MSSQL (upsert / replication-style)
│
├── mssql_to_mysql_sync/           # MSSQL → MySQL (upsert / replication-style)
│
├── mssql_to_mssql_sync/           # MSSQL → MSSQL (upsert / fixed connections)
│
├── mssql_to_mongo_sync/           # MSSQL → MongoDB (upsert / replication-style)
│
├── mongo_to_mssql_sync/           # MongoDB → MSSQL (upsert / replication-style)
│
├── kafka_to_mssql_sync/           # Kafka → MSSQL (upsert / replication-style)
│
├── clickhouse_to_mssql_sync/      # ClickHouse → MSSQL (upsert / replication-style)
│
├── sales_inventory/               # فروش و موجودی چندمنبعی → Kafka
│
├── replication/                   # تعمیر Replication MD
│   ├── tables/                    # ~200+ DAG sync تک‌جدول برای فروشگاه
│   ├── orchestrator/              # زنجیره چند جدول برای یک فروشگاه
│   └── reconcile_and_sync/        # تشخیص gap و trigger خودکار
│
├── clickhouse_optimizer/          # بهینه‌سازی جداول ClickHouse
│   ├── dwh/
│   └── erp/
│
└── kafka_health_monitor/          # مانیتورینگ سلامت pipeline / Kafka
    ├── mssql_sync/
    │   ├── dwh/                   # مانیتور topicهای DWH
    │   └── erp/                   # مانیتور topicهای AX ERP
    └── sales_inventory/           # مانیتور topicهای فروش و موجودی
```

### قرارداد نام‌گذاری DAGها

| حوزه | الگوی فایل | مثال |
|------|------------|------|
| Table sync DWH | `table_<domain>_<name>_sync.py` | `table_rtl_fact_sales_trans_sync.py` |
| Query sync ERP | `query_ax_<name>_sync.py` | `query_ax_invent_sum_sync.py` |
| Replication table | `ax_<table>_sync.py` | `ax_invent_table_sync.py` |
| MySQL → MSSQL | `<name>_to_mssql_sync.py` | `example_table_to_mssql_sync.py` |
| MSSQL → MySQL | `<name>_to_mysql_sync.py` | `example_table_to_mysql_sync.py` |
| MSSQL → MongoDB | `<name>_to_mongo_sync.py` | `example_table_to_mongo_sync.py` |
| MongoDB → MSSQL | `<name>_to_mssql_sync.py` | `example_collection_to_mssql_sync.py` |
| Kafka → MSSQL | `<name>_to_mssql_sync.py` | `example_topic_to_mssql_sync.py` |
| ClickHouse → MSSQL | `<name>_to_mssql_sync.py` | `example_table_to_mssql_sync.py` |
| ClickHouse optimize | `<name>_clickhouse_optimizer.py` | `com_dim_item_clickhouse_optimizer.py` |
| Kafka health monitor | `<name>_health_monitor.py` | `dim_date_health_monitor.py` |

دامنه‌های رایج در `replication/tables/`: `retail_*`، `invent_*`، `ecores_*`، `logistics_*`، `hcm_*`، `tax_*`، `om_*`، و جداول پایه مثل `company_*` / `currency` / `cust_*`.

برای جداول فیلترشده بر اساس فروشگاه، query می‌تواند `{store_number}` داشته باشد (جایگزینی در factory).

### الگوی ثبت DAG

فایل‌های مبتنی بر factory معمولاً فقط config می‌سازند و در زمان import، factory را صدا می‌زنند:

```python
from template.table_mssql_sync_dag_factory import create_table_sync_dag

create_table_sync_dag(
    dag_config=...,
    connection_config=...,
    sync_config=...,
    table_config=...,
    kafka_topic_config=...,
    clickhouse_config=None,
)
```

برای health monitor:

```python
from pipeline.config.ConnectionConfig import ConnectionConfig
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

DAGهای پیچیده (مثل `sales_inventory` و `reconcile_and_sync`) به‌صورت سفارشی نوشته شده‌اند.

---

## `pipeline/` — هستهٔ مشترک

```
pipeline/
├── __init__.py                 # صادرات عمومی پکیج
├── interfaces/                 # ABCها
│   ├── DataReader.py
│   ├── DataWriter.py
│   ├── MessageProducer.py
│   ├── TopicManager.py
│   ├── MessageSerializerInterface.py
│   └── DatabaseOptimizer.py
│
├── config/                     # dataclassهای پیکربندی
│   ├── DAGConfig.py
│   ├── SyncConfig.py
│   ├── TableConfiguration.py
│   ├── QueryConfiguration.py
│   ├── MasterDataSyncConfig.py
│   ├── MongoSyncConfig.py
│   ├── KafkaSyncConfig.py
│   ├── ConnectionConfig.py
│   ├── KafkaTopicConfig.py
│   ├── KafkaProducerConfig.py
│   ├── ClickHouseConfig.py
│   ├── ClickHouseOptimizationConfig.py
│   ├── KafkaHealthMonitorConfig.py
│   ├── AuditConfig.py
│   └── ...
│
├── database/
│   ├── SafeMsSqlHook.py        # pymssql / pyodbc + Kerberos
│   ├── ConnectionFactory.py
│   ├── SQLQueryBuilder.py
│   ├── MSSQLDataReader.py
│   ├── MSSQLServerWriter.py
│   ├── MySQLConnectionFactory.py
│   ├── MySQLDataReader.py
│   ├── MySQLServerWriter.py
│   ├── MongoDBConnectionFactory.py
│   ├── MongoDBDataReader.py
│   ├── MongoDBServerWriter.py
│   ├── ClickHouseConnectionFactory.py
│   ├── ClickHouseDataReader.py
│   ├── ClickHouseWriter.py
│   └── ClickHouseTableOptimizer.py
│
├── kafka/
│   ├── KafkaConnectionFactory.py   # Airflow conn → AdminClient / bootstrap
│   ├── IdempotentKafkaProducer.py
│   ├── KafkaDataConsumer.py        # batch consume for Kafka→MSSQL sync
│   ├── KafkaTopicManager.py        # topic ops via KafkaConnectionFactory(conn_id)
│   └── MessageSerializer.py
│
├── core/
│   ├── MSSQLDataTransferOrchestrator.py      # SQL → Kafka/CH
│   ├── MSSQLToMSSQLQueryOrchestrator.py      # MSSQL → MSSQL (store URI یا conn ثابت)
│   ├── MySQLToMSSQLQueryOrchestrator.py      # MySQL → MSSQL
│   ├── MSSQLToMySQLQueryOrchestrator.py      # MSSQL → MySQL
│   ├── MSSQLToMongoDBQueryOrchestrator.py    # MSSQL → MongoDB
│   ├── MongoDBToMSSQLQueryOrchestrator.py    # Mongo → MSSQL
│   ├── KafkaToMSSQLQueryOrchestrator.py      # Kafka → MSSQL
│   ├── ClickHouseToMSSQLQueryOrchestrator.py # ClickHouse → MSSQL
│   ├── ClickHouseOptimizationOrchestrator.py
│   ├── DagSyncTrigger.py
│   ├── ExecutionDateExtractor.py
│   ├── TransferResult.py / TransferMetrics.py
│   ├── OptimizationResult.py
│   └── exceptions.py
│
├── utils/
│   ├── validation.py
│   ├── retry_helper.py
│   ├── AuditLogger.py
│   ├── kafka_utils.py
│   └── connection_utils.py
│
└── compat/
    └── airflow_compat.py       # سازگاری Airflow 2.x / 3.x
```

| پوشه | مسئولیت |
|------|---------|
| `interfaces/` | قراردادها برای Dependency Inversion |
| `config/` | جداسازی تنظیمات از منطق کسب‌وکار |
| `database/` | دسترسی به SQL Server و ClickHouse |
| `kafka/` | تولید/مصرف پیام و مدیریت topic |
| `core/` | هماهنگی انتقال و نتایج |
| `utils/` | ابزارهای مشترک |
| `compat/` | abstraction نسخه Airflow |

---

## `docker/`

هر زیرپوشه یک استقرار مستقل است (Dockerfile + compose + config):

| پوشه | توضیح |
|------|-------|
| `airflow-data-platform-winauth_3.0.6/` | Airflow 3.0.6 + WinAuth |
| `airflow-data-platform-winauth_3.2.1/` | Airflow 3.2.1 + WinAuth |
| `airflow-data-platform-winauth_3.2.1_harden/` | نسخه hardened (پیشنهادی production) |
| `airflow-sqlserver-kafka-winauth_3.0.6/` | Airflow + SQL Server + Kafka |
| `airflow-sqlserver-kafka-winauth-timezone_3.0.6/` | + timezone |
| `airflow-sqlserver-kafka-winauth-timezone-mysql_3.0.6/` | + MySQL + timezone |

راهنما: [docker/README.md](docker/README.md)

---

## `docs/`

| فایل | موضوع |
|------|--------|
| `ARCHITECTURE_FA.md` | معماری سیستم |
| `QUICKSTART.md` | راه‌اندازی سریع |
| `QUICK_START_IMPROVEMENTS.md` | راهنمای بهبودهای reliability |
| `MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی SQL Server → Kafka |
| `MASTERDATA_STORE_SYNC_GUIDE.md` | ساخت DAG Replication MD |
| `MYSQL_TO_MSSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی MySQL → MSSQL |
| `MSSQL_TO_MYSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی MSSQL → MySQL |
| `MSSQL_TO_MSSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی MSSQL → MSSQL (conn ثابت) |
| `MSSQL_TO_MONGO_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی MSSQL → MongoDB |
| `MONGO_TO_MSSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی MongoDB → MSSQL |
| `KAFKA_TO_MSSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی Kafka → MSSQL |
| `CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md` | ساخت DAG همگام‌سازی ClickHouse → MSSQL |
| `KAFKA_HEALTH_MONITOR_GUIDE.md` | ساخت DAG مانیتور سلامت Kafka |
| `CLICKHOUSE_OPTIMIZER_GUIDE.md` | ساخت DAG بهینه‌سازی ClickHouse |

---

## `tests/`

```
tests/
├── conftest.py
├── test_transient_sql_server_error.py
└── __init__.py
```

اجرا:

```bash
pytest tests/ -v
```

---

## `images/`

آرشیو Docker image (`.tar`). در `.gitignore` است و به GitHub push نمی‌شود. برای استقرار، ایمیج را از registry بسازید یا فایل tar را جداگانه منتقل کنید.

---

## استقرار در Airflow

حداقل فایل‌های لازم روی worker/scheduler:

```
$AIRFLOW_HOME/dags/
├── template/
├── mssql_to_kafka_clickhouse_sync/
├── mysql_to_mssql_sync/
├── mssql_to_mysql_sync/
├── mssql_to_mssql_sync/
├── mssql_to_mongo_sync/
├── mongo_to_mssql_sync/
├── kafka_to_mssql_sync/
├── clickhouse_to_mssql_sync/
├── sales_inventory/
├── replication/
├── clickhouse_optimizer/
├── kafka_health_monitor/
└── pipeline/          # کل پکیج pipeline
```

یا نصب editable:

```bash
pip install -e .
```

---

## وابستگی‌های اصلی (`requirements.txt`)

- `apache-airflow>=2.8.0`
- `apache-airflow-providers-microsoft-mssql`
- `confluent-kafka` / `kafka-python`
- `pymssql` / `pyodbc`
- `clickhouse-driver`
- `sqlalchemy`

---

**آخرین بروزرسانی:** ژوئیه ۲۰۲۶
