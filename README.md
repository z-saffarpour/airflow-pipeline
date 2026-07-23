# SQL Server Data Pipeline Platform

پلتفرم تولیدی Apache Airflow برای همگام‌سازی و انتقال داده بین **SQL Server**، **Apache Kafka**، **ClickHouse**، **MySQL** و **MongoDB**، به‌همراه تعمیر Replication فروشگاهی و بهینه‌سازی جداول ClickHouse.

## قابلیت‌های اصلی

- **SQL Server → Kafka / ClickHouse**: sync جدولی و query-based با streaming و batching
- **مسیرهای upsert دوطرفه**: MySQL↔MSSQL، Mongo↔MSSQL، Kafka→MSSQL، ClickHouse→MSSQL، MSSQL→MSSQL
- **Sales & Inventory**: استخراج فروش خرده‌فروشی (~۵۰۰۰ فروشگاه)، EC، سفارش و موجودی به Kafka
- **Replication MD Repair**: بازیابی دادهٔ ازدست‌رفتهٔ Replication از Publisher به Subscriber (~۲۰۰+ جدول AX/Retail، پشتیبانی فیلتر `{store_number}`)
- **ClickHouse Optimizer**: بهینه‌سازی پارتیشن، FINAL merge و deduplication
- **Health Monitoring**: مانیتورینگ lag، topic و سلامت اتصال‌ها
- **Windows Authentication**: پشتیبانی Kerberos/ODBC و ایمیج‌های Docker آماده
- **معماری ماژولار**: `pipeline/` بر اساس SOLID، configهای immutable و factoryهای DAG

## معماری کلی

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  SQL Server      │────►│  Airflow DAGs   │────►│  Kafka Topics    │
│  MySQL / Mongo   │◄───►│  + pipeline/    │────►│  ClickHouse      │
│  Kafka / CH      │     │                 │────►│  Store DBs       │
└──────────────────┘     └─────────────────┘     └──────────────────┘
```

| جریان | منبع | مقصد | مسیر DAG |
|-------|------|------|----------|
| Table / Query Sync | SQL Server (DWH/ERP) | Kafka (± ClickHouse) | `dags/mssql_to_kafka_clickhouse_sync/` |
| MSSQL → Kafka Sync | SQL Server | Kafka | `dags/mssql_to_kafka_sync/` |
| MSSQL → ClickHouse Sync | SQL Server | ClickHouse | `dags/mssql_to_clickhouse_sync/` |
| MySQL → MSSQL | MySQL | SQL Server | `dags/mysql_to_mssql_sync/` |
| MSSQL → MySQL | SQL Server | MySQL | `dags/mssql_to_mysql_sync/` |
| MSSQL → PostgreSQL | SQL Server | PostgreSQL | `dags/mssql_to_postgresql_sync/` |
| MSSQL → MSSQL | SQL Server | SQL Server | `dags/mssql_to_mssql_sync/` |
| MSSQL → MongoDB | SQL Server | MongoDB | `dags/mssql_to_mongo_sync/` |
| MongoDB → MSSQL | MongoDB | SQL Server | `dags/mongo_to_mssql_sync/` |
| Kafka → MSSQL | Kafka | SQL Server | `dags/kafka_to_mssql_sync/` |
| ClickHouse → MSSQL | ClickHouse | SQL Server | `dags/clickhouse_to_mssql_sync/` |
| Sales & Inventory | فروشگاه‌ها + ERP AX | Kafka | `dags/sales_inventory/` |
| Replication MD Sync | Publisher (`mssql_replication_md`) | دیتابیس فروشگاه | `dags/masterdata_store_sync/` |
| ClickHouse Optimize | ClickHouse | ClickHouse | `dags/clickhouse_optimizer/` |
| Health Monitor | Kafka / SQL Server | گزارش / هشدار | `dags/kafka_health_monitor/` |

## ساختار پروژه

```
sqlserver-kafka-pipeline/
├── dags/
│   ├── template/                      # Factoryهای ساخت DAG
│   ├── mssql_to_kafka_clickhouse_sync/  # Sync جداول/کوئری DWH و ERP → Kafka
│   ├── mssql_to_kafka_sync/           # Sync اختصاصی MSSQL → Kafka
│   ├── mssql_to_clickhouse_sync/      # MSSQL → ClickHouse
│   ├── mysql_to_mssql_sync/           # MySQL → MSSQL
│   ├── mssql_to_mysql_sync/           # MSSQL → MySQL
│   ├── mssql_to_postgresql_sync/      # MSSQL → PostgreSQL
│   ├── mssql_to_mssql_sync/           # MSSQL → MSSQL (conn ثابت)
│   ├── mssql_to_mongo_sync/           # MSSQL → MongoDB
│   ├── mongo_to_mssql_sync/           # MongoDB → MSSQL
│   ├── kafka_to_mssql_sync/           # Kafka → MSSQL
│   ├── clickhouse_to_mssql_sync/      # ClickHouse → MSSQL
│   ├── sales_inventory/               # فروش و موجودی چندمنبعی → Kafka
│   ├── masterdata_store_sync/         # تعمیر Replication MD (~200+ table DAG)
│   │   ├── tables/
│   │   ├── orchestrator/
│   │   └── reconcile_and_sync/
│   ├── clickhouse_optimizer/          # بهینه‌سازی جداول ClickHouse
│   └── kafka_health_monitor/          # مانیتورینگ سلامت topicهای Kafka
│
├── pipeline/                     # هستهٔ مشترک (SOLID)
│   ├── interfaces/
│   ├── config/
│   ├── database/
│   ├── kafka/
│   ├── core/
│   ├── utils/
│   └── compat/
│
├── docker/
├── docs/
├── tests/
├── images/                       # آرشیو Docker (در Git نیست)
├── requirements.txt
├── setup.py
└── README.md
```

> پوشه `images/` و فایل‌های `*.tar` در `.gitignore` هستند (حجم بالا برای GitHub). آرشیوها را جداگانه نگه دارید یا از registry بسازید.

## Templateهای DAG

برای DAG جدید، معمولاً فقط یک فایل config کافی است:

| Factory | کاربرد |
|---------|--------|
| `create_table_sync_dag` | Incremental table sync بر اساس `execution_date` |
| `create_query_sync_dag` | اجرای query/CTE سفارشی و ارسال به Kafka |
| `mssql_to_kafka_sync_dag_factory` | Sync اختصاصی MSSQL → Kafka (chunk + audit) |
| `mssql_to_clickhouse_sync_dag_factory` | Sync مستقیم MSSQL → ClickHouse |
| `mysql_to_mssql_sync_dag_factory` | Sync MySQL → MSSQL (upsert) |
| `mssql_to_mysql_sync_dag_factory` | Sync MSSQL → MySQL (upsert) |
| `mssql_to_postgresql_sync_dag_factory` | Sync MSSQL → PostgreSQL (upsert) |
| `mssql_to_mssql_sync_dag_factory` | Sync MSSQL → MSSQL با connection ثابت |
| `mssql_to_mongo_sync_dag_factory` | Sync MSSQL → MongoDB |
| `mongo_to_mssql_sync_dag_factory` | Sync MongoDB → MSSQL |
| `kafka_to_mssql_sync_dag_factory` | Sync Kafka → MSSQL |
| `clickhouse_to_mssql_sync_dag_factory` | Sync ClickHouse → MSSQL |
| `mssql_masterdata_to_mssql_store_sync_dag_factory` | Sync Publisher → فروشگاه با chunk موازی |
| `clickhouse_optimizer_dag_factory` | بهینه‌سازی جدول ClickHouse |
| `kafka_health_monitor_dag_factory` | مانیتور lag / topic / سلامت Kafka |

نمونه table sync:

```python
from template.table_mssql_sync_dag_factory import create_table_sync_dag
from pipeline.config import DAGConfig, ConnectionConfig, KafkaTopicConfig, TableConfiguration, SyncConfig

create_table_sync_dag(
    dag_config=dag_config,
    connection_config=connection_config,
    sync_config=sync_config,
    table_config=table_config,
    kafka_topic_config=kafka_topic_config,
    clickhouse_config=None,
)
```

راهنماهای مسیرهای sync در بخش [مستندات](#مستندات) فهرست شده‌اند.

نمونه health monitor:

```python
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag
from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

## لایه `pipeline/`

| ماژول | نقش |
|-------|-----|
| `MSSQLDataTransferOrchestrator` | هماهنگی انتقال SQL Server → Kafka / ClickHouse |
| `MSSQLToKafkaQueryOrchestrator` | همگام‌سازی مستقیم MSSQL → Kafka (produce + chunk) |
| `MSSQLToClickHouseQueryOrchestrator` | همگام‌سازی مستقیم MSSQL → ClickHouse (bulk INSERT + chunk) |
| `MSSQLToMSSQLQueryOrchestrator` | انتقال MSSQL → MSSQL (store پویا یا conn ثابت) با staging و chunk |
| `MySQLToMSSQLQueryOrchestrator` / `MSSQLToMySQLQueryOrchestrator` | همگام‌سازی MySQL ↔ MSSQL |
| `MSSQLToPostgreSQLQueryOrchestrator` | همگام‌سازی MSSQL → PostgreSQL |
| `MSSQLToMongoDBQueryOrchestrator` / `MongoDBToMSSQLQueryOrchestrator` | همگام‌سازی Mongo ↔ MSSQL |
| `KafkaToMSSQLQueryOrchestrator` | مصرف Kafka و upsert به MSSQL |
| `ClickHouseToMSSQLQueryOrchestrator` | خواندن ClickHouse و upsert به MSSQL |
| `ClickHouseOptimizationOrchestrator` | بهینه‌سازی جداول ClickHouse |
| `MSSQLDataReader` / `MSSQLServerWriter` | خواندن streaming و نوشتن batch در SQL Server |
| `IdempotentKafkaProducer` / `KafkaDataConsumer` | produce/consume با compression و offset commit |
| `SQLQueryBuilder` | کوئری امن و keyset pagination |
| `SafeMsSqlHook` | اتصال pymssql / pyodbc + Kerberos |
| `DagSyncTrigger` | trigger زنجیره‌ای DAGهای وابسته |

## شروع سریع

### ۱. نصب وابستگی‌ها

```bash
pip install -r requirements.txt
pip install -e .
```

نیازمندی‌های اصلی: Airflow ≥ 2.8، `confluent-kafka`، `pymssql` / `pyodbc`، `clickhouse-driver`.

### ۲. کپی به Airflow

```bash
cp -r dags/ $AIRFLOW_HOME/dags/
cp -r pipeline/ $AIRFLOW_HOME/dags/
```

### ۳. Connections رایج

| Connection ID | نقش |
|---------------|-----|
| `mssql_default` / اتصالات DWH-ERP | منبع داده SQL Server |
| `kafka_default` | Kafka brokers |
| `mssql_replication_md` | Publisher Replication MD |
| `mssql_store_connectionInfo` | لیست/اطلاعات اتصال فروشگاه‌ها |
| `mssql_store_template` | الگوی احراز هویت SQL برای Subscriber |
| ClickHouse / MySQL / Mongo conn | sink و مسیرهای sync مربوطه |

جزئیات ساخت connection و WinAuth: [docs/QUICKSTART.md](docs/QUICKSTART.md)

### ۴. Poolهای توصیه‌شده

```bash
airflow pools set data_sync_pool 5 "SQL to Kafka transfers"
airflow pools set replication_md_store_sync_pool 32 "Replication MD store chunk sync"
```

تعداد slot در `replication_md_store_sync_pool` باید ≥ `max_global_parallel_chunks` باشد.

### ۵. اجرای نمونه

```bash
airflow dags trigger table_dwh_rtl_fact_sales_trans_sync
airflow dags trigger query_inventory_and_sales_sync
airflow dags trigger table_dwh_rtl_fact_sales_trans_health_monitor
```

## Docker

نسخه‌های آماده Airflow با Windows Authentication در `docker/` هستند. جزئیات: [docker/README.md](docker/README.md)

نسخه پیشنهادی production:

```bash
cd docker/airflow-data-platform-winauth_3.2.1_harden
cp .env.example .env
docker compose -f docker-compose.winauth.yml up -d
```

## پیکربندی مهم

پارامترهای رایج از Airflow Variables و `params` هر DAG خوانده می‌شوند؛ از جمله:

- `batch_size`، compression و تنظیمات idempotent Kafka
- `mssql_staging_schema` (پیش‌فرض `crt`) برای Replication MD
- `max_global_parallel_chunks_replication_md_store`
- `delete_missing` / `delete_scope_column` برای حذف رکوردهای گم‌شده در Subscriber

برای table sync، ستون تاریخ، PK و لیست `columns` در `TableConfiguration` / `SyncConfig` تنظیم می‌شود.

## تست

```bash
pytest tests/ -v
```

## مستندات

| سند | موضوع |
|-----|--------|
| [docs/ARCHITECTURE_FA.md](docs/ARCHITECTURE_FA.md) | معماری سیستم |
| [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) | ساختار پوشه‌ها و ماژول‌ها |
| [docs/QUICKSTART.md](docs/QUICKSTART.md) | راه‌اندازی سریع |
| [docs/QUICK_START_IMPROVEMENTS.md](docs/QUICK_START_IMPROVEMENTS.md) | بهبودهای reliability و error-handling |
| [docs/MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md](docs/MSSQL_TO_KAFKA_CLICKHOUSE_SYNC_GUIDE.md) | SQL Server → Kafka (± ClickHouse) |
| [docs/MSSQL_TO_KAFKA_SYNC_GUIDE.md](docs/MSSQL_TO_KAFKA_SYNC_GUIDE.md) | MSSQL → Kafka (Gen-2) |
| [docs/MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md](docs/MSSQL_TO_CLICKHOUSE_SYNC_GUIDE.md) | MSSQL → ClickHouse |
| [docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md](docs/MYSQL_TO_MSSQL_SYNC_GUIDE.md) | MySQL → MSSQL |
| [docs/MSSQL_TO_MYSQL_SYNC_GUIDE.md](docs/MSSQL_TO_MYSQL_SYNC_GUIDE.md) | MSSQL → MySQL |
| [docs/MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md](docs/MSSQL_TO_POSTGRESQL_SYNC_GUIDE.md) | MSSQL → PostgreSQL |
| [docs/MSSQL_TO_MSSQL_SYNC_GUIDE.md](docs/MSSQL_TO_MSSQL_SYNC_GUIDE.md) | MSSQL → MSSQL (conn ثابت) |
| [docs/MSSQL_TO_MONGO_SYNC_GUIDE.md](docs/MSSQL_TO_MONGO_SYNC_GUIDE.md) | MSSQL → MongoDB |
| [docs/MONGO_TO_MSSQL_SYNC_GUIDE.md](docs/MONGO_TO_MSSQL_SYNC_GUIDE.md) | MongoDB → MSSQL |
| [docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md](docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md) | Kafka → MSSQL |
| [docs/CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](docs/CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md) | ClickHouse → MSSQL |
| [docs/MASTERDATA_STORE_SYNC_GUIDE.md](docs/MASTERDATA_STORE_SYNC_GUIDE.md) | Replication MD → Store |
| [docs/KAFKA_HEALTH_MONITOR_GUIDE.md](docs/KAFKA_HEALTH_MONITOR_GUIDE.md) | مانیتور سلامت Kafka |
| [docs/CLICKHOUSE_OPTIMIZER_GUIDE.md](docs/CLICKHOUSE_OPTIMIZER_GUIDE.md) | بهینه‌سازی ClickHouse |
| [docker/README.md](docker/README.md) | استقرار Docker و WinAuth |
| `docker/*/SECURITY_GUIDE.md` | راهنمای امنیتی نسخه harden |

## License

MIT License

---

**نسخه بسته**: 1.0.0 (`setup.py`) · **pipeline**: 2.1.0  
**آخرین بروزرسانی README**: ژوئیه ۲۰۲۶
