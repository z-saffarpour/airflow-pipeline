# SQL Server Data Pipeline Platform

پلتفرم تولیدی Apache Airflow برای همگام‌سازی و انتقال داده بین **SQL Server**، **PostgreSQL**، **Apache Kafka**، **ClickHouse**، **MySQL** و **MongoDB**، به‌همراه تعمیر Master Data فروشگاهی و بهینه‌سازی جداول ClickHouse.

## قابلیت‌های اصلی

- **SQL Server → Kafka (± ClickHouse)**: sync جدولی (`create_table_sync_dag`) و query/CTE (`create_query_sync_dag`) با streaming و batching
- **MSSQL → Kafka / ClickHouse (Gen-2)**: مسیرهای اختصاصی با chunk و audit
- **مسیرهای upsert دوطرفه**: MySQL↔MSSQL، PostgreSQL↔MSSQL، Mongo↔MSSQL، Kafka→MSSQL، ClickHouse→MSSQL، MSSQL→MSSQL
- **Master Data → Store**: Publisher → Subscriber با chunk موازی و فیلتر `{store_number}`
- **ClickHouse Optimizer**: OPTIMIZE / FINAL / deduplicate + health check
- **Kafka Health Monitor**: lag، topic stats و نمونه‌گیری پیام
- **Windows Authentication**: Kerberos/ODBC و ایمیج‌های Docker آماده
- **معماری ماژولار**: `pipeline/` بر اساس SOLID، configهای immutable و ۱۶ factory در `dags/template/`

## معماری کلی

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  SQL Server      │────►│  Airflow DAGs   │────►│  Kafka Topics    │
│  PostgreSQL      │◄───►│  + pipeline/    │────►│  ClickHouse      │
│  MySQL / Mongo   │     │                 │────►│  Store / DBs     │
│  Kafka / CH      │     │                 │     │                  │
└──────────────────┘     └─────────────────┘     └──────────────────┘
```

| جریان | منبع | مقصد | مسیر DAG | نمونه |
|-------|------|------|----------|-------|
| Table Sync → Kafka | SQL Server | Kafka (± CH) | `dags/mssql_to_kafka_clickhouse_sync/` | `example_table_to_kafka_sync.py` |
| Query Sync → Kafka | SQL Server | Kafka (± CH) | `dags/mssql_to_kafka_clickhouse_sync/` | `example_query_to_kafka_sync.py` |
| MSSQL → Kafka | SQL Server | Kafka | `dags/mssql_to_kafka_sync/` | `example_table_to_kafka_sync.py` |
| MSSQL → ClickHouse | SQL Server | ClickHouse | `dags/mssql_to_clickhouse_sync/` | `example_table_to_clickhouse_sync.py` |
| MySQL → MSSQL | MySQL | SQL Server | `dags/mysql_to_mssql_sync/` | `example_table_to_mssql_sync.py` |
| MSSQL → MySQL | SQL Server | MySQL | `dags/mssql_to_mysql_sync/` | `example_table_to_mysql_sync.py` |
| MSSQL → PostgreSQL | SQL Server | PostgreSQL | `dags/mssql_to_postgresql_sync/` | `example_table_to_postgresql_sync.py` |
| PostgreSQL → MSSQL | PostgreSQL | SQL Server | `dags/postgresql_to_mssql_sync/` | `example_table_to_mssql_sync.py` |
| MSSQL → MSSQL | SQL Server | SQL Server | `dags/mssql_to_mssql_sync/` | `example_table_to_mssql_sync.py` |
| MSSQL → MongoDB | SQL Server | MongoDB | `dags/mssql_to_mongo_sync/` | `example_table_to_mongo_sync.py` |
| MongoDB → MSSQL | MongoDB | SQL Server | `dags/mongo_to_mssql_sync/` | `example_collection_to_mssql_sync.py` |
| Kafka → MSSQL | Kafka | SQL Server | `dags/kafka_to_mssql_sync/` | `example_topic_to_mssql_sync.py` |
| ClickHouse → MSSQL | ClickHouse | SQL Server | `dags/clickhouse_to_mssql_sync/` | `example_table_to_mssql_sync.py` |
| Master Data → Store | Publisher | Store DB | `dags/masterdata_store_sync/` | `example_table_to_store_sync.py` |
| ClickHouse Optimize | ClickHouse | ClickHouse | `dags/clickhouse_optimizer/` | `example_table_clickhouse_optimizer.py` |
| Kafka Health Monitor | Kafka | گزارش | `dags/kafka_health_monitor/` | `example_topic_health_monitor.py` |

## ساختار پروژه

```
sqlserver-kafka-pipeline/
├── dags/
│   ├── template/                         # ۱۶ factory ساخت DAG
│   ├── mssql_to_kafka_clickhouse_sync/   # Table/Query → Kafka (± CH)
│   │   ├── example_table_to_kafka_sync.py
│   │   └── example_query_to_kafka_sync.py
│   ├── mssql_to_kafka_sync/
│   │   └── example_table_to_kafka_sync.py
│   ├── mssql_to_clickhouse_sync/
│   │   └── example_table_to_clickhouse_sync.py
│   ├── mysql_to_mssql_sync/
│   │   └── example_table_to_mssql_sync.py
│   ├── mssql_to_mysql_sync/
│   │   └── example_table_to_mysql_sync.py
│   ├── mssql_to_postgresql_sync/
│   │   └── example_table_to_postgresql_sync.py
│   ├── postgresql_to_mssql_sync/
│   │   └── example_table_to_mssql_sync.py
│   ├── mssql_to_mssql_sync/
│   │   └── example_table_to_mssql_sync.py
│   ├── mssql_to_mongo_sync/
│   │   └── example_table_to_mongo_sync.py
│   ├── mongo_to_mssql_sync/
│   │   └── example_collection_to_mssql_sync.py
│   ├── kafka_to_mssql_sync/
│   │   └── example_topic_to_mssql_sync.py
│   ├── clickhouse_to_mssql_sync/
│   │   └── example_table_to_mssql_sync.py
│   ├── masterdata_store_sync/
│   │   └── example_table_to_store_sync.py
│   ├── clickhouse_optimizer/
│   │   └── example_table_clickhouse_optimizer.py
│   └── kafka_health_monitor/
│       └── example_topic_health_monitor.py
│
├── pipeline/                             # هستهٔ مشترک
│   ├── interfaces/
│   ├── config/
│   ├── database/                         # Reader / Writer / ConnectionFactory
│   ├── kafka/
│   ├── core/                             # Orchestratorها + exceptions
│   ├── utils/
│   └── compat/
│
├── docker/
├── docs/
├── tests/
├── images/                               # آرشیو Docker
├── requirements.txt
├── setup.py
├── PROJECT_STRUCTURE.md
└── README.md
```

جزئیات بیشتر: [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

## Templateهای DAG (`dags/template/`)

برای DAG جدید معمولاً از یک `example_*.py` کپی کنید و config را عوض کنید.

| Factory module | Entry point | کاربرد |
|----------------|-------------|--------|
| `table_mssql_sync_dag_factory` | `create_table_sync_dag` | Table → Kafka (± ClickHouse) |
| `mssql_to_kafka_clickhouse_sync_dag_factory` | `create_query_sync_dag` | Query/CTE → Kafka (± ClickHouse) |
| `mssql_to_kafka_sync_dag_factory` | `create_dag` | MSSQL → Kafka (Gen-2) |
| `mssql_to_clickhouse_sync_dag_factory` | `create_dag` | MSSQL → ClickHouse |
| `mysql_to_mssql_sync_dag_factory` | `create_dag` | MySQL → MSSQL upsert |
| `mssql_to_mysql_sync_dag_factory` | `create_dag` | MSSQL → MySQL upsert |
| `mssql_to_postgresql_sync_dag_factory` | `create_dag` | MSSQL → PostgreSQL upsert |
| `postgresql_to_mssql_sync_dag_factory` | `create_dag` | PostgreSQL → MSSQL upsert |
| `mssql_to_mssql_sync_dag_factory` | `create_dag` | MSSQL → MSSQL (conn ثابت) |
| `mssql_to_mongo_sync_dag_factory` | `create_dag` | MSSQL → MongoDB |
| `mongo_to_mssql_sync_dag_factory` | `create_dag` | MongoDB → MSSQL |
| `kafka_to_mssql_sync_dag_factory` | `create_dag` | Kafka → MSSQL |
| `clickhouse_to_mssql_sync_dag_factory` | `create_dag` | ClickHouse → MSSQL |
| `mssql_masterdata_to_mssql_store_sync_dag_factory` | `create_dag` | Publisher → Store |
| `clickhouse_optimizer_dag_factory` | `clickhouse_optimizer_dag` | OPTIMIZE جدول ClickHouse |
| `kafka_health_monitor_dag_factory` | `kafka_health_monitor_dag` | مانیتور سلامت Kafka |

نمونه Table → Kafka:

```python
from template.table_mssql_sync_dag_factory import create_table_sync_dag
from pipeline.config import (
    DAGConfig, ConnectionConfig, KafkaTopicConfig,
    TableConfiguration, SyncConfig,
)

dag = create_table_sync_dag(
    dag_config=dag_config,
    sync_config=sync_config,
    conn_config=conn_config,
    table_config=table_config,
    kafka_topic_config=kafka_topic_config,
    clickhouse_config=None,
)
```

نمونه Health Monitor:

```python
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag
from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")
dag = kafka_health_monitor_dag(dag_config, conn_config, health_config)
```

راهنماهای هر مسیر در بخش [مستندات](#مستندات) فهرست شده‌اند.

## لایه `pipeline/`

| ماژول | نقش |
|-------|-----|
| `MSSQLDataTransferOrchestrator` | انتقال SQL Server → Kafka / ClickHouse (table/query) |
| `MSSQLToKafkaQueryOrchestrator` | MSSQL → Kafka (Gen-2) |
| `MSSQLToClickHouseQueryOrchestrator` | MSSQL → ClickHouse bulk INSERT |
| `MSSQLToMSSQLQueryOrchestrator` | MSSQL → MSSQL (store پویا یا conn ثابت) |
| `MySQLToMSSQLQueryOrchestrator` / `MSSQLToMySQLQueryOrchestrator` | MySQL ↔ MSSQL |
| `MSSQLToPostgreSQLQueryOrchestrator` / `PostgreSQLToMSSQLQueryOrchestrator` | PostgreSQL ↔ MSSQL |
| `MSSQLToMongoDBQueryOrchestrator` / `MongoDBToMSSQLQueryOrchestrator` | Mongo ↔ MSSQL |
| `KafkaToMSSQLQueryOrchestrator` | Kafka → MSSQL upsert |
| `ClickHouseToMSSQLQueryOrchestrator` | ClickHouse → MSSQL upsert |
| `ClickHouseOptimizationOrchestrator` | بهینه‌سازی جداول ClickHouse |
| `MSSQLDataReader` / `MSSQLServerWriter` | خواندن streaming و نوشتن batch در SQL Server |
| `MySQLDataReader` / `MySQLServerWriter` | MySQL |
| `PostgreSQLDataReader` / `PostgreSQLServerWriter` | PostgreSQL |
| `MongoDBDataReader` / `MongoDBServerWriter` | MongoDB |
| `ClickHouseDataReader` / `ClickHouseWriter` / `ClickHouseTableOptimizer` | ClickHouse |
| `IdempotentKafkaProducer` / `KafkaDataConsumer` | produce/consume |
| `SQLQueryBuilder` | کوئری امن و keyset pagination |
| `SafeMsSqlHook` | pymssql / pyodbc + Kerberos |
| `DagSyncTrigger` | trigger زنجیره‌ای DAGها |
| `core.exceptions` | استثناهای مشترک لایه database/pipeline |

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
| `mssql_default` / اتصالات DWH-ERP | منبع SQL Server |
| `kafka_default` | Kafka brokers |
| `mssql_replication_md` | Publisher Master Data |
| `mssql_store_connectionInfo` | لیست/اطلاعات اتصال فروشگاه‌ها |
| `mssql_store_template` | الگوی احراز هویت SQL برای Subscriber |
| `clickhouse_default` | ClickHouse |
| `mysql_*` / `mongo_*` / `postgres_*` | مسیرهای sync مربوطه |

جزئیات: [docs/QUICKSTART.md](docs/QUICKSTART.md)

### ۴. Poolهای توصیه‌شده

```bash
airflow pools set data_sync_pool 5 "SQL to Kafka transfers"
airflow pools set replication_md_store_sync_pool 32 "Master data store chunk sync"
```

تعداد slot در `replication_md_store_sync_pool` باید ≥ `max_global_parallel_chunks` باشد.

### ۵. اجرای نمونه

```bash
airflow dags trigger example_table_to_kafka_sync
airflow dags trigger example_query_to_kafka_sync
airflow dags trigger example_table_to_store_sync
airflow dags trigger example_table_clickhouse_optimizer
airflow dags trigger example_topic_health_monitor
airflow dags trigger mysql_example_table_to_mssql_sync
airflow dags trigger mssql_example_table_to_postgresql_sync
airflow dags trigger kafka_example_topic_to_mssql_sync
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
- `mssql_staging_schema` (پیش‌فرض `crt`) برای مسیرهای upsert به MSSQL / Store
- `max_global_parallel_chunks_replication_md_store`
- `delete_missing` / `delete_scope_column` برای حذف رکوردهای گم‌شده در مقصد

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
| [docs/POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md](docs/POSTGRESQL_TO_MSSQL_SYNC_GUIDE.md) | PostgreSQL → MSSQL |
| [docs/MSSQL_TO_MSSQL_SYNC_GUIDE.md](docs/MSSQL_TO_MSSQL_SYNC_GUIDE.md) | MSSQL → MSSQL (conn ثابت) |
| [docs/MSSQL_TO_MONGO_SYNC_GUIDE.md](docs/MSSQL_TO_MONGO_SYNC_GUIDE.md) | MSSQL → MongoDB |
| [docs/MONGO_TO_MSSQL_SYNC_GUIDE.md](docs/MONGO_TO_MSSQL_SYNC_GUIDE.md) | MongoDB → MSSQL |
| [docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md](docs/KAFKA_TO_MSSQL_SYNC_GUIDE.md) | Kafka → MSSQL |
| [docs/CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md](docs/CLICKHOUSE_TO_MSSQL_SYNC_GUIDE.md) | ClickHouse → MSSQL |
| [docs/MASTERDATA_STORE_SYNC_GUIDE.md](docs/MASTERDATA_STORE_SYNC_GUIDE.md) | Master Data → Store |
| [docs/KAFKA_HEALTH_MONITOR_GUIDE.md](docs/KAFKA_HEALTH_MONITOR_GUIDE.md) | مانیتور سلامت Kafka |
| [docs/CLICKHOUSE_OPTIMIZER_GUIDE.md](docs/CLICKHOUSE_OPTIMIZER_GUIDE.md) | بهینه‌سازی ClickHouse |
| [docker/README.md](docker/README.md) | استقرار Docker و WinAuth |
| `docker/*/SECURITY_GUIDE.md` | راهنمای امنیتی نسخه harden |

## License

MIT License

---

**نسخه بسته**: 1.0.0 (`setup.py`) · **pipeline**: 2.1.0  
**آخرین بروزرسانی README**: ژوئیه ۲۰۲۶
