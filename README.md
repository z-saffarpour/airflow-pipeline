# SQL Server Data Pipeline Platform

پلتفرم تولیدی Apache Airflow برای همگام‌سازی و انتقال داده بین **SQL Server**، **Apache Kafka** و **ClickHouse**، به‌همراه تعمیر Replication فروشگاهی و بهینه‌سازی جداول ClickHouse.

## قابلیت‌های اصلی

- **SQL Server → Kafka**: sync جدولی و query-based با streaming و batching
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
│  (ERP / DWH /    │     │  + pipeline/    │     │  ClickHouse      │
│   Store / MD)    │◄────│                 │────►│  Store DBs       │
└──────────────────┘     └─────────────────┘     └──────────────────┘
```

| جریان | منبع | مقصد | مسیر DAG |
|-------|------|------|----------|
| Table / Query Sync | SQL Server (DWH/ERP) | Kafka (اختیاری ClickHouse) | `dags/mssql_sync/` |
| Sales & Inventory | فروشگاه‌ها + ERP AX | Kafka | `dags/sales_inventory/` |
| Replication MD Sync | Publisher (`mssql_replication_md`) | دیتابیس فروشگاه | `dags/replication/` |
| ClickHouse Optimize | ClickHouse | ClickHouse | `dags/clickhouse_optimizer/` |
| Health Monitor | Kafka / SQL Server | گزارش / هشدار | `dags/kafka_health_monitor/` |

## ساختار پروژه

```
sqlserver-kafka-pipeline/
├── dags/
│   ├── template/                 # Factoryهای ساخت DAG
│   │   ├── table_mssql_sync_dag_factory.py
│   │   ├── mssql_to_kafka_clickhouse_sync_dag_factory.py
│   │   ├── mssql_masterdata_to_mssql_store_sync_dag_factory.py
│   │   ├── clickhouse_optimizer_dag_factory.py
│   │   └── kafka_health_monitor_dag_factory.py
│   ├── mssql_sync/               # Sync جداول/کوئری DWH و ERP → Kafka
│   │   ├── dwh/
│   │   └── erp/
│   ├── sales_inventory/          # فروش و موجودی چندمنبعی → Kafka
│   ├── replication/              # تعمیر Replication MD (~200+ table DAG)
│   │   ├── tables/               # sync تک‌جدول برای یک فروشگاه
│   │   ├── orchestrator/         # زنجیره sync چند جدول
│   │   └── reconcile_and_sync/   # تشخیص gap و trigger خودکار
│   ├── clickhouse_optimizer/     # بهینه‌سازی جداول ClickHouse
│   └── kafka_health_monitor/     # مانیتورینگ سلامت topicهای Kafka (~24)
│       ├── mssql_sync/dwh|erp/
│       └── sales_inventory/
│
├── pipeline/                     # هستهٔ مشترک (SOLID)
│   ├── interfaces/               # ABCها (Reader/Writer/Producer/...)
│   ├── config/                   # dataclassهای پیکربندی
│   ├── database/                 # MSSQL reader/writer، ClickHouse، SafeMsSqlHook
│   ├── kafka/                    # Idempotent producer، topic manager
│   ├── core/                     # Orchestratorها و متریک‌ها
│   ├── utils/                    # validation، retry، audit
│   └── compat/                   # سازگاری نسخه‌های Airflow
│
├── docker/                       # ایمیج‌ها و compose با Windows Auth
├── docs/                         # راهنماهای تخصصی
├── tests/                        # تست‌های واحد
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

راهنمای کامل Replication MD: [docs/REPLICATION_MD_STORE_SYNC_GUIDE.md](docs/REPLICATION_MD_STORE_SYNC_GUIDE.md)

نمونه health monitor:

```python
from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag
from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig

conn_config = ConnectionConfig(kafka_conn_id="kafka_default")
kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)
```

راهنمای کامل Health Monitor: [docs/KAFKA_HEALTH_MONITOR_GUIDE.md](docs/KAFKA_HEALTH_MONITOR_GUIDE.md)

## لایه `pipeline/`

| ماژول | نقش |
|-------|-----|
| `MSSQLDataTransferOrchestrator` | هماهنگی انتقال SQL Server → Kafka / ClickHouse |
| `MSSQLToMSSQLQueryOrchestrator` | انتقال Publisher → Subscriber با staging و chunk |
| `ClickHouseOptimizationOrchestrator` | بهینه‌سازی جداول ClickHouse |
| `MSSQLDataReader` / `MSSQLServerWriter` | خواندن streaming و نوشتن batch در SQL Server |
| `IdempotentKafkaProducer` | Exactly-once با compression |
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
| ClickHouse conn | بهینه‌سازی و sink اختیاری |

#### SQL Server (SQL Auth)

```bash
airflow connections add 'mssql_default' \
  --conn-type 'mssql' \
  --conn-host 'your-sql-server' \
  --conn-schema 'your_database' \
  --conn-login 'username' \
  --conn-password 'password' \
  --conn-port 1433
```

#### SQL Server (Windows Auth / Kerberos)

در `Extra`:

```json
{
  "auth_mode": "kerberos",
  "driver": "ODBC Driver 18 for SQL Server",
  "trusted_connection": true,
  "encrypt": true,
  "trustservercertificate": false
}
```

#### Kafka

```bash
airflow connections add 'kafka_default' \
  --conn-type 'http' \
  --conn-host 'kafka-broker' \
  --conn-port 9092 \
  --conn-extra '{"bootstrap_servers": "kafka-broker:9092", "client_id": "airflow"}'
```

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
| [docs/REPLICATION_MD_STORE_SYNC_GUIDE.md](docs/REPLICATION_MD_STORE_SYNC_GUIDE.md) | ساخت DAG جدید Replication MD |
| [docs/KAFKA_HEALTH_MONITOR_GUIDE.md](docs/KAFKA_HEALTH_MONITOR_GUIDE.md) | ساخت DAG مانیتور سلامت Kafka |
| [docker/README.md](docker/README.md) | استقرار Docker و WinAuth |
| `docker/*/SECURITY_GUIDE.md` | راهنمای امنیتی نسخه harden |

## License

MIT License

---

**نسخه بسته**: 1.0.0 (`setup.py`) · **pipeline**: 2.1.0  
**آخرین بروزرسانی README**: ژوئیه ۲۰۲۶
