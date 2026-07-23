# SQL Server to Kafka Data Pipeline

این پروژه یک DAG تولیدی Apache Airflow برای انتقال داده‌ها به صورت روزانه از SQL Server به Apache Kafka است.

## ویژگی‌های اصلی

✅ **Incremental Loading**: فقط داده‌های روز جاری بارگذاری می‌شود  
✅ **Memory-Efficient**: استفاده از streaming و chunking (50k records/batch)  
✅ **Exactly-Once Semantics**: جلوگیری از duplicate messages  
✅ **Production-Ready**: Error handling، retry، monitoring کامل  
✅ **Modular Architecture**: 18 ماژول مجزا با معماری SOLID  
✅ **Configurable**: همه پارامترها قابل تنظیم از Airflow Variables  
✅ **Backfill Support**: امکان اجرای مجدد برای تاریخ‌های گذشته  
✅ **SQL Injection Prevention**: استفاده از parameterized queries و validation  
✅ **Clean Code**: رعایت اصول OOP، SOLID، و Clean Coding  
✅ **Observability**: متریک‌ها و گزارش‌های جامع  
✅ **ABC Interfaces**: اینترفیس‌های انتزاعی برای Dependency Inversion  
✅ **Immutable Configs**: استفاده از frozen dataclasses  
🆕 **Custom Exceptions**: سلسله مراتب کامل exception classes برای error handling بهتر  
🆕 **Structured Logging**: لاگ‌های structured برای monitoring و observability  
🆕 **Retry Strategy**: Exponential backoff با jitter برای reliability بیشتر  
🆕 **Input Validation**: اعتبارسنجی جامع configuration و connection strings  

## 🆕 بهبودهای اخیر (2026-02-22)

- ✨ اضافه شدن **Custom Exception Classes** برای مدیریت بهتر خطاها
- ✨ پیاده‌سازی **Structured Logging** برای production readiness
- ✨ **Server Name Validation** برای امنیت بیشتر
- ✨ **Retry Helper** با exponential backoff و jitter
- ✨ **Configuration Validation** جامع
- ✨ بهبود **Exception Handling** با exception chaining

📘 **[docs/IMPROVEMENTS_2026_02_22.md](docs/IMPROVEMENTS_2026_02_22.md)**: مستندات کامل بهبودها  
🚀 **[docs/QUICK_START_IMPROVEMENTS.md](docs/QUICK_START_IMPROVEMENTS.md)**: راهنمای سریع استفاده از ویژگی‌های جدید

## مستندات

📖 **[docs/ARCHITECTURE_FA.md](docs/ARCHITECTURE_FA.md)**: مستندات کامل فارسی معماری سیستم و راهنمای جامع  
🧪 **[tests/README.md](tests/README.md)**: مستندات تست‌ها و راهنمای اجرای تست‌ها  
📝 **[docs/COLUMNS_FEATURE_CHANGELOG.md](docs/COLUMNS_FEATURE_CHANGELOG.md)**: مستندات ویژگی انتخاب ستون‌ها  
📋 **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)**: توضیح کامل ساختار پروژه

## ساختار پروژه

```
sqlserver-kafka-pipeline/
├── 📁 dags/                            # Airflow DAG definitions
│   ├── __init__.py
│   ├── 📁 kafka_sync/                  # DAG های انتقال داده به Kafka
│   │   ├── sqlserver_kafka_sync.py      # DAG اصلی (Template)
│   │   ├── fact_sales_trans_sync.py     # انتقال Fact_SalesTrans
│   │   └── dim_date_sync.py             # انتقال Dim_Date
│   ├── 📁 clickhouse/                  # DAG های بهینه‌سازی ClickHouse
│   │   ├── clickhouse_optimizer.py      # DAG اصلی (Template)
│   │   ├── fact_sales_trans_clickhouse_optimizer.py
│   │   └── dim_date_clickhouse_optimizer.py
│   └── 📁 monitoring/                  # DAG های مانیتورینگ
│       ├── pipeline_health_monitor.py   # DAG اصلی (Template)
│       ├── fact_sales_trans_health_monitor.py
│       └── dim_date_health_monitor.py
│
├── 📁 pipeline/                         # Pipeline core modules (SOLID)
│   ├── __init__.py                     # Main package exports
│   │
│   ├── 📁 interfaces/                  # 🔷 Abstract Base Classes (ABC)
│   │   ├── __init__.py
│   │   ├── DataReader.py               # اینترفیس خواندن داده
│   │   ├── MessageProducer.py          # اینترفیس ارسال پیام
│   │   ├── TopicManager.py             # اینترفیس مدیریت topic
│   │   └── MessageSerializerInterface.py # اینترفیس سریالایزر
│   │
│   ├── 📁 config/                      # ⚙️ Configuration
│   │   ├── __init__.py
│   │   ├── ConfigurationManager.py     # مدیریت تنظیمات متمرکز
│   │   ├── TableConfiguration.py       # تنظیمات جدول (frozen)
│   │   ├── ConnectionConfiguration.py  # تنظیمات اتصال (frozen)
│   │   └── KafkaProducerConfig.py      # ثوابت Kafka (frozen)
│   │
│   ├── 📁 database/                    # 🗄️ Database Layer
│   │   ├── __init__.py
│   │   ├── ConnectionFactory.py        # مدیریت اتصالات
│   │   ├── SQLQueryBuilder.py          # ساخت کوئری‌های امن
│   │   └── MSSQLDataReader.py # خواندن استریمینگ
│   │
│   ├── 📁 kafka/                       # 📨 Kafka Layer
│   │   ├── __init__.py
│   │   ├── IdempotentKafkaProducer.py  # Producer با Exactly-once
│   │   ├── KafkaTopicManager.py        # مدیریت Topics
│   │   └── MessageSerializer.py        # سریالایز پیام‌ها
│   │
│   └── 📁 core/                        # 🎯 Core/Orchestration
│       ├── __init__.py
│       ├── MSSQLDataTransferOrchestrator.py # هماهنگی انتقال داده
│       ├── TransferMetrics.py          # ردیابی متریک‌ها
│       ├── TransferResult.py           # نتایج عملیات
│       └── ExecutionDateExtractor.py   # ابزارهای تاریخ
│
├── 📁 tests/                           # 🧪 تست‌های واحد (117 tests)
│   ├── __init__.py
│   ├── conftest.py                     # pytest configuration
│   ├── test_sql_query_builder.py       # تست‌های SQLQueryBuilder
│   ├── test_configuration_manager.py   # تست‌های ConfigurationManager
│   ├── test_message_serializer.py      # تست‌های MessageSerializer
│   ├── test_transfer_metrics.py        # تست‌های TransferMetrics
│   ├── test_execution_date_extractor.py # تست‌های ExecutionDateExtractor
│   ├── test_transfer_result.py         # تست‌های TransferResult
│   ├── test_columns_feature.py         # تست‌های ویژگی columns
│   └── README.md                       # راهنمای تست‌ها
│
├── 📁 docs/                            # Documentation
│   ├── README.md                       # راهنمای مستندات
│   ├── ARCHITECTURE_FA.md              # مستندات فارسی معماری سیستم
│   ├── CHANGELOG.md                    # تاریخچه تغییرات
│   └── COLUMNS_FEATURE_CHANGELOG.md    # مستندات ویژگی columns
│
├── 📄 PROJECT_STRUCTURE.md             # توضیح کامل ساختار پروژه
├── 📄 README.md                        # این فایل
├── 📄 setup.py                         # Python package setup
├── 📄 MANIFEST.in                      # Package manifest
├── 📄 LICENSE                          # MIT License
├── 📄 .gitignore                       # Git ignore rules
├── 📄 requirements.txt                 # وابستگی‌های پایتون
├── 📄 requirements-test.txt            # وابستگی‌های تست
├── 📄 run_tests.py                     # اسکریپت اجرای سریع تست‌ها
├── 📄 docker-compose.example.yml       # نمونه Docker Compose
└── 📄 example_queries.sql              # نمونه کوئری‌های SQL
```

## معماری (Architecture)

```
SQL Server (Partitioned Table)
    ↓
[SQL Streaming Reader] ← Chunking (50k records/batch)
    ↓
[Kafka Producer] ← Idempotent, Exactly-once semantics
    ↓
Kafka Topic (JSON messages)
```

### ماژول‌های اصلی

#### 1. MSSQLDataReader (`pipeline/MSSQLDataReader.py`)
- خواندن streaming از SQL Server
- Keyset Pagination برای performance
- Generator-based برای مدیریت حافظه
- استفاده از SQLQueryBuilder برای کوئری‌های امن
- استفاده از ConnectionFactory برای مدیریت اتصالات

#### 2. IdempotentKafkaProducer (`pipeline/IdempotentKafkaProducer.py`)
- Exactly-once delivery semantics
- Idempotent producer configuration
- Delivery callbacks و error tracking
- Message compression (Snappy)

#### 3. KafkaTopicManager (`pipeline/KafkaTopicManager.py`)
- ایجاد خودکار Kafka topics
- مدیریت partitions و replication
- بررسی وجود topic

#### 4. SQLQueryBuilder (`pipeline/SQLQueryBuilder.py`)
- ساخت کوئری‌های SQL امن با parameterization
- جلوگیری از SQL Injection
- پشتیبانی از Keyset Pagination
- متدهای کمکی برای COUNT و MIN/MAX

#### 5. ConfigurationManager (`pipeline/ConfigurationManager.py`)
- مدیریت متمرکز تنظیمات
- جداسازی configuration از business logic
- پشتیبانی از multiple table configs

#### 6. MessageSerializer (`pipeline/MessageSerializer.py`)
- سریالایز کردن داده‌ها به JSON
- مدیریت انواع داده‌های خاص (datetime, Decimal)
- ایجاد message keys و headers

#### 7. MSSQLDataTransferOrchestrator (`pipeline/DataTransferOrchestrator.py`)
- هماهنگی عملیات انتقال داده
- استفاده از تمام کامپوننت‌ها
- مدیریت متریک‌ها و خطاها

#### 8. TransferMetrics (`pipeline/TransferMetrics.py`)
- ردیابی متریک‌های انتقال
- محاسبه performance metrics
- گزارش پیشرفت

#### 9. ConnectionFactory (`pipeline/ConnectionFactory.py`)
- مدیریت اتصالات دیتابیس
- Context managers برای safety
- پشتیبانی از parameterized queries

#### 10. Pipeline Health Monitors (`dags/monitoring/`)
- `validate_kafka_health()`: اعتبارسنجی اتصال Kafka
- `check_consumer_lag()`: بررسی lag
- `check_topic_stats()`: آمار topic
- `generate_health_report()`: گزارش سلامت

### Flow Diagram

```
┌─────────────────┐
│ Validate        │
│ Connections     │
└────────┬────────┘
         │
┌────────▼────────┐
│ Ensure Kafka    │
│ Topic Exists    │
└────────┬────────┘
         │
┌────────▼────────┐
│ Transfer Data   │
│ SQL → Kafka     │
│ (Streaming)     │
└────────┬────────┘
         │
┌────────▼────────┐
│ Verify Transfer │
└─────────────────┘
```

## شروع سریع (Quick Start)

### 1. نصب وابستگی‌ها
```bash
# Production dependencies
pip install -r requirements.txt

# Test dependencies (optional)
pip install -r requirements-test.txt
```

### 2. اجرای تست‌ها (اختیاری)
```bash
# Quick test run (without pytest)
python run_tests.py

# Full test suite with pytest
pytest tests/ -v

# With coverage report
pytest tests/ --cov=pipeline --cov-report=html
```

### 3. تنظیم Airflow Connections

#### SQL Server:
```bash
airflow connections add 'mssql_default' \
    --conn-type 'mssql' \
    --conn-host 'your-sql-server' \
    --conn-schema 'your_database' \
    --conn-login 'username' \
    --conn-password 'password' \
    --conn-port 1433
```

> نکته: اگر از مسیر پیش‌فرض `pymssql` استفاده می‌کنید، در `Extra` کلیدهایی مثل `Trusted_Connection` یا `driver` قرار ندهید.

#### SQL Server (Windows Authentication / Domain Account):
```bash
airflow connections add 'mssql_default' \
    --conn-type 'mssql' \
    --conn-host 'sqlserver.your-domain.local' \
    --conn-schema 'your_database' \
    --conn-login 'YOURDOMAIN\\your_user' \
    --conn-password 'your_domain_password' \
    --conn-port 1433
```

> نکته ۱: برای Windows Authentication در این پروژه از اکانت دامنه (فرمت `DOMAIN\\username`) استفاده کنید و `Extra` را خالی بگذارید.
>
> نکته ۲: کلیدهای `Trusted_Connection`، `driver`، `odbc_connect` فقط وقتی معتبر هستند که `auth_mode=kerberos` تنظیم شده باشد.
>

#### SQL Server (Windows Integrated Authentication - Production)

برای Production امن، مسیر Kerberos/ODBC را با `Extra` فعال کنید:

```json
{
    "auth_mode": "kerberos",
    "driver": "ODBC Driver 18 for SQL Server",
    "trusted_connection": true,
    "encrypt": true,
    "trustservercertificate": false,
    "login_timeout": 15,
    "timeout": 30
}
```

> در این حالت Hook به صورت خودکار از `pyodbc` استفاده می‌کند و از `pymssql` عبور می‌کند.

#### Kafka:
```bash
airflow connections add 'kafka_default' \
    --conn-type 'http' \
    --conn-host 'kafka-broker' \
    --conn-port 9092 \
    --conn-extra '{"bootstrap_servers": "kafka-broker:9092", "client_id": "airflow"}'
```

### 4. تنظیم Variables
```bash
airflow variables set sql_to_kafka_config '{
    "batch_size": 50000,
    "max_batch_timeout_sec": 300,
    "enable_idempotence": true,
    "compression_type": "snappy"
}'
```

### 5. کپی فایل‌ها
```bash
cp -r dags/ $AIRFLOW_HOME/dags/
cp -r pipeline/ $AIRFLOW_HOME/dags/
```

### 6. اجرای DAG
```bash
# Manual trigger
airflow dags trigger sqlserver_kafka_sync

# با تاریخ خاص
airflow dags trigger sqlserver_kafka_sync --exec-date 2024-12-28

# Backfill
airflow dags backfill sqlserver_kafka_sync \
    --start-date 2024-12-01 \
    --end-date 2024-12-31
```

## ویژگی‌های کلیدی

### 1. Incremental Loading
- فقط داده‌های مربوط به `execution_date` خوانده می‌شوند
- Query بهینه با استفاده از index روی ستون تاریخ
- پشتیبانی از Backfill برای روزهای گذشته
- استفاده از Keyset Pagination به جای OFFSET/FETCH

**مثال Query**:
```sql
-- با استفاده از SQLQueryBuilder و parameterized queries:

-- Count Query
SELECT COUNT(1) as total_count
FROM RTL.Fact_SalesTrans
WHERE COM_Date_TransRef = ?
-- Parameter: ('20241228',)

-- Batch اول (با ستون‌های مشخص شده)
SELECT ID, COM_Date_TransRef, CustomerID, ProductID, Quantity, Amount
FROM RTL.Fact_SalesTrans
WHERE COM_Date_TransRef = ?
ORDER BY ID
OFFSET 0 ROWS FETCH NEXT 50000 ROWS ONLY
-- Parameter: ('20241228',)

-- Batch بعدی (Keyset Pagination)
SELECT *
FROM RTL.Fact_SalesTrans
WHERE COM_Date_TransRef = ? AND ID > ?
ORDER BY ID
OFFSET 0 ROWS FETCH NEXT 50000 ROWS ONLY
-- Parameters: ('20241228', last_id)
```

### 2. Memory Management
- **Chunking/Batching**: پردازش داده‌ها در batch های 50k رکورد (قابل تنظیم)
- **Streaming با Generator**: عدم Load کامل دیتاست در RAM
- **Keyset Pagination**: استفاده از cursor-based pagination به جای OFFSET/FETCH
- **Context Manager**: مدیریت خودکار اتصالات و آزادسازی منابع

### 3. Exactly-once Semantics
- Idempotent Kafka Producer
- `enable.idempotence=true`
- `max.in.flight.requests.per.connection=1`
- `acks=all`

### 4. Performance Optimization

#### SQL Server:
```sql
-- Query بهینه شده
SELECT *
FROM transactions WITH (NOLOCK)  -- اگر consistency trade-off قابل قبول است
WHERE transaction_date = '20240115'
ORDER BY id  -- استفاده از index
```

- استفاده از `TOP` و Keyset Pagination
- Index روی ستون تاریخ و primary key
- `NOLOCK` hint (اختیاری، برای performance بهتر)

#### Kafka:
- Compression (Snappy)
- Batch producing
- Proper partitioning (key-based)

### 5. Error Handling & Resilience
- Retry mechanism (3 retries با exponential backoff)
- Timeout handling
- Detailed logging
- Task-level retries
- Connection validation

### 6. Configuration
تمام پارامترها از طریق Airflow Variables و DAG Params قابل تنظیم هستند:

```python
# Airflow Variables (Admin → Variables)
# برای تنظیمات عمومی pipeline

# Pipeline Configuration (JSON)
sql_to_kafka_config = {
    "batch_size": 50000,
    "max_batch_timeout_sec": 300,
    "max_poll_interval_ms": 300000,
    "message_timeout_ms": 300000,
    "retry_backoff_ms": 100,
    "enable_idempotence": true,
    "max_in_flight_requests_per_connection": 1,
    "acks": "all",
    "compression_type": "snappy",
    "sql_nolock": false
}
```

**DAG Params** (در کد DAG تعریف شده):
```python
# این پارامترها در تعریف DAG قرار دارند و برای هر table مخصوص است
params={
    'table_name': 'RTL.Fact_SalesTrans',
    'date_column': 'COM_Date_TransRef',
    'primary_key_column': 'ID',
    'kafka_topic': 'RTL-Fact_SalesTrans',
    'num_partitions': 1,
    'replication_factor': 1,
    'columns': ['ID', 'COM_Date_TransRef', 'CustomerID', 'ProductID', 'Quantity', 'Amount']  # لیست ستون‌ها یا None برای SELECT *
}
```

## نصب و راه‌اندازی

### 1. نصب Dependencies

```bash
pip install -r requirements.txt
```

### 2. تنظیم Airflow Connections

#### SQL Server Connection:
```
Conn Id: mssql_default
Conn Type: Microsoft SQL Server
Host: your-sql-server-host
Schema: your_database
Login: your_username
Password: your_password
Port: 1433
```

**نمونه Windows Authentication (Domain Account):**
```
Conn Id: mssql_default
Conn Type: Microsoft SQL Server
Host: sqlserver.your-domain.local
Schema: your_database
Login: YOURDOMAIN\your_user
Password: your_domain_password
Port: 1433
Extra: (خالی)
```

> اگر Conn Id را با تایپ اشتباه `mssql_defualt` ساخته‌اید، آن را به `mssql_default` اصلاح کنید (یا کانفیگ `mssql_conn_id` را همان مقدار قرار دهید).

#### Kafka (برای monitoring):
می‌توانید Connection ایجاد کنید یا از Variables استفاده کنید.

### 3. تنظیم Airflow Variables

از UI یا CLI:

```bash
# Set Config JSON
airflow variables set sql_to_kafka_config '{
    "batch_size": 50000,
    "max_batch_timeout_sec": 300,
    "kafka_partitions": 1,
    "kafka_replication_factor": 1,
    "enable_idempotence": true,
    "compression_type": "snappy"
}'
```

**نکته:** پارامترهای DAG (table_name, date_column, primary_key_column, kafka_topic) در بخش `params` خود DAG تعریف شده‌اند:
```python
params={
    'table_name': 'RTL.Fact_SalesTrans',
    'date_column': 'COM_Date_TransRef',
    'primary_key_column': 'ID',
    'kafka_topic': 'RTL-Fact_SalesTrans',
    'num_partitions': 1,
    'replication_factor': 1
}
```

### 4. ایجاد Pool (اختیاری)

برای کنترل parallelism:

```bash
airflow pools set data_sync_pool 5 "Pool for SQL to Kafka transfers"
```

### 5. کپی فایل‌ها به Airflow

```bash
# کپی DAG file
cp dags/sqlserver_kafka_sync.py $AIRFLOW_HOME/dags/

# کپی pipeline modules
cp -r pipeline/ $AIRFLOW_HOME/dags/
```

**ساختار نهایی در Airflow**:
```
$AIRFLOW_HOME/dags/
├── kafka_sync/
│   ├── sqlserver_kafka_sync.py
│   ├── fact_sales_trans_sync.py
│   └── dim_date_sync.py
├── clickhouse/
│   └── ...
├── monitoring/
│   ├── pipeline_health_monitor.py
│   └── ...
└── pipeline/
    ├── IdempotentKafkaProducer.py
    ├── KafkaTopicManager.py
    └── MSSQLDataReader.py
```

## مثال Query SQL

### مثال 1: جدول Partitioned
```sql
-- جدول با Partition بر اساس transaction_date
SELECT *
FROM transactions WITH (NOLOCK)
WHERE transaction_date = '20240115'
ORDER BY id
```

### مثال 2: با فیلترهای اضافی
```sql
SELECT *
FROM transactions WITH (NOLOCK)
WHERE transaction_date = '20240115'
  AND status = 'completed'
  AND amount > 0
ORDER BY id
```

### Index Recommendations
```sql
-- Index برای performance بهینه
CREATE INDEX IX_transactions_date_id 
ON transactions (transaction_date, id);

-- یا
CREATE CLUSTERED INDEX IX_transactions_date 
ON transactions (transaction_date, id);
```

## مثال Kafka Producer Configuration

```python
# مثال استفاده مستقیم
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'airflow-producer',
    'enable.idempotence': True,
    'acks': 'all',
    'compression.type': 'snappy',
    'max.in.flight.requests.per.connection': 1,
})

# Produce message
producer.produce(
    topic='sql_server_transactions',
    key=b'record_id_123',
    value=b'{"id": 123, "amount": 100.50}',
    callback=delivery_callback
)

producer.flush()
```

## Monitoring & Observability

### 1. Airflow UI
- Task Logs
- Task Duration
- Task Status
- Gantt Chart

### 2. Kafka Monitoring

#### Consumer Lag:
```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group your_consumer_group \
  --describe
```

#### Topic Metrics:
```bash
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic sql_server_transactions \
  --time -1
```

### 3. Custom Metrics (پیشنهاد)

می‌توانید metrics زیر را اضافه کنید:

- **Throughput**: Records per second
- **Lag**: زمان بین execution_date و current time
- **Batch Processing Time**: میانگین زمان پردازش هر batch
- **Error Rate**: درصد خطاها
- **Memory Usage**: مصرف RAM در طول پردازش

### 4. Alerting

```python
# مثال alert در صورت خطا
@task(on_failure_callback=send_slack_alert)
def transfer_data_to_kafka(**context):
    # ...
```

## Performance & Scalability

### Tuning Parameters

1. **Batch Size**: 
   - کوچک (10k): کمتر RAM، بیشتر network overhead
   - بزرگ (100k+): بیشتر RAM، کمتر network overhead
   - پیشنهاد: 50k-100k

2. **Kafka Partitions**:
   - تعداد partitions = parallelism
   - پیشنهاد: 3-6 partitions برای throughput بالا

3. **Producer Configuration**:
   - `linger.ms`: زمان انتظار برای batch (10-100ms)
   - `batch.size`: سایز batch در bytes
   - `compression.type`: snappy یا lz4

4. **SQL Server**:
   - Index روی date column
   - Partition elimination
   - Query hints (NOLOCK اگر مناسب است)

### Scalability Options

1. **Task Mapping** (Airflow 2.x):
   - تقسیم یک روز به چند ساعت
   - Parallel execution برای هر ساعت

2. **Multiple DAGs**:
   - DAG جداگانه برای هر table
   - استفاده از Pools برای resource management

3. **Kubernetes**:
   - استفاده از KubernetesPodOperator
   - Auto-scaling

## Troubleshooting

### خطا: Connection Timeout
- بررسی network connectivity
- بررسی firewall rules
- افزایش timeout در configuration

### خطا: Memory Error
- کاهش `batch_size`
- بررسی query optimization
- استفاده از streaming به جای load کامل

### خطا: Kafka Producer Timeout
- بررسی Kafka broker health
- افزایش `message.timeout.ms`
- بررسی network latency

### خطا: Duplicate Messages
- بررسی `enable.idempotence`
- بررسی `max.in.flight.requests.per.connection=1`
- استفاده از idempotent consumer در سمت consumer

## Best Practices

1. **Idempotency**: همیشه از idempotent producer استفاده کنید
2. **Monitoring**: Lag و throughput را monitor کنید
3. **Error Handling**: Retry با exponential backoff
4. **Configuration**: استفاده از Variables به جای hardcode
5. **Testing**: Test با volume کم قبل از production
6. **Backfill**: Test backfill strategy
7. **Documentation**: مستندسازی configuration و changes

## پیشنهادات برای Enhancement

1. **Schema Registry**: استفاده از Avro با Schema Registry
2. **Metrics Export**: Prometheus/Grafana integration
3. **Dead Letter Queue**: برای records با خطا
4. **Data Quality Checks**: validation قبل از send به Kafka
5. **Multi-table Support**: support برای چندین table
6. **Incremental State Tracking**: track آخرین processed record

## سوالات متداول (FAQ)

### چگونه برای table دیگری DAG بسازم؟
`params` در تعریف DAG را تغییر دهید یا یک DAG جدید با `dag_id` متفاوت ایجاد کنید.

### چگونه batch_size را تنظیم کنم؟
در Airflow Variable با نام `sql_to_kafka_config`، مقدار `batch_size` را تغییر دهید.

### چگونه می‌توانم backfill انجام دهم؟
```bash
airflow dags backfill sqlserver_kafka_sync \
  --start-date 2024-12-01 \
  --end-date 2024-12-31
```

### آیا می‌توانم چند table را همزمان اجرا کنم؟
بله، با ایجاد DAG جداگانه برای هر table و استفاده از Pools.

### چگونه می‌توانم performance را بهبود ببخشم؟
- افزایش batch_size
- اضافه کردن index مناسب
- افزایش Kafka partitions
- تنظیم compression

## تست‌ها (Testing)

### 🧪 Test Coverage
پروژه شامل **100+ unit tests** با پوشش کامل تمام ماژول‌های اصلی است:

- ✅ **SQLQueryBuilder**: 16 tests - تست کوئری‌های امن و جلوگیری از SQL Injection
- ✅ **ConfigurationManager**: 16 tests - تست مدیریت تنظیمات
- ✅ **MessageSerializer**: 23 tests - تست سریالایز و انواع داده
- ✅ **TransferMetrics**: 21 tests - تست ردیابی متریک‌ها
- ✅ **ExecutionDateExtractor**: 23 tests - تست مدیریت تاریخ‌ها
- ✅ **TransferResult**: 18 tests - تست نتایج انتقال

### اجرای تست‌ها

#### روش سریع (بدون pytest):
```bash
python run_tests.py
```

#### با pytest (توصیه می‌شود):
```bash
# نصب pytest
pip install -r requirements-test.txt

# اجرای تمام تست‌ها
pytest tests/ -v

# اجرای تست خاص
pytest tests/test_sql_query_builder.py -v

# با گزارش coverage
pytest tests/ --cov=pipeline --cov-report=html
open htmlcov/index.html
```

### نمونه خروجی تست:
```
Testing SQLQueryBuilder...
✅ Count query test passed
✅ First batch pagination test passed
✅ Subsequent batch pagination test passed
✅ SQL injection prevention test passed

🎉 ALL TESTS PASSED! 🎉
```

### مستندات تست‌ها
برای اطلاعات بیشتر، [راهنمای کامل تست‌ها](tests/README.md) را مطالعه کنید.

## اطلاعات تماس و پشتیبانی

برای سوالات و پیشنهادات با تیم Data Engineering تماس بگیرید.

📖 **مستندات کامل فارسی**: [ARCHITECTURE_FA.md](ARCHITECTURE_FA.md)

## License

MIT License

---

**نسخه**: 1.0  
**آخرین بروزرسانی**: دسامبر 2024

