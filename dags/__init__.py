"""
DAGs Package - Airflow DAG Definitions
======================================
This package contains all Airflow DAG definitions organized by category.

Structure:
----------
dags/
├── template/                          # Reusable DAG factories (16)
│   ├── table_mssql_sync_dag_factory.py
│   ├── mssql_to_kafka_clickhouse_sync_dag_factory.py
│   ├── mssql_to_kafka_sync_dag_factory.py
│   ├── mssql_to_clickhouse_sync_dag_factory.py
│   ├── mysql_to_mssql_sync_dag_factory.py
│   ├── mssql_to_mysql_sync_dag_factory.py
│   ├── mssql_to_postgresql_sync_dag_factory.py
│   ├── postgresql_to_mssql_sync_dag_factory.py
│   ├── mssql_to_mssql_sync_dag_factory.py
│   ├── mssql_to_mongo_sync_dag_factory.py
│   ├── mongo_to_mssql_sync_dag_factory.py
│   ├── kafka_to_mssql_sync_dag_factory.py
│   ├── clickhouse_to_mssql_sync_dag_factory.py
│   ├── mssql_masterdata_to_mssql_store_sync_dag_factory.py
│   ├── clickhouse_optimizer_dag_factory.py
│   └── kafka_health_monitor_dag_factory.py
│
├── mssql_to_kafka_clickhouse_sync/    # SQL Server → Kafka (± ClickHouse)
├── mssql_to_kafka_sync/               # MSSQL → Kafka (Gen-2)
├── mssql_to_clickhouse_sync/          # MSSQL → ClickHouse
├── mysql_to_mssql_sync/               # MySQL → MSSQL
├── mssql_to_mysql_sync/               # MSSQL → MySQL
├── mssql_to_postgresql_sync/          # MSSQL → PostgreSQL
├── postgresql_to_mssql_sync/          # PostgreSQL → MSSQL
├── mssql_to_mssql_sync/               # MSSQL → MSSQL (fixed connections)
├── mssql_to_mongo_sync/               # MSSQL → MongoDB
├── mongo_to_mssql_sync/               # MongoDB → MSSQL
├── kafka_to_mssql_sync/               # Kafka → MSSQL
├── clickhouse_to_mssql_sync/          # ClickHouse → MSSQL
├── masterdata_store_sync/             # Master Data Publisher → Store
├── clickhouse_optimizer/              # ClickHouse table optimization
└── kafka_health_monitor/              # Kafka topic health monitoring
    ├── example_topic_health_monitor.py
    ├── mssql_sync/dwh/                # DWH topic monitors
    ├── mssql_sync/erp/                # AX ERP topic monitors
    └── sales_inventory/               # Sales & inventory topic monitors

Author: Senior Data Engineer
Version: 2.6
"""

__version__ = "2.6.0"
