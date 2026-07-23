"""
DAGs Package - Airflow DAG Definitions
======================================
This package contains all Airflow DAG definitions organized by category.

Structure:
----------
dags/
├── kafka_sync/           # SQL Server to Kafka sync DAGs
│   ├── sqlserver_kafka_sync.py (Template)
│   ├── dim_date_sync.py
│   └── fact_sales_trans_sync.py
├── clickhouse/           # ClickHouse optimization DAGs
│   ├── clickhouse_optimizer.py (Template)
│   ├── dim_date_clickhouse_optimizer.py
│   └── fact_sales_trans_clickhouse_optimizer.py
└── monitoring/           # Health monitoring DAGs
    ├── pipeline_health_monitor.py (Template)
    ├── dim_date_health_monitor.py
    └── fact_sales_trans_health_monitor.py

DAGs:
-----
Kafka Sync:
- sqlserver_kafka_sync: Template DAG for SQL Server to Kafka sync (disabled)
- dim_date_kafka_sync: Dimension date table sync
- fact_sales_trans_kafka_sync: Fact sales transactions sync

ClickHouse Optimization:
- clickhouse_table_optimizer: Template DAG for ClickHouse optimization (disabled)
- dim_date_clickhouse_optimizer: Dimension date table optimization
- fact_sales_trans_clickhouse_optimizer: Fact sales transactions optimization

Health Monitoring:
- pipeline_health_monitor: Template DAG for pipeline health monitoring (disabled)
- dim_date_health_monitor: Dimension date table health monitoring (manual)
- fact_sales_trans_health_monitor: Fact sales transactions health monitoring (every 4h)

Author: Senior Data Engineer
Version: 2.4
"""

__version__ = "2.4.0"
