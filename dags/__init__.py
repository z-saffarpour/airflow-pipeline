"""
DAGs Package - Airflow DAG Definitions
======================================
This package contains all Airflow DAG definitions organized by category.

Structure:
----------
dags/
├── template/                      # Reusable DAG factories
├── mssql_to_kafka_clickhouse_sync/ # SQL Server → Kafka (± ClickHouse)
├── sales_inventory/               # Sales & inventory multi-source → Kafka
├── replication/                   # Replication MD repair (Publisher → Store)
├── clickhouse_optimizer/          # ClickHouse table optimization
└── kafka_health_monitor/          # Kafka pipeline health monitoring
    ├── mssql_sync/dwh/            # DWH topic monitors
    ├── mssql_sync/erp/            # AX ERP topic monitors
    └── sales_inventory/           # Sales & inventory topic monitors

Author: Senior Data Engineer
Version: 2.5
"""

__version__ = "2.5.0"
