"""
Kafka Health Monitor DAGs
=========================
Config-based health monitoring DAGs for Kafka pipelines.

Structure:
- mssql_sync/dwh/   : DWH dimension and fact table/query monitors (14)
- mssql_sync/erp/   : AX ERP query sync monitors (4)
- sales_inventory/  : Sales and inventory query monitors (6)

Factory:
- template.kafka_health_monitor_dag_factory.kafka_health_monitor_dag

Config:
- pipeline.config.KafkaHealthMonitorConfig

Author: Senior Data Engineer
Version: 2.0.0
"""

__version__ = "2.0.0"
