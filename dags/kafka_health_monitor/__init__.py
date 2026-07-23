"""
Monitoring DAGs Package
=======================
This package contains monitoring, health check, and alerting DAGs.

DAGs:
-----
- pipeline_health_monitor: Template DAG for pipeline monitoring
- fact_sales_trans_health_monitor: Monitor Fact_SalesTrans pipeline
- dim_date_health_monitor: Monitor Dim_Date pipeline

Available Tasks (from pipeline_health_monitor):
- validate_kafka_health: Validate Kafka cluster health (reads from Airflow Connection)
- check_consumer_lag: Check consumer group lag
- check_topic_stats: Get topic statistics
- sample_recent_messages: Sample messages from topic
- generate_health_report: Generate comprehensive health report

Helper Functions:
- get_kafka_brokers: Get Kafka bootstrap servers from Airflow Connection
- get_kafka_config: Get full Kafka config including security settings

Legacy Functions (for backward compatibility):
- check_kafka_topic_lag: Check lag for consumer group
- get_topic_stats_legacy: Get topic statistics
- sample_messages_from_topic: Sample messages from topic
- check_pipeline_health: Check overall pipeline health

Author: Senior Data Engineer
Version: 1.2.0
"""

__version__ = "1.2.0"

from kafka_health_monitor.pipeline_health_monitor import (
    # Tasks
    validate_kafka_health,
    check_consumer_lag,
    check_topic_stats,
    sample_recent_messages,
    generate_health_report,
    default_args,
    # Helper functions
    get_kafka_brokers,
    get_kafka_config,
    # Legacy functions
    check_kafka_topic_lag,
    get_topic_stats_legacy,
    sample_messages_from_topic,
    check_pipeline_health,
)

__all__ = [
    # Tasks
    'validate_kafka_health',
    'check_consumer_lag',
    'check_topic_stats',
    'sample_recent_messages',
    'generate_health_report',
    'default_args',
    # Helper functions
    'get_kafka_brokers',
    'get_kafka_config',
    # Legacy
    'check_kafka_topic_lag',
    'get_topic_stats_legacy',
    'sample_messages_from_topic',
    'check_pipeline_health',
]
