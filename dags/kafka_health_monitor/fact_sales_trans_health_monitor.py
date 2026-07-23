"""
Airflow DAG: Fact_SalesTrans Pipeline Health Monitor
=====================================================
Monitor health of Fact_SalesTrans SQL Server to Kafka pipeline.

Schedule:
- Every 4 hours during business hours
- Monitors consumer lag, topic stats, and cluster health

Author: Senior Data Engineer
Version: 1.0
"""

from datetime import datetime

from airflow.decorators import dag  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.models.param import Param  # type: ignore

from kafka_health_monitor.pipeline_health_monitor import (
    validate_kafka_health,
    check_consumer_lag,
    check_topic_stats,
    sample_recent_messages,
    generate_health_report,
    default_args,
)

# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id='fact_sales_trans_health_monitor',
    description='Monitor Fact_SalesTrans Kafka Pipeline Health',
    default_args=default_args,
    schedule='0 */4 * * *',  # Every 4 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'fact-table', 'health-check', 'kafka'],
    params={
        'kafka_conn_id': Param(
            default='kafka_default',
            type='string',
            description='Airflow connection ID for Kafka'
        ),
        'kafka_topic': Param(
            default='RTL-Fact_SalesTrans-AirFlow',
            type='string',
            description='Kafka topic to monitor'
        ),
        'consumer_group': Param(
            default='clickhouse-consumer-fact-sales',
            type='string',
            description='Consumer group to check lag for'
        ),
        'sample_count': Param(
            default=5,
            type='integer',
            minimum=1,
            maximum=100,
            description='Number of messages to sample'
        ),
        'max_lag_records': Param(
            default=500000,
            type='integer',
            description='Maximum acceptable lag (records) - 500k for Fact table'
        ),
        'max_lag_minutes': Param(
            default=60,
            type='integer',
            description='Maximum acceptable lag (minutes)'
        ),
        'expected_daily_records': Param(
            default=5000000,
            type='integer',
            description='Expected daily records (5M for Fact_SalesTrans)'
        ),
    },
    doc_md="""
    ## Fact_SalesTrans Pipeline Health Monitor
    
    This DAG monitors the health of Fact_SalesTrans SQL Server to Kafka pipeline.
    
    ### Prerequisites
    - **Airflow Connection**: `kafka_default` must be configured with Kafka broker details
      - Host: Kafka broker hostname
      - Port: Kafka broker port (default: 9092)
      - Extra: `{"bootstrap_servers": "broker1:9092,broker2:9092", "security_protocol": "SASL_SSL", ...}`
    
    ### Schedule
    - **Every 4 hours** (0 */4 * * *)
    - Runs during all hours to catch any issues
    
    ### Checks Performed
    1. **Kafka Cluster Health** - Broker availability (reads from Airflow Connection)
    2. **Consumer Lag** - ClickHouse consumer lag
    3. **Topic Statistics** - Partition info, message count
    4. **Message Sampling** - Verify recent messages
    
    ### Thresholds
    - Max lag: 500,000 records
    - Max lag time: 60 minutes
    - Expected daily volume: 5,000,000 records
    
    ### Related DAGs
    - `fact_sales_trans_kafka_sync` - Data transfer DAG
    - `fact_sales_trans_clickhouse_optimizer` - ClickHouse optimization DAG
    """
)
def fact_sales_trans_health_monitor():
    """
    Monitor Fact_SalesTrans pipeline health.
    """
    
    with TaskGroup(group_id='health_checks') as health_checks:
        kafka_health = validate_kafka_health()
        lag_info = check_consumer_lag()
        topic_stats = check_topic_stats()
    
    samples = sample_recent_messages()
    
    report = generate_health_report(
        kafka_health=kafka_health,
        lag_info=lag_info,
        topic_stats=topic_stats,
    )
    
    health_checks >> samples >> report


# Create DAG instance
dag_instance = fact_sales_trans_health_monitor()
