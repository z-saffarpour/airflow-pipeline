"""
Airflow DAG: Dim_Date Pipeline Health Monitor
==============================================
Monitor health of Dim_Date SQL Server to Kafka pipeline.

Schedule:
- Manual trigger (Dim_Date is rarely updated)
- Run after dim_date_kafka_sync execution

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
    dag_id='dim_date_health_monitor',
    description='Monitor Dim_Date Kafka Pipeline Health',
    default_args=default_args,
    schedule=None,  # Manual trigger - Dim_Date is rarely updated
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'dimension-table', 'health-check', 'kafka'],
    params={
        'kafka_conn_id': Param(
            default='kafka_default',
            type='string',
            description='Airflow connection ID for Kafka'
        ),
        'kafka_topic': Param(
            default='COM-Dim_Date-AirFlow',
            type='string',
            description='Kafka topic to monitor'
        ),
        'consumer_group': Param(
            default='clickhouse-consumer-dim-date',
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
            default=10000,
            type='integer',
            description='Maximum acceptable lag (records) - 10k for Dim table'
        ),
        'max_lag_minutes': Param(
            default=15,
            type='integer',
            description='Maximum acceptable lag (minutes)'
        ),
        'expected_daily_records': Param(
            default=0,
            type='integer',
            description='Expected daily records (0 - Dim_Date is static)'
        ),
    },
    doc_md="""
    ## Dim_Date Pipeline Health Monitor
    
    This DAG monitors the health of Dim_Date SQL Server to Kafka pipeline.
    
    ### Prerequisites
    - **Airflow Connection**: `kafka_default` must be configured with Kafka broker details
      - Host: Kafka broker hostname
      - Port: Kafka broker port (default: 9092)
      - Extra: `{"bootstrap_servers": "broker1:9092,broker2:9092", "security_protocol": "SASL_SSL", ...}`
    
    ### Schedule
    - **Manual trigger** (no automatic schedule)
    - Run after `dim_date_kafka_sync` when needed
    
    ### Why Manual?
    - Dim_Date is a **Dimension table** (static/reference data)
    - Updated rarely (yearly or monthly)
    - No need for continuous monitoring
    
    ### Checks Performed
    1. **Kafka Cluster Health** - Broker availability (reads from Airflow Connection)
    2. **Consumer Lag** - ClickHouse consumer lag
    3. **Topic Statistics** - Partition info, message count
    4. **Message Sampling** - Verify recent messages
    
    ### Thresholds
    - Max lag: 10,000 records (small table)
    - Max lag time: 15 minutes
    - Expected daily volume: 0 (static table)
    
    ### Related DAGs
    - `dim_date_kafka_sync` - Data transfer DAG
    - `dim_date_clickhouse_optimizer` - ClickHouse optimization DAG
    """
)
def dim_date_health_monitor():
    """
    Monitor Dim_Date pipeline health.
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
dag_instance = dim_date_health_monitor()
