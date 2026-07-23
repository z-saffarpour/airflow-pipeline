"""
Airflow DAG: Pipeline Health Monitoring (TEMPLATE)
==================================================
This is a TEMPLATE DAG for monitoring SQL Server to Kafka pipeline health.
Do NOT run this DAG directly - create specific DAGs for each monitoring scenario.

Features:
- Kafka topic lag monitoring
- Consumer group health check
- SQL Server connection validation
- Throughput monitoring
- Alerting support
- Configurable thresholds

Usage:
    Copy this template to create monitoring-specific DAGs:
    - kafka_lag_monitor.py
    - daily_health_check.py

    Example usage in child DAG:
    ```python
    from monitoring.pipeline_health_monitor import (
        validate_kafka_health,
        check_consumer_lag,
        check_topic_stats,
        generate_health_report,
        default_args,
    )
    ```

Author: Senior Data Engineer
Version: 1.0
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from airflow.decorators import task, dag  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.operators.python import get_current_context  # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore

from confluent_kafka.admin import AdminClient  # type: ignore

from pipeline.kafka.KafkaTopicManager import KafkaTopicManager
from pipeline.compat.airflow_compat import get_connection 
# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = LoggingMixin().log

# ============================================================================
# DEFAULT CONFIGURATION
# ============================================================================

DEFAULT_THRESHOLDS = {
    'max_lag_records': 100000,        # Alert if lag > 100k records
    'max_lag_minutes': 30,            # Alert if lag > 30 minutes
    'min_throughput_per_min': 1000,   # Alert if throughput < 1k/min
    'expected_daily_records': 5000000, # Expected daily records
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_kafka_brokers(conn_id: str = "kafka_default") -> str:
    """
    Read kafka brokers from Airflow Connection and return bootstrap.servers string.
    Uses the same logic as sqlserver_kafka_sync for consistency.
    
    Args:
        conn_id: Airflow connection ID
        
    Returns:
        Comma-separated list of broker addresses
    """
    conn = get_connection(conn_id)
    extra = json.loads(conn.extra or "{}")
    # Match sqlserver_kafka_sync: use bootstrap_servers from extra or build from host:port
    brokers = extra.get("bootstrap_servers") or f"{conn.host}:{conn.port or 9092}"
    return brokers


def get_kafka_config(conn_id: str = "kafka_default") -> Dict[str, Any]:
    """
    Get full Kafka configuration from Airflow Connection.
    Includes security settings if configured.
    
    Args:
        conn_id: Airflow connection ID
        
    Returns:
        Dictionary with Kafka configuration
    """
    conn = get_connection(conn_id)
    extra = json.loads(conn.extra or "{}")
    
    kafka_conf = {
        "bootstrap.servers": extra.get("bootstrap_servers") or f"{conn.host}:{conn.port or 9092}",
        "client.id": extra.get("client_id", "airflow-monitor")
    }
    
    # Add security settings if present
    if extra.get("security_protocol"):
        kafka_conf["security.protocol"] = extra["security_protocol"]
    
    if extra.get("sasl_mechanism"):
        kafka_conf["sasl.mechanism"] = extra["sasl_mechanism"]
        kafka_conf["sasl.username"] = extra.get("sasl_username", "")
        kafka_conf["sasl.password"] = extra.get("sasl_password", "")
    
    return kafka_conf


def get_thresholds(**context) -> Dict[str, Any]:
    """
    Get monitoring thresholds from params or defaults.
    """
    params = context.get("params", {})
    thresholds = DEFAULT_THRESHOLDS.copy()
    
    if params.get("max_lag_records"):
        thresholds['max_lag_records'] = params['max_lag_records']
    if params.get("max_lag_minutes"):
        thresholds['max_lag_minutes'] = params['max_lag_minutes']
    if params.get("min_throughput_per_min"):
        thresholds['min_throughput_per_min'] = params['min_throughput_per_min']
    if params.get("expected_daily_records"):
        thresholds['expected_daily_records'] = params['expected_daily_records']
    
    return thresholds


# ============================================================================
# AIRFLOW TASKS
# ============================================================================

@task(retries=3, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=5))
def validate_kafka_health(**context) -> Dict[str, Any]:
    """
    Validate Kafka connection and cluster health.
    Reads kafka_conn_id from params.
    
    Returns:
        Dictionary with cluster health information
    """
    params = context["params"]
    conn_id = params.get("kafka_conn_id", "kafka_default")
    
    try:
        # Get full Kafka config including security settings
        kafka_conf = get_kafka_config(conn_id)
        
        # Create admin client to check cluster
        admin_client = AdminClient(kafka_conf)
        
        # Get cluster metadata
        cluster_metadata = admin_client.list_topics(timeout=30)
        
        broker_count = len(cluster_metadata.brokers)
        topic_count = len(cluster_metadata.topics)
        
        if broker_count == 0:
            raise AirflowException("No Kafka brokers available")
        
        logger.info(f"Kafka cluster healthy: {broker_count} brokers, {topic_count} topics")
        
        return {
            'status': 'healthy',
            'broker_count': broker_count,
            'topic_count': topic_count,
            'bootstrap_servers': kafka_conf['bootstrap.servers'],
            'conn_id': conn_id,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error("Kafka health check failed", exc_info=True)
        raise AirflowException(f"Kafka health check failed: {str(e)}")


@task(retries=2, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=5))
def check_consumer_lag(**context) -> Dict[str, Any]:
    """
    Check consumer lag for specified topic and consumer group.
    
    Returns:
        Dictionary with lag information per partition
    """
    params = context["params"]
    conn_id = params.get("kafka_conn_id", "kafka_default")
    topic = params.get("kafka_topic", "")
    consumer_group = params.get("consumer_group", "")
    
    if not topic or not consumer_group:
        logger.warning("Topic or consumer_group not specified, skipping lag check")
        return {'status': 'skipped', 'reason': 'missing parameters'}
    
    try:
        bootstrap_servers = get_kafka_brokers(conn_id)
        topic_manager = KafkaTopicManager(bootstrap_servers)
        
        lag_info = topic_manager.check_consumer_lag(topic, consumer_group)
        
        # Calculate totals
        total_lag = sum(p.get('lag', 0) for p in lag_info.get('partitions', []))
        
        thresholds = get_thresholds(**context)
        
        status = 'healthy'
        alerts = []
        
        if total_lag > thresholds['max_lag_records']:
            status = 'warning'
            alerts.append(f"Total lag ({total_lag}) exceeds threshold ({thresholds['max_lag_records']})")
        
        logger.info(f"Consumer lag check: {topic}/{consumer_group} - Total lag: {total_lag}")
        
        return {
            'status': status,
            'topic': topic,
            'consumer_group': consumer_group,
            'total_lag': total_lag,
            'partition_count': len(lag_info.get('partitions', [])),
            'partitions': lag_info.get('partitions', []),
            'alerts': alerts,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Consumer lag check failed for {topic}", exc_info=True)
        return {
            'status': 'error',
            'topic': topic,
            'consumer_group': consumer_group,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@task(retries=2, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=5))
def check_topic_stats(**context) -> Dict[str, Any]:
    """
    Get topic statistics.
    
    Returns:
        Dictionary with topic statistics
    """
    params = context["params"]
    conn_id = params.get("kafka_conn_id", "kafka_default")
    topic = params.get("kafka_topic", "")
    
    if not topic:
        logger.warning("Topic not specified, skipping stats check")
        return {'status': 'skipped', 'reason': 'missing topic'}
    
    try:
        bootstrap_servers = get_kafka_brokers(conn_id)
        topic_manager = KafkaTopicManager(bootstrap_servers)
        
        stats = topic_manager.get_topic_stats(topic)
        
        logger.info(f"Topic stats for {topic}: {stats}")
        
        return {
            'status': 'success',
            'topic': topic,
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Topic stats check failed for {topic}", exc_info=True)
        return {
            'status': 'error',
            'topic': topic,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@task(retries=1, execution_timeout=timedelta(minutes=5))
def sample_recent_messages(**context) -> Dict[str, Any]:
    """
    Sample recent messages from topic for verification.
    
    Returns:
        Dictionary with sampled messages
    """
    params = context["params"]
    conn_id = params.get("kafka_conn_id", "kafka_default")
    topic = params.get("kafka_topic", "")
    sample_count = params.get("sample_count", 5)
    
    if not topic:
        logger.warning("Topic not specified, skipping message sampling")
        return {'status': 'skipped', 'reason': 'missing topic'}
    
    try:
        bootstrap_servers = get_kafka_brokers(conn_id)
        topic_manager = KafkaTopicManager(bootstrap_servers)
        
        messages = topic_manager.sample_messages(
            topic=topic,
            partition=0,
            count=sample_count,
            offset='latest'
        )
        
        # Summarize messages (don't log full content)
        message_summaries = []
        for msg in messages:
            summary = {
                'offset': msg.get('offset'),
                'partition': msg.get('partition', 0),
                'timestamp': msg.get('timestamp'),
                'key': msg.get('key'),
                'value_preview': str(msg.get('value', ''))[:100] + '...' if msg.get('value') else None
            }
            message_summaries.append(summary)
        
        logger.info(f"Sampled {len(messages)} messages from {topic}")
        
        return {
            'status': 'success',
            'topic': topic,
            'sample_count': len(messages),
            'messages': message_summaries,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Message sampling failed for {topic}", exc_info=True)
        return {
            'status': 'error',
            'topic': topic,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@task(execution_timeout=timedelta(minutes=2))
def generate_health_report(
    kafka_health: Dict[str, Any],
    lag_info: Dict[str, Any],
    topic_stats: Dict[str, Any],
    **context
) -> Dict[str, Any]:
    """
    Generate comprehensive health report from all checks.
    
    Args:
        kafka_health: Kafka cluster health info
        lag_info: Consumer lag information
        topic_stats: Topic statistics
        
    Returns:
        Comprehensive health report
    """
    params = context["params"]
    thresholds = get_thresholds(**context)
    
    # Determine overall status
    statuses = [
        kafka_health.get('status', 'unknown'),
        lag_info.get('status', 'unknown'),
        topic_stats.get('status', 'unknown'),
    ]
    
    if 'error' in statuses:
        overall_status = 'error'
    elif 'warning' in statuses:
        overall_status = 'warning'
    elif all(s in ['healthy', 'success', 'skipped'] for s in statuses):
        overall_status = 'healthy'
    else:
        overall_status = 'unknown'
    
    # Collect all alerts
    alerts = []
    alerts.extend(lag_info.get('alerts', []))
    
    if kafka_health.get('status') == 'error':
        alerts.append(f"Kafka cluster error: {kafka_health.get('error', 'unknown')}")
    
    if topic_stats.get('status') == 'error':
        alerts.append(f"Topic stats error: {topic_stats.get('error', 'unknown')}")
    
    report = {
        'overall_status': overall_status,
        'timestamp': datetime.now().isoformat(),
        'alerts': alerts,
        'alert_count': len(alerts),
        'thresholds': thresholds,
        'checks': {
            'kafka_cluster': kafka_health,
            'consumer_lag': lag_info,
            'topic_stats': topic_stats,
        },
        'summary': {
            'broker_count': kafka_health.get('broker_count', 0),
            'total_lag': lag_info.get('total_lag', 0),
            'topic': params.get('kafka_topic', 'N/A'),
            'consumer_group': params.get('consumer_group', 'N/A'),
        }
    }
    
    # Log summary
    logger.info(f"Health Report: {overall_status} - {len(alerts)} alerts")
    if alerts:
        for alert in alerts:
            logger.warning(f"  ALERT: {alert}")
    
    return report


# ============================================================================
# DEFAULT ARGS
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================================
# DAG DEFINITION (TEMPLATE - DISABLED)
# ============================================================================

@dag(
    dag_id='pipeline_health_monitor',
    description='Template DAG for Pipeline Health Monitoring - DO NOT USE DIRECTLY',
    default_args=default_args,
    schedule=None,  # Template - no schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['template', 'monitoring', 'health-check'],
    is_paused_upon_creation=True,  # Template DAG should not run
    params={
        'kafka_conn_id': Param(
            default='kafka_default',
            type='string',
            description='Airflow connection ID for Kafka'
        ),
        'kafka_topic': Param(
            default='',
            type='string',
            description='Kafka topic to monitor'
        ),
        'consumer_group': Param(
            default='',
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
            default=100000,
            type='integer',
            description='Maximum acceptable lag (records)'
        ),
        'max_lag_minutes': Param(
            default=30,
            type='integer',
            description='Maximum acceptable lag (minutes)'
        ),
    },
    doc_md="""
    ## Pipeline Health Monitor (TEMPLATE)
    
    ⚠️ **This is a TEMPLATE DAG - Do not run directly!**
    
    This DAG monitors the health of SQL Server to Kafka pipeline.
    Create specific monitoring DAGs for each pipeline.
    
    ### Checks Performed:
    1. Kafka cluster health
    2. Consumer lag
    3. Topic statistics
    
    ### Usage:
    Import tasks from this template to create specific monitoring DAGs.
    """
)
def pipeline_health_monitor():
    """
    Template DAG for pipeline health monitoring.
    """
    
    with TaskGroup(group_id='health_checks') as health_checks:
        kafka_health = validate_kafka_health()
        lag_info = check_consumer_lag()
        topic_stats = check_topic_stats()
    
    report = generate_health_report(
        kafka_health=kafka_health,
        lag_info=lag_info,
        topic_stats=topic_stats,
    )
    
    health_checks >> report


# Create DAG instance
dag_instance = pipeline_health_monitor()


# ============================================================================
# LEGACY FUNCTIONS (for backward compatibility)
# ============================================================================

def check_kafka_topic_lag(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str
) -> Dict:
    """
    Check Lag in Kafka Consumer Group.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic name
        consumer_group: Consumer group ID
    
    Returns:
        Dictionary with lag information for each partition
    """
    topic_manager = KafkaTopicManager(bootstrap_servers)
    return topic_manager.check_consumer_lag(topic, consumer_group)


def get_topic_stats_legacy(
    bootstrap_servers: str,
    topic: str
) -> Dict:
    """
    Get general topic statistics (number of messages, partitions, etc.).
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic name
        
    Returns:
        Dictionary with topic statistics
    """
    topic_manager = KafkaTopicManager(bootstrap_servers)
    return topic_manager.get_topic_stats(topic)


def sample_messages_from_topic(
    bootstrap_servers: str,
    topic: str,
    partition: int = 0,
    count: int = 10,
    offset: str = 'latest'
) -> List[Dict]:
    """
    Sample messages from topic.
    
    Args:
        bootstrap_servers: Kafka brokers
        topic: Topic name
        partition: Partition number
        count: Number of messages to sample
        offset: 'earliest' or 'latest'
        
    Returns:
        List of sampled messages
    """
    topic_manager = KafkaTopicManager(bootstrap_servers)
    return topic_manager.sample_messages(topic, partition, count, offset)


def check_pipeline_health(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    expected_daily_records: int = 5000000
) -> Dict:
    """
    Check overall pipeline health.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic name
        consumer_group: Consumer group ID
        expected_daily_records: Expected daily record count
    
    Returns:
        Health report
    """
    topic_manager = KafkaTopicManager(bootstrap_servers)
    return topic_manager.check_health(topic, consumer_group, expected_daily_records)
