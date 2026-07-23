"""
Airflow DAG Factory: Kafka Pipeline Health Monitoring
=====================================================
Reusable factory for Kafka / pipeline health monitoring DAGs.

Features:
- Kafka cluster health validation
- Consumer group lag check
- Topic statistics
- Optional message sampling
- Threshold-based health report

Usage:
    from pipeline.config.DAGConfig import DAGConfig
    from pipeline.config.ConnectionConfig import ConnectionConfig
    from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
    from template.kafka_health_monitor_dag_factory import kafka_health_monitor_dag

    conn_config = ConnectionConfig(kafka_conn_id='kafka_default')
    kafka_health_monitor_dag(DAG_CONFIG, conn_config, HEALTH_CONFIG)

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.KafkaHealthMonitorConfig import KafkaHealthMonitorConfig
from pipeline.kafka.KafkaConnectionFactory import KafkaConnectionFactory
from pipeline.kafka.KafkaTopicManager import KafkaTopicManager

# ============================================================================
# LOGGING
# ============================================================================

logger = LoggingMixin().log


# ============================================================================
# HELPERS
# ============================================================================

def _normalize_partitions(lag_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize KafkaTopicManager lag output into a list of partition dicts.

    KafkaTopicManager returns either:
    - {'error': '...'}
    - {'partition_0': {'lag': N, ...}, 'partition_1': {...}, ...}
    """
    if not lag_info or "error" in lag_info:
        return []

    partitions: List[Dict[str, Any]] = []
    for key, value in lag_info.items():
        if not isinstance(value, dict):
            continue
        if "lag" not in value and "error" not in value:
            continue
        entry = dict(value)
        entry.setdefault("partition", key)
        partitions.append(entry)
    return partitions


def _sum_lag(partitions: List[Dict[str, Any]]) -> int:
    return sum(int(p.get("lag", 0) or 0) for p in partitions if "lag" in p)


# ============================================================================
# TASK FACTORIES
# ============================================================================

def make_validate_kafka_health_task(kafka_conn_id: str):
    """Factory: validate Kafka cluster health."""

    @task(
        task_id="validate_kafka_health",
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def validate_kafka_health() -> Dict[str, Any]:
        factory = KafkaConnectionFactory(kafka_conn_id)
        cluster = factory.get_cluster_info(timeout=30)

        broker_count = cluster["broker_count"]
        topic_count = cluster["topic_count"]

        if broker_count == 0:
            raise AirflowException("No Kafka brokers available")

        logger.info(
            "Kafka cluster healthy: %s brokers, %s topics",
            broker_count,
            topic_count,
        )

        return {
            "status": "healthy",
            "broker_count": broker_count,
            "topic_count": topic_count,
            "bootstrap_servers": cluster["bootstrap_servers"],
            "conn_id": kafka_conn_id,
            "timestamp": datetime.now().isoformat(),
        }

    return validate_kafka_health


def make_check_consumer_lag_task(kafka_conn_id: str, health_config: KafkaHealthMonitorConfig):
    """Factory: check consumer lag for configured topic / group."""

    @task(
        task_id="check_consumer_lag",
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def check_consumer_lag() -> Dict[str, Any]:
        topic = health_config.kafka_topic
        consumer_group = health_config.consumer_group

        if not topic or not consumer_group:
            logger.warning(
                "Topic or consumer_group not specified, skipping lag check"
            )
            return {"status": "skipped", "reason": "missing parameters"}

        try:
            topic_manager = KafkaTopicManager(kafka_conn_id)
            lag_info = topic_manager.check_consumer_lag(topic, consumer_group)

            if "error" in lag_info:
                return {
                    "status": "error",
                    "topic": topic,
                    "consumer_group": consumer_group,
                    "error": lag_info["error"],
                    "timestamp": datetime.now().isoformat(),
                }

            partitions = _normalize_partitions(lag_info)
            total_lag = _sum_lag(partitions)

            status = "healthy"
            alerts: List[str] = []
            if total_lag > health_config.max_lag_records:
                status = "warning"
                alerts.append(
                    f"Total lag ({total_lag}) exceeds threshold "
                    f"({health_config.max_lag_records})"
                )

            logger.info(
                "Consumer lag check: %s/%s - Total lag: %s",
                topic,
                consumer_group,
                total_lag,
            )

            return {
                "status": status,
                "topic": topic,
                "consumer_group": consumer_group,
                "total_lag": total_lag,
                "partition_count": len(partitions),
                "partitions": partitions,
                "alerts": alerts,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as exc:
            logger.error("Consumer lag check failed for %s", topic, exc_info=True)
            return {
                "status": "error",
                "topic": topic,
                "consumer_group": consumer_group,
                "error": str(exc),
                "timestamp": datetime.now().isoformat(),
            }

    return check_consumer_lag


def make_check_topic_stats_task(kafka_conn_id: str, health_config: KafkaHealthMonitorConfig):
    """Factory: fetch topic statistics."""

    @task(
        task_id="check_topic_stats",
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def check_topic_stats() -> Dict[str, Any]:
        topic = health_config.kafka_topic
        if not topic:
            logger.warning("Topic not specified, skipping stats check")
            return {"status": "skipped", "reason": "missing topic"}

        try:
            topic_manager = KafkaTopicManager(kafka_conn_id)
            stats = topic_manager.get_topic_stats(topic)

            if "error" in stats:
                return {
                    "status": "error",
                    "topic": topic,
                    "error": stats["error"],
                    "timestamp": datetime.now().isoformat(),
                }

            logger.info("Topic stats for %s: %s", topic, stats)
            return {
                "status": "success",
                "topic": topic,
                "stats": stats,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as exc:
            logger.error("Topic stats check failed for %s", topic, exc_info=True)
            return {
                "status": "error",
                "topic": topic,
                "error": str(exc),
                "timestamp": datetime.now().isoformat(),
            }

    return check_topic_stats


def make_sample_recent_messages_task(kafka_conn_id: str, health_config: KafkaHealthMonitorConfig):
    """Factory: sample recent messages from topic."""

    @task(
        task_id="sample_recent_messages",
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )
    def sample_recent_messages() -> Dict[str, Any]:
        topic = health_config.kafka_topic
        if not topic:
            logger.warning("Topic not specified, skipping message sampling")
            return {"status": "skipped", "reason": "missing topic"}

        try:
            topic_manager = KafkaTopicManager(kafka_conn_id)
            messages = topic_manager.sample_messages(
                topic_name=topic,
                partition=0,
                count=health_config.sample_count,
                offset="latest",
            )

            message_summaries = []
            for msg in messages:
                value = msg.get("value")
                preview = None
                if value is not None:
                    text = str(value)
                    preview = text[:100] + ("..." if len(text) > 100 else "")
                message_summaries.append(
                    {
                        "offset": msg.get("offset"),
                        "partition": msg.get("partition", 0),
                        "timestamp": msg.get("timestamp"),
                        "key": msg.get("key"),
                        "value_preview": preview,
                    }
                )

            logger.info("Sampled %s messages from %s", len(messages), topic)
            return {
                "status": "success",
                "topic": topic,
                "sample_count": len(messages),
                "messages": message_summaries,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as exc:
            logger.error("Message sampling failed for %s", topic, exc_info=True)
            return {
                "status": "error",
                "topic": topic,
                "error": str(exc),
                "timestamp": datetime.now().isoformat(),
            }

    return sample_recent_messages


def make_generate_health_report_task(health_config: KafkaHealthMonitorConfig):
    """Factory: aggregate check results into a health report."""

    @task(
        task_id="generate_health_report",
        execution_timeout=timedelta(minutes=2),
    )
    def generate_health_report(
        kafka_health: Dict[str, Any],
        lag_info: Dict[str, Any],
        topic_stats: Dict[str, Any],
    ) -> Dict[str, Any]:
        statuses = [
            kafka_health.get("status", "unknown"),
            lag_info.get("status", "unknown"),
            topic_stats.get("status", "unknown"),
        ]

        if "error" in statuses:
            overall_status = "error"
        elif "warning" in statuses:
            overall_status = "warning"
        elif all(s in ("healthy", "success", "skipped") for s in statuses):
            overall_status = "healthy"
        else:
            overall_status = "unknown"

        alerts: List[str] = []
        alerts.extend(lag_info.get("alerts", []))

        if kafka_health.get("status") == "error":
            alerts.append(
                f"Kafka cluster error: {kafka_health.get('error', 'unknown')}"
            )
        if topic_stats.get("status") == "error":
            alerts.append(
                f"Topic stats error: {topic_stats.get('error', 'unknown')}"
            )

        thresholds = {
            "max_lag_records": health_config.max_lag_records,
            "max_lag_minutes": health_config.max_lag_minutes,
            "min_throughput_per_min": health_config.min_throughput_per_min,
            "expected_daily_records": health_config.expected_daily_records,
        }

        report = {
            "overall_status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "alerts": alerts,
            "alert_count": len(alerts),
            "thresholds": thresholds,
            "checks": {
                "kafka_cluster": kafka_health,
                "consumer_lag": lag_info,
                "topic_stats": topic_stats,
            },
            "summary": {
                "broker_count": kafka_health.get("broker_count", 0),
                "total_lag": lag_info.get("total_lag", 0),
                "topic": health_config.kafka_topic or "N/A",
                "consumer_group": health_config.consumer_group or "N/A",
            },
        }

        logger.info(
            "Health Report: %s - %s alerts", overall_status, len(alerts)
        )
        for alert in alerts:
            logger.warning("  ALERT: %s", alert)

        return report

    return generate_health_report


# ============================================================================
# DAG FACTORY
# ============================================================================

def kafka_health_monitor_dag(
    dag_config: DAGConfig,
    conn_config: ConnectionConfig,
    health_config: KafkaHealthMonitorConfig,
):
    """
    Build a Kafka pipeline health monitoring DAG from config.

    Flow:
    1. Validate Kafka cluster health
    2. Check consumer lag (parallel)
    3. Check topic stats (parallel)
    4. Optionally sample recent messages
    5. Generate health report
    """
    kafka_conn_id = conn_config.kafka_conn_id
    if not kafka_conn_id:
        raise ValueError(
            "ConnectionConfig.kafka_conn_id is required for kafka_health_monitor_dag"
        )

    default_args = {
        "owner": dag_config.owner,
        "depends_on_past": dag_config.depends_on_past,
        "email_on_failure": True,
        "email_on_retry": False,
        "email": ["Saffarpour.Zahra@okco.ir"],
        "retries": dag_config.retries,
        "retry_delay": dag_config.retry_delay,
        "execution_timeout": dag_config.execution_timeout,
        "pool": dag_config.pool,
    }

    with DAG(
        dag_id=dag_config.dag_id,
        default_args=default_args,
        description=dag_config.description,
        schedule=dag_config.schedule,
        start_date=dag_config.start_date,
        catchup=dag_config.catchup,
        is_paused_upon_creation=dag_config.is_paused_upon_creation,
        max_active_runs=dag_config.max_active_runs,
        max_active_tasks=dag_config.max_active_tasks,
        tags=dag_config.tags,
        doc_md=__doc__,
    ) as dag:

        with TaskGroup(group_id="health_checks") as health_checks:
            kafka_health = make_validate_kafka_health_task(kafka_conn_id)()
            lag_info = make_check_consumer_lag_task(kafka_conn_id, health_config)()
            topic_stats = make_check_topic_stats_task(kafka_conn_id, health_config)()

            [kafka_health, lag_info, topic_stats]

        report = make_generate_health_report_task(health_config)(
            kafka_health=kafka_health,
            lag_info=lag_info,
            topic_stats=topic_stats,
        )

        if health_config.include_message_sampling:
            samples = make_sample_recent_messages_task(kafka_conn_id, health_config)()
            health_checks >> samples >> report
        else:
            health_checks >> report

    return dag
