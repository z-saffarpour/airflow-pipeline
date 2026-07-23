"""
Configuration dataclass for Kafka pipeline health monitoring.
Immutable configuration holder.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaHealthMonitorConfig:
    """Configuration for Kafka pipeline health monitoring. Immutable."""

    kafka_conn_id: str = "kafka_default"
    kafka_topic: str = ""
    consumer_group: str = ""
    sample_count: int = 5
    max_lag_records: int = 100000
    max_lag_minutes: int = 30
    min_throughput_per_min: int = 1000
    expected_daily_records: int = 5000000
    include_message_sampling: bool = True

    def __post_init__(self):
        if self.sample_count < 1:
            raise ValueError("'sample_count' must be >= 1")
        if self.max_lag_records < 0:
            raise ValueError("'max_lag_records' must be >= 0")
        if self.max_lag_minutes < 0:
            raise ValueError("'max_lag_minutes' must be >= 0")
        if self.expected_daily_records < 0:
            raise ValueError("'expected_daily_records' must be >= 0")
