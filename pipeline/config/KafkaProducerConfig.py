"""
Configuration constants for Kafka Producer.
Immutable dataclass with all Kafka-related settings.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaProducerConfig:
    """Immutable configuration constants for Kafka Producer."""
    # Queue settings
    MAX_QUEUE_MESSAGES: int = 500000
    MAX_QUEUE_KB: int = 2097152  # 2GB
    
    # Batch settings
    BATCH_SIZE: int = 65536  # 64KB
    LINGER_MS: int = 5
    
    # Timeout settings
    DEFAULT_FLUSH_TIMEOUT: float = 30.0
    MESSAGE_TIMEOUT_MS: int = 300000
    RETRY_BACKOFF_MS: int = 100
    
    # Polling settings
    POLL_INTERVAL: int = 100  # Poll every N messages
    POLL_TIMEOUT: int = 0
    QUEUE_FULL_POLL_TIMEOUT: int = 1


# Default configuration instance
KAFKA_CONFIG = KafkaProducerConfig()
