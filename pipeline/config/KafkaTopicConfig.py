from dataclasses import dataclass

# ============================================================
# Kafka Topic Configuration
# ============================================================

@dataclass(frozen=True)
class KafkaTopicConfig:
    """
    Immutable configuration for a single Kafka topic.

    Attributes:
        name               : Kafka topic name
        num_partitions     : Number of partitions for parallel consumption
        replication_factor : Number of replicas for fault tolerance
    """
    name:               str
    num_partitions:     int
    replication_factor: int
    
    def __post_init__(self):
        if not self.name:
            raise ValueError(
                f"[{self.__class__.__name__}] 'name' is required and cannot be None or empty."
            )
        if self.num_partitions <= 0:
            raise ValueError("'num_partitions' must be > 0")
        if self.replication_factor <= 0:
            raise ValueError("'replication_factor' must be > 0")