"""
Pipeline Kafka Package
======================
Contains Kafka-related components for message production and consumption.
"""
from __future__ import annotations

from typing import Any

from pipeline._lazy_import import getattr_lazy

_LAZY_EXPORTS = {
    "IdempotentKafkaProducer": (
        "pipeline.kafka.IdempotentKafkaProducer:IdempotentKafkaProducer"
    ),
    "KafkaTopicManager": "pipeline.kafka.KafkaTopicManager:KafkaTopicManager",
    "MessageSerializer": "pipeline.kafka.MessageSerializer:MessageSerializer",
    "KafkaConnectionFactory": (
        "pipeline.kafka.KafkaConnectionFactory:KafkaConnectionFactory"
    ),
    "KafkaDataConsumer": "pipeline.kafka.KafkaDataConsumer:KafkaDataConsumer",
}


def __getattr__(name: str) -> Any:
    return getattr_lazy(globals(), name, _LAZY_EXPORTS)


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(__all__))


__all__ = [
    "IdempotentKafkaProducer",
    "KafkaTopicManager",
    "MessageSerializer",
    "KafkaConnectionFactory",
    "KafkaDataConsumer",
]
