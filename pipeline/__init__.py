"""
Pipeline Package - SQL Server to Kafka Data Pipeline
====================================================
This package contains all pipeline components following SOLID principles.

Structure:
----------
- interfaces/  : Abstract Base Classes (ABC) for dependency inversion
- config/      : Configuration classes and dataclasses
- database/    : Database access layer (SQL Server)
- kafka/       : Kafka message production and consumption layer
- core/        : Core orchestration and utilities

Author: Senior Data Engineer
Version: 2.1.0
"""
from __future__ import annotations

from typing import Any

__version__ = "2.1.0"
__author__ = "Senior Data Engineer"

# Exceptions are lightweight; keep them eager so ``from pipeline import X``
# for error types does not pay for Kafka/DB drivers at Airflow parse time.
from pipeline.core.exceptions import (
    PipelineException,
    DatabaseException,
    SQLServerConnectionError,
    SQLServerQueryError,
    PostgreSQLConnectionError,
    PostgreSQLQueryError,
    MySQLConnectionError,
    MySQLQueryError,
    MongoDBConnectionError,
    MongoDBQueryError,
    DataReadError,
    KafkaException,
    KafkaConnectionError,
    KafkaProducerError,
    KafkaTopicError,
    KafkaConsumerError,
    ConfigurationException,
    InvalidConfigurationError,
    MissingConfigurationError,
    ValidationException,
    InvalidIdentifierError,
    InvalidServerNameError,
    DataTransferException,
    TransferTimeoutError,
    TransferVerificationError,
)
from pipeline._lazy_import import getattr_lazy

_LAZY_EXPORTS = {
    "DataReader": "pipeline.interfaces.DataReader:DataReader",
    "MessageProducer": "pipeline.interfaces.MessageProducer:MessageProducer",
    "TopicManager": "pipeline.interfaces.TopicManager:TopicManager",
    "MessageSerializerInterface": (
        "pipeline.interfaces.MessageSerializerInterface:MessageSerializerInterface"
    ),
    "TableConfiguration": "pipeline.config.TableConfiguration:TableConfiguration",
    "DAGConfig": "pipeline.config.DAGConfig:DAGConfig",
    "ConnectionConfig": "pipeline.config.ConnectionConfig:ConnectionConfig",
    "KafkaProducerConfig": "pipeline.config.KafkaProducerConfig:KafkaProducerConfig",
    "KafkaTopicConfig": "pipeline.config.KafkaTopicConfig:KafkaTopicConfig",
    "KAFKA_CONFIG": "pipeline.config.KafkaProducerConfig:KAFKA_CONFIG",
    "MSSQLConnectionFactory": (
        "pipeline.database.MSSQLConnectionFactory:MSSQLConnectionFactory"
    ),
    "SQLQueryBuilder": "pipeline.database.SQLQueryBuilder:SQLQueryBuilder",
    "MSSQLDataReader": "pipeline.database.MSSQLDataReader:MSSQLDataReader",
    "IdempotentKafkaProducer": (
        "pipeline.kafka.IdempotentKafkaProducer:IdempotentKafkaProducer"
    ),
    "KafkaTopicManager": "pipeline.kafka.KafkaTopicManager:KafkaTopicManager",
    "MessageSerializer": "pipeline.kafka.MessageSerializer:MessageSerializer",
    "KafkaConnectionFactory": (
        "pipeline.kafka.KafkaConnectionFactory:KafkaConnectionFactory"
    ),
    "KafkaDataConsumer": "pipeline.kafka.KafkaDataConsumer:KafkaDataConsumer",
    "MSSQLDataTransferOrchestrator": (
        "pipeline.core.MSSQLDataTransferOrchestrator:MSSQLDataTransferOrchestrator"
    ),
    "KafkaToMSSQLQueryOrchestrator": (
        "pipeline.core.KafkaToMSSQLQueryOrchestrator:KafkaToMSSQLQueryOrchestrator"
    ),
    "TransferMetrics": "pipeline.core.TransferMetrics:TransferMetrics",
    "TransferResult": "pipeline.core.TransferResult:TransferResult",
    "ExecutionDateExtractor": (
        "pipeline.core.ExecutionDateExtractor:ExecutionDateExtractor"
    ),
    "retry_with_backoff": "pipeline.utils.retry_helper:retry_with_backoff",
    "exponential_backoff_with_jitter": (
        "pipeline.utils.retry_helper:exponential_backoff_with_jitter"
    ),
    "RetryContext": "pipeline.utils.retry_helper:RetryContext",
    "ValidationResult": "pipeline.utils.validation:ValidationResult",
    "IdentifierValidator": "pipeline.utils.IdentifierValidator:IdentifierValidator",
}


def __getattr__(name: str) -> Any:
    return getattr_lazy(globals(), name, _LAZY_EXPORTS)


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(__all__))


__all__ = [
    "DataReader",
    "MessageProducer",
    "TopicManager",
    "MessageSerializerInterface",
    "TableConfiguration",
    "DAGConfig",
    "ConnectionConfig",
    "KafkaTopicConfig",
    "KafkaProducerConfig",
    "KAFKA_CONFIG",
    "MSSQLConnectionFactory",
    "SQLQueryBuilder",
    "MSSQLDataReader",
    "IdempotentKafkaProducer",
    "KafkaTopicManager",
    "MessageSerializer",
    "KafkaConnectionFactory",
    "KafkaDataConsumer",
    "MSSQLDataTransferOrchestrator",
    "KafkaToMSSQLQueryOrchestrator",
    "TransferMetrics",
    "TransferResult",
    "ExecutionDateExtractor",
    "PipelineException",
    "DatabaseException",
    "SQLServerConnectionError",
    "SQLServerQueryError",
    "PostgreSQLConnectionError",
    "PostgreSQLQueryError",
    "MySQLConnectionError",
    "MySQLQueryError",
    "MongoDBConnectionError",
    "MongoDBQueryError",
    "DataReadError",
    "KafkaException",
    "KafkaConnectionError",
    "KafkaProducerError",
    "KafkaTopicError",
    "KafkaConsumerError",
    "ConfigurationException",
    "InvalidConfigurationError",
    "MissingConfigurationError",
    "ValidationException",
    "InvalidIdentifierError",
    "InvalidServerNameError",
    "DataTransferException",
    "TransferTimeoutError",
    "TransferVerificationError",
    "retry_with_backoff",
    "exponential_backoff_with_jitter",
    "RetryContext",
    "ValidationResult",
    "IdentifierValidator",
]
