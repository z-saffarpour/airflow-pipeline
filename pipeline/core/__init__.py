"""
Pipeline Core Package
=====================
Contains core orchestration and utility components.
"""
from __future__ import annotations

from typing import Any

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
    ClickHouseConnectionError,
)
from pipeline._lazy_import import getattr_lazy

_LAZY_EXPORTS = {
    "MSSQLDataTransferOrchestrator": (
        "pipeline.core.MSSQLDataTransferOrchestrator:MSSQLDataTransferOrchestrator"
    ),
    "MSSQLToMSSQLQueryOrchestrator": (
        "pipeline.core.MSSQLToMSSQLQueryOrchestrator:MSSQLToMSSQLQueryOrchestrator"
    ),
    "MySQLToMSSQLQueryOrchestrator": (
        "pipeline.core.MySQLToMSSQLQueryOrchestrator:MySQLToMSSQLQueryOrchestrator"
    ),
    "MSSQLToMySQLQueryOrchestrator": (
        "pipeline.core.MSSQLToMySQLQueryOrchestrator:MSSQLToMySQLQueryOrchestrator"
    ),
    "MSSQLToPostgreSQLQueryOrchestrator": (
        "pipeline.core.MSSQLToPostgreSQLQueryOrchestrator:"
        "MSSQLToPostgreSQLQueryOrchestrator"
    ),
    "PostgreSQLToMSSQLQueryOrchestrator": (
        "pipeline.core.PostgreSQLToMSSQLQueryOrchestrator:"
        "PostgreSQLToMSSQLQueryOrchestrator"
    ),
    "MSSQLToClickHouseQueryOrchestrator": (
        "pipeline.core.MSSQLToClickHouseQueryOrchestrator:"
        "MSSQLToClickHouseQueryOrchestrator"
    ),
    "MSSQLToKafkaQueryOrchestrator": (
        "pipeline.core.MSSQLToKafkaQueryOrchestrator:MSSQLToKafkaQueryOrchestrator"
    ),
    "MSSQLToMongoDBQueryOrchestrator": (
        "pipeline.core.MSSQLToMongoDBQueryOrchestrator:MSSQLToMongoDBQueryOrchestrator"
    ),
    "MongoDBToMSSQLQueryOrchestrator": (
        "pipeline.core.MongoDBToMSSQLQueryOrchestrator:MongoDBToMSSQLQueryOrchestrator"
    ),
    "KafkaToMSSQLQueryOrchestrator": (
        "pipeline.core.KafkaToMSSQLQueryOrchestrator:KafkaToMSSQLQueryOrchestrator"
    ),
    "ClickHouseToMSSQLQueryOrchestrator": (
        "pipeline.core.ClickHouseToMSSQLQueryOrchestrator:"
        "ClickHouseToMSSQLQueryOrchestrator"
    ),
    "TransferMetrics": "pipeline.core.TransferMetrics:TransferMetrics",
    "TransferResult": "pipeline.core.TransferResult:TransferResult",
    "ExecutionDateExtractor": (
        "pipeline.core.ExecutionDateExtractor:ExecutionDateExtractor"
    ),
    "OptimizationResult": "pipeline.core.OptimizationResult:OptimizationResult",
    "ClickHouseOptimizationOrchestrator": (
        "pipeline.core.ClickHouseOptimizationOrchestrator:"
        "ClickHouseOptimizationOrchestrator"
    ),
}


def __getattr__(name: str) -> Any:
    return getattr_lazy(globals(), name, _LAZY_EXPORTS)


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(__all__))


__all__ = [
    "MSSQLDataTransferOrchestrator",
    "MSSQLToMSSQLQueryOrchestrator",
    "MySQLToMSSQLQueryOrchestrator",
    "MSSQLToMySQLQueryOrchestrator",
    "MSSQLToPostgreSQLQueryOrchestrator",
    "PostgreSQLToMSSQLQueryOrchestrator",
    "MSSQLToClickHouseQueryOrchestrator",
    "MSSQLToKafkaQueryOrchestrator",
    "MSSQLToMongoDBQueryOrchestrator",
    "MongoDBToMSSQLQueryOrchestrator",
    "KafkaToMSSQLQueryOrchestrator",
    "ClickHouseToMSSQLQueryOrchestrator",
    "TransferMetrics",
    "TransferResult",
    "ExecutionDateExtractor",
    "OptimizationResult",
    "ClickHouseOptimizationOrchestrator",
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
    "ClickHouseConnectionError",
]
