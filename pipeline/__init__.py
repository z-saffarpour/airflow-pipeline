"""
Pipeline Package - SQL Server to Kafka Data Pipeline
====================================================
This package contains all pipeline components following SOLID principles.

Structure:
----------
- interfaces/  : Abstract Base Classes (ABC) for dependency inversion
- config/      : Configuration classes and dataclasses
- database/    : Database access layer (SQL Server)
- kafka/       : Kafka message production layer
- core/        : Core orchestration and utilities

Author: Senior Data Engineer
Version: 2.1.0
"""

__version__ = "2.1.0"
__author__ = "Senior Data Engineer"

# ============================================================================
# EXCEPTIONS
# ============================================================================
from pipeline.core.exceptions import (
    PipelineException,
    DatabaseException,
    SQLServerConnectionError,
    SQLServerQueryError,
    DataReadError,
    KafkaException,
    KafkaConnectionError,
    KafkaProducerError,
    KafkaTopicError,
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

# ============================================================================
# INTERFACES (ABC)
# ============================================================================
from pipeline.interfaces import (
    DataReader,
    MessageProducer,
    TopicManager,
    MessageSerializerInterface,
)

# ============================================================================
# CONFIGURATION
# ============================================================================
from pipeline.config import (
    TableConfiguration,
    DAGConfig,
    ConnectionConfig,
    KafkaProducerConfig,
    KafkaTopicConfig,
    KAFKA_CONFIG,
)

# ============================================================================
# DATABASE LAYER
# ============================================================================
from pipeline.database import (
    ConnectionFactory,
    SQLQueryBuilder,
    MSSQLDataReader,
)

# ============================================================================
# KAFKA LAYER
# ============================================================================
from pipeline.kafka import (
    IdempotentKafkaProducer,
    KafkaTopicManager,
    MessageSerializer,
    KafkaConnectionFactory,
)

# ============================================================================
# CORE / ORCHESTRATION
# ============================================================================
from pipeline.core import (
    MSSQLDataTransferOrchestrator,
    TransferMetrics,
    TransferResult,
    ExecutionDateExtractor,
)

# ============================================================================
# UTILITIES
# ============================================================================
from pipeline.utils import (
    retry_with_backoff,
    exponential_backoff_with_jitter,
    RetryContext,
)


__all__ = [
    # Interfaces
    "DataReader",
    "MessageProducer",
    "TopicManager",
    "MessageSerializerInterface",
    
    # Configuration
    "TableConfiguration",
    "DAGConfig",
    "ConnectionConfig",
    "KafkaTopicConfig",
    "KafkaProducerConfig",
    "KAFKA_CONFIG",
    
    # Database
    "ConnectionFactory",
    "SQLQueryBuilder",
    "MSSQLDataReader",
    
    # Kafka
    "IdempotentKafkaProducer",
    "KafkaTopicManager",
    "MessageSerializer",
    "KafkaConnectionFactory",
    
    # Core
    "MSSQLDataTransferOrchestrator",
    "TransferMetrics",
    "TransferResult",
    "ExecutionDateExtractor",
    
    # Exceptions
    "PipelineException",
    "DatabaseException",
    "SQLServerConnectionError",
    "SQLServerQueryError",
    "DataReadError",
    "KafkaException",
    "KafkaConnectionError",
    "KafkaProducerError",
    "KafkaTopicError",
    "ConfigurationException",
    "InvalidConfigurationError",
    "MissingConfigurationError",
    "ValidationException",
    "InvalidIdentifierError",
    "InvalidServerNameError",
    "DataTransferException",
    "TransferTimeoutError",
    "TransferVerificationError",
    
    # Utilities
    "retry_with_backoff",
    "exponential_backoff_with_jitter",
    "RetryContext",
]
