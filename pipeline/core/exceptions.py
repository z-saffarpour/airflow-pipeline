"""
Custom exceptions for the SQL Server to Kafka Pipeline.
Provides specific exception types for better error handling and debugging.
"""
import re
from typing import Optional, Set

_TRANSIENT_SQLSTATE_CODES = frozenset({"08S01", "HYT00", "HYT01", "08001"})
_TRANSIENT_MESSAGE_PATTERN = re.compile(
    r"connection|timeout|communication link|tcp provider|login timeout|broken pipe",
    re.IGNORECASE,
)


class PipelineException(Exception):
    """Base exception for all pipeline-related errors."""
    pass


class DatabaseException(PipelineException):
    """Base exception for database-related errors."""
    pass


class SQLServerConnectionError(DatabaseException):
    """Raised when SQL Server connection fails."""
    pass


class SQLServerQueryError(DatabaseException):
    """Raised when SQL Server query execution fails."""
    pass


class SQLServerDeadlockError(SQLServerQueryError):
    """Raised when SQL Server reports a deadlock (error 1205 / SQLSTATE 40001)."""
    pass


def is_sql_server_deadlock(exc: BaseException) -> bool:
    """Return True if exc represents a SQL Server deadlock (error 1205 / SQLSTATE 40001)."""
    seen: Set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _exception_indicates_deadlock(current):
            return True
        current = current.__cause__ or current.__context__
    return False


def _exception_indicates_deadlock(exc: BaseException) -> bool:
    if isinstance(exc, SQLServerDeadlockError):
        return True

    args = getattr(exc, "args", ())
    if args:
        if str(args[0]) == "40001":
            return True
        combined_args = " ".join(str(arg) for arg in args)
        if "1205" in combined_args and "deadlock" in combined_args.lower():
            return True

    message = str(exc)
    if "1205" in message and "deadlock" in message.lower():
        return True
    return False


def is_transient_sql_server_error_message(message: str) -> bool:
    """Return True if an error message indicates a transient SQL Server connection error."""
    if not message:
        return False
    if _TRANSIENT_MESSAGE_PATTERN.search(message):
        return True
    return any(code in message for code in _TRANSIENT_SQLSTATE_CODES)


def is_transient_sql_server_error(exc: BaseException) -> bool:
    """Return True if exc represents a transient SQL Server connection error."""
    seen: Set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _exception_indicates_transient_connection(current):
            return True
        current = current.__cause__ or current.__context__
    return False


def _exception_indicates_transient_connection(exc: BaseException) -> bool:
    if isinstance(exc, SQLServerConnectionError):
        return True

    args = getattr(exc, "args", ())
    if args:
        first_arg = str(args[0])
        if first_arg in _TRANSIENT_SQLSTATE_CODES:
            return True
        combined_args = " ".join(str(arg) for arg in args)
        if _TRANSIENT_MESSAGE_PATTERN.search(combined_args):
            return True

    exc_type_name = type(exc).__name__
    if exc_type_name in {"Error", "OperationalError", "InterfaceError"}:
        module_name = getattr(type(exc), "__module__", "")
        if module_name.startswith("pyodbc") or module_name.startswith("pymssql"):
            message = str(exc)
            if _TRANSIENT_MESSAGE_PATTERN.search(message):
                return True
            if args and str(args[0]) in _TRANSIENT_SQLSTATE_CODES:
                return True

    message = str(exc)
    if _TRANSIENT_MESSAGE_PATTERN.search(message):
        return True
    return False


def raise_sync_task_error(message: str, exc: Optional[BaseException] = None) -> None:
    """Raise AirflowException for transient errors, AirflowFailException otherwise."""
    from airflow.exceptions import AirflowException, AirflowFailException  # type: ignore

    is_transient = (
        is_transient_sql_server_error(exc)
        if exc is not None
        else is_transient_sql_server_error_message(message)
    )
    if is_transient:
        raise AirflowException(message) from exc
    raise AirflowFailException(message) from exc


class DataReadError(DatabaseException):
    """Raised when data reading from database fails."""
    pass


class KafkaException(PipelineException):
    """Base exception for Kafka-related errors."""
    pass


class KafkaConnectionError(KafkaException):
    """Raised when Kafka connection fails."""
    pass


class KafkaProducerError(KafkaException):
    """Raised when Kafka producer encounters an error."""
    pass


class KafkaTopicError(KafkaException):
    """Raised when Kafka topic operations fail."""
    pass


class ConfigurationException(PipelineException):
    """Base exception for configuration-related errors."""
    pass


class InvalidConfigurationError(ConfigurationException):
    """Raised when configuration is invalid."""
    pass


class MissingConfigurationError(ConfigurationException):
    """Raised when required configuration is missing."""
    pass


class ValidationException(PipelineException):
    """Base exception for validation errors."""
    pass


class InvalidIdentifierError(ValidationException):
    """Raised when SQL identifier validation fails."""
    pass


class InvalidServerNameError(ValidationException):
    """Raised when server name validation fails."""
    pass


class DataTransferException(PipelineException):
    """Base exception for data transfer errors."""
    pass


class TransferTimeoutError(DataTransferException):
    """Raised when data transfer times out."""
    pass


class TransferVerificationError(DataTransferException):
    """Raised when data transfer verification fails."""
    pass


class ClickHouseConnectionError(Exception):
    """Exception raised for ClickHouse connection errors."""
    pass
