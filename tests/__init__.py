"""Unit tests for transient SQL Server connection error detection."""

import pytest  # type: ignore

from tests.conftest import load_exceptions_module

_exceptions = load_exceptions_module()
SQLServerConnectionError = _exceptions.SQLServerConnectionError
is_transient_sql_server_error = _exceptions.is_transient_sql_server_error
is_transient_sql_server_error_message = _exceptions.is_transient_sql_server_error_message
raise_sync_task_error = _exceptions.raise_sync_task_error


class _PyodbcLikeError(Exception):
    """Stand-in for pyodbc.Error without requiring pyodbc installed."""

    def __init__(self, sqlstate: str, message: str):
        super().__init__(sqlstate, message)
        self.sqlstate = sqlstate


class Error(Exception):
    """Stand-in for pymssql.Error without requiring pymssql installed."""


_PyodbcLikeError.__module__ = "pyodbc"
Error.__module__ = "pymssql"


@pytest.mark.parametrize(
    "message",
    [
        "TCP Provider: Error opening connection",
        "Communication link failure",
        "Login timeout expired",
        "Connection timeout while connecting",
        "broken pipe during write",
        "HYT00 query timeout",
    ],
)
def test_is_transient_sql_server_error_message_detects_common_messages(message):
    assert is_transient_sql_server_error_message(message) is True


@pytest.mark.parametrize(
    "message",
    [
        "Invalid column name 'foo'",
        "Violation of PRIMARY KEY constraint",
        "Permission denied for table dbo.Items",
        "",
    ],
)
def test_is_transient_sql_server_error_message_rejects_permanent_errors(message):
    assert is_transient_sql_server_error_message(message) is False


@pytest.mark.parametrize(
    "sqlstate",
    ["08S01", "HYT00", "HYT01", "08001"],
)
def test_is_transient_sql_server_error_detects_sqlstate_codes(sqlstate):
    exc = _PyodbcLikeError(sqlstate, "driver reported transient failure")
    assert is_transient_sql_server_error(exc) is True


def test_is_transient_sql_server_error_detects_custom_connection_error():
    assert is_transient_sql_server_error(SQLServerConnectionError("host unreachable")) is True


def test_is_transient_sql_server_error_detects_pyodbc_message_pattern():
    exc = _PyodbcLikeError("42000", "General network error during connection reset")
    assert is_transient_sql_server_error(exc) is True


def test_is_transient_sql_server_error_detects_pymssql_timeout_message():
    exc = Error("Adaptive Server connection failed (timeout)")
    assert is_transient_sql_server_error(exc) is True


def test_is_transient_sql_server_error_walks_exception_chain():
    root = SQLServerConnectionError("connection reset")
    wrapped = RuntimeError("sync wrapper")
    wrapped.__cause__ = root
    assert is_transient_sql_server_error(wrapped) is True


def test_is_transient_sql_server_error_rejects_logic_errors():
    exc = ValueError("Invalid store_number format")
    assert is_transient_sql_server_error(exc) is False


def test_raise_sync_task_error_raises_airflow_exception_for_transient_error():
    from airflow.exceptions import AirflowException, AirflowFailException

    with pytest.raises(AirflowException, match="sync failed"):
        raise_sync_task_error(
            "sync failed: connection timeout",
            SQLServerConnectionError("connection timeout"),
        )

    with pytest.raises(AirflowFailException, match="sync failed"):
        raise_sync_task_error(
            "sync failed: invalid column",
            ValueError("invalid column"),
        )
