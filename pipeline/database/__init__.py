"""
Pipeline Database Package
=========================
Contains database-related components for SQL Server access.
"""
from __future__ import annotations

from typing import Any

from pipeline._lazy_import import getattr_lazy

_LAZY_EXPORTS = {
    "MSSQLConnectionFactory": (
        "pipeline.database.MSSQLConnectionFactory:MSSQLConnectionFactory"
    ),
    "SafeMsSqlHook": "pipeline.database.SafeMsSqlHook:SafeMsSqlHook",
    "sanitize_mssql_extra_kwargs": (
        "pipeline.database.SafeMsSqlHook:sanitize_mssql_extra_kwargs"
    ),
    "SQLQueryBuilder": "pipeline.database.SQLQueryBuilder:SQLQueryBuilder",
    "MSSQLDataReader": "pipeline.database.MSSQLDataReader:MSSQLDataReader",
    "MySQLConnectionFactory": (
        "pipeline.database.MySQLConnectionFactory:MySQLConnectionFactory"
    ),
    "MySQLDataReader": "pipeline.database.MySQLDataReader:MySQLDataReader",
    "MySQLServerWriter": "pipeline.database.MySQLServerWriter:MySQLServerWriter",
    "PostgreSQLConnectionFactory": (
        "pipeline.database.PostgreSQLConnectionFactory:PostgreSQLConnectionFactory"
    ),
    "PostgreSQLDataReader": (
        "pipeline.database.PostgreSQLDataReader:PostgreSQLDataReader"
    ),
    "PostgreSQLServerWriter": (
        "pipeline.database.PostgreSQLServerWriter:PostgreSQLServerWriter"
    ),
    "MongoDBConnectionFactory": (
        "pipeline.database.MongoDBConnectionFactory:MongoDBConnectionFactory"
    ),
    "MongoDBDataReader": "pipeline.database.MongoDBDataReader:MongoDBDataReader",
    "MongoDBServerWriter": "pipeline.database.MongoDBServerWriter:MongoDBServerWriter",
    "ClickHouseConnectionFactory": (
        "pipeline.database.ClickHouseConnectionFactory:ClickHouseConnectionFactory"
    ),
    "ClickHouseDataReader": (
        "pipeline.database.ClickHouseDataReader:ClickHouseDataReader"
    ),
    "ClickHouseWriter": "pipeline.database.ClickHouseWriter:ClickHouseWriter",
}


def __getattr__(name: str) -> Any:
    return getattr_lazy(globals(), name, _LAZY_EXPORTS)


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(__all__))


__all__ = [
    "MSSQLConnectionFactory",
    "SafeMsSqlHook",
    "sanitize_mssql_extra_kwargs",
    "SQLQueryBuilder",
    "MSSQLDataReader",
    "MySQLConnectionFactory",
    "MySQLDataReader",
    "MySQLServerWriter",
    "PostgreSQLConnectionFactory",
    "PostgreSQLDataReader",
    "PostgreSQLServerWriter",
    "MongoDBConnectionFactory",
    "MongoDBDataReader",
    "MongoDBServerWriter",
    "ClickHouseConnectionFactory",
    "ClickHouseDataReader",
    "ClickHouseWriter",
]
