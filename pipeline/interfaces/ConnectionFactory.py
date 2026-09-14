"""
Abstract Base Classes for Connection Factories.
Provides a common contract for managing database/broker connections.
"""
from abc import ABC, abstractmethod
from typing import Any, List, Optional


class ConnectionFactory(ABC):
    """
    Abstract interface for connection factories.

    Every connection factory in this pipeline (SQL Server, MySQL,
    PostgreSQL, MongoDB, ClickHouse, Kafka) is constructed from an Airflow
    ``conn_id`` and exposes a way to verify that the underlying connection
    is reachable. Implement this interface for any new data source/sink so
    it can be validated the same way as the rest (see
    ``pipeline.utils.validation``).
    """

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Verify the connection is reachable and usable.

        Returns:
            True if the connection succeeded, False otherwise.
        """
        pass


class SQLConnectionFactory(ConnectionFactory):
    """
    Abstract interface for SQL-style connection factories.

    Implemented by the factories that share a relational-style contract
    (SQL Server, MySQL, PostgreSQL, ClickHouse): opening a raw connection
    and running ad-hoc queries, in addition to ``test_connection``.
    MongoDB and Kafka implement ``ConnectionFactory`` directly instead,
    since their connection shape (client/database/collection, or
    broker/admin-client) does not fit this contract.
    """

    @abstractmethod
    def get_connection(self) -> Any:
        """
        Get (or open) the underlying database connection.

        Returns:
            A connection object (exact type depends on the driver); used
            as a context manager by callers where supported.
        """
        pass

    @abstractmethod
    def execute_query(
        self,
        query: str,
        parameters: Optional[tuple] = None,
    ) -> List[Any]:
        """
        Execute a query and return all resulting rows.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            List of rows (shape depends on the implementation, typically
            list[dict]).
        """
        pass

    @abstractmethod
    def execute_scalar(
        self,
        query: str,
        parameters: Optional[tuple] = None,
    ) -> Any:
        """
        Execute a query and return a single scalar value.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters

        Returns:
            The first column of the first row, or None.
        """
        pass
