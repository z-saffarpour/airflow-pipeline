"""
Abstract Base Class for Connection Factory.
Provides a common contract for managing database/broker connections.
"""
from abc import ABC, abstractmethod


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
