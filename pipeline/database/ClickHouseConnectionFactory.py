"""
ClickHouse Connection Factory for managing database connections.
Provides abstraction for connection creation and lifecycle management.
"""
import logging
from typing import Any, Optional
from contextlib import contextmanager
import json

                
from pipeline.core.exceptions import ClickHouseConnectionError
from pipeline.compat.airflow_compat import get_connection 

class ClickHouseConnectionFactory:
    """
    Factory class for creating and managing ClickHouse connections.
    Implements Dependency Inversion Principle by providing abstraction layer.
    """

    def __init__(self, conn_id: str) -> None:
        """
        Initialize ClickHouse connection factory.

        Args:
            conn_id: Airflow connection ID or connection string
            is_connection_string: If True, conn_id is treated as a connection string
        """
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    @contextmanager
    def get_connection(self):
        """
        Context manager for ClickHouse connections.
        Ensures proper connection cleanup.

        Yields:
            ClickHouse Client connection object

        Example:
            with factory.get_connection() as conn:
                conn.execute("SELECT 1")
        """
        from clickhouse_driver import Client # type: ignore
        
        client = None
        try:
            conn = get_connection(self.conn_id)
            extra = json.loads(conn.extra or "{}")

            client = Client(
                host=conn.host or 'localhost',
                port=conn.port or 9000,
                database=extra.get('database', conn.schema or 'default'),
                user=conn.login or 'default',
                password=conn.password or ''
            )
                
            yield client
            
        except ClickHouseConnectionError:
            raise
        except Exception as e:
            self.logger.error(
                "ClickHouse connection failed",
                extra={"error": str(e), "conn_id": self.conn_id},
                exc_info=True
            )
            raise ClickHouseConnectionError(
                f"Failed to connect to ClickHouse: {str(e)}"
            ) from e
        finally:
            if client:
                try:
                    client.disconnect()
                except Exception as close_error:
                    self.logger.warning(
                        "Connection close failed",
                        extra={"error": str(close_error), "conn_id": self.conn_id}
                    )

    def test_connection(self) -> bool:
        """
        Test ClickHouse connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as client:
                client.execute("SELECT 1 AS Test")
            return True
        except Exception:
            return False

    def execute_query(self, query: str, params: Optional[dict] = None) -> Any:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries

        Returns:
            Query results
        """
        try:
            with self.get_connection() as client:
                return client.execute(query, params or {})
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
        
    def close_client(self) -> None:
        """Close client connection."""
        # client = self.get_connection()
        # if client is not None:
        #     client.disconnect()
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_client()
        return False