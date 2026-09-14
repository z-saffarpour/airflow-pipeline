"""
PostgreSQL Connection Factory for managing database connections.
Provides abstraction for connection creation and lifecycle management.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple
from contextlib import contextmanager

from pipeline.core.exceptions import (
    PostgreSQLConnectionError,
    PostgreSQLQueryError,
)
from pipeline.interfaces.ConnectionFactory import SQLConnectionFactory


class PostgreSQLConnectionFactory(SQLConnectionFactory):
    """
    Factory for creating and managing PostgreSQL connections via Airflow PostgresHook.
    """

    def __init__(self, conn_id: str) -> None:
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_hook(self):
        """Create a fresh PostgresHook instance per call."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

        self.logger.debug("Creating PostgresHook for conn_id=%s", self.conn_id)
        return PostgresHook(postgres_conn_id=self.conn_id)

    @contextmanager
    def get_connection(self):
        """Context manager for PostgreSQL connections with proper cleanup."""
        connection = None
        try:
            self.logger.info(
                "[PostgreSQLConnectionFactory.get_connection] Opening PostgreSQL "
                "connection | conn_id='%s'",
                self.conn_id,
            )
            hook = self.get_hook()
            connection = hook.get_conn()
            yield connection
        except PostgreSQLConnectionError:
            raise
        except Exception as exc:
            self.logger.error(
                "[PostgreSQLConnectionFactory.get_connection] Database connection failed | "
                "conn_id='%s' | error=%s",
                self.conn_id,
                exc,
                exc_info=True,
            )
            raise PostgreSQLConnectionError(
                f"Failed to connect to PostgreSQL: {exc}"
            ) from exc
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    self.logger.error(
                        "[PostgreSQLConnectionFactory.get_connection] Failed to close connection.",
                        exc_info=True,
                    )

    @contextmanager
    def get_cursor(self, as_dict: bool = True):
        """
        Context manager for PostgreSQL cursors.

        Args:
            as_dict: If True, return RealDictCursor rows when the driver supports it.
        """
        with self.get_connection() as connection:
            cursor = None
            try:
                if as_dict:
                    try:
                        from psycopg2.extras import RealDictCursor  # type: ignore

                        cursor = connection.cursor(cursor_factory=RealDictCursor)
                    except Exception:
                        cursor = connection.cursor()
                else:
                    cursor = connection.cursor()
                yield cursor
            except Exception as exc:
                self.logger.error(
                    "[PostgreSQLConnectionFactory.get_cursor] ERROR creating cursor | "
                    "conn_id='%s' | error=%s",
                    self.conn_id,
                    exc,
                    exc_info=True,
                )
                raise PostgreSQLConnectionError(str(exc)) from exc
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception as cursor_error:
                        self.logger.warning(
                            "[PostgreSQLConnectionFactory.get_cursor] Failed to close cursor: %s",
                            cursor_error,
                        )

    def test_connection(self) -> bool:
        """Return True if a simple SELECT 1 succeeds."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1 AS test")
                cursor.fetchone()
            return True
        except Exception:
            return False

    def _normalize_rows(self, cursor, rows: list) -> List[Dict[str, Any]]:
        """Convert raw cursor rows into dictionaries."""
        if not rows:
            return []
        first_row = rows[0]
        if isinstance(first_row, dict):
            return rows
        description = getattr(cursor, "description", None)
        if not description:
            return rows
        columns = [col[0] for col in description]
        return [dict(zip(columns, row)) for row in rows]

    def execute_query(
        self,
        query: str,
        parameters: Optional[Tuple] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return all rows as dictionaries."""
        self.logger.debug(
            "[PostgreSQLConnectionFactory.execute_query] START | params_provided=%s",
            parameters is not None,
        )
        try:
            with self.get_cursor(as_dict=True) as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                rows = cursor.fetchall() or []
                return self._normalize_rows(cursor, list(rows))
        except Exception as exc:
            self.logger.error(
                "[PostgreSQLConnectionFactory.execute_query] Query failed: %s",
                exc,
                exc_info=True,
            )
            raise PostgreSQLQueryError(f"PostgreSQL query failed: {exc}") from exc

    def execute_scalar(
        self,
        query: str,
        parameters: Optional[Tuple] = None,
    ) -> Any:
        """Execute a query and return the first column of the first row."""
        try:
            with self.get_cursor(as_dict=False) as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                row = cursor.fetchone()
                if row is None:
                    return None
                if isinstance(row, dict):
                    return next(iter(row.values()))
                return row[0]
        except Exception as exc:
            self.logger.error(
                "[PostgreSQLConnectionFactory.execute_scalar] Query failed: %s",
                exc,
                exc_info=True,
            )
            raise PostgreSQLQueryError(f"PostgreSQL scalar query failed: {exc}") from exc
