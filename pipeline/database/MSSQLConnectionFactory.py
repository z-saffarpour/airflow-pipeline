"""
MSSQL Connection Factory for managing SQL Server database connections.
Provides abstraction for connection creation and lifecycle management.
"""
import logging
from typing import Any, List, Dict
from contextlib import contextmanager
import pymssql # type: ignore
import pyodbc # type: ignore
from urllib.parse import urlparse, unquote, parse_qs

from pipeline.database.SafeMsSqlHook import SafeMsSqlHook
from pipeline.core.exceptions import (
    SQLServerConnectionError,
    SQLServerQueryError,
    SQLServerDeadlockError,
    is_sql_server_deadlock,
)
from pipeline.interfaces.SQLConnectionFactory import SQLConnectionFactory

class MSSQLConnectionFactory(SQLConnectionFactory):
    """
    Factory class for creating and managing SQL Server database connections.
    Hook instances are lightweight — no caching needed.
    Implements Dependency Inversion Principle by providing abstraction layer.
    """

    def __init__(self, conn_id: str, is_connection_string: bool = False) -> None:
        """
        Initialize connection factory.

        Args:
            conn_id: Airflow connection ID for the database
            is_connection_string: If True, conn_id is treated as a connection string
        """
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.is_connection_string = is_connection_string
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_hook(self) -> SafeMsSqlHook:
        """
        Create a fresh SafeMsSqlHook instance per call.
        
        Hook objects are lightweight config wrappers.
        Caching them risks stale state in multi-threaded Airflow workers.
        """
        self.logger.debug(f"[MSSQLConnectionFactory.get_hook] Creating SafeMsSqlHook for conn_id={self.conn_id}")
        return SafeMsSqlHook(mssql_conn_id=self.conn_id)

    def create_connection(self, server, port, database, username, password, appname, timeout, login_timeout, query_timeout, driver_type, driver) -> Any:
        """
        driver_type: 'pymssql' or 'pyodbc'
        """
        try:        
            if driver_type == 'pyodbc':
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={server},{port};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    f"APP={appname};"
                    f"LoginTimeout={login_timeout};"
                    f"Encrypt=no;"
                    f"TrustServerCertificate=no;"
                )
                
                conn = pyodbc.connect(connection_string, timeout=timeout or 0)
                conn.timeout = query_timeout
                self.logger.info(
                    "[MSSQLConnectionFactory.create_connection] Creating PYODBC connection "
                    # f"| encrypt={encrypt} | trust={trust_cert}"
                )            
                return conn
            elif driver_type == "pymssql":
                self.logger.info(
                    "[MSSQLConnectionFactory.create_connection] Creating PYMSSQL connection"
                )
                return pymssql.connect(
                    server=server,
                    port = port,
                    database=database,
                    user=username,
                    password=password,
                    appname=appname or 'airflow',
                    timeout=timeout or 0,
                    login_timeout=login_timeout or 60,
                    charset="UTF-8",                
                )
            else:
                raise SQLServerConnectionError(f"Unsupported driver: {self.driver_name}")

        except Exception as e:
            self.logger.error(
                f"[MSSQLConnectionFactory.create_connection] ERROR creating connection | error={str(e)}",
                exc_info=True,
            )
            raise SQLServerConnectionError(str(e))
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures proper connection cleanup.

        Yields:
            Database connection object

        Example:
            with factory.get_connection() as conn:
                cursor = conn.cursor()
                # ... use cursor
        """
        connection = None
        
        try:
            self.logger.info(f"[MSSQLConnectionFactory.get_connection] Opening SQL Server connection | conn_id='{self.conn_id}'")
            if self.is_connection_string:
                # Parse connection string: mssql+pymssql://user:pass@host:port/db?appname=MyApp&timeout=600&login_timeout=30&query_timeout=300
                # Parse connection string: mssql+pyodbc://user:pass@host:port/db?driver=ODBC+Driver+18+for+SQL+Server&appname=MyApp&timeout=600&login_timeout=30&query_timeout=300
                
                parsed = urlparse(self.conn_id)
                query_params = parse_qs(parsed.query)
                
                # Parse host and port
                host_parts = parsed.hostname.split(',') if parsed.hostname else ['localhost']
                server = host_parts[0]
                port = int(host_parts[1]) if len(host_parts) > 1 else (parsed.port or 1433)
                
                database=parsed.path.lstrip('/') if parsed.path else ''
                username=unquote(parsed.username) if parsed.username else ''
                password=unquote(parsed.password) if parsed.password else ''
                
                # Extract parameters from URI with defaults
                appname=query_params.get('application_name', ['Airflow-DataPipeline'])[0]
                timeout=int(query_params.get('timeout', ['600'])[0])
                login_timeout=int(query_params.get('login_timeout', ['30'])[0])
                query_timeout = int(query_params.get('query_timeout', ['300'])[0])
                
                # Determine driver type from URI scheme
                driver_type = 'pyodbc' if 'pyodbc' in parsed.scheme else 'pymssql'
                
                if driver_type == 'pyodbc':
                    driver = query_params.get('driver', ['ODBC Driver 18 for SQL Server'])[0]
                else:
                    driver = None
                    
                connection = self.create_connection(server, port, database, username, password, appname, timeout, login_timeout, query_timeout, driver_type, driver)
            else:
                # Original hook-based approach
                hook = self.get_hook()
                connection = hook.get_conn()
            
            self.logger.debug( f"[MSSQLConnectionFactory.get_connection] SQL Server connection established successfully | conn_id='{self.conn_id}'")    
            yield connection
            
        except SQLServerConnectionError:
            raise
        except SQLServerDeadlockError:
            raise
        except Exception as e:
            if connection:
                try:
                    connection.rollback()
                except Exception as rollback_error:
                    self.logger.warning(
                        f"[MSSQLConnectionFactory.get_connection] Rollback failed | "
                        f"conn_id={self.conn_id} | error={str(rollback_error)}"
                    )
            if is_sql_server_deadlock(e):
                self.logger.warning(
                    f"[MSSQLConnectionFactory.get_connection] SQL Server deadlock detected | conn_id='{self.conn_id}' | error={e}"
                )
                raise SQLServerDeadlockError(
                    f"[MSSQLConnectionFactory.get_connection] SQL Server deadlock detected: {str(e)}"
                ) from e
            self.logger.error(
                f"[MSSQLConnectionFactory.get_connection] Database connection failed | conn_id='{self.conn_id}' | error={e}",
                exc_info=True
            )
            raise SQLServerConnectionError(
                f"[MSSQLConnectionFactory.get_connection] Failed to connect to SQL Server: {str(e)}"
            ) from e
        finally:
            if connection:
                try:
                    connection.close()
                    self.logger.debug("[MSSQLConnectionFactory.get_connection] Connection closed.")
                except Exception:
                    self.logger.error(
                        "[MSSQLConnectionFactory.get_connection] Failed to close connection.",
                        exc_info=True,
                    )
    
    @contextmanager
    def get_cursor(self, as_dict: bool = True):
        """
        Context manager for database cursors.
        Automatically handles connection and cursor lifecycle.

        Args:
            as_dict: If True, returns rows as dictionaries

        Yields:
            Database cursor object

        Example:
            with factory.get_cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                rows = cursor.fetchall()
        """
        with self.get_connection() as connection:
            cursor = None
            try:
                if as_dict:
                    try:
                        cursor = connection.cursor(as_dict=True)
                    except TypeError:
                        cursor = connection.cursor()
                else:
                    cursor = connection.cursor()

                self.logger.debug(f"[MSSQLConnectionFactory.get_cursor] SQL Server cursor created successfully | "
                                 f"conn_id='{self.conn_id}'")
                yield cursor
            except Exception as e:
                self.logger.error(f"[MSSQLConnectionFactory.get_cursor] ERROR creating cursor | "
                                  f"conn_id='{self.conn_id}' | "
                                  f"error={e}",
                                  exc_info=True,)
                raise SQLServerConnectionError(str(e))
            finally:
                if cursor:
                    try:
                        cursor.close()
                        self.logger.debug("[MSSQLConnectionFactory.get_cursor] Cursor closed.")                        
                    except Exception as cursor_error:
                        self.logger.warning(f"[MSSQLConnectionFactory.get_cursor] Failed to close cursor.: {cursor_error}")
                self.logger.debug("[MSSQLConnectionFactory.get_cursor] SQL Server connection closed | "
                                 f"conn_id='{self.conn_id}'")

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1 AS Test")
                cursor.fetchone()
            return True
        except Exception:
            return False

    def _normalize_rows(self, cursor, rows: list) -> List[Dict[str, Any]]:
        """Convert raw cursor rows into dictionaries."""
        if not rows:
            return []
        
        # pymssql sometimes returns dicts already
        first_row = rows[0]
        if isinstance(first_row, dict):
            return rows

        # If description is missing, return raw result
        description = getattr(cursor, "description", None)
        if not description:
            return rows

        columns = [col[0] for col in description]
        return [dict(zip(columns, row)) for row in rows]

    def execute_query(self, query: str, parameters: tuple = None) -> list:
        """
        Execute a SQL query or stored procedure and return all results.
        Works for pymssql, pyodbc, and Airflow-managed connections.

        Automatically handles:
        - pyodbc multi-result sets ("No results. Previous SQL was not a query.")
        - stored procedures that emit multiple initial result sets
        - optional parameters
        - column name extraction for tuple-based drivers (pyodbc)
        """
        self.logger.debug(
            f"[MSSQLConnectionFactory.execute_query] START | params_provided={parameters is not None}"
        )
        self.logger.debug(f"[MSSQLConnectionFactory.execute_query] Query:\n{query}")
        self.logger.debug(f"[MSSQLConnectionFactory.execute_query] Params={parameters}")        
        try:
            # Use get_connection to detect underlying driver and adapt paramstyle
            with self.get_connection() as connection:
                cursor = connection.cursor()

                # Detect driver by connection module
                conn_module = type(connection).__module__ if connection is not None else ""
                is_pyodbc = "pyodbc" in conn_module

                # If pyodbc and query uses %s placeholders, convert to ? placeholders
                exec_query = query
                exec_params = parameters
                if is_pyodbc and parameters is not None:
                    # Convert Python DB-API paramstyle '%s' to pyodbc '?' markers
                    if "%s" in query:
                        exec_query = query.replace("%s", "?")
                # --- Part 1: Execute query ---
                if exec_params:
                    cursor.execute(exec_query, exec_params)
                else:
                    cursor.execute(exec_query)
                # -------------------------------------------------------------
                # Part 2: Skip result sets that are NOT actual SELECT queries
                # (pyodbc_behavior) - ensures stored procedures work correctly
                # -------------------------------------------------------------
                try:
                    # Skip empty or non-query result sets (pymssql will simply ignore this)
                    while True:
                        desc = cursor.description
                        if desc:  
                            # Found a real result-set
                            break
                        if not cursor.nextset():
                            break
                except Exception:
                    # Fallback: If driver doesn't support nextset(), ignore
                    pass
                # --- Part 3: Fetch rows ---
                rows = cursor.fetchall()

                normalized_rows = self._normalize_rows(cursor, rows)

                return normalized_rows
        except Exception as e:
                self.logger.error(
                    f"[MSSQLConnectionFactory.execute_query] ERROR executing query | error={e}",
                    exc_info=True,
                )
                raise SQLServerQueryError(str(e))

    def execute_scalar(self, query: str, parameters: tuple = None):
        """
        Execute a query or stored procedure and return the first scalar value.

        Fully compatible with:
            - pyodbc (handles multi-result sets)
            - pymssql
            - Airflow-managed connections

        Returns:
            Single scalar value, or None if no rows
        """
        self.logger.debug(
            f"[MSSQLConnectionFactory.execute_scalar] START | params_provided={parameters is not None}"
        )
        self.logger.debug(f"[MSSQLConnectionFactory.execute_scalar] Query:\n{query}")

        try:
            # Use get_connection to detect driver and adapt paramstyle
            with self.get_connection() as connection:
                cursor = connection.cursor()

                conn_module = type(connection).__module__ if connection is not None else ""
                is_pyodbc = "pyodbc" in conn_module

                exec_query = query
                exec_params = parameters
                if is_pyodbc and parameters is not None:
                    if "%s" in query:
                        exec_query = query.replace("%s", "?")

                # --- Execute the query ---
                if exec_params:
                    cursor.execute(exec_query, exec_params)
                else:
                    cursor.execute(exec_query)
                # -------------------------------------------------------------
                # Skip non-query result sets (for pyodbc + stored procedures)
                # -------------------------------------------------------------
                try:
                    while True:
                        desc = cursor.description
                        if desc:
                            break
                        if not cursor.nextset():
                            break
                except Exception:
                    pass  # pymssql: safe to ignore

                # --- Fetch first row ---                       
                result = cursor.fetchone()
                if not result:
                    return None

                return result[0]
        except Exception as e:
            self.logger.error(
                f"[MSSQLConnectionFactory.execute_scalar] ERROR | error={e}",
                exc_info=True,
            )
            raise SQLServerQueryError(str(e))
