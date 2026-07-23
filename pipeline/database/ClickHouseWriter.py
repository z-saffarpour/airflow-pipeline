"""
ClickHouse Writer for INSERT/UPDATE operations
"""
from typing import List, Dict, Any, Optional
import logging

from pipeline.database.ClickHouseConnectionFactory import ClickHouseConnectionFactory
from pipeline.interfaces.DataWriter import DataWriter
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder


class ClickHouseWriter(DataWriter):
    """Handles write operations to ClickHouse.

    Notes:
    - ClickHouse prefers bulk INSERTs. Row-level UPDATE/DELETE are inefficient
      for large volumes; prefer using `execute_command` with proper validation.
    """

    def __init__(self, conn_id: str):
        """
        Initialize ClickHouse writer.

        Args:
            config: ClickHouse write configuration
            conn_id: Connection ID or connection string
        """
        # self.database = database
        # self.table_name = table_name
        self.factory = ClickHouseConnectionFactory(conn_id)
        self.logger = logging.getLogger(self.__class__.__name__)

    def upsert_batch(self, database: str, table_name: str, batch: List[Dict[str, Any]], version_id: int = None) -> int:
        """
        Insert or update batch in ClickHouse.
        Uses ReplacingMergeTree engine for upsert behavior.

        Args:
            database: Database name
            table_name: Table name
            batch: List of row dictionaries to insert
            version_id: Optional version ID to add to all rows if not present

        Returns:
            Number of rows inserted
        """
        if not batch:
            self.logger.warning("Empty batch provided for %s.%s", database, table_name)
            return 0

        # Validate identifiers (database and table) to avoid injection
        SQLQueryBuilder._validate_and_raise(f"{database}.{table_name}", "table name")

        table_full = f"{database}.{table_name}"
        self.logger.info("Starting upsert_batch for %s, batch size: %d, version_id=%s", table_full, len(batch), str(version_id))

        # Add version_id to batch if provided and not already present
        if version_id is not None:
            for row in batch:
                if 'version_id' not in row:
                    row['version_id'] = version_id

        # Convert any existing string version_id in rows to int
        for row in batch:
            if 'version_id' in row and isinstance(row['version_id'], str):
                try:
                    row['version_id'] = int(row['version_id'])
                except ValueError:
                    self.logger.warning("Could not coerce version_id to int for a row; leaving as-is")

        columns = list(batch[0].keys())
        # Validate column names
        for col in columns:
            SQLQueryBuilder._validate_and_raise(col, "column name")

        self.logger.info("Columns for insert: %s", columns)

        with self.factory.get_connection() as client:
            # Prepare data for insertion as list of lists (bulk insert)
            data = [[row.get(col) for col in columns] for row in batch]
            self.logger.info("Prepared %d rows for insertion into %s", len(data), table_full)

            # Build INSERT query; table and columns were validated above
            query = f"INSERT INTO {table_full} ({', '.join(columns)}) VALUES"
            self.logger.debug("Executing query: %s", query)

            try:
                client.execute(query, data)
                self.logger.info("Successfully inserted %d rows into %s", len(batch), table_full)
                return len(batch)
            except Exception as e:
                self.logger.error("Failed to insert into ClickHouse table %s: %s", table_full, str(e))
                self.logger.debug("Query: %s", query)
                self.logger.debug("Columns: %s", columns)
                raise

    def execute_query(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as list of dictionaries.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of dictionaries representing query results
        """
        self.logger.info("Executing query")
        if params:
            self.logger.debug("Query parameters: %s", params)
        with self.factory.get_connection() as client:
            try:
                result = client.execute(query, params or {}, with_column_types=True)
                if not result or not result[0]:
                    self.logger.info("Query returned empty result")
                    return []

                rows, columns_with_types = result
                column_names = [col[0] for col in columns_with_types]

                self.logger.info("Query executed successfully, returned %d rows", len(rows))
                self.logger.debug("Result columns: %s", column_names)

                return [dict(zip(column_names, row)) for row in rows]
            except Exception as e:
                self.logger.error("Failed to execute query: %s", str(e))
                self.logger.debug("Query: %s", query)
                if params:
                    self.logger.debug("Parameters: %s", params)
                raise


    def execute_command(self, command: str, params: dict = None) -> None:
        """
        Execute a non-SELECT command (TRUNCATE, INSERT, DELETE, ALTER, etc.).

        Args:
            command: SQL command to execute
            params: Optional command parameters
        """
        self.logger.info("Executing command")
        if params:
            self.logger.debug("Command parameters: %s", params)

        with self.factory.get_connection() as client:
            try:
                client.execute(command, params or {})
                self.logger.info("Command executed successfully")
            except Exception as e:
                self.logger.error("Failed to execute command: %s", str(e))
                self.logger.debug("Command: %s", command)
                if params:
                    self.logger.debug("Parameters: %s", params)
                raise


    # DataWriter interface compatibility
    def insert_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """Compatibility wrapper for DataWriter.insert_batch -> upsert behavior."""
        return self.upsert_batch(schema, table, data)

    def update_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        raise NotImplementedError("Row-level update is not supported efficiently for ClickHouse. Use execute_command with appropriate ALTER/UPDATE or ReplacingMergeTree strategy.")

    def delete_batch(
        self,
        schema: str,
        table: str,
        key_column: str,
        key_values: List[Any],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        raise NotImplementedError("Row-level delete is not supported efficiently for ClickHouse. Use execute_command with appropriate strategy.")
