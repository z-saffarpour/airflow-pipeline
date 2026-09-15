"""
ClickHouse Writer for INSERT/UPDATE operations
"""
from typing import List, Dict, Any, Optional
import logging

from pipeline.database.ClickHouseConnectionFactory import ClickHouseConnectionFactory
from pipeline.interfaces.DataWriter import DataWriter
from pipeline.utils.IdentifierValidator import IdentifierValidator


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
            self.logger.warning('[ClickHouseWriter.upsert_batch] Empty batch provided for %s.%s', database, table_name)
            return 0

        # Validate identifiers (database and table) to avoid injection
        IdentifierValidator.validate_and_raise(f"{database}.{table_name}", "table name")

        table_full = f"{database}.{table_name}"
        self.logger.info(
            '[ClickHouseWriter.upsert_batch] Starting upsert_batch for %s, batch size: %s, version_id=%s',
            table_full,
            len(batch),
            str(version_id),
        )

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
                    self.logger.warning("[ClickHouseWriter.upsert_batch] Could not coerce version_id to int for a row; leaving as-is")

        columns = list(batch[0].keys())
        # Validate column names
        for col in columns:
            IdentifierValidator.validate_and_raise(col, "column name")

        self.logger.info('[ClickHouseWriter.upsert_batch] Columns for insert: %s', columns)

        with self.factory.get_connection() as client:
            # Prepare data for insertion as list of lists (bulk insert)
            data = [[row.get(col) for col in columns] for row in batch]
            self.logger.info(
                '[ClickHouseWriter.upsert_batch] Prepared %s rows for insertion into %s',
                len(data),
                table_full,
            )

            # Build INSERT query; table and columns were validated above
            query = f"INSERT INTO {table_full} ({', '.join(columns)}) VALUES"
            self.logger.debug('[ClickHouseWriter.upsert_batch] Executing query: %s', query)

            try:
                client.execute(query, data)
                self.logger.info(
                    '[ClickHouseWriter.upsert_batch] Successfully inserted %s rows into %s',
                    len(batch),
                    table_full,
                )
                return len(batch)
            except Exception as e:
                self.logger.error(
                    '[ClickHouseWriter.upsert_batch] Failed to insert into ClickHouse table %s: %s',
                    table_full,
                    str(e),
                )
                self.logger.debug('[ClickHouseWriter.upsert_batch] Query: %s', query)
                self.logger.debug('[ClickHouseWriter.upsert_batch] Columns: %s', columns)
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
        self.logger.info("[ClickHouseWriter.execute_query] Executing query")
        if params:
            self.logger.debug('[ClickHouseWriter.execute_query] Query parameters: %s', params)
        with self.factory.get_connection() as client:
            try:
                result = client.execute(query, params or {}, with_column_types=True)
                if not result or not result[0]:
                    self.logger.info("[ClickHouseWriter.execute_query] Query returned empty result")
                    return []

                rows, columns_with_types = result
                column_names = [col[0] for col in columns_with_types]

                self.logger.info(
                    '[ClickHouseWriter.execute_query] Query executed successfully, returned %s rows',
                    len(rows),
                )
                self.logger.debug('[ClickHouseWriter.execute_query] Result columns: %s', column_names)

                return [dict(zip(column_names, row)) for row in rows]
            except Exception as e:
                self.logger.error('[ClickHouseWriter.execute_query] Failed to execute query: %s', str(e))
                self.logger.debug('[ClickHouseWriter.execute_query] Query: %s', query)
                if params:
                    self.logger.debug('[ClickHouseWriter.execute_query] Parameters: %s', params)
                raise


    def execute_command(self, command: str, params: dict = None) -> None:
        """
        Execute a non-SELECT command (TRUNCATE, INSERT, DELETE, ALTER, etc.).

        Args:
            command: SQL command to execute
            params: Optional command parameters
        """
        self.logger.info("[ClickHouseWriter.execute_command] Executing command")
        if params:
            self.logger.debug('[ClickHouseWriter.execute_command] Command parameters: %s', params)

        with self.factory.get_connection() as client:
            try:
                client.execute(command, params or {})
                self.logger.info("[ClickHouseWriter.execute_command] Command executed successfully")
            except Exception as e:
                self.logger.error('[ClickHouseWriter.execute_command] Failed to execute command: %s', str(e))
                self.logger.debug('[ClickHouseWriter.execute_command] Command: %s', command)
                if params:
                    self.logger.debug('[ClickHouseWriter.execute_command] Parameters: %s', params)
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
