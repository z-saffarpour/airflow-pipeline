"""
SQL Server Writer with comprehensive CRUD operations and staging support.
Designed for Airflow ETL pipelines with batch processing.
"""
import logging
from typing import Dict, List, Optional, Any, Tuple

from pipeline.database.MSSQLConnectionFactory import MSSQLConnectionFactory
from pipeline.interfaces.DataWriter import DataWriter
from pipeline.core.exceptions import is_sql_server_deadlock
from pipeline.utils.retry_helper import RetryContext
from pipeline.utils.IdentifierValidator import IdentifierValidator

_UPSERT_DEADLOCK_MAX_ATTEMPTS = 5
_UPSERT_DEADLOCK_BASE_DELAY = 2.0
_UPSERT_DEADLOCK_MAX_DELAY = 30.0

class MSSQLServerWriter(DataWriter):
    """
    SQL Server writer with batch operations and staging support.
    
    Features:
    - Batch INSERT/UPDATE/DELETE/UPSERT operations
    - Staging table approach for safe large operations
    - Transaction management
    - Context manager support
    """
    
    def __init__(
        self,
        conn_id: str,
        is_connection_string: bool = False,
        autocommit: bool = False
    ):
        self.conn_id = conn_id
        self.is_connection_string = is_connection_string
        self.autocommit = autocommit
        self.connection_factory = MSSQLConnectionFactory(conn_id, is_connection_string)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._cached_unique_keys: Dict[str, Tuple[Tuple[str, ...], ...]] = {}

    @staticmethod
    def _build_null_safe_equality(
        columns: List[str],
        left_alias: str,
        right_alias: str,
    ) -> str:
        """Build NULL-safe equality conditions for JOIN/MERGE ON clauses."""
        conditions = []
        for column in columns:
            left = f"{left_alias}.[{column}]"
            right = f"{right_alias}.[{column}]"
            conditions.append(
                f"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))"
            )
        return " AND ".join(conditions)

    def _discover_unique_keys(
        self,
        schema: str,
        table: str,
    ) -> Tuple[Tuple[str, ...], ...]:
        """Discover non-primary unique indexes from the target table."""
        cache_key = f"{schema}.{table}".lower()
        if cache_key in self._cached_unique_keys:
            return self._cached_unique_keys[cache_key]

        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")

        query = """
        SELECT
            i.name AS index_name,
            c.name AS column_name,
            ic.key_ordinal
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic
            ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
        INNER JOIN sys.columns c
            ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        INNER JOIN sys.tables t
            ON i.object_id = t.object_id
        INNER JOIN sys.schemas s
            ON t.schema_id = s.schema_id
        WHERE s.name = ?
          AND t.name = ?
          AND i.is_unique = 1
          AND i.is_primary_key = 0
        ORDER BY i.name, ic.key_ordinal
        """

        rows = self.connection_factory.execute_query(
            query,
            parameters=(schema, table),
        )

        indexes: Dict[str, List[str]] = {}
        for row in rows:
            index_name = row.get("index_name") or row.get("INDEX_NAME")
            column_name = row.get("column_name") or row.get("COLUMN_NAME")
            if not index_name or not column_name:
                continue
            indexes.setdefault(str(index_name), []).append(str(column_name))

        unique_keys = tuple(tuple(columns) for columns in indexes.values())
        self._cached_unique_keys[cache_key] = unique_keys

        if unique_keys:
            self.logger.info(
                "[MSSQLServerWriter._discover_unique_keys] Discovered %d unique key constraint(s) for %s.%s: %s",
                len(unique_keys),
                schema,
                table,
                unique_keys,
            )

        return unique_keys

    def _resolve_unique_keys(
        self,
        schema: str,
        table: str,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]],
        resolve_unique_key_conflicts: bool,
    ) -> Tuple[Tuple[str, ...], ...]:
        if unique_keys:
            return unique_keys
        if resolve_unique_key_conflicts:
            return self._discover_unique_keys(schema, table)
        return ()

    def _delete_unique_key_conflicts(
        self,
        cursor,
        table_full: str,
        staging_table: str,
        key_columns: List[str],
        unique_keys: Tuple[Tuple[str, ...], ...],
        available_columns: List[str],
    ) -> int:
        """
        Remove target rows that match staging on a unique key but not on the primary key.

        Prevents MERGE insert/update failures when stale rows exist under a different RECID.
        """
        if not unique_keys:
            return 0

        available = {column.upper() for column in available_columns}
        primary_key_match = self._build_null_safe_equality(
            key_columns,
            "target",
            "source",
        )
        primary_key_mismatch = f"NOT ({primary_key_match})"
        total_deleted = 0

        for unique_key in unique_keys:
            filtered_columns = [
                column
                for column in unique_key
                if column.upper() in available
            ]
            if len(filtered_columns) != len(unique_key):
                missing = [
                    column
                    for column in unique_key
                    if column.upper() not in available
                ]
                self.logger.warning(
                    "[MSSQLServerWriter._delete_unique_key_conflicts] Skipping unique key %s for %s; missing staging columns: %s",
                    unique_key,
                    table_full,
                    missing,
                )
                continue

            for column in filtered_columns:
                IdentifierValidator.validate_and_raise(column, "column name")

            unique_key_match = self._build_null_safe_equality(
                filtered_columns,
                "target",
                "source",
            )
            delete_query = f"""
            DELETE target
            FROM {table_full} AS target
            INNER JOIN {staging_table} AS source
                ON {unique_key_match}
            WHERE {primary_key_mismatch}
            """
            cursor.execute(delete_query)
            deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            if deleted:
                total_deleted += deleted
                self.logger.info(
                    "[MSSQLServerWriter._delete_unique_key_conflicts] Deleted %d conflicting row(s) from %s for unique key (%s)",
                    deleted,
                    table_full,
                    ", ".join(filtered_columns),
                )

        return total_deleted

    @staticmethod
    def _build_concat_expression(expressions: List[str]) -> str:
        """Build a CONCAT expression within SQL Server's 2..254 argument limit."""
        if not expressions:
            raise ValueError("At least one expression is required for row hashing")
        if len(expressions) == 1:
            return expressions[0]

        # Nest chunks so very wide tables also stay below CONCAT's 254-argument
        # limit.  The nested form preserves the same concatenated value.
        concat_expression = f"CONCAT({', '.join(expressions[:254])})"
        for offset in range(254, len(expressions), 253):
            chunk = expressions[offset : offset + 253]
            concat_expression = f"CONCAT({concat_expression}, {', '.join(chunk)})"
        return concat_expression

    @staticmethod
    def _build_row_hash_expression(columns: List[str], table_alias: str) -> str:
        hash_columns = [
            f"ISNULL(CAST({table_alias}.[{column}] AS NVARCHAR(MAX)),'NULL')"
            for column in columns
        ]
        concat_expression = MSSQLServerWriter._build_concat_expression(hash_columns)
        return f"HASHBYTES('SHA2_256', {concat_expression})"

    def _build_merge_source_and_match_condition(
        self,
        staging_table: str,
        update_columns: List[str],
        use_hash_change_detection: bool,
    ) -> Tuple[str, str]:
        if use_hash_change_detection and update_columns:
            staging_hash_columns = [
                f"ISNULL(CAST([{column}] AS NVARCHAR(MAX)),'NULL')"
                for column in update_columns
            ]
            staging_concat = self._build_concat_expression(staging_hash_columns)
            source_subquery = f"""
                (
                    SELECT *,
                           HASHBYTES('SHA2_256', {staging_concat}) AS row_hash
                    FROM {staging_table}
                ) AS source
            """
            target_hash = self._build_row_hash_expression(update_columns, "target")
            match_condition = f"source.row_hash <> {target_hash}"
            return source_subquery, match_condition

        change_conditions = " OR ".join(
            [
                f"""
                (
                    target.[{column}] <> source.[{column}]
                    OR (target.[{column}] IS NULL AND source.[{column}] IS NOT NULL)
                    OR (target.[{column}] IS NOT NULL AND source.[{column}] IS NULL)
                )
                """
                for column in update_columns
            ]
        )
        return f"{staging_table} AS source", change_conditions

    def _move_to_result_set(self, cursor) -> bool:
        """
        Move cursor to the first result set that contains columns.

        Compatible with:
        - pyodbc
        - pymssql

        Returns:
            bool: True if a valid result set is found
        """

        while True:
            # current result set contains rows/columns
            if cursor.description is not None:
                return True

            # move to next result set
            has_next = cursor.nextset()

            if not has_next:
                return False

    def _staging_table_name(self, staging_schema: str, table: str, suffix: str) -> str:
        return f"[{staging_schema}].[{table}_staging_{suffix}]"

    def _keys_staging_table_name(
        self, staging_schema: str, table: str, suffix: str
    ) -> str:
        return f"[{staging_schema}].[{table}_sync_keys_{suffix}]"

    def _prepare_staging_table(self, cursor, staging_table: str, table_full: str) -> None:
        self._drop_staging_table(cursor, staging_table)
        cursor.execute(f"SELECT TOP 0 * INTO {staging_table} FROM {table_full}")

    def _load_staging_rows(
        self,
        cursor,
        staging_table: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
    ) -> None:
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join([f"[{col}]" for col in columns])
        insert_query = (
            f"INSERT INTO {staging_table} ({column_list}) VALUES ({placeholders})"
        )
        cursor.fast_executemany = True
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            values = [tuple(row[col] for col in columns) for row in batch]
            cursor.executemany(insert_query, values)

    def _drop_staging_table(self, cursor, staging_table: str) -> None:
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

    def _cleanup_staging_table(self, conn, cursor, staging_table: str) -> None:
        """Drop staging table and commit so cleanup survives connection close."""
        try:
            self._drop_staging_table(cursor, staging_table)
            if not self.autocommit:
                conn.commit()
        except Exception as exc:
            if not self.autocommit:
                conn.rollback()
            self.logger.warning(
                "[MSSQLServerWriter._cleanup_staging_table] Failed to drop staging table %s: %s",
                staging_table,
                exc,
            )

    def _finalize_connection(
        self,
        conn,
        cursor,
        *,
        commit: bool = True,
    ) -> None:
        """Commit or rollback, drain pyodbc result sets, and close the cursor."""
        try:
            if commit:
                if not self.autocommit:
                    conn.commit()
            elif not self.autocommit:
                conn.rollback()
        except Exception as exc:
            self.logger.warning(
                "[MSSQLServerWriter._finalize_connection] Commit/rollback failed: %s",
                exc,
            )
        try:
            while cursor.nextset():
                pass
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass

    def _load_staging_table(
        self,
        staging_table: str,
        table_full: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
    ) -> None:
        """Prepare and load a staging table using a short-lived connection."""
        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                self._prepare_staging_table(cursor, staging_table, table_full)
                self._load_staging_rows(cursor, staging_table, data, columns, batch_size)
                success = True
            finally:
                self._finalize_connection(conn, cursor, commit=success)

    def _execute_and_fetch_one(self, cursor, sql: str) -> tuple:
        cursor.execute(sql)
        if not self._move_to_result_set(cursor):
            raise RuntimeError("No result set returned from query")
        result = cursor.fetchone()
        if result is None:
            raise RuntimeError("Empty result set returned from query")
        return result

    def _execute_with_output_count(self, cursor, sql: str) -> int:
        result = self._execute_and_fetch_one(cursor, sql)
        return result[0] or 0

    # ==================== INSERT OPERATIONS ====================
    
    def insert_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """Insert data in batches via a staging table."""
        if not data:
            self.logger.warning("No data to insert")
            return 0
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")

        effective_staging_schema = staging_schema or schema
        IdentifierValidator.validate_and_raise(
            f"{effective_staging_schema}.{table}",
            "staging table name",
        )

        table_full = f"[{schema}].[{table}]"
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "insert"
        )
        columns = list(data[0].keys())
        column_list = ", ".join([f"[{col}]" for col in columns])
        insert_query = (
            f"INSERT INTO {table_full} ({column_list}) "
            f"SELECT {column_list} FROM {staging_table}"
        )

        self._load_staging_table(
            staging_table, table_full, data, columns, batch_size
        )

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(insert_query)
                total_inserted = (
                    cursor.rowcount
                    if cursor.rowcount and cursor.rowcount > 0
                    else len(data)
                )
                success = True
                self.logger.info(
                    "[MSSQLServerWriter.insert_batch] Successfully inserted %d records into %s | staging: %s",
                    total_inserted,
                    table_full,
                    staging_table,
                )
                return total_inserted
            except Exception as e:
                self.logger.error(f"Insert failed for {table_full}: {e}")
                raise
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """Update records in batches via a staging table."""
        if not data:
            self.logger.warning("No data to update")
            return 0
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")
        for kc in key_columns:
            IdentifierValidator.validate_and_raise(kc, "column name")

        effective_staging_schema = staging_schema or schema
        IdentifierValidator.validate_and_raise(
            f"{effective_staging_schema}.{table}",
            "staging table name",
        )

        table_full = f"[{schema}].[{table}]"
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "update"
        )
        columns = [col for col in data[0].keys() if col not in key_columns]
        set_clause = ", ".join([f"target.[{col}] = source.[{col}]" for col in columns])
        join_clause = " AND ".join(
            [f"target.[{col}] = source.[{col}]" for col in key_columns]
        )
        update_query = f"""
        UPDATE target
        SET {set_clause}
        FROM {table_full} AS target
        INNER JOIN {staging_table} AS source
            ON {join_clause}
        """

        self._load_staging_table(
            staging_table,
            table_full,
            data,
            list(data[0].keys()),
            batch_size,
        )

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(update_query)
                total_updated = (
                    cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                )
                success = True
                self.logger.info(
                    "[MSSQLServerWriter.update_batch] Successfully updated %d records in %s | staging: %s",
                    total_updated,
                    table_full,
                    staging_table,
                )
                return total_updated
            except Exception as e:
                self.logger.error(
                    f"[MSSQLServerWriter.update_batch] Update failed for {table_full}: {e}"
                )
                raise
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_batch(
        self,
        schema: str,
        table: str,
        key_column: str,
        key_values: List[Any],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """Delete records in batches via a staging table of key values."""
        if not key_values:
            self.logger.warning("No keys to delete")
            return 0
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")
        IdentifierValidator.validate_and_raise(key_column, "column name")

        effective_staging_schema = staging_schema or schema
        IdentifierValidator.validate_and_raise(
            f"{effective_staging_schema}.{table}",
            "staging table name",
        )

        table_full = f"[{schema}].[{table}]"
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "delete"
        )
        delete_query = f"""
        DELETE target
        FROM {table_full} AS target
        INNER JOIN {staging_table} AS source
            ON target.[{key_column}] = source.[{key_column}]
        """
        staging_insert_query = (
            f"INSERT INTO {staging_table} ([{key_column}]) VALUES (?)"
        )

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                self._drop_staging_table(cursor, staging_table)
                cursor.execute(
                    f"SELECT TOP 0 [{key_column}] INTO {staging_table} FROM {table_full}"
                )
                cursor.fast_executemany = True
                for i in range(0, len(key_values), batch_size):
                    batch = key_values[i:i + batch_size]
                    cursor.executemany(
                        staging_insert_query,
                        [(value,) for value in batch],
                    )
                success = True
            finally:
                self._finalize_connection(conn, cursor, commit=success)

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(delete_query)
                total_deleted = (
                    cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                )
                success = True
                self.logger.info(
                    "[MSSQLServerWriter.delete_batch] Successfully deleted %d records from %s | staging: %s",
                    total_deleted,
                    table_full,
                    staging_table,
                )
                return total_deleted
            except Exception as e:
                self.logger.error(f"Delete failed for {table_full}: {e}")
                raise
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)

    # ==================== KEYS STAGING (delete_missing phase 2) ====================

    def prepare_keys_staging_table(
        self,
        schema: str,
        table: str,
        key_columns: List[str],
        suffix: str,
        staging_schema: Optional[str] = None,
    ) -> str:
        """Create a persistent staging table holding only primary-key columns."""
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")
        for column in key_columns:
            IdentifierValidator.validate_and_raise(column, "column name")
        IdentifierValidator.validate_and_raise(suffix, "staging suffix")

        effective_staging_schema = staging_schema or schema
        IdentifierValidator.validate_and_raise(
            f"{effective_staging_schema}.{table}",
            "staging table name",
        )

        table_full = f"[{schema}].[{table}]"
        keys_staging_table = self._keys_staging_table_name(
            effective_staging_schema, table, suffix
        )
        key_column_list = ", ".join(f"[{col}]" for col in key_columns)

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            try:
                self._drop_staging_table(cursor, keys_staging_table)
                cursor.execute(
                    f"SELECT TOP 0 {key_column_list} INTO {keys_staging_table} "
                    f"FROM {table_full}"
                )
                if not self.autocommit:
                    conn.commit()
                self.logger.info(
                    "[MSSQLServerWriter.prepare_keys_staging_table] Prepared keys staging table %s for %s",
                    keys_staging_table,
                    table_full,
                )
                return keys_staging_table
            except Exception as exc:
                if not self.autocommit:
                    conn.rollback()
                self.logger.error(
                    "Failed to prepare keys staging table for %s: %s",
                    table_full,
                    exc,
                )
                raise
            finally:
                cursor.close()

    def append_keys_to_staging(
        self,
        keys_staging_table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
    ) -> int:
        """Append primary-key rows from a batch into the persistent keys staging table."""
        if not data:
            return 0

        for column in key_columns:
            IdentifierValidator.validate_and_raise(column, "column name")

        column_list = ", ".join(f"[{col}]" for col in key_columns)
        placeholders = ", ".join(["?" for _ in key_columns])
        insert_query = (
            f"INSERT INTO {keys_staging_table} ({column_list}) "
            f"VALUES ({placeholders})"
        )

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                cursor.fast_executemany = True
                total_appended = 0
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [
                        tuple(row[col] for col in key_columns) for row in batch
                    ]
                    cursor.executemany(insert_query, values)
                    total_appended += len(batch)
                success = True
                self.logger.debug(
                    "Appended %d key row(s) to %s",
                    total_appended,
                    keys_staging_table,
                )
                return total_appended
            except Exception as exc:
                self.logger.error(
                    "[MSSQLServerWriter.append_keys_to_staging] Failed to append keys to %s: %s",
                    keys_staging_table,
                    exc,
                )
                raise
            finally:
                self._finalize_connection(conn, cursor, commit=success)

    def delete_missing_in_scope(
        self,
        schema: str,
        table: str,
        key_columns: List[str],
        keys_staging_table: str,
        scope_column: str,
        min_key: Any,
        max_key: Any,
    ) -> int:
        """
        Delete target rows in [min_key, max_key] whose primary key is absent from keys staging.
        """
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")
        for column in key_columns:
            IdentifierValidator.validate_and_raise(column, "column name")
        IdentifierValidator.validate_and_raise(scope_column, "column name")

        if min_key is None or max_key is None:
            self.logger.warning(
                "[MSSQLServerWriter.delete_missing_in_scope] Skipping scoped delete for %s.%s; min_key or max_key is NULL",
                schema,
                table,
            )
            return 0

        table_full = f"[{schema}].[{table}]"
        key_match = self._build_null_safe_equality(
            key_columns,
            "target",
            "source",
        )
        delete_query = f"""
        DELETE target
        FROM {table_full} AS target
        WHERE target.[{scope_column}] >= ?
          AND target.[{scope_column}] <= ?
          AND NOT EXISTS (
              SELECT 1
              FROM {keys_staging_table} AS source
              WHERE {key_match}
          )
        """

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            try:
                cursor.execute(delete_query, (min_key, max_key))
                deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

                if not self.autocommit:
                    conn.commit()

                self.logger.info(
                    "[MSSQLServerWriter.delete_missing_in_scope] Scoped delete completed for %s | scope_column=%s | "
                    "min_key=%s | max_key=%s | deleted=%d",
                    table_full,
                    scope_column,
                    min_key,
                    max_key,
                    deleted,
                )
                return deleted
            except Exception as exc:
                if not self.autocommit:
                    conn.rollback()
                self.logger.error(
                    "Scoped delete failed for %s: %s",
                    table_full,
                    exc,
                )
                raise
            finally:
                cursor.close()

    def drop_keys_staging_table(self, keys_staging_table: str) -> None:
        """Drop the persistent keys staging table."""
        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            try:
                self._drop_staging_table(cursor, keys_staging_table)
                if not self.autocommit:
                    conn.commit()
                self.logger.debug("[MSSQLServerWriter.drop_keys_staging_table] Dropped keys staging table %s", keys_staging_table)
            except Exception as exc:
                if not self.autocommit:
                    conn.rollback()
                self.logger.error(
                    "Failed to drop keys staging table %s: %s",
                    keys_staging_table,
                    exc,
                )
                raise
            finally:
                cursor.close()

    # ==================== UPSERT OPERATIONS ====================
    
    def upsert_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
        delete_missing: bool = False,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]] = None,
        resolve_unique_key_conflicts: bool = False,
        use_hash_change_detection: bool = True,
        staging_schema: Optional[str] = None,
        staging_suffix: str = "upsert",
    ) -> Dict[str, int]:
        """
        Full UPSERT with MERGE and optional delete_missing support.

        Args:
            delete_missing: If True, delete records in target that are not in source
            use_hash_change_detection: Compare row versions with SHA2_256 hash instead of per-column checks
            staging_schema: Schema for temporary staging tables; defaults to schema (target) when omitted
            staging_suffix: Unique suffix for the staging table name; use per-chunk values for parallel syncs
        """
        if not data:
            self.logger.warning("[MSSQLServerWriter.upsert_batch] No data to upsert")
            return {"inserted": 0, "updated": 0, "deleted": 0}

        # Validate identifiers
        IdentifierValidator.validate_and_raise(f"{schema}.{table}", "table name")
        for kc in key_columns:
            IdentifierValidator.validate_and_raise(kc, "column name")

        effective_staging_schema = staging_schema or schema
        IdentifierValidator.validate_and_raise(
            f"{effective_staging_schema}.{table}",
            "staging table name",
        )
        IdentifierValidator.validate_and_raise(staging_suffix, "staging suffix")

        table_full = f"[{schema}].[{table}]"
        staging_table = self._staging_table_name(
            effective_staging_schema, table, staging_suffix
        )

        retry_ctx = RetryContext(
            max_attempts=_UPSERT_DEADLOCK_MAX_ATTEMPTS,
            base_delay=_UPSERT_DEADLOCK_BASE_DELAY,
            max_delay=_UPSERT_DEADLOCK_MAX_DELAY,
        )
        while True:
            try:
                return self._upsert_batch_once(
                    schema=schema,
                    table=table,
                    data=data,
                    key_columns=key_columns,
                    batch_size=batch_size,
                    delete_missing=delete_missing,
                    unique_keys=unique_keys,
                    resolve_unique_key_conflicts=resolve_unique_key_conflicts,
                    use_hash_change_detection=use_hash_change_detection,
                    table_full=table_full,
                    staging_table=staging_table,
                )
            except Exception as exc:
                if not is_sql_server_deadlock(exc):
                    raise
                attempt = retry_ctx.current_attempt + 1
                if attempt >= retry_ctx.max_attempts:
                    self.logger.error(
                        f"[MSSQLServerWriter.upsert_batch] Deadlock persisted after "
                        f"{retry_ctx.max_attempts} attempts for {table_full}: {exc}"
                    )
                    raise
                self.logger.warning(
                    f"[MSSQLServerWriter.upsert_batch] Deadlock victim on attempt "
                    f"{attempt}/{retry_ctx.max_attempts} for {table_full}, retrying..."
                )
                retry_ctx.record_failure(exc)

    def _upsert_batch_once(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int,
        delete_missing: bool,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]],
        resolve_unique_key_conflicts: bool,
        use_hash_change_detection: bool,
        table_full: str,
        staging_table: str,
    ) -> Dict[str, int]:
        columns = list(data[0].keys())

        resolved_unique_keys = self._resolve_unique_keys(
            schema,
            table,
            unique_keys,
            resolve_unique_key_conflicts,
        )

        match_conditions = " AND ".join(
            [f"target.[{col}] = source.[{col}]" for col in key_columns]
        )
        update_columns = [col for col in columns if col not in key_columns]
        update_set = ", ".join(
            [f"target.[{col}] = source.[{col}]" for col in update_columns]
        )
        merge_source, change_match_condition = self._build_merge_source_and_match_condition(
            staging_table,
            update_columns,
            use_hash_change_detection,
        )
        insert_columns = ", ".join([f"[{col}]" for col in columns])
        insert_values = ", ".join([f"source.[{col}]" for col in columns])

        matched_clause = ""
        if update_columns:
            matched_clause = f"""
                WHEN MATCHED
                AND (
                    {change_match_condition}
                )
                THEN
                    UPDATE SET {update_set}
                    """

        merge_query = f"""
                SET NOCOUNT ON;
                DECLARE @merge_actions TABLE (action NVARCHAR(10));

                MERGE {table_full} AS target
                USING {merge_source}
                ON {match_conditions}
                {matched_clause}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})

                OUTPUT $action INTO @merge_actions;

                SELECT
                    SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END) AS inserted_count,
                    SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END) AS updated_count,
                    SUM(CASE WHEN action = 'DELETE' THEN 1 ELSE 0 END) AS deleted_count
                FROM @merge_actions;
                """

        self._load_staging_table(
            staging_table, table_full, data, columns, batch_size
        )

        with self.connection_factory.get_connection() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            success = False
            try:
                self._delete_unique_key_conflicts(
                    cursor,
                    table_full,
                    staging_table,
                    key_columns,
                    resolved_unique_keys,
                    columns,
                )

                result = self._execute_and_fetch_one(cursor, merge_query)

                inserted_count = result[0] or 0
                updated_count = result[1] or 0
                deleted_count = result[2] or 0
                success = True

                self.logger.info(
                    f"[MSSQLServerWriter.upsert_batch] {table_full} sync completed | "
                    f"staging: {staging_table} | "
                    f"Inserted: {inserted_count} | "
                    f"Updated: {updated_count} | "
                    f"Deleted: {deleted_count} | "
                    f"hash_change_detection={use_hash_change_detection}"
                )

                self.logger.debug(
                    f"[MSSQLServerWriter.upsert_batch] Upsert completed for {table_full} "
                    f"(delete_missing={delete_missing})"
                )

                return {
                    "success": True,
                    "inserted": inserted_count,
                    "updated": updated_count,
                    "deleted": deleted_count,
                    "total": inserted_count + updated_count + deleted_count,
                }

            except Exception as e:
                self.logger.error(
                    f"[MSSQLServerWriter.upsert_batch] Upsert failed for {table_full}: {e}"
                )
                raise
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)
