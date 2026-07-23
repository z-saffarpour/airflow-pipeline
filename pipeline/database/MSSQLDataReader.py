
import logging
from typing import Dict, List, Optional, Any, Generator, Tuple

from airflow.exceptions import AirflowException # type: ignore

from pipeline.database.SQLQueryBuilder import SQLQueryBuilder
from pipeline.database.ConnectionFactory import ConnectionFactory
from pipeline.interfaces.DataReader import DataReader
from pipeline.core.exceptions import DataReadError, SQLServerQueryError


# ============================================================================
# SQL SERVER DATA READER
# ============================================================================

class MSSQLDataReader(DataReader):
    """
    Reads data from SQL Server in Streaming mode using Generator.
    Uses chunking to manage memory efficiently.
    Refactored to use SQLQueryBuilder for safe query construction.
    Implements DataReader interface for dependency inversion.
    """
    
    def __init__(
        self,
        conn_id: str,
        batch_size: int = 50000,
        is_connection_string: bool = False,
    ):
        """
        Initialize SQL Server reader.
        
        Args:
            conn_id: Airflow connection ID for SQL Server
            table_name: Name of the table to read from
            order_by_column: Column for ordering and pagination
            batch_size: Number of records per batch
            columns: List of column names to select (None for all columns)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn_id = conn_id
        self.batch_size = batch_size
        self.is_connection_string = is_connection_string
        
        self.connection_factory = ConnectionFactory(conn_id, is_connection_string)
        self.query_builder = SQLQueryBuilder()
    
    def get_total_count(
        self,
        table_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
        custom_query: Optional[str] = None,
        params: Optional[Tuple] = None
    ) -> int:
        """
        Retrieve the total count of records.

        This method supports two modes:
        1. Automatic mode: Builds a COUNT query using the query builder 
           (requires table_name).
        2. Manual mode: Executes a custom raw COUNT query directly 
           (using `custom_query` and optional parameters).

        Args:
            table_name (str, optional): Name of the table to count records from.
            date_column (str, optional): Name of the date column for filtering.
            date_key (str, optional): Date value in YYYYMMDD format.
            date_column_type (str): Type of date column ('int' or 'date').
            custom_query (str, optional): A raw SQL query to execute directly.
            params (tuple, optional): Parameters for the raw SQL query.

        Returns:
            int: Total number of records found.

        Raises:
            ValueError: If neither table_name nor custom_query is provided.
            SQLServerQueryError: For SQL-specific execution issues.
            DataReadError: For any unexpected execution error.
        """
        mode = "custom_query" if custom_query else "query_builder"

        self.logger.info(
            f"[MSSQLDataReader.get_total_count] START | mode={mode} | "
            f"table={table_name} | date_column={date_column} | date_key={date_key} | "
            f"date_column_type={date_column_type} | params_provided={params is not None}"
        )

        try:
            if custom_query:
                query = custom_query
                query_params = params
                self.logger.info("[MSSQLDataReader.get_total_count] Using CUSTOM")

            elif table_name:
                query, query_params  = self.query_builder.build_count_query(
                    table_name=table_name,
                    date_column=date_column,
                    date_key=date_key,
                    date_column_type=date_column_type,
                )
                self.logger.info("[MSSQLDataReader.get_total_count] Using QUERY-BUILDER")

            else:
                msg = "Either 'table_name' or 'custom_query' must be provided."
                self.logger.error(f"[MSSQLDataReader.get_total_count] ERROR | {msg}")
                raise ValueError(msg)

            self.logger.info("[MSSQLDataReader.get_total_count] Executing COUNT query...")
            result = self.connection_factory.execute_scalar(query, query_params)
            count = result if result is not None else 0

            self.logger.info(
                f"[MSSQLDataReader.get_total_count] SUCCESS | mode={mode} | "
                f"table={table_name} | count={count}"
            )

            return count

        except SQLServerQueryError as e:
            self.logger.error(
                f"[MSSQLDataReader.get_total_count] SQLServerQueryError | mode={mode} | "
                f"table={table_name} | error={str(e)}",
                exc_info=True,
            )
            raise

        except Exception as e:
            self.logger.error(
                f"[MSSQLDataReader.get_total_count] Unexpected ERROR | mode={mode} | "
                f"table={table_name} | error={str(e)}",
                exc_info=True,
            )
            raise DataReadError(f"Failed to get total count: {e}")
    
    def stream_data(
        self,
        table_name: str,
        order_by_column: str,
        columns: list = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream data for a specific date using keyset pagination.
        Each batch is read in a separate connection to prevent session from staying in Sleeping state.
        
        Args:
            table_name: Name of the table to read from
            order_by_column: Column for ordering and pagination
            columns: List of column names to select (None for all columns)
            date_column: Column containing the date (optional)
            date_key: Date value in YYYYMMDD format (optional)
            date_column_type: Type of date column - 'int' or 'date'
        
        Yields:
            List of dictionaries (records) - one batch per yield
        """
        self.logger.info(
            f"[MSSQLDataReader.stream_data] START | table={table_name} | "
            f"order_by={order_by_column} | batch_size={self.batch_size} | "
            f"date_column={date_column} | date_key={date_key} | date_column_type={date_column_type} | "
            f"columns={'*' if not columns else columns}"
        )
        
        # Get total count first
        total_count = self.get_total_count(
            table_name=table_name,
            date_column=date_column,
            date_key=date_key,
            date_column_type=date_column_type,
        )
        
        if total_count == 0:
            self.logger.warning(
                f"[MSSQLDataReader.stream_data] NO DATA | table={table_name} | "
                f"date_key={date_key} | date_column={date_column}"
            )
            return
        
        # Keyset pagination
        last_key = None
        processed = 0
        batch_number = 0

        self.logger.info(
            f"[MSSQLDataReader.stream_data] Total rows to stream: {total_count:,}"
        )

        while processed < total_count:
            batch_number += 1
            
            # Build query with parameterization
            query, params = self.query_builder.build_keyset_pagination_query(
                table_name=table_name,
                order_by_column=order_by_column,
                batch_size=self.batch_size,
                date_column=date_column,
                date_key=date_key,
                date_column_type=date_column_type,
                last_key=last_key,
                columns=columns,
            )

            self.logger.info(
                f"[MSSQLDataReader.stream_data] Batch {batch_number} | "
                f"last_key={last_key} | executing query with params={params}"
            )
            self.logger.debug(
                f"[MSSQLDataReader.stream_data] Batch {batch_number} query:\n{query}"
            )

            # Execute query in separate connection
            try:
                batch = self.connection_factory.execute_query(query, params)
                
                if not batch:
                    self.logger.info(
                        f"[MSSQLDataReader.stream_data] Batch {batch_number} returned 0 rows. STOP."
                    )                    
                    break
                
                # Update last key for next iteration
                if batch and order_by_column in batch[-1]:
                    last_key = batch[-1][order_by_column]
                    self.logger.debug(
                        f"[MSSQLDataReader.stream_data] Batch {batch_number} last_key updated => {last_key}"
                    )

                processed += len(batch)
                percentage = (processed / total_count * 100) if total_count > 0 else 0
                
                self.logger.info(
                    f"[MSSQLDataReader.stream_data] Batch {batch_number} SUCCESS | "
                    f"rows={len(batch)} | processed={processed:,}/{total_count:,} ({percentage:.1f}%) | "
                    f"last_key={last_key}"
                )
                
                yield batch
                
                # Safety check
                if processed >= total_count or len(batch) < self.batch_size:
                    self.logger.info(
                        f"[MSSQLDataReader.stream_data] STOP CONDITION MET | processed={processed:,} | "
                        f"batch_rows={len(batch)} | batch_size={self.batch_size}"
                    )                    
                    break
                    
            except Exception as e:
                self.logger.error(
                    f"[MSSQLDataReader.stream_data] ERROR reading batch {batch_number} | "
                    f"table={table_name} | last_key={last_key} | error={str(e)}",
                    exc_info=True,
                )
                raise AirflowException(f"SQL Server read error: {e}") from e
        

        self.logger.info(
            f"[MSSQLDataReader.stream_data] FINISH | streamed={processed:,} | total={total_count:,} | table={table_name}"
        )

    def stream_query(
        self,
        query: str,
        count_query:str = None,
        parameters: Optional[tuple] = None
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream results of an arbitrary SQL query using fetchmany for large datasets.
        This method bypasses automatic query building and pagination, reading data in chunks.

        Args:
            query: The SQL query to execute. Ensure it's optimized for large datasets.
            parameters: Optional tuple of parameters for parameterized queries

        Yields:
            List of dictionaries, where each dictionary represents a row in the batch.
        """
        processed = 0
        batch_number = 0

        self.logger.info(
            f"[MSSQLDataReader.stream_query] START | batch_size={self.batch_size} | "
            f"parameters_provided={parameters is not None} | has_count_query={count_query is not None}"
        )
        # self.logger.info(f"[MSSQLDataReader.stream_query] Query:\n{query}")
        # self.logger.info(f"[MSSQLDataReader.stream_query] Parameters={parameters}")

        # Get total count first
        if count_query is not None:          
            total_count = self.get_total_count(custom_query = count_query, params= parameters)
            self.logger.info(f"[MSSQLDataReader.stream_query] Total rows to stream (from count_query): {total_count:,}")            
        else:
            total_count = None
            self.logger.info("[MSSQLDataReader.stream_query] No count_query provided; progress will be based on processed rows only.")            

        try:
            # Use get_cursor to obtain a cursor that supports fetchmany
            with self.connection_factory.get_cursor(as_dict=True) as cursor:

                # Execute query
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)

                # Skip non-query resultsets � REQUIRED for pyodbc + stored procedures
                try:
                    while True:
                        if cursor.description:
                            break
                        if not cursor.nextset():
                            break
                except Exception:
                    pass

                # Stream using fetchmany
                while True:
                    batch = cursor.fetchmany(self.batch_size)
                    if not batch:
                        break  # No more rows to fetch                   
                                        
                    # Convert to dict if needed
                    if batch and not isinstance(batch[0], dict):
                        description = getattr(cursor, "description", None)
                        columns = [col[0] for col in description] if description else []
                        batch = [dict(zip(columns, row)) for row in batch]
                        
                    processed += len(batch)
                    batch_number += 1
                    
                    if total_count:
                        percentage = (processed / total_count * 100)
                        self.logger.info(
                            f"[MSSQLDataReader.stream_query] Batch {batch_number} | "
                            f"rows={len(batch)} | processed={processed:,}/{total_count:,} ({percentage:.1f}%)"
                        )
                    else:
                        self.logger.info(
                            f"[MSSQLDataReader.stream_query] Batch {batch_number} | "
                            f"rows={len(batch)} | processed={processed:,}"
                        )

                    yield batch

        except Exception as e:
            self.logger.error(
                f"[MSSQLDataReader.stream_query] ERROR | processed={processed:,} | batch_number={batch_number} | error={str(e)}",
                exc_info=True,
            )
            raise AirflowException(f"Streaming query failed: {e}") from e

        final_total = total_count if total_count is not None else processed
        self.logger.info(
            f"[MSSQLDataReader.stream_query] FINISH | total_streamed={final_total:,} | processed={processed:,} | batches={batch_number}"
        )
        
    # Backward compatibility method
    def read_incremental(
        self,
        execution_date: str,
        table_name: str,
        date_column: str,
        order_by_column: str,
        additional_filters: Optional[str] = None
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Legacy method for backward compatibility.
        Delegates to stream_data method.
        
        .. deprecated:: 2.0.0
            Use :meth:`stream_data` instead. This method will be removed in version 3.0.0.
        
        Args:
            execution_date: Execution date in YYYYMMDD format
            table_name: Table name (ignored, uses self.table_name)
            date_column: Date column
            order_by_column: Column for ordering (ignored, uses self.order_by_column)
            additional_filters: Additional filters (not supported yet)
        
        Yields:
            List of dictionaries (records)
        """        
        if additional_filters:
            self.logger.warning(
                "[MSSQLDataReader.read_incremental] additional_filters is not supported in new implementation"
            )

        return self.stream_data(
            table_name = table_name,
            order_by_column = order_by_column,
            date_column=date_column,
            date_key=execution_date,
        )
        
    def _convert_to_dict(self, cursor, rows: List[Any]) -> List[Dict[str, Any]]:
        """
        Helper method to convert fetched rows (tuples) to dictionaries.
        Requires cursor.description to be available.
        """
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return rows

        description = getattr(cursor, "description", None)
        columns = [col[0] for col in description] if description else []
        return [dict(zip(columns, row)) for row in rows]