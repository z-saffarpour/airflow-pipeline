"""
SQL Query Builder with parameterized queries to prevent SQL injection.
Provides safe and reusable query construction patterns.
"""
from typing import Optional, Tuple, List

from pipeline.utils.IdentifierValidator import IdentifierValidator


class SQLQueryBuilder:
    """
    Builder class for constructing safe SQL queries with parameterization.
    Prevents SQL injection by using parameter binding instead of string formatting.
    Identifier validation is delegated to ``IdentifierValidator``.
    """

    @staticmethod
    def build_keyset_pagination_query(
        table_name: str,
        order_by_column: str,
        batch_size: int,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
        last_key: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> Tuple[str, tuple]:
        """
        Build a keyset pagination query with optional WHERE clause for date filtering.

        Args:
            table_name: Name of the database table
            order_by_column: Column to order and paginate by
            batch_size: Number of records to fetch
            date_column: Column containing the date (optional)
            date_key: Date value in YYYYMMDD format (optional)
            date_column_type: Type of date column - 'int' or 'date'
            last_key: Last value from previous batch for pagination
            columns: List of column names to select (uses * if None)

        Returns:
            Tuple of (query_string, parameters_tuple)

        Raises:
            InvalidIdentifierError: If any identifier is invalid
        """
        IdentifierValidator.validate_and_raise(table_name, "table name")
        IdentifierValidator.validate_and_raise(order_by_column, "order by column")
        if date_column:
            IdentifierValidator.validate_and_raise(date_column, "date column")
        IdentifierValidator.validate_columns(columns)

        column_list = '*' if not columns else ', '.join(columns)

        conditions = []
        parameters = []

        if date_column and date_key:
            conditions.append(f"{date_column} = %s")
            if date_column_type == 'int':
                parameters.append(int(date_key))
            else:
                parameters.append(date_key)

        if last_key is not None:
            conditions.append(f"{order_by_column} > %s")
            parameters.append(last_key)

        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
        else:
            where_clause = ""

        base_query = f"""
            SELECT TOP {batch_size} {column_list}
            FROM {table_name}
            {where_clause}
            ORDER BY {order_by_column}
        """

        return base_query.strip(), tuple(parameters)

    @staticmethod
    def build_count_query(
        table_name: str,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
    ) -> Tuple[str, tuple]:
        """
        Build a count query for a specific date or all records.

        Args:
            table_name: Name of the database table
            date_column: Column containing the date (optional)
            date_key: Date value in YYYYMMDD format (optional)
            date_column_type: Type of date column - 'int' or 'date'

        Returns:
            Tuple of (query_string, parameters_tuple)

        Raises:
            InvalidIdentifierError: If any identifier is invalid
        """
        IdentifierValidator.validate_and_raise(table_name, "table name")
        if date_column:
            IdentifierValidator.validate_and_raise(date_column, "date column")

        if date_column and date_key:
            query = f"""
                SELECT COUNT(1) as total_count
                FROM {table_name}
                WHERE {date_column} = %s
            """
            if date_column_type == 'int':
                return query.strip(), (int(date_key),)
            else:
                return query.strip(), (date_key,)
        else:
            query = f"""
                SELECT COUNT(1) as total_count
                FROM {table_name}
            """
            return query.strip(), ()

    @staticmethod
    def build_min_max_query(
        table_name: str,
        order_by_column: str,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
    ) -> Tuple[str, tuple]:
        """
        Build a query to get min and max values of a column for a specific date or all records.

        Args:
            table_name: Name of the database table
            order_by_column: Column to get min/max from
            date_column: Column containing the date (optional)
            date_key: Date value in YYYYMMDD format (optional)
            date_column_type: Type of date column - 'int' or 'date'

        Returns:
            Tuple of (query_string, parameters_tuple)

        Raises:
            InvalidIdentifierError: If any identifier is invalid
        """
        IdentifierValidator.validate_and_raise(table_name, "table name")
        IdentifierValidator.validate_and_raise(order_by_column, "order by column")
        if date_column:
            IdentifierValidator.validate_and_raise(date_column, "date column")

        if date_column and date_key:
            query = f"""
                SELECT 
                    MIN({order_by_column}) as min_value,
                    MAX({order_by_column}) as max_value
                FROM {table_name}
                WHERE {date_column} = %s
            """
            if date_column_type == 'int':
                return query.strip(), (int(date_key),)
            else:
                return query.strip(), (date_key,)
        else:
            query = f"""
                SELECT 
                    MIN({order_by_column}) as min_value,
                    MAX({order_by_column}) as max_value
                FROM {table_name}
            """
            return query.strip(), ()
