"""
SQL Query Builder with parameterized queries to prevent SQL injection.
Provides safe and reusable query construction patterns.
"""
import re
from typing import Optional, Tuple, List

from pipeline.core.exceptions import InvalidIdentifierError


class SQLQueryBuilder:
    """
    Builder class for constructing safe SQL queries with parameterization.
    Prevents SQL injection by using parameter binding instead of string formatting.
    """
    
    # Pattern for valid SQL identifiers (allows schema.table format and brackets)
    _IDENTIFIER_PATTERN = re.compile(r'^[\w]+(?:\.[\w]+)?$|^\[[\w\s]+\](?:\.\[[\w\s]+\])?$')
    
    @staticmethod
    def validate_identifier(name: str) -> bool:
        """
        Validate SQL identifier to prevent injection.
        
        Args:
            name: SQL identifier (table name, column name, etc.)
            
        Returns:
            True if valid, False otherwise
        """
        if not name or not isinstance(name, str):
            return False
        return bool(SQLQueryBuilder._IDENTIFIER_PATTERN.match(name))
    
    @staticmethod
    def _validate_and_raise(identifier: str, identifier_type: str = "identifier") -> None:
        """
        Validate identifier and raise exception if invalid.
        
        Args:
            identifier: SQL identifier to validate
            identifier_type: Type description for error message
            
        Raises:
            InvalidIdentifierError: If identifier is invalid
        """
        if not SQLQueryBuilder.validate_identifier(identifier):
            raise InvalidIdentifierError(
                f"Invalid SQL {identifier_type}: '{identifier}'. "
                f"Only alphanumeric characters, underscores, dots, and brackets are allowed."
            )
    
    @staticmethod
    def _validate_columns(columns: Optional[List[str]]) -> None:
        """
        Validate a list of column names.
        
        Args:
            columns: List of column names to validate
            
        Raises:
            InvalidIdentifierError: If any column name is invalid
        """
        if columns:
            for col in columns:
                SQLQueryBuilder._validate_and_raise(col, "column name")

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
        # Validate identifiers to prevent SQL injection
        SQLQueryBuilder._validate_and_raise(table_name, "table name")
        SQLQueryBuilder._validate_and_raise(order_by_column, "order by column")
        if date_column:
            SQLQueryBuilder._validate_and_raise(date_column, "date column")
        SQLQueryBuilder._validate_columns(columns)
        
        # Build column list
        column_list = '*' if not columns else ', '.join(columns)
        
        # Build WHERE conditions
        conditions = []
        parameters = []
        
        # Add date filter if date_column is provided
        if date_column and date_key:
            conditions.append(f"{date_column} = %s")
            # Convert date_key based on date_column_type
            if date_column_type == 'int':
                parameters.append(int(date_key))
            else:  # 'date' or string type
                parameters.append(date_key)
        
        # Add keyset pagination filter
        if last_key is not None:
            conditions.append(f"{order_by_column} > %s")
            parameters.append(last_key)
        
        # Build query
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
        # Validate identifiers
        SQLQueryBuilder._validate_and_raise(table_name, "table name")
        if date_column:
            SQLQueryBuilder._validate_and_raise(date_column, "date column")
        
        if date_column and date_key:
            query = f"""
                SELECT COUNT(1) as total_count
                FROM {table_name}
                WHERE {date_column} = %s
            """
            # Convert date_key based on date_column_type
            if date_column_type == 'int':
                return query.strip(), (int(date_key),)
            else:  # 'date' or string type
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
        # Validate identifiers
        SQLQueryBuilder._validate_and_raise(table_name, "table name")
        SQLQueryBuilder._validate_and_raise(order_by_column, "order by column")
        if date_column:
            SQLQueryBuilder._validate_and_raise(date_column, "date column")
        
        if date_column and date_key:
            query = f"""
                SELECT 
                    MIN({order_by_column}) as min_value,
                    MAX({order_by_column}) as max_value
                FROM {table_name}
                WHERE {date_column} = %s
            """
            # Convert date_key based on date_column_type
            if date_column_type == 'int':
                return query.strip(), (int(date_key),)
            else:  # 'date' or string type
                return query.strip(), (date_key,)
        else:
            query = f"""
                SELECT 
                    MIN({order_by_column}) as min_value,
                    MAX({order_by_column}) as max_value
                FROM {table_name}
            """
            return query.strip(), ()
