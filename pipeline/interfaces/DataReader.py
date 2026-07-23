"""
Abstract Base Class for Data Reader.
Provides contract for implementing different data sources.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Generator, Optional


class DataReader(ABC):
    """
    Abstract interface for reading data from various sources.
    Implement this interface to add support for different databases.
    """
    
    @abstractmethod
    def get_total_count(
        self,
        table_name: str,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = 'int',
    ) -> int:
        """
        Get total count of records.
        
        Args:
            table_name: Name of the table to read from
            date_column: Column containing the date (optional)
            date_key: Date value for filtering (optional)
            date_column_type: Type of date column
            
        Returns:
            Total count of records
        """
        pass
    
    @abstractmethod
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
        Stream data in batches.
        
        Args:
            table_name: Name of the table to read from
            order_by_column: Column for ordering and pagination
            columns: List of column names to select (None for all columns)
            date_column: Column containing the date (optional)
            date_key: Date value for filtering (optional)
            date_column_type: Type of date column
            
        Yields:
            List of dictionaries (records) - one batch per yield
        """
        pass
 
    @abstractmethod   
    def stream_query(
        self,
        query: str,
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