"""
Abstract Base Class for Database Optimizer.
Provides contract for optimizing database tables.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class DatabaseOptimizer(ABC):
    """
    Abstract interface for database optimization operations.
    Supports table optimization, partition management, and health checks.
    """
    
    @abstractmethod
    def optimize_table(
        self,
        table_name: str,
        partition: Optional[str] = None,
        final: bool = False,
        deduplicate: bool = False,
    ) -> Dict:
        """
        Optimize a database table.
        
        Args:
            table_name: Full table name (database.table)
            partition: Optional partition to optimize
            final: Whether to perform final merge (ClickHouse specific)
            deduplicate: Whether to deduplicate rows
            
        Returns:
            Dictionary with optimization result
        """
        pass

    @abstractmethod
    def get_table_stats(self, table_name: str) -> Dict:
        """
        Get table statistics (row count, size, partitions, etc.).
        
        Args:
            table_name: Full table name
            
        Returns:
            Dictionary with table statistics
        """
        pass

    @abstractmethod
    def get_parts_info(self, table_name: str) -> List[Dict]:
        """
        Get information about table parts/partitions.
        
        Args:
            table_name: Full table name
            
        Returns:
            List of dictionaries with part information
        """
        pass

    @abstractmethod
    def check_table_health(self, table_name: str) -> Dict:
        """
        Check table health and integrity.
        
        Args:
            table_name: Full table name
            
        Returns:
            Dictionary with health status
        """
        pass
