"""
ClickHouse Table Optimizer - Implementation of DatabaseOptimizer interface.
Handles table optimization operations for ClickHouse.
"""
import logging
import re
from typing import Dict, List, Optional
from datetime import datetime

from clickhouse_driver import Client  # type: ignore

from pipeline.database.ClickHouseConnectionFactory import ClickHouseConnectionFactory
from pipeline.interfaces.DatabaseOptimizer import DatabaseOptimizer


class ClickHouseTableOptimizer(DatabaseOptimizer):
    """
    ClickHouse-specific implementation of DatabaseOptimizer.
    Handles OPTIMIZE TABLE operations for MergeTree family tables.
    """
    ALLOWED_OPERATIONS = {'OPTIMIZE', 'FINAL', 'DEDUPLICATE'}
    TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        
    def __init__(
        self,
        conn_id: str,
    ):
        """
        Initialize ClickHouse optimizer.
        
        Args:
            host: ClickHouse server host
            port: ClickHouse native port (default 9000)
            user: Username
            password: Password
            database: Default database
            settings: Additional client settings
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._client: Optional[Client] = None
        self.factory = ClickHouseConnectionFactory(conn_id)

    def _validate_identifier(self, name: str, context: str) -> str:
        if not self.TABLE_NAME_PATTERN.match(name):
            raise ValueError(f"Invalid {context}: {name}")
        return name
        
    def optimize_table(
        self,
        database: str,
        table_name: str,
        cluster_name: str = None,
        partition: Optional[str] = None,
        final: bool = False,
        deduplicate: bool = False,
    ) -> Dict:
        """
        Optimize a ClickHouse table using OPTIMIZE TABLE command.
        
        Args:
            table_name: Full table name (database.table)
            partition: Optional partition to optimize (e.g., '20260107')
            final: Whether to perform FINAL merge
            deduplicate: Whether to deduplicate rows
            
        Returns:
            Dictionary with optimization result
        """
        start_time = datetime.now()
        # Validate all identifiers
        database = self._validate_identifier(database, "database name")
        table_name = self._validate_identifier(table_name, "table name")
        if cluster_name:
            cluster_name = self._validate_identifier(cluster_name, "cluster")
        
        # Use parameterized query or whitelist-validated identifiers
        query = f"OPTIMIZE TABLE {database}.{table_name}"
        if cluster_name:
            query += f" ON CLUSTER {cluster_name}"
        
        if partition:
            # Partition values should be parameterized
            query += f" PARTITION {partition}"
            
        if final:
            query += " FINAL"
        
        if deduplicate:
            query += " DEDUPLICATE"
        
        self.logger.info(f"Executing optimization: {query}")
        
        try:
            self.factory.execute_query(query)
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'status': 'success',
                'table_name': table_name,
                'partition': partition,
                'final': final,
                'deduplicate': deduplicate,
                'duration_seconds': duration,
                'query': query,
                'timestamp': datetime.now().isoformat(),
            }
            
            self.logger.info(f"Optimization completed in {duration:.2f}s")
            return result
            
        except Exception as e:
            self.logger.error(f"Optimization failed: {str(e)}", exc_info=True)
            return {
                'status': 'failed',
                'table_name': table_name,
                'error': str(e),
                'query': query,
                'timestamp': datetime.now().isoformat(),
            }
    
    def get_table_stats(self, table_name: str) -> Dict:
        """
        Get table statistics from system.parts.
        
        Args:
            table_name: Full table name (database.table)
            
        Returns:
            Dictionary with table statistics
        """
        # Parse database and table from full name
        if '.' in table_name:
            database, table = table_name.split('.', 1)
        else:
            database = self.database
            table = table_name
        
        query = """
            SELECT 
                sum(rows) as total_rows,
                sum(bytes_on_disk) as bytes_on_disk,
                sum(data_compressed_bytes) as compressed_bytes,
                sum(data_uncompressed_bytes) as uncompressed_bytes,
                count() as parts_count,
                min(modification_time) as oldest_part,
                max(modification_time) as newest_part
            FROM system.parts
            WHERE database = %(database)s 
              AND table = %(table)s
              AND active = 1
        """
        
        try:            
            result = self.factory.execute_query(
                query, 
                {'database': database, 'table': table}
            )
            
            if result:
                row = result[0]
                return {
                    'database': database,
                    'table': table,
                    'total_rows': row[0] or 0,
                    'bytes_on_disk': row[1] or 0,
                    'compressed_bytes': row[2] or 0,
                    'uncompressed_bytes': row[3] or 0,
                    'parts_count': row[4] or 0,
                    'oldest_part': row[5].isoformat() if row[5] else None,
                    'newest_part': row[6].isoformat() if row[6] else None,
                    'compression_ratio': (
                        round(row[3] / row[2], 2) if row[2] and row[3] else None
                    ),
                }
            
            return {'database': database, 'table': table, 'total_rows': 0}
            
        except Exception as e:
            self.logger.error(f"Failed to get table stats: {str(e)}")
            return {'error': str(e)}
    
    def get_parts_info(self, table_name: str) -> List[Dict]:
        """
        Get detailed information about table parts.
        
        Args:
            table_name: Full table name
            
        Returns:
            List of dictionaries with part information
        """
        if '.' in table_name:
            database, table = table_name.split('.', 1)
        else:
            database = self.database
            table = table_name
        
        query = """
            SELECT 
                partition,
                name as part_name,
                rows,
                bytes_on_disk,
                modification_time,
                level,
                primary_key_bytes_in_memory
            FROM system.parts
            WHERE database = %(database)s 
              AND table = %(table)s
              AND active = 1
            ORDER BY partition, name
        """
        
        try:
            results = self.factory.execute_query(
                query,
                {'database': database, 'table': table}
            )
            
            return [
                {
                    'partition': row[0],
                    'part_name': row[1],
                    'rows': row[2],
                    'bytes_on_disk': row[3],
                    'modification_time': row[4].isoformat() if row[4] else None,
                    'level': row[5],
                    'primary_key_bytes': row[6],
                }
                for row in results
            ]
            
        except Exception as e:
            self.logger.error(f"Failed to get parts info: {str(e)}")
            return []
    
    def check_table_health(self, table_name: str) -> Dict:
        """
        Check table health including parts count, merge status, etc.
        
        Args:
            table_name: Full table name
            
        Returns:
            Dictionary with health status
        """
        stats = self.get_table_stats(table_name)
        parts = self.get_parts_info(table_name)
        
        # Health checks
        issues = []
        
        # Check for too many parts (fragmentation)
        parts_count = stats.get('parts_count', 0)
        if parts_count > 300:
            issues.append(f"High fragmentation: {parts_count} parts (threshold: 300)")
        
        # Check for unmerged parts (level 0)
        level_0_parts = sum(1 for p in parts if p.get('level') == 0)
        if level_0_parts > 50:
            issues.append(f"Many unmerged parts: {level_0_parts} level-0 parts")
        
        # Determine health status
        if not issues:
            health_status = 'healthy'
        elif len(issues) == 1:
            health_status = 'warning'
        else:
            health_status = 'critical'
        
        return {
            'table_name': table_name,
            'health_status': health_status,
            'parts_count': parts_count,
            'level_0_parts': level_0_parts,
            'total_rows': stats.get('total_rows', 0),
            'bytes_on_disk': stats.get('bytes_on_disk', 0),
            'issues': issues,
            'timestamp': datetime.now().isoformat(),
        }
    
    def optimize_partition(
        self,
        table_name: str,
        partition_value: str,
        final: bool = True,
        deduplicate: bool = True,
    ) -> Dict:
        """
        Convenience method to optimize a specific partition.
        
        Args:
            table_name: Full table name
            partition_value: Partition value (e.g., '20260107')
            final: Perform FINAL merge
            deduplicate: Remove duplicates
            
        Returns:
            Optimization result
        """
        return self.optimize_table(
            table_name=table_name,
            partition=partition_value,
            final=final,
            deduplicate=deduplicate,
        )
        
    def close_client(self) -> None:
        """Close client connection."""
        self.factory.close_client()