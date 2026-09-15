"""
ClickHouse Optimization Orchestrator - High-level orchestration of optimization operations.
Coordinates between different components following SOLID principles.
"""
import logging
from typing import Optional,Dict,Any
from datetime import datetime

from pipeline.database.ClickHouseTableOptimizer import ClickHouseTableOptimizer
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig
from pipeline.core.OptimizationResult import OptimizationResult

class ClickHouseOptimizationOrchestrator:
    """
    Orchestrates ClickHouse table optimization operations.
    Follows Single Responsibility Principle and Dependency Inversion Principle.
    """
    
    def __init__(
        self,
        conn_id: str
    ):
        """
        Initialize orchestrator with connection parameters.
        
        Args:
        """
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def optimize_table(
        self,
        config: ClickHouseOptimizationConfig,
        execution_date: str,
    ) -> OptimizationResult:
        """
        Optimize a ClickHouse table based on configuration.
        
        Args:
            config: Optimization configuration
            execution_date: Execution date in YYYYMMDD format
            
        Returns:
            OptimizationResult with operation statistics
        """
        start_time = datetime.now()
        
        self.logger.info(
            f"[ClickHouseOptimizationOrchestrator.optimize_table] Starting optimization: {config.full_table_name} "
            f"for date {execution_date}"
        )
        
        optimizer: Optional[ClickHouseTableOptimizer] = None
        
        try:
            # Create optimizer instance
            optimizer = ClickHouseTableOptimizer(self.conn_id)
            
            # Get parts count before optimization
            stats_before = optimizer.get_table_stats(config.full_table_name)
            parts_before = stats_before.get('parts_count', 0)
            rows_before = stats_before.get('total_rows', 0)
            
            self.logger.info(
                f"[ClickHouseOptimizationOrchestrator.optimize_table] Before optimization: {parts_before} parts, {rows_before} rows"
            )
            
            # Determine partition to optimize
            partition_value = config.get_partition_value(execution_date)
            
            # Execute optimization
            result = optimizer.optimize_table(
                database=config.database,                
                table_name=config.table_name,
                cluster_name=config.cluster_name,
                partition=partition_value,
                final=config.final,
                deduplicate=config.deduplicate,
            )
            
            if result.get('status') != 'success':
                raise Exception(result.get('error', 'Unknown optimization error'))
            
            # Get parts count after optimization
            stats_after = optimizer.get_table_stats(config.full_table_name)
            parts_after = stats_after.get('parts_count', 0)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                f"[ClickHouseOptimizationOrchestrator.optimize_table] Optimization completed: {parts_before} -> {parts_after} parts "
                f"in {duration:.2f}s"
            )
            
            return OptimizationResult(
                table_name=config.full_table_name,
                execution_date=execution_date,
                status='success',
                duration_seconds=duration,
                partition=partition_value,
                parts_before=parts_before,
                parts_after=parts_after,
                rows_optimized=rows_before,
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(
                f"[ClickHouseOptimizationOrchestrator.optimize_table] Optimization failed for {config.full_table_name}: {str(e)}",
                exc_info=True
            )
            
            return OptimizationResult(
                table_name=config.full_table_name,
                execution_date=execution_date,
                status='failed',
                duration_seconds=duration,
                error_message=str(e),
            )
            
        finally:
            if optimizer:
                optimizer.close_client()
    
    def check_and_optimize_if_needed(
        self,
        config: ClickHouseOptimizationConfig,
        execution_date: str,
        parts_threshold: int = 100,
    ) -> OptimizationResult:
        """
        Check table health and optimize only if needed.
        
        Args:
            config: Optimization configuration
            execution_date: Execution date
            parts_threshold: Minimum parts count to trigger optimization
            
        Returns:
            OptimizationResult
        """
        optimizer: Optional[ClickHouseTableOptimizer] = None
        
        try:
            optimizer = ClickHouseTableOptimizer(self.conn_id)
            
            # Check current state
            health = optimizer.check_table_health(config.full_table_name)
            parts_count = health.get('parts_count', 0)
            
            self.logger.info(
                f"[ClickHouseOptimizationOrchestrator.check_and_optimize_if_needed] Table {config.full_table_name}: {parts_count} parts, "
                f"health={health.get('health_status')}"
            )
            
            # Decide if optimization is needed
            if parts_count < parts_threshold:
                self.logger.info(
                    f"[ClickHouseOptimizationOrchestrator.check_and_optimize_if_needed] Skipping optimization: parts count ({parts_count}) "
                    f"below threshold ({parts_threshold})"
                )
                return OptimizationResult(
                    table_name=config.full_table_name,
                    execution_date=execution_date,
                    status='skipped',
                    parts_before=parts_count,
                    parts_after=parts_count,
                )
            
            # Proceed with optimization
            return self.optimize_table(config, execution_date)
            
        finally:
            if optimizer:
                optimizer.close_client()

    def check_table_health(self, database:str, table_name: str) -> Dict[str, Any] :
        """
        Check table health before optimization.
        """ 
        full_table_name = f"{database}.{table_name}"
        
        try:
            optimizer = ClickHouseTableOptimizer(self.conn_id)
            
            health = optimizer.check_table_health(full_table_name)
            stats = optimizer.get_table_stats(full_table_name)
            
            self.logger.info(f"[ClickHouseOptimizationOrchestrator.check_table_health] Table health before optimization: {health}")
            self.logger.info(f"[ClickHouseOptimizationOrchestrator.check_table_health] Table stats: {stats}")
            
            return {
                'status':'success',
                'table_name': full_table_name,
                'health_status': health.get('health_status'),
                'parts_count': health.get('parts_count', 0),
                'total_rows': stats.get('total_rows', 0),
                'bytes_on_disk': stats.get('bytes_on_disk', 0),
                'issues': health.get('issues', []),
            }
            
        except Exception as e:
            self.logger.error(f"[ClickHouseOptimizationOrchestrator.check_table_health] Health check failed: {str(e)}", exc_info=True)
            return {
                'status':'failed',
                'table_name': full_table_name,
                'error_message': str(e),
            }
        finally:
            if optimizer:
                optimizer.close_client()