"""
Execution Date Extractor utility for handling Airflow execution dates.
Provides utilities for date manipulation and formatting.
"""
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Union


class ExecutionDateExtractor:
    """
    Utility class for extracting and formatting execution dates.
    Follows Single Responsibility Principle by handling only date operations.
    """

    @staticmethod
    def extract_date_key(ds_nodash: str) -> str:
        """
        Extract date key from Airflow ds_nodash parameter.

        Args:
            ds_nodash: Date string in YYYYMMDD format

        Returns:
            Date key in YYYYMMDD format (same as input)
        """
        return ds_nodash

    @staticmethod
    def format_date(date_obj: Union[datetime, date]) -> str:
        """
        Format a date object to YYYYMMDD string.

        Args:
            date_obj: datetime or date object

        Returns:
            Date string in YYYYMMDD format
        """
        if isinstance(date_obj, datetime):
            return date_obj.strftime('%Y%m%d')
        elif isinstance(date_obj, date):
            return date_obj.strftime('%Y%m%d')
        else:
            raise ValueError(f"Expected datetime or date object, got {type(date_obj)}")

    @staticmethod
    def parse_date_key(date_key: str) -> date:
        """
        Parse a YYYYMMDD date key to date object.

        Args:
            date_key: Date string in YYYYMMDD format

        Returns:
            date object

        Raises:
            ValueError: If date_key format is invalid
        """
        try:
            return datetime.strptime(date_key, '%Y%m%d').date()
        except ValueError as e:
            raise ValueError(f"Invalid date key format: {date_key}. Expected YYYYMMDD.") from e

    @staticmethod
    def validate_date_key(date_key: str) -> bool:
        """
        Validate if a string is a valid YYYYMMDD date key.

        Args:
            date_key: Date string to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            ExecutionDateExtractor.parse_date_key(date_key)
            return True
        except ValueError:
            return False

    @staticmethod
    def get_date_range_keys(start_date: str, end_date: str) -> list:
        """
        Generate list of date keys between start and end dates.

        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            List of date keys in YYYYMMDD format

        Raises:
            ValueError: If date format is invalid
        """
        start = ExecutionDateExtractor.parse_date_key(start_date)
        end = ExecutionDateExtractor.parse_date_key(end_date)
        
        date_keys = []
        current = start
        while current <= end:
            date_keys.append(ExecutionDateExtractor.format_date(current))
            # Move to next day
            from datetime import timedelta
            current += timedelta(days=1)
        
        return date_keys

    @staticmethod
    def _get_full_context(context: dict) -> dict:
        """
        Merge task kwargs with Airflow's current execution context.

        Airflow 3 Task SDK tasks may receive a partial context via **kwargs.
        """
        if context.get("dag_run") or context.get("logical_date") or context.get("ds"):
            return context

        try:
            from airflow.operators.python import get_current_context  # type: ignore

            current_context = get_current_context()
            if current_context:
                return {**current_context, **context}
        except Exception:
            pass

        return context

    @staticmethod
    def _get_logical_datetime(context: dict) -> Optional[datetime]:
        """
        Resolve logical datetime from Airflow context.

        Airflow 3 manual/API-triggered runs may omit top-level logical_date/ds keys,
        but still expose them on dag_run.
        """
        context = ExecutionDateExtractor._get_full_context(context)
        logical_date = context.get("logical_date") or context.get("execution_date")
        if logical_date:
            return logical_date

        dag_run = context.get("dag_run")
        if dag_run is not None:
            for attr in ("logical_date", "data_interval_start", "run_after"):
                value = getattr(dag_run, attr, None)
                if value:
                    return value

        return None

    @staticmethod
    def _get_data_interval_start(context: dict) -> Optional[datetime]:
        """Resolve data_interval_start from context or dag_run."""
        context = ExecutionDateExtractor._get_full_context(context)
        data_interval_start = context.get("data_interval_start")
        if data_interval_start:
            return data_interval_start

        dag_run = context.get("dag_run")
        if dag_run is not None:
            return getattr(dag_run, "data_interval_start", None)

        return None

    @staticmethod
    def _get_ds_from_context(context: dict) -> Optional[str]:
        """Resolve ds (YYYY-MM-DD) from context or dag_run."""
        context = ExecutionDateExtractor._get_full_context(context)
        if "ds" in context:
            return context["ds"]

        logical_dt = ExecutionDateExtractor._get_logical_datetime(context)
        if logical_dt:
            return logical_dt.strftime("%Y-%m-%d")

        return None

    @staticmethod
    def _get_ds_nodash_from_context(context: dict) -> Optional[str]:
        """Resolve ds_nodash (YYYYMMDD) from context or dag_run."""
        context = ExecutionDateExtractor._get_full_context(context)
        if "ds_nodash" in context:
            return context["ds_nodash"]

        logical_dt = ExecutionDateExtractor._get_logical_datetime(context)
        if logical_dt:
            return logical_dt.strftime("%Y%m%d")

        return None

    @staticmethod
    def resolve_logical_datetime(context: dict) -> Optional[datetime]:
        """
        Return a timezone-aware logical datetime when available in context.

        Used by downstream DAG triggers to propagate the parent run's date.
        """
        logical_dt = ExecutionDateExtractor._get_logical_datetime(context)
        if not logical_dt:
            return None

        if logical_dt.tzinfo is None:
            return logical_dt.replace(tzinfo=timezone.utc)

        return logical_dt
    
    @staticmethod
    def get_date_from_context(context: dict, date_offset: int = None) -> str:
        """
        Extract date from Airflow context with optional offset.
        
        Args:
            context: Airflow task context dictionary
            date_offset: Days to add/subtract from logical_date.
                         0 = same day (today's data)
                         -1 = previous day (yesterday's data)
                         None = use data_interval_start (default behavior for scheduled runs)

        Returns:
            Date in YYYY-MM-DD format
        """

        # If date_offset is explicitly provided, use logical_date + offset
        if date_offset is not None:
            logical_date = ExecutionDateExtractor._get_logical_datetime(context)
            if logical_date:
                target_date = logical_date + timedelta(days=date_offset)
                return target_date.strftime('%Y-%m-%d')
            ds = ExecutionDateExtractor._get_ds_from_context(context)
            if ds:
                base_date = datetime.strptime(ds, '%Y-%m-%d')
                target_date = base_date + timedelta(days=date_offset)
                return target_date.strftime('%Y-%m-%d')
                    
        # Default behavior: use data_interval_start for scheduled DAG runs
        # This gives the START of the data interval (yesterday for daily DAGs)
        data_interval_start = ExecutionDateExtractor._get_data_interval_start(context)
        if data_interval_start:
            return data_interval_start.strftime('%Y-%m-%d')
        
        # Fallback to ds (logical_date in YYYY-MM-DD format)
        ds = ExecutionDateExtractor._get_ds_from_context(context)
        if ds:
            return ds

        # Manual/API-triggered runs without logical_date (e.g. replication sync)
        return datetime.now().strftime('%Y-%m-%d')    

    @staticmethod
    def get_date_key_from_context(context: dict, date_offset: int = None) -> str:
        """
        Extract date key from Airflow context with optional offset.
        
        Args:
            context: Airflow task context dictionary
            date_offset: Days to add/subtract from logical_date.
                         0 = same day (today's data)
                         -1 = previous day (yesterday's data)
                         None = use data_interval_start (default behavior for scheduled runs)

        Returns:
            Date key in YYYYMMDD format
        """
        # If date_offset is explicitly provided, use logical_date + offset
        if date_offset is not None:
            logical_date = ExecutionDateExtractor._get_logical_datetime(context)
            if logical_date:
                target_date = logical_date + timedelta(days=date_offset)
                return target_date.strftime('%Y%m%d')
            ds_nodash = ExecutionDateExtractor._get_ds_nodash_from_context(context)
            if ds_nodash:
                base_date = datetime.strptime(ds_nodash, '%Y%m%d')
                target_date = base_date + timedelta(days=date_offset)
                return target_date.strftime('%Y%m%d')
        
        # Default behavior: use data_interval_start for scheduled DAG runs
        # This gives the START of the data interval (yesterday for daily DAGs)
        data_interval_start = ExecutionDateExtractor._get_data_interval_start(context)
        if data_interval_start:
            return data_interval_start.strftime('%Y%m%d')
        
        # Fallback to ds_nodash (logical_date in YYYYMMDD format)
        ds_nodash = ExecutionDateExtractor._get_ds_nodash_from_context(context)
        if ds_nodash:
            return ds_nodash

        # Manual/API-triggered runs without logical_date (e.g. replication sync)
        return datetime.now().strftime('%Y%m%d')
    
    # @staticmethod
    # def get_date_key_from_context(context: dict) -> str:
    #     """
    #     Extract date key from Airflow context.

    #     Args:
    #         context: Airflow task context dictionary

    #     Returns:
    #         Date key in YYYYMMDD format

    #     Raises:
    #         KeyError: If ds_nodash not in context
    #     """
    #     if 'ds_nodash' not in context:
    #         raise KeyError("ds_nodash not found in Airflow context")
    #     return context['ds_nodash']
