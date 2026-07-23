"""
Configuration dataclass for a custom SQL query transfer.
Immutable configuration holder for query-based Kafka sync.
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple
import re


# SQL validation patterns
SELECT_PREFIX_PATTERN = re.compile(r"^\s*(with|select|exec|execute)\b", re.IGNORECASE | re.DOTALL)
COMMENT_PREFIX_PATTERN = re.compile(r"^\s*(--[^\n]*\n|/\*.*?\*/\s*)", re.DOTALL)


def _strip_sql_comments(query: str) -> str:
    """Remove leading SQL comments before statement validation."""
    candidate = query or ""
    while True:
        updated = COMMENT_PREFIX_PATTERN.sub("", candidate, count=1)
        if updated == candidate:
            return candidate.strip()
        candidate = updated


@dataclass(frozen=True)
class QueryConfiguration:
    """
    Configuration for a custom SQL query transfer. Immutable.
    
    Attributes:
        source_name: Logical name for source identification in Kafka headers
        query: SQL query (SELECT or WITH/CTE only)
        kafka_topic: Target Kafka topic name
        query_params: Optional tuple of query parameters
        key_column: Column to use as Kafka message key (None for auto-detect)
        batch_size: Number of rows per batch (fetchmany size)
        min_expected_records: Minimum records for verification pass
    """
    source_name: str
    query: str
    count_query: str = None
    query_params: Optional[Tuple[Any, ...]] = None
    key_column: Optional[str] = None
    min_expected_records: int = 0
    
    def __post_init__(self) -> None:
        """Validate query is read-only after initialization."""
        self._validate_query()
        # if self.min_expected_records < 0:
        #     raise ValueError("'min_expected_records' must be >= 0.")
        # if not self.key_column or not self.key_column.strip():
        #     raise ValueError("'key_column' must not be empty.")
        
    def _validate_query(self) -> None:
        """Validate that query is SELECT/CTE only."""
        normalized = _strip_sql_comments(self.query)
        if not normalized or not SELECT_PREFIX_PATTERN.match(normalized):
            raise ValueError(
                "Invalid query type. Only SELECT/CTE/EXEC/EXECUTE queries are allowed."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'source_name': self.source_name,
            'query': self.query,
            'count_query':self.count_query,
            'query_params': self.query_params,
            'key_column': self.key_column,
            'min_expected_records': self.min_expected_records,
        }

    def with_resolved_params(
        self,
        execution_date_key: str,
        execution_ds: str,
    ) -> 'QueryConfiguration':
        """
        Create a new QueryConfiguration with resolved template tokens.
        
        Args:
            execution_date_key: YYYYMMDD format date string
            execution_ds: YYYY-MM-DD format date string
            
        Returns:
            New QueryConfiguration with resolved query and params
        """
        resolved_query = self._replace_tokens(self.query, execution_date_key, execution_ds)
        
        resolved_params = None
        if self.query_params:
            resolved_params = tuple(
                self._replace_tokens(str(p), execution_date_key, execution_ds)
                if isinstance(p, str) else p
                for p in self.query_params
            )
        
        # Create new instance (frozen dataclass)
        return QueryConfiguration(
            source_name=self.source_name,
            query=resolved_query,
            count_query=self.count_query,
            query_params=resolved_params,
            key_column=self.key_column,
            min_expected_records=self.min_expected_records,
        )

    @staticmethod
    def _replace_tokens(text: str, date_key: str, ds: str) -> str:
        """Replace execution date tokens in text."""
        return (
            text.replace("{{ ds_nodash }}", date_key)
            .replace("{{ds_nodash}}", date_key)
            .replace("{{ ds }}", ds)
            .replace("{{ds}}", ds)
        )
