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

# Known-dangerous SQL Server system/extended stored procedures. Rejected
# wherever they appear in the query text (not only right after EXEC), so a
# stacked statement such as "SELECT 1; EXEC xp_cmdshell 'dir'" is caught
# too. sp_executesql is included because it runs an arbitrary SQL string
# at runtime, which would otherwise bypass this whole check.
_DANGEROUS_PROC_NAMES = (
    "xp_cmdshell",
    "xp_regwrite", "xp_regread", "xp_regdeletekey", "xp_regdeletevalue",
    "xp_regenumvalues", "xp_regenumkeys",
    "xp_regaddmultistring", "xp_regremovemultistring",
    "xp_instance_regwrite", "xp_instance_regread",
    "xp_instance_regdeletekey", "xp_instance_regdeletevalue",
    "xp_instance_regenumvalues", "xp_instance_regenumkeys",
    "xp_instance_regaddmultistring", "xp_instance_regremovemultistring",
    "xp_servicecontrol",
    "xp_dirtree", "xp_fileexist", "xp_subdirs", "xp_availablemedia",
    "xp_fixeddrives", "xp_delete_file", "xp_copy_file",
    "sp_configure",
    "sp_addextendedproc",
    "sp_oacreate", "sp_oamethod", "sp_oagetproperty", "sp_oasetproperty",
    "sp_oadestroy", "sp_oastop",
    "sp_addlinkedserver", "sp_addlinkedsrvlogin",
    "sp_addsrvrolemember", "sp_addrolemember",
    "sp_addlogin", "sp_grantlogin",
    "sp_password",
    "sp_executesql",
    "sp_send_dbmail",
    # SQL Server Agent job procedures - a CmdExec job step is an
    # alternate OS-command-execution path even when xp_cmdshell itself
    # is disabled.
    "sp_add_job", "sp_add_jobstep", "sp_add_jobserver",
    "sp_update_jobstep", "sp_start_job",
)
DANGEROUS_PROC_PATTERN = re.compile(
    r"\b(?:" + "|".join(_DANGEROUS_PROC_NAMES) + r")\b",
    re.IGNORECASE,
)

# OPENROWSET/OPENDATASOURCE/OPENQUERY reach a remote/linked server or run
# dynamic SQL through it, and can appear inside a plain SELECT's FROM
# clause (not only after EXEC), so they are always rejected too.
DANGEROUS_FUNCTION_PATTERN = re.compile(
    r"\b(?:openrowset|opendatasource|openquery)\s*\(", re.IGNORECASE
)

# EXEC/EXECUTE must be followed by a plain (optionally schema-qualified,
# optionally bracketed) procedure name -- never a parenthesized string or
# expression ("EXEC ('...')") or a bare variable ("EXEC @sql" /
# "EXECUTE (@sql)"). Both of those run an arbitrary SQL string built at
# runtime and call no "named" procedure at all, so they would otherwise
# sail through the denylist above untouched -- this is the classic
# dynamic-SQL pattern, and arguably more dangerous than any single
# missing name in _DANGEROUS_PROC_NAMES. Checked anywhere in the text
# (not just the leading statement) to also catch it after a stacked
# ";".
DYNAMIC_EXEC_PATTERN = re.compile(
    r"\b(?:exec|execute)\s*(?:\(|@)", re.IGNORECASE
)


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
        """
        Validate that the query is SELECT/CTE/EXEC only, and never calls a
        known-dangerous system/extended stored procedure, a
        dynamic-SQL/linked-server function (OPENROWSET/OPENDATASOURCE/
        OPENQUERY), or EXEC/EXECUTE of a parenthesized expression or bare
        variable (dynamic SQL), wherever in the query text they appear.
        """
        normalized = _strip_sql_comments(self.query)
        if not normalized or not SELECT_PREFIX_PATTERN.match(normalized):
            raise ValueError(
                "Invalid query type. Only SELECT/CTE/EXEC queries are allowed."
            )
        if DANGEROUS_PROC_PATTERN.search(normalized):
            raise ValueError(
                "Invalid query: this stored procedure is not allowed "
                "(system/extended procedure or dynamic SQL)."
            )
        if DANGEROUS_FUNCTION_PATTERN.search(normalized):
            raise ValueError(
                "Invalid query: OPENROWSET/OPENDATASOURCE/OPENQUERY are not allowed."
            )
        if DYNAMIC_EXEC_PATTERN.search(normalized):
            raise ValueError(
                "Invalid query: EXEC/EXECUTE of a parenthesized expression or a "
                "variable (dynamic SQL) is not allowed; call a named procedure "
                "instead."
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
