"""
Safe MSSQL hook implementation.
Supports two secure connection modes:
1) pymssql (default, backward compatible)
2) pyodbc (Kerberos/Windows Integrated Authentication)
"""

import re
from typing import Any, Dict, Optional

import pymssql  # type: ignore
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # type: ignore


# Pattern for valid server names (alphanumeric, dots, hyphens, underscores)
_SERVER_NAME_PATTERN = re.compile(r'^[\w\.\-]+$')

UNSUPPORTED_PYMSSQL_EXTRA_KEYS = {
    "trusted_connection",
    "driver",
    "odbc_connect",
    "dsn",
    "authentication",
    "integrated_security",
}

ODBC_TRIGGER_KEYS = {
    "trusted_connection",
    "driver",
    "odbc_connect",
    "dsn",
    "authentication",
    "integrated_security",
    "auth_mode",
}


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def validate_server_name(server: str) -> bool:
    """
    Validate server name to prevent injection attacks.
    
    Args:
        server: Server name or hostname
        
    Returns:
        True if valid server name format
    """
    if not server or not isinstance(server, str):
        return False
    # Remove whitespace
    server = server.strip()
    if not server:
        return False
    return bool(_SERVER_NAME_PATTERN.match(server))


def should_use_odbc(extra_kwargs: Dict[str, Any]) -> bool:
    """
    Determine whether pyodbc mode should be used.

    pyodbc mode is enabled when any ODBC-related key exists in extras,
    or when auth_mode explicitly requests kerberos/integrated authentication.
    """
    normalized = {str(k).lower(): v for k, v in extra_kwargs.items()}

    auth_mode = str(normalized.get("auth_mode", "")).strip().lower()
    if auth_mode in {"odbc", "kerberos", "windows_integrated", "integrated"}:
        return True

    return any(key in normalized for key in ODBC_TRIGGER_KEYS)


def build_secure_odbc_connection_string(
    server: str,
    database: Optional[str],
    port: Optional[int],
    login: Optional[str],
    password: Optional[str],
    extra_kwargs: Dict[str, Any],
) -> str:
    """
    Build a secure ODBC connection string for SQL Server.

    Security defaults are production-safe:
    - Encrypt=yes
    - TrustServerCertificate=no
    
    Args:
        server: Server name or hostname
        database: Database name
        port: Server port
        login: Login username
        password: Login password
        extra_kwargs: Additional connection parameters
        
    Returns:
        ODBC connection string
        
    Raises:
        ValueError: If server name is invalid
    """
    # Validate server name
    if not validate_server_name(server):
        raise ValueError(
            f"Invalid server name: '{server}'. "
            "Server names must contain only alphanumeric characters, dots, hyphens, and underscores."
        )
    
    driver = extra_kwargs.get("driver", "ODBC Driver 18 for SQL Server")
    dsn = extra_kwargs.get("dsn")

    encrypt = _is_truthy(extra_kwargs.get("encrypt", True))
    trust_server_certificate = _is_truthy(
        extra_kwargs.get("trustservercertificate", False)
        or extra_kwargs.get("trust_server_certificate", False)
    )
    trusted_connection = _is_truthy(extra_kwargs.get("trusted_connection", False))
    integrated_security = _is_truthy(extra_kwargs.get("integrated_security", False))

    authentication = extra_kwargs.get("authentication")
    login_timeout = extra_kwargs.get("login_timeout", 15)
    timeout = extra_kwargs.get("timeout", 30)

    parts = []
    if dsn:
        parts.append(f"DSN={dsn}")
    else:
        parts.append(f"DRIVER={{{driver}}}")
        if port:
            parts.append(f"SERVER={server},{port}")
        else:
            parts.append(f"SERVER={server}")

    if database:
        parts.append(f"DATABASE={database}")

    if trusted_connection or integrated_security:
        parts.append("Trusted_Connection=yes")
    else:
        if login:
            parts.append(f"UID={login}")
        if password:
            parts.append(f"PWD={password}")

    if authentication:
        parts.append(f"Authentication={authentication}")

    parts.append(f"Encrypt={'yes' if encrypt else 'no'}")
    parts.append(
        f"TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"
    )
    parts.append(f"LoginTimeout={login_timeout}")
    parts.append(f"Timeout={timeout}")

    return ";".join(parts)


def sanitize_mssql_extra_kwargs(extra_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove extras that are incompatible with pymssql.connect.

    Args:
        extra_kwargs: Extra kwargs loaded from Airflow connection extras

    Returns:
        Sanitized dictionary safe for pymssql.connect
    """
    sanitized: Dict[str, Any] = {}

    for key, value in extra_kwargs.items():
        if key.lower() in UNSUPPORTED_PYMSSQL_EXTRA_KEYS:
            continue
        if value is None:
            continue
        sanitized[key] = value

    return sanitized


class SafeMsSqlHook(MsSqlHook):
    """
    MsSqlHook variant that guards against unsupported kwargs for pymssql.
    Adds server name validation for security.
    """

    def get_conn(self):
        conn = self.connection
        
        # Validate server name for security
        if conn.host and not validate_server_name(conn.host):
            raise ValueError(
                f"Invalid server name in connection '{self.mssql_conn_id}': '{conn.host}'. "
                "Server names must contain only alphanumeric characters, dots, hyphens, and underscores."
            )
        
        original_extras = dict(conn.extra_dejson)

        if should_use_odbc(original_extras):
            return self._get_odbc_connection(conn, original_extras)

        return self._get_pymssql_connection(conn, original_extras)
    
    def _get_odbc_connection(self, conn, extras):
        try:
            import pyodbc # type: ignore
        except ImportError as exc:
            raise RuntimeError("pyodbc is required for ODBC/Kerberos SQL Server connections") from exc
        
        connection_string = build_secure_odbc_connection_string(
            server=conn.host,
            database=self.schema or conn.schema,
            port=conn.port,
            login=conn.login,
            password=conn.password,
            extra_kwargs=extras,
        )
        self.log.info("Using pyodbc SQL Server connection mode (integrated/ODBC path)")
        return pyodbc.connect(connection_string)
    
    def _get_pymssql_connection(self, conn, extras):
        extra_kwargs = sanitize_mssql_extra_kwargs(extras)

        if len(extra_kwargs) != len(extras):
            self.log.warning(
                "Removed unsupported MSSQL connection extras for pymssql driver"
            )

        connect_kwargs: Dict[str, Any] = {
            "server": conn.host,
            "user": conn.login,
            "password": conn.password,
            "database": self.schema or conn.schema,
            "port": conn.port,
            **extra_kwargs,
        }
        connect_kwargs = {k: v for k, v in connect_kwargs.items() if v is not None}
        
        return pymssql.connect(**connect_kwargs)