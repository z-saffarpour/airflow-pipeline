"""
SQL Identifier Validator
========================
Shared helpers for validating SQL identifiers (table/column/schema names)
to prevent SQL injection via unsafe identifiers.
"""

import re
from typing import List, Optional

from pipeline.core.exceptions import InvalidIdentifierError


class IdentifierValidator:
    """
    Shared helpers for validating SQL identifiers (table/column/schema names).

    Used by readers, writers, query builders, and orchestrators to prevent
    SQL injection via unsafe identifiers.
    """

    _IDENTIFIER_PATTERN = re.compile(
        r'^[\w]+(?:\.[\w]+)?$|^\[[\w\s]+\](?:\.\[[\w\s]+\])?$'
    )

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
        return bool(IdentifierValidator._IDENTIFIER_PATTERN.match(name))

    @staticmethod
    def validate_and_raise(identifier: str, identifier_type: str = "identifier") -> None:
        """
        Validate identifier and raise exception if invalid.

        Args:
            identifier: SQL identifier to validate
            identifier_type: Type description for error message

        Raises:
            InvalidIdentifierError: If identifier is invalid
        """
        if not IdentifierValidator.validate_identifier(identifier):
            raise InvalidIdentifierError(
                f"Invalid SQL {identifier_type}: '{identifier}'. "
                f"Only alphanumeric characters, underscores, dots, and brackets are allowed."
            )

    @staticmethod
    def validate_columns(columns: Optional[List[str]]) -> None:
        """
        Validate a list of column names.

        Args:
            columns: List of column names to validate

        Raises:
            InvalidIdentifierError: If any column name is invalid
        """
        if columns:
            for col in columns:
                IdentifierValidator.validate_and_raise(col, "column name")
