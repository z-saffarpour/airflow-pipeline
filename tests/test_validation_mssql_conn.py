"""Regression tests for pipeline/utils/validation.py's duplicate
`validate_mssql_conn` definition bug.

The file used to define `validate_mssql_conn` twice: an unused first version
accepting `(conn_id, hook_class)`, silently shadowed by a second version
accepting only `(conn_id)`. The first definition was dead code and, per its
own docstring, misleading about the real accepted signature.

These tests inspect validation.py's source via `ast` instead of importing the
module, because importing it pulls in MSSQL/MySQL/PostgreSQL/MongoDB/
ClickHouse connection factories and Kafka/Airflow, none of which unit tests
should need just to check for an accidental duplicate function definition.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VALIDATION_PATH = ROOT / "pipeline" / "utils" / "validation.py"


def _parse_validation_module() -> ast.Module:
    # utf-8-sig: validation.py starts with a UTF-8 BOM.
    source = VALIDATION_PATH.read_text(encoding="utf-8-sig")
    return ast.parse(source, filename=str(VALIDATION_PATH))


def _top_level_function_defs(tree: ast.Module, name: str):
    return [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]


def test_validate_mssql_conn_is_defined_exactly_once():
    tree = _parse_validation_module()
    defs = _top_level_function_defs(tree, "validate_mssql_conn")
    assert len(defs) == 1, (
        f"expected exactly one top-level `validate_mssql_conn` definition, "
        f"found {len(defs)}. A second definition silently shadows the "
        f"first, turning it into unreachable dead code - see the fixed bug "
        f"this test guards against."
    )


def test_validate_mssql_conn_signature_takes_only_conn_id():
    tree = _parse_validation_module()
    [func] = _top_level_function_defs(tree, "validate_mssql_conn")
    param_names = [arg.arg for arg in func.args.args]
    assert param_names == ["conn_id"], (
        "validate_mssql_conn's signature changed. Every DAG factory under "
        "dags/template/ calls it with a single positional conn_id argument "
        "(see the make_validate_mssql_connection_task() helpers) - update "
        "every call site if this signature is intentionally changing."
    )


def test_validation_module_has_no_accidental_duplicate_function_definitions():
    """Broader guard: none of validation.py's top-level functions should
    have the same duplicate-definition problem as validate_mssql_conn did."""
    tree = _parse_validation_module()
    names = [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    assert not duplicates, f"duplicate top-level function definitions found: {duplicates}"
