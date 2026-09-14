"""Unit tests for MSSQLServerWriter covering two review fixes:

1. Column names taken from source data (not just table/key names) are now
   validated with IdentifierValidator before they reach generated SQL in
   insert_batch / update_batch / upsert_batch. This matters most for
   Mongo-to-MSSQL and Kafka-to-MSSQL syncs, where column names come from
   document/message field names that are not controlled by a fixed schema.
2. Hash-based change detection (_build_row_hash_expression /
   _build_merge_source_and_match_condition) now interleaves a separator
   between concatenated column values before hashing, so a value shifting
   across a column boundary (e.g. ('AB', 'C') vs ('A', 'BC')) no longer
   produces an identical hash and silently skips an UPDATE.

These tests load pipeline/database/MSSQLServerWriter.py in isolation via
importlib, stubbing out MSSQLConnectionFactory (which needs pyodbc/pymssql)
so the suite does not require real SQL Server drivers to be installed -
mirroring the isolation pattern already used by
tests/test_transient_sql_server_error.py and
tests/test_persian_partition_optimizer.py.
"""
import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

import pytest  # type: ignore

ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, relative_path: str) -> ModuleType:
    """Load a single module file under its real dotted name, without
    executing the package __init__.py chain above it."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _ensure_package(name: str) -> ModuleType:
    if name in sys.modules:
        return sys.modules[name]
    pkg = ModuleType(name)
    pkg.__path__ = []  # type: ignore[attr-defined]
    sys.modules[name] = pkg
    return pkg


class StopAtConnection(Exception):
    """Raised by the stub connection factory below. Proves that execution
    reached the point of opening a real DB connection - i.e. that SQL
    building and identifier validation ran (and, for the "valid columns"
    tests, ran without raising) before any actual database work."""


def _stub_mssql_connection_factory() -> ModuleType:
    """Stand-in for pipeline.database.MSSQLConnectionFactory.

    The real factory needs pyodbc/pymssql. Nothing under test here (SQL
    string building, identifier validation) touches a real connection, so a
    lightweight stand-in keeps this suite runnable without DB drivers
    installed.
    """
    module_name = "pipeline.database.MSSQLConnectionFactory"
    if module_name in sys.modules:
        return sys.modules[module_name]

    stub = ModuleType(module_name)

    class MSSQLConnectionFactory:
        def __init__(self, conn_id, is_connection_string: bool = False):
            self.conn_id = conn_id
            self.is_connection_string = is_connection_string

        @contextmanager
        def get_connection(self):
            raise StopAtConnection("get_connection called")
            yield  # pragma: no cover - unreachable, keeps this a generator

        def execute_query(self, *args, **kwargs):
            raise StopAtConnection("execute_query called")

    stub.MSSQLConnectionFactory = MSSQLConnectionFactory
    sys.modules[module_name] = stub
    return stub


@pytest.fixture(scope="module")
def mssql_writer_module():
    _ensure_package("pipeline")
    _ensure_package("pipeline.core")
    _ensure_package("pipeline.utils")
    _ensure_package("pipeline.interfaces")
    _ensure_package("pipeline.database")

    _load_module("pipeline.core.exceptions", "pipeline/core/exceptions.py")
    _load_module(
        "pipeline.utils.IdentifierValidator", "pipeline/utils/IdentifierValidator.py"
    )
    _load_module("pipeline.utils.retry_helper", "pipeline/utils/retry_helper.py")
    _load_module("pipeline.interfaces.DataWriter", "pipeline/interfaces/DataWriter.py")
    _stub_mssql_connection_factory()

    return _load_module(
        "pipeline.database.MSSQLServerWriter",
        "pipeline/database/MSSQLServerWriter.py",
    )


@pytest.fixture
def MSSQLServerWriter(mssql_writer_module):
    return mssql_writer_module.MSSQLServerWriter


@pytest.fixture
def InvalidIdentifierError():
    return sys.modules["pipeline.core.exceptions"].InvalidIdentifierError


@pytest.fixture
def writer(MSSQLServerWriter):
    return MSSQLServerWriter(conn_id="mssql_default")


# ---------------------------------------------------------------------------
# Item 1: column names sourced from data must be validated before they reach
# generated SQL in insert_batch / update_batch / upsert_batch.
# ---------------------------------------------------------------------------

MALICIOUS_COLUMN = "id]; DROP TABLE dbo.Users; --"


class TestColumnNameValidation:
    def test_insert_batch_rejects_invalid_column_name(self, writer, InvalidIdentifierError):
        data = [{"id": 1, MALICIOUS_COLUMN: "x"}]
        with pytest.raises(InvalidIdentifierError):
            writer.insert_batch("dbo", "Target", data)

    def test_update_batch_rejects_invalid_column_name(self, writer, InvalidIdentifierError):
        data = [{"id": 1, MALICIOUS_COLUMN: "x"}]
        with pytest.raises(InvalidIdentifierError):
            writer.update_batch("dbo", "Target", data, key_columns=["id"])

    def test_upsert_batch_rejects_invalid_column_name(self, writer, InvalidIdentifierError):
        data = [{"id": 1, MALICIOUS_COLUMN: "x"}]
        with pytest.raises(InvalidIdentifierError):
            writer.upsert_batch("dbo", "Target", data, key_columns=["id"])

    def test_insert_batch_passes_validation_for_normal_columns(self, writer):
        # No InvalidIdentifierError here: execution should reach the (stubbed)
        # database layer instead, proving valid columns are not rejected.
        data = [{"id": 1, "name": "ok"}]
        with pytest.raises(StopAtConnection):
            writer.insert_batch("dbo", "Target", data)

    def test_update_batch_passes_validation_for_normal_columns(self, writer):
        data = [{"id": 1, "name": "ok"}]
        with pytest.raises(StopAtConnection):
            writer.update_batch("dbo", "Target", data, key_columns=["id"])

    def test_upsert_batch_passes_validation_for_normal_columns(self, writer):
        data = [{"id": 1, "name": "ok"}]
        with pytest.raises(StopAtConnection):
            writer.upsert_batch("dbo", "Target", data, key_columns=["id"])


# ---------------------------------------------------------------------------
# Item 2: hash-based change detection must be boundary-safe (separator
# between concatenated column values).
# ---------------------------------------------------------------------------


class TestHashChangeDetectionBoundarySafety:
    def test_row_hash_expression_interleaves_separator_between_columns(
        self, MSSQLServerWriter
    ):
        expr = MSSQLServerWriter._build_row_hash_expression(["col_a", "col_b"], "target")
        assert "CHAR(31)" in expr
        assert expr.index("[col_a]") < expr.index("CHAR(31)") < expr.index("[col_b]")

    def test_interleave_hash_separator_preserves_edge_cases(self, MSSQLServerWriter):
        assert MSSQLServerWriter._interleave_hash_separator([]) == []
        assert MSSQLServerWriter._interleave_hash_separator(["only"]) == ["only"]
        assert MSSQLServerWriter._interleave_hash_separator(["a", "b", "c"]) == [
            "a",
            "CHAR(31)",
            "b",
            "CHAR(31)",
            "c",
        ]

    def test_row_hash_distinguishes_values_that_shift_across_column_boundary(
        self, MSSQLServerWriter
    ):
        """Regression test for the collision this fix closes.

        HASHBYTES('SHA2_256', CONCAT(a, b)) with no separator hashes
        ('AB', 'C') identically to ('A', 'BC'), which would make the MERGE's
        WHEN MATCHED clause treat a real data change as "unchanged" and
        silently skip the UPDATE. This simulates the same concatenation the
        generated SQL performs (substituting CHAR(31) with the literal
        separator character) and checks the two rows now differ.
        """

        def concat_like_generated_sql(values):
            parts = [v if v is not None else "NULL" for v in values]
            interleaved = MSSQLServerWriter._interleave_hash_separator(parts)
            return "".join(
                "\x1f" if part == "CHAR(31)" else part for part in interleaved
            )

        row_1 = concat_like_generated_sql(["AB", "C"])
        row_2 = concat_like_generated_sql(["A", "BC"])

        assert row_1 != row_2  # would be equal (and hash equal) without the fix
        assert concat_like_generated_sql(["AB", "C"]) == row_1  # deterministic

    def test_target_and_staging_hash_use_the_same_column_order_and_separator(
        self, writer
    ):
        """The staging-side hash (built inline in
        _build_merge_source_and_match_condition) and the target-side hash
        (_build_row_hash_expression) must be structurally symmetric, or a
        correct row would never compare equal across a MERGE."""
        source_subquery, match_condition = writer._build_merge_source_and_match_condition(
            staging_table="[dbo].[Target_staging_upsert]",
            update_columns=["col_a", "col_b"],
            use_hash_change_detection=True,
        )
        assert "CHAR(31)" in source_subquery
        assert "source.row_hash <> " in match_condition
        assert "CHAR(31)" in match_condition

    def test_build_concat_expression_handles_wide_tables_with_interleaved_separators(
        self, MSSQLServerWriter
    ):
        columns = [f"col_{i}" for i in range(300)]
        exprs = MSSQLServerWriter._interleave_hash_separator([f"[{c}]" for c in columns])
        assert len(exprs) == 2 * len(columns) - 1

        result = MSSQLServerWriter._build_concat_expression(exprs)
        assert result.count("CONCAT(") > 1  # must nest past the 254-arg limit
        for column in columns:
            assert f"[{column}]" in result
