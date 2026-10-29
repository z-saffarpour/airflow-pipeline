"""Tests for parse-time config helpers (no metadata DB)."""
from __future__ import annotations

from pipeline.utils.parse_config import parse_int, parse_optional_str, parse_str


def test_parse_str_uses_default_when_env_missing(monkeypatch):
    monkeypatch.delenv("AIRFLOW_VAR_MSSQL_SOURCE_CONN_ID", raising=False)
    assert parse_str("mssql_source_conn_id", "mssql_default") == "mssql_default"


def test_parse_str_reads_airflow_var_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_MSSQL_SOURCE_CONN_ID", "mssql_prod")
    assert parse_str("mssql_source_conn_id", "mssql_default") == "mssql_prod"


def test_parse_int_uses_default_and_env(monkeypatch):
    monkeypatch.delenv("AIRFLOW_VAR_BATCH_SIZE_EXAMPLE", raising=False)
    assert parse_int("batch_size_example", 10000) == 10000
    monkeypatch.setenv("AIRFLOW_VAR_BATCH_SIZE_EXAMPLE", "250")
    assert parse_int("batch_size_example", 10000) == 250


def test_parse_optional_str_blank_becomes_none(monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_MONGO_SOURCE_DATABASE", "")
    assert parse_optional_str("mongo_source_database", "ignored") is None
    monkeypatch.delenv("AIRFLOW_VAR_MONGO_SOURCE_DATABASE", raising=False)
    assert parse_optional_str("mongo_source_database", "") is None
    monkeypatch.setenv("AIRFLOW_VAR_MONGO_SOURCE_DATABASE", "sales")
    assert parse_optional_str("mongo_source_database", "") == "sales"


def test_importing_pipeline_package_does_not_load_drivers():
    """Dag parse must not import Kafka/DB drivers via pipeline/__init__.py."""
    # Isolate from other tests that may have imported submodules already.
    driver_modules = [
        "pipeline.kafka.KafkaTopicManager",
        "pipeline.database.SafeMsSqlHook",
        "pipeline.database.MongoDBConnectionFactory",
        "confluent_kafka",
        "pymssql",
    ]
    # Only assert modules that would be newly loaded by `import pipeline`.
    import importlib
    import sys

    preserved = {
        name: sys.modules[name]
        for name in list(sys.modules)
        if name == "pipeline" or name.startswith("pipeline.")
    }
    for name in list(preserved):
        del sys.modules[name]
    try:
        import pipeline  # noqa: F401

        for name in driver_modules:
            assert name not in sys.modules, name
        assert "pipeline.core.exceptions" in sys.modules
    finally:
        for name in list(sys.modules):
            if name == "pipeline" or name.startswith("pipeline."):
                del sys.modules[name]
        sys.modules.update(preserved)
        importlib.invalidate_caches()
