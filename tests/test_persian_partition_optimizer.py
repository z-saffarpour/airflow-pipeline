"""Unit tests for Persian calendar conversion and ClickHouse partition keys."""
import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, relative_path: str):
    """Load a module file without importing the full pipeline package tree."""
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


@pytest.fixture(scope="module")
def persian_calendar():
    _ensure_package("pipeline")
    _ensure_package("pipeline.utils")
    return _load_module(
        "pipeline.utils.persian_calendar",
        "pipeline/utils/persian_calendar.py",
    )


@pytest.fixture(scope="module")
def optimization_config(persian_calendar):
    # persian_calendar already registered under pipeline.utils.persian_calendar
    _ensure_package("pipeline.config")
    return _load_module(
        "pipeline.config.ClickHouseOptimizationConfig",
        "pipeline/config/ClickHouseOptimizationConfig.py",
    ).ClickHouseOptimizationConfig


@pytest.mark.parametrize(
    "gy,gm,gd,expected",
    [
        (2026, 3, 21, (1405, 1, 1)),  # Nowruz 1405
        (2025, 3, 21, (1404, 1, 1)),  # Nowruz 1404
        (2026, 7, 23, (1405, 5, 1)),  # Mordad 1, 1405
        (2024, 3, 20, (1403, 1, 1)),  # Nowruz 1403
    ],
)
def test_gregorian_to_jalali(persian_calendar, gy, gm, gd, expected):
    assert persian_calendar.gregorian_to_jalali(gy, gm, gd) == expected


def test_to_persian_year_month_from_yyyymmdd(persian_calendar):
    assert persian_calendar.to_persian_year_month("20260723") == "140505"
    assert persian_calendar.to_persian_year("20260723") == "1405"
    assert persian_calendar.to_persian_date_key("20260723") == "14050501"
    assert persian_calendar.to_persian_date("20260723") == "1405/05/01"
    assert persian_calendar.to_persian_date(date(2026, 3, 21)) == "1405/01/01"
    assert persian_calendar.to_persian_year_month(date(2026, 3, 21)) == "140501"


def test_partition_value_persian_slash_date(optimization_config):
    config = optimization_config(
        database="RTL",
        table_name="t",
        partition_column="PersianDate",
        partition_format="PERSIAN_YYYY/MM/DD",
    )
    assert config.get_partition_value("20260723") == "1405/05/01"


def test_partition_value_persian_yyyymm(optimization_config):
    config = optimization_config(
        database="RTL",
        table_name="Local_Fact_SalesTrans_V01",
        partition_column="PersianYearMonthInt",
        partition_format="PERSIAN_YYYYMM",
    )
    # Gregorian 2026-07-23 → Jalali 1405-05-01 → partition 140505
    assert config.get_partition_value("20260723") == "140505"


def test_partition_value_gregorian_yyyymm_unchanged(optimization_config):
    config = optimization_config(
        database="RTL",
        table_name="Local_Fact_SalesTrans",
        partition_column="COM_DIM_Date_TransRef",
        partition_format="YYYYMM",
    )
    assert config.get_partition_value("20260723") == "202607"


def test_partition_value_none_without_column(optimization_config):
    config = optimization_config(
        database="RTL",
        table_name="Local_Fact_SalesTrans_V01",
        partition_column=None,
        partition_format="PERSIAN_YYYYMM",
    )
    assert config.get_partition_value("20260723") is None


def test_partition_format_aliases(optimization_config):
    for fmt in ("SHAMSI_YYYYMM", "JALALI_YYYYMM", "persian_yyyymm"):
        config = optimization_config(
            database="RTL",
            table_name="t",
            partition_column="PersianYearMonthInt",
            partition_format=fmt,
        )
        assert config.get_partition_value("20260321") == "140501"


def test_unsupported_partition_format(optimization_config):
    config = optimization_config(
        database="RTL",
        table_name="t",
        partition_column="x",
        partition_format="INVALID",
    )
    with pytest.raises(ValueError, match="Unsupported partition_format"):
        config.get_partition_value("20260723")
