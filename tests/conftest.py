"""Pytest configuration for pipeline unit tests."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _ensure_airflow_stub() -> None:
    if "airflow.exceptions" in sys.modules:
        return

    airflow = ModuleType("airflow")
    airflow_exceptions = ModuleType("airflow.exceptions")

    class AirflowException(Exception):
        pass

    class AirflowFailException(Exception):
        pass

    airflow_exceptions.AirflowException = AirflowException
    airflow_exceptions.AirflowFailException = AirflowFailException
    airflow.exceptions = airflow_exceptions
    sys.modules["airflow"] = airflow
    sys.modules["airflow.exceptions"] = airflow_exceptions


_ensure_airflow_stub()


def load_exceptions_module():
    """Load exceptions.py without importing the full pipeline package."""
    path = ROOT / "pipeline" / "core" / "exceptions.py"
    spec = importlib.util.spec_from_file_location("pipeline_core_exceptions", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load exceptions module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
