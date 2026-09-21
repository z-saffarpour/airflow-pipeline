"""Lazy attribute loading for package ``__init__`` modules.

Keeps Airflow DAG parse time low by importing heavy database/Kafka
modules only when an attribute is actually accessed.
"""
from __future__ import annotations

from importlib import import_module
from typing import Any, Mapping


def getattr_lazy(
    module_globals: dict[str, Any],
    name: str,
    exports: Mapping[str, str],
) -> Any:
    """Resolve ``name`` from ``exports`` (attr -> ``module.path:Attribute``)."""
    target = exports.get(name)
    if target is None:
        module_name = module_globals.get("__name__", "")
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

    module_path, attr_name = target.rsplit(":", 1)
    value = getattr(import_module(module_path), attr_name)
    module_globals[name] = value
    return value
