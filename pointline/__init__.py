"""Pointline data lake ETL utilities."""

from __future__ import annotations

from typing import Any


def get_schema(*args: Any, **kwargs: Any) -> Any:
    from pointline.introspection import get_schema as _get_schema

    return _get_schema(*args, **kwargs)


def list_columns(*args: Any, **kwargs: Any) -> Any:
    from pointline.introspection import list_columns as _list_columns

    return _list_columns(*args, **kwargs)


def get_schema_info(*args: Any, **kwargs: Any) -> Any:
    from pointline.introspection import get_schema_info as _get_schema_info

    return _get_schema_info(*args, **kwargs)


__all__ = ["get_schema", "list_columns", "get_schema_info"]
