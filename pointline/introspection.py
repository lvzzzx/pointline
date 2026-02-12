"""Schema introspection utilities for the Pointline data lake.

This module provides programmatic access to table schemas, enabling:
- LLM agents to discover available columns and types
- Researchers to explore data structure without reading source code
- Automated documentation generation

All schemas are loaded from the canonical {TABLE_NAME}_SCHEMA definitions
in the pointline/tables/ modules.
"""

from __future__ import annotations

import importlib
from typing import Any

import polars as pl

from pointline._error_messages import table_not_found_error
from pointline.config import TABLE_PATHS
from pointline.types import TableName


def get_schema(table_name: TableName) -> dict[str, pl.DataType]:
    """Get the canonical Polars schema for a table.

    Returns a dictionary mapping column names to Polars data types.
    The schema is loaded from the table's module definition.

    Args:
        table_name: Name of the table (e.g., "trades", "quotes")

    Returns:
        Dictionary with column names as keys and pl.DataType objects as values

    Raises:
        ValueError: If table_name is not a registered table
        ImportError: If table module cannot be loaded or schema not found

    Examples:
        >>> from pointline.introspection import get_schema
        >>> schema = get_schema("trades")
        >>> schema["px_int"]
        Int64
        >>> list(schema.keys())
        ['date', 'exchange', 'symbol', 'ts_local_us', ...]
    """
    if table_name not in TABLE_PATHS:
        available = sorted(TABLE_PATHS.keys())
        raise ValueError(table_not_found_error(table_name, available))

    schema = _load_schema_from_module(table_name)

    if schema is None:
        raise ImportError(
            f"Could not load schema for table '{table_name}'. "
            f"The table module may not define the expected schema constant."
        )

    return schema


def list_columns(table_name: TableName) -> list[str]:
    """Get the list of column names for a table.

    Args:
        table_name: Name of the table (e.g., "trades", "quotes")

    Returns:
        List of column names in schema order

    Raises:
        ValueError: If table_name is not a registered table
        ImportError: If table module cannot be loaded

    Examples:
        >>> from pointline.introspection import list_columns
        >>> columns = list_columns("trades")
        >>> columns
        ['date', 'exchange', 'symbol', 'ts_local_us', ...]
    """
    schema = get_schema(table_name)
    return list(schema.keys())


def get_schema_info(table_name: TableName) -> dict[str, dict[str, Any]]:
    """Get detailed schema information including types and metadata.

    Returns a dictionary with column names as keys and metadata dictionaries
    as values. Each metadata dict contains:
    - 'type': Polars data type name (e.g., "Int64", "Utf8")
    - 'dtype': Polars DataType object

    Future enhancements may add 'description', 'nullable', 'units', etc.

    Args:
        table_name: Name of the table

    Returns:
        Dictionary mapping column names to metadata dictionaries

    Raises:
        ValueError: If table_name is not a registered table
        ImportError: If table module cannot be loaded

    Examples:
        >>> from pointline.introspection import get_schema_info
        >>> info = get_schema_info("trades")
        >>> info["px_int"]
        {'type': 'Int64', 'dtype': Int64}
    """
    schema = get_schema(table_name)
    return {
        col: {
            "type": str(dtype),
            "dtype": dtype,
        }
        for col, dtype in schema.items()
    }


def _load_schema_from_module(table_name: str) -> dict[str, pl.DataType] | None:
    """Dynamically load schema from table module.

    Table modules follow the naming convention:
    - pointline/tables/trades.py → TRADES_SCHEMA
    - pointline/tables/quotes.py → QUOTES_SCHEMA
    - pointline/tables/book_snapshots.py → BOOK_SNAPSHOTS_SCHEMA

    Some tables have special mappings:
    - book_snapshot_25 → book_snapshots.BOOK_SNAPSHOTS_SCHEMA
    - kline_1h/kline_1d → klines.KLINE_SCHEMA
    - dim_symbol → dim_symbol.SCHEMA (not in tables/)
    - ingest_manifest → io.delta_manifest_repo.MANIFEST_SCHEMA

    Args:
        table_name: Table name (e.g., "trades", "book_snapshot_25")

    Returns:
        Schema dictionary or None if not found
    """
    # Special case: dim_symbol is in pointline/, not pointline/tables/
    if table_name == "dim_symbol":
        try:
            module = importlib.import_module("pointline.dim_symbol")
            return getattr(module, "SCHEMA", None)
        except (ImportError, AttributeError):
            return None
    if table_name == "ingest_manifest":
        try:
            module = importlib.import_module("pointline.io.delta_manifest_repo")
            return getattr(module, "MANIFEST_SCHEMA", None)
        except (ImportError, AttributeError):
            return None

    # Map table names to module names (most are 1:1)
    module_name_map = {
        "book_snapshot_25": "book_snapshots",
        "kline_1h": "klines",
        "kline_1d": "klines",
        "l3_orders": "l3_orders",
        "l3_ticks": "l3_ticks",
        "dim_asset_stats": "dim_asset_stats",
        "stock_basic_cn": "stock_basic_cn",
        "validation_log": "validation_log",
        "dq_summary": "dq_summary",
    }

    module_name = module_name_map.get(table_name, table_name)

    # Map table names to schema constant names
    schema_constant_map = {
        "book_snapshot_25": "BOOK_SNAPSHOTS_SCHEMA",
        "kline_1h": "KLINE_SCHEMA",
        "kline_1d": "KLINE_SCHEMA",
        "l3_orders": "L3_ORDERS_SCHEMA",
        "l3_ticks": "L3_TICKS_SCHEMA",
        "dim_asset_stats": "SCHEMA",
        "stock_basic_cn": "SCHEMA",
        "validation_log": "VALIDATION_LOG_SCHEMA",
        "dq_summary": "DQ_SUMMARY_SCHEMA",
    }

    schema_constant = schema_constant_map.get(table_name, f"{table_name.upper()}_SCHEMA")

    try:
        module = importlib.import_module(f"pointline.tables.{module_name}")
        return getattr(module, schema_constant, None)
    except (ImportError, AttributeError):
        return None


__all__ = [
    "get_schema",
    "list_columns",
    "get_schema_info",
]
