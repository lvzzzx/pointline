"""Central schema registry for all Pointline tables.

Provides a single source of truth for table schemas, replacing scattered schema
dicts across 16+ modules. Each table module registers its schema at import time.

Usage:
    from pointline.schema_registry import register_schema, get_schema, get_entry

    # In table modules (at module level):
    register_schema("trades", TRADES_SCHEMA, partition_by=["exchange", "date"], has_date=True)

    # In consumers:
    schema = get_schema("trades")
    entry = get_entry("trades")
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class SchemaEntry:
    """Frozen metadata for a registered table schema."""

    table_name: str
    schema: dict[str, pl.DataType]
    partition_by: tuple[str, ...] | None
    has_date: bool


_REGISTRY: dict[str, SchemaEntry] = {}


def register_schema(
    table_name: str,
    schema: dict[str, pl.DataType],
    *,
    partition_by: list[str] | tuple[str, ...] | None = None,
    has_date: bool = False,
) -> None:
    """Register a table schema in the global registry.

    Called at module level in each table module after the schema dict is defined.

    Args:
        table_name: Canonical table name (e.g., "trades", "dim_symbol").
        schema: Mapping of column name to Polars DataType.
        partition_by: Partition columns (e.g., ["exchange", "date"]).
        has_date: Whether the table has a date partition column.

    Raises:
        ValueError: If table_name is already registered.
    """
    if table_name in _REGISTRY:
        raise ValueError(
            f"Schema for '{table_name}' is already registered. "
            "Each table should only be registered once."
        )
    _REGISTRY[table_name] = SchemaEntry(
        table_name=table_name,
        schema=dict(schema),
        partition_by=tuple(partition_by) if partition_by else None,
        has_date=has_date,
    )


def get_schema(table_name: str) -> dict[str, pl.DataType]:
    """Get the registered schema for a table.

    Args:
        table_name: Canonical table name.

    Returns:
        Copy of the schema dict.

    Raises:
        KeyError: If table_name is not registered.
    """
    if table_name not in _REGISTRY:
        raise KeyError(
            f"No schema registered for '{table_name}'. Available tables: {sorted(_REGISTRY.keys())}"
        )
    return dict(_REGISTRY[table_name].schema)


def get_entry(table_name: str) -> SchemaEntry:
    """Get the full SchemaEntry for a table.

    Args:
        table_name: Canonical table name.

    Returns:
        Frozen SchemaEntry dataclass.

    Raises:
        KeyError: If table_name is not registered.
    """
    if table_name not in _REGISTRY:
        raise KeyError(
            f"No schema registered for '{table_name}'. Available tables: {sorted(_REGISTRY.keys())}"
        )
    return _REGISTRY[table_name]


def list_tables() -> list[str]:
    """Return sorted list of all registered table names."""
    return sorted(_REGISTRY.keys())


def validate_df(table_name: str, df: pl.DataFrame) -> None:
    """Validate a DataFrame against the registered schema for a table.

    Delegates to ``validate_schema()`` from ``io/base_repository.py``.

    Args:
        table_name: Canonical table name.
        df: DataFrame to validate.

    Raises:
        KeyError: If table_name is not registered.
        SchemaValidationError: If validation fails.
    """
    from pointline.io.base_repository import validate_schema

    schema = get_schema(table_name)
    validate_schema(df, schema, table_path=table_name)


def _clear_registry() -> None:
    """Clear all registrations. For testing only."""
    _REGISTRY.clear()
