"""Tests for the central schema registry (Phase 5.2)."""

from __future__ import annotations

import polars as pl
import pytest

from pointline.io.base_repository import SchemaValidationError
from pointline.schema_registry import (
    SchemaEntry,
    _clear_registry,
    get_entry,
    get_schema,
    list_tables,
    register_schema,
    validate_df,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Save and restore registry state around each test."""
    from pointline.schema_registry import _REGISTRY

    backup = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(backup)


# ── Unit tests using isolated registry ──────────────────────────────────────


def test_register_and_get_schema():
    _clear_registry()
    schema = {"col_a": pl.Int64, "col_b": pl.Utf8}
    register_schema("test_table", schema, partition_by=["col_a"], has_date=True)

    result = get_schema("test_table")
    assert result == schema
    # Should be a copy
    assert result is not schema


def test_duplicate_registration_raises():
    _clear_registry()
    schema = {"col_a": pl.Int64}
    register_schema("dup_table", schema)
    with pytest.raises(ValueError, match="already registered"):
        register_schema("dup_table", schema)


def test_get_schema_unknown_raises():
    _clear_registry()
    with pytest.raises(KeyError, match="No schema registered"):
        get_schema("nonexistent_table")


def test_get_entry():
    _clear_registry()
    schema = {"col_a": pl.Int64, "col_b": pl.Utf8}
    register_schema("entry_table", schema, partition_by=["col_a"], has_date=True)

    entry = get_entry("entry_table")
    assert isinstance(entry, SchemaEntry)
    assert entry.table_name == "entry_table"
    assert entry.schema == schema
    assert entry.partition_by == ("col_a",)
    assert entry.has_date is True


def test_get_entry_unknown_raises():
    _clear_registry()
    with pytest.raises(KeyError, match="No schema registered"):
        get_entry("missing")


def test_validate_df_valid():
    _clear_registry()
    schema = {"col_a": pl.Int64, "col_b": pl.Utf8}
    register_schema("valid_table", schema)

    df = pl.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
    validate_df("valid_table", df)  # Should not raise


def test_validate_df_invalid():
    _clear_registry()
    schema = {"col_a": pl.Int64, "col_b": pl.Utf8}
    register_schema("invalid_table", schema)

    df = pl.DataFrame({"col_a": [1, 2], "col_c": ["x", "y"]})
    with pytest.raises(SchemaValidationError):
        validate_df("invalid_table", df)


def test_list_tables_sorted():
    _clear_registry()
    register_schema("z_table", {"a": pl.Int64})
    register_schema("a_table", {"a": pl.Int64})
    register_schema("m_table", {"a": pl.Int64})

    result = list_tables()
    assert result == ["a_table", "m_table", "z_table"]


# ── Integration test: all TABLE_PATHS have schemas ──────────────────────────


def test_all_table_paths_registered():
    """Ensure every table in TABLE_PATHS has a registered schema.

    This imports all table modules to trigger registration, then checks
    that every TABLE_PATHS key exists in the schema registry.
    """
    # Import all table modules to trigger registration
    import pointline.dim_symbol  # noqa: F401
    import pointline.io.delta_manifest_repo  # noqa: F401
    import pointline.tables.book_snapshots  # noqa: F401
    import pointline.tables.derivative_ticker  # noqa: F401
    import pointline.tables.dim_asset_stats  # noqa: F401
    import pointline.tables.dim_exchange  # noqa: F401
    import pointline.tables.dim_trading_calendar  # noqa: F401
    import pointline.tables.dq_summary  # noqa: F401
    import pointline.tables.klines  # noqa: F401
    import pointline.tables.quotes  # noqa: F401
    import pointline.tables.stock_basic_cn  # noqa: F401
    import pointline.tables.szse_l3_orders  # noqa: F401
    import pointline.tables.szse_l3_ticks  # noqa: F401
    import pointline.tables.trades  # noqa: F401
    import pointline.tables.validation_log  # noqa: F401
    from pointline.config import TABLE_PATHS

    registered = set(list_tables())
    table_paths_keys = set(TABLE_PATHS.keys())

    missing = table_paths_keys - registered
    assert not missing, (
        f"Tables in TABLE_PATHS but not in schema registry: {sorted(missing)}. "
        "Add register_schema() calls to the corresponding table modules."
    )
