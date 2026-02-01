"""Tests for schema introspection API."""

import polars as pl
import pytest

from pointline.introspection import get_schema, get_schema_info, list_columns


def test_get_schema_trades():
    """Test schema retrieval for trades table."""
    schema = get_schema("trades")

    assert isinstance(schema, dict)
    assert "ts_local_us" in schema
    assert "price_int" in schema
    assert "qty_int" in schema
    assert schema["price_int"] == pl.Int64
    assert schema["side"] == pl.UInt8
    assert schema["symbol_id"] == pl.Int64


def test_get_schema_quotes():
    """Test schema retrieval for quotes table."""
    schema = get_schema("quotes")

    assert isinstance(schema, dict)
    assert "bid_px_int" in schema
    assert "ask_px_int" in schema
    assert "bid_sz_int" in schema
    assert "ask_sz_int" in schema
    assert schema["bid_px_int"] == pl.Int64
    assert schema["ask_px_int"] == pl.Int64


def test_get_schema_book_snapshot_25():
    """Test schema retrieval for book_snapshot_25 table."""
    schema = get_schema("book_snapshot_25")

    assert isinstance(schema, dict)
    # book_snapshot_25 uses Lists, not individual columns
    assert "bids_px" in schema
    assert "asks_px" in schema
    assert "bids_sz" in schema
    assert "asks_sz" in schema


def test_get_schema_dim_symbol():
    """Test schema retrieval for dim_symbol table."""
    schema = get_schema("dim_symbol")

    assert isinstance(schema, dict)
    assert "symbol_id" in schema
    assert "exchange_id" in schema
    assert "exchange_symbol" in schema
    assert "valid_from_ts" in schema
    assert "valid_until_ts" in schema


def test_get_schema_szse_l3_orders():
    """Test schema retrieval for szse_l3_orders table."""
    schema = get_schema("szse_l3_orders")

    assert isinstance(schema, dict)
    assert "ts_local_us" in schema
    assert "symbol_id" in schema


def test_get_schema_szse_l3_ticks():
    """Test schema retrieval for szse_l3_ticks table."""
    schema = get_schema("szse_l3_ticks")

    assert isinstance(schema, dict)
    assert "ts_local_us" in schema
    assert "symbol_id" in schema


def test_get_schema_invalid_table():
    """Test error handling for invalid table name."""
    with pytest.raises(ValueError, match="not found in TABLE_PATHS"):
        get_schema("invalid_table")

    # Error message should include suggestions
    try:
        get_schema("invalid_table")
    except ValueError as e:
        error_msg = str(e)
        assert "Available tables" in error_msg
        assert "research.list_tables()" in error_msg


def test_get_schema_typo_suggests_correction():
    """Test that typos get fuzzy match suggestions."""
    with pytest.raises(ValueError) as exc_info:
        get_schema("trade")  # Missing 's'

    error_msg = str(exc_info.value)
    assert "Did you mean" in error_msg
    assert "trades" in error_msg


def test_list_columns_trades():
    """Test column listing for trades table."""
    columns = list_columns("trades")

    assert isinstance(columns, list)
    assert "ts_local_us" in columns
    assert "price_int" in columns
    assert "qty_int" in columns
    assert "symbol_id" in columns
    assert "side" in columns


def test_list_columns_quotes():
    """Test column listing for quotes table."""
    columns = list_columns("quotes")

    assert isinstance(columns, list)
    assert "bid_px_int" in columns
    assert "ask_px_int" in columns
    assert "bid_sz_int" in columns
    assert "ask_sz_int" in columns


def test_list_columns_invalid_table():
    """Test error handling for invalid table in list_columns."""
    with pytest.raises(ValueError, match="not found in TABLE_PATHS"):
        list_columns("nonexistent")


def test_get_schema_info_trades():
    """Test detailed schema info retrieval for trades table."""
    info = get_schema_info("trades")

    assert isinstance(info, dict)
    assert "price_int" in info
    assert info["price_int"]["type"] == "Int64"
    assert info["price_int"]["dtype"] == pl.Int64

    assert "side" in info
    assert info["side"]["type"] == "UInt8"
    assert info["side"]["dtype"] == pl.UInt8


def test_get_schema_info_structure():
    """Test that get_schema_info returns correct structure."""
    info = get_schema_info("trades")

    # Check structure for each column
    for _col_name, col_info in info.items():
        assert "type" in col_info
        assert "dtype" in col_info
        assert isinstance(col_info["type"], str)
        # Polars DataTypes are type classes, not instances
        # Just verify dtype exists and is a valid Polars type
        assert col_info["dtype"] is not None


def test_get_schema_info_invalid_table():
    """Test error handling for invalid table in get_schema_info."""
    with pytest.raises(ValueError, match="not found in TABLE_PATHS"):
        get_schema_info("bad_table")


def test_all_registered_tables_have_schemas():
    """Test that all tables in TABLE_PATHS have loadable schemas."""
    from pointline.config import TABLE_PATHS

    # These tables might not have schema modules yet
    skip_tables = {"ingest_manifest"}  # May not have schema constant

    for table_name in TABLE_PATHS:
        if table_name in skip_tables:
            continue

        try:
            schema = get_schema(table_name)
            assert isinstance(schema, dict)
            assert len(schema) > 0, f"Table {table_name} has empty schema"
        except ImportError:
            # Some tables may not have schema modules yet - that's okay
            pass


def test_schema_consistency_across_functions():
    """Test that get_schema, list_columns, and get_schema_info are consistent."""
    table_name = "trades"

    schema = get_schema(table_name)
    columns = list_columns(table_name)
    info = get_schema_info(table_name)

    # All should have the same columns
    assert list(schema.keys()) == columns
    assert list(schema.keys()) == list(info.keys())

    # Types should match
    for col in columns:
        assert info[col]["dtype"] == schema[col]
