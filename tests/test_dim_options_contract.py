"""Tests for dim_options_contract satellite table (Phase 5.1)."""

from __future__ import annotations

import polars as pl

from pointline.tables.dim_options_contract import (
    DIM_OPTIONS_CONTRACT_SCHEMA,
    bootstrap_from_dim_symbol,
    canonical_columns,
    normalize_schema,
)


def test_schema_columns():
    """Schema has expected columns."""
    assert "symbol_id" in DIM_OPTIONS_CONTRACT_SCHEMA
    assert "strike" in DIM_OPTIONS_CONTRACT_SCHEMA
    assert "put_call" in DIM_OPTIONS_CONTRACT_SCHEMA
    assert "exercise_style" in DIM_OPTIONS_CONTRACT_SCHEMA
    assert "expiry_ts_us" in DIM_OPTIONS_CONTRACT_SCHEMA
    assert "underlying_symbol_id" in DIM_OPTIONS_CONTRACT_SCHEMA


def test_canonical_columns():
    assert canonical_columns() == tuple(DIM_OPTIONS_CONTRACT_SCHEMA.keys())


def test_normalize_fills_missing():
    """normalize_schema fills missing nullable columns with null."""
    df = pl.DataFrame({"symbol_id": [1], "strike": [100.0]})
    result = normalize_schema(df)
    assert set(result.columns) == set(DIM_OPTIONS_CONTRACT_SCHEMA.keys())
    assert result["exercise_style"][0] is None
    assert result["put_call"][0] is None


def test_normalize_casts_types():
    """normalize_schema casts columns to correct types."""
    df = pl.DataFrame(
        {
            "symbol_id": [1],
            "strike": [100.0],
            "put_call": ["call"],
            "exercise_style": ["european"],
            "expiry_ts_us": [1700000000000000],
            "underlying_symbol_id": [50],
        }
    )
    result = normalize_schema(df)
    assert result.schema["symbol_id"] == pl.Int64
    assert result.schema["strike"] == pl.Float64
    assert result.schema["put_call"] == pl.Utf8


def test_bootstrap_filters_options():
    """bootstrap_from_dim_symbol only keeps asset_type==3 rows."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100, 200, 300],
            "asset_type": [0, 2, 3],  # spot, future, option
            "expiry_ts_us": [None, 1700000000000000, 1700000000000000],
            "underlying_symbol_id": [None, 50, 60],
            "settlement_type": [None, "cash", None],
            "strike": [None, None, 30000.0],
            "put_call": [None, None, "call"],
            "isin": [None, None, None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.height == 1
    assert result["symbol_id"][0] == 300
    assert result["strike"][0] == 30000.0
    assert result["put_call"][0] == "call"


def test_bootstrap_empty_when_no_options():
    """bootstrap returns empty DataFrame when no options in dim_symbol."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100],
            "asset_type": [0],  # spot only
            "expiry_ts_us": [None],
            "underlying_symbol_id": [None],
            "settlement_type": [None],
            "strike": [None],
            "put_call": [None],
            "isin": [None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.is_empty()
    assert set(result.columns) == set(DIM_OPTIONS_CONTRACT_SCHEMA.keys())


def test_bootstrap_skips_all_null_satellite_cols():
    """bootstrap filters out options rows where all satellite cols are null."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100, 200],
            "asset_type": [3, 3],
            "expiry_ts_us": [None, 1700000000000000],
            "underlying_symbol_id": [None, None],
            "settlement_type": [None, None],
            "strike": [None, 50000.0],
            "put_call": [None, "put"],
            "isin": [None, None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.height == 1
    assert result["symbol_id"][0] == 200
