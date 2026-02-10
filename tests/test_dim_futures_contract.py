"""Tests for dim_futures_contract satellite table (Phase 5.1)."""

from __future__ import annotations

import polars as pl

from pointline.tables.dim_futures_contract import (
    DIM_FUTURES_CONTRACT_SCHEMA,
    bootstrap_from_dim_symbol,
    canonical_columns,
    normalize_schema,
)


def test_schema_columns():
    """Schema has expected columns."""
    assert "symbol_id" in DIM_FUTURES_CONTRACT_SCHEMA
    assert "expiry_ts_us" in DIM_FUTURES_CONTRACT_SCHEMA
    assert "underlying_symbol_id" in DIM_FUTURES_CONTRACT_SCHEMA
    assert "settlement_type" in DIM_FUTURES_CONTRACT_SCHEMA
    assert "contract_month" in DIM_FUTURES_CONTRACT_SCHEMA
    assert "multiplier" in DIM_FUTURES_CONTRACT_SCHEMA


def test_canonical_columns():
    assert canonical_columns() == tuple(DIM_FUTURES_CONTRACT_SCHEMA.keys())


def test_normalize_fills_missing():
    """normalize_schema fills missing nullable columns with null."""
    df = pl.DataFrame({"symbol_id": [1], "expiry_ts_us": [1000000]})
    result = normalize_schema(df)
    assert set(result.columns) == set(DIM_FUTURES_CONTRACT_SCHEMA.keys())
    assert result["settlement_type"][0] is None
    assert result["contract_month"][0] is None


def test_normalize_casts_types():
    """normalize_schema casts columns to correct types."""
    df = pl.DataFrame(
        {
            "symbol_id": [1],
            "expiry_ts_us": [1000000],
            "underlying_symbol_id": [2],
            "settlement_type": ["cash"],
            "contract_month": ["2024-06"],
            "multiplier": [10.0],
        }
    )
    result = normalize_schema(df)
    assert result.schema["symbol_id"] == pl.Int64
    assert result.schema["expiry_ts_us"] == pl.Int64
    assert result.schema["multiplier"] == pl.Float64


def test_bootstrap_filters_futures():
    """bootstrap_from_dim_symbol only keeps asset_type==2 rows."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100, 200, 300],
            "asset_type": [0, 2, 3],  # spot, future, option
            "expiry_ts_us": [None, 1700000000000000, None],
            "underlying_symbol_id": [None, 50, None],
            "settlement_type": [None, "cash", None],
            "strike": [None, None, 100.0],
            "put_call": [None, None, "call"],
            "isin": [None, None, None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.height == 1
    assert result["symbol_id"][0] == 200
    assert result["settlement_type"][0] == "cash"


def test_bootstrap_empty_when_no_futures():
    """bootstrap returns empty DataFrame when no futures in dim_symbol."""
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
    assert set(result.columns) == set(DIM_FUTURES_CONTRACT_SCHEMA.keys())


def test_bootstrap_skips_all_null_satellite_cols():
    """bootstrap filters out futures rows where all satellite cols are null."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100, 200],
            "asset_type": [2, 2],
            "expiry_ts_us": [None, 1700000000000000],
            "underlying_symbol_id": [None, None],
            "settlement_type": [None, "physical"],
            "strike": [None, None],
            "put_call": [None, None],
            "isin": [None, None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    # symbol_id=100 has all satellite cols null, should be filtered
    assert result.height == 1
    assert result["symbol_id"][0] == 200
