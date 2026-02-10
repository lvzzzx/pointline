"""Tests for dim_equity_listing satellite table (Phase 5.1)."""

from __future__ import annotations

import polars as pl

from pointline.tables.dim_equity_listing import (
    DIM_EQUITY_LISTING_SCHEMA,
    bootstrap_from_dim_symbol,
    canonical_columns,
    normalize_schema,
)


def test_schema_columns():
    """Schema has expected columns."""
    assert "symbol_id" in DIM_EQUITY_LISTING_SCHEMA
    assert "isin" in DIM_EQUITY_LISTING_SCHEMA
    assert "cusip" in DIM_EQUITY_LISTING_SCHEMA
    assert "figi" in DIM_EQUITY_LISTING_SCHEMA
    assert "listing_exchange" in DIM_EQUITY_LISTING_SCHEMA
    assert "sector" in DIM_EQUITY_LISTING_SCHEMA
    assert "industry" in DIM_EQUITY_LISTING_SCHEMA


def test_canonical_columns():
    assert canonical_columns() == tuple(DIM_EQUITY_LISTING_SCHEMA.keys())


def test_normalize_fills_missing():
    """normalize_schema fills missing nullable columns with null."""
    df = pl.DataFrame({"symbol_id": [1], "isin": ["US0378331005"]})
    result = normalize_schema(df)
    assert set(result.columns) == set(DIM_EQUITY_LISTING_SCHEMA.keys())
    assert result["cusip"][0] is None
    assert result["figi"][0] is None
    assert result["sector"][0] is None


def test_normalize_casts_types():
    """normalize_schema casts columns to correct types."""
    df = pl.DataFrame(
        {
            "symbol_id": [1],
            "isin": ["US0378331005"],
            "cusip": ["037833100"],
            "figi": ["BBG000B9XRY4"],
            "listing_exchange": ["nasdaq"],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
        }
    )
    result = normalize_schema(df)
    assert result.schema["symbol_id"] == pl.Int64
    assert result.schema["isin"] == pl.Utf8


def test_bootstrap_filters_by_isin():
    """bootstrap_from_dim_symbol only keeps rows where isin is not null."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100, 200, 300],
            "asset_type": [0, 0, 1],
            "isin": ["US0378331005", None, None],
            "expiry_ts_us": [None, None, None],
            "underlying_symbol_id": [None, None, None],
            "settlement_type": [None, None, None],
            "strike": [None, None, None],
            "put_call": [None, None, None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.height == 1
    assert result["symbol_id"][0] == 100
    assert result["isin"][0] == "US0378331005"


def test_bootstrap_empty_when_no_isin():
    """bootstrap returns empty DataFrame when no isin values in dim_symbol."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100],
            "asset_type": [1],
            "isin": [None],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.is_empty()
    assert set(result.columns) == set(DIM_EQUITY_LISTING_SCHEMA.keys())


def test_bootstrap_handles_missing_isin_column():
    """bootstrap handles dim_symbol without isin column gracefully."""
    dim = pl.DataFrame(
        {
            "symbol_id": [100],
            "asset_type": [0],
        }
    )
    result = bootstrap_from_dim_symbol(dim)
    assert result.is_empty()
    assert set(result.columns) == set(DIM_EQUITY_LISTING_SCHEMA.keys())
