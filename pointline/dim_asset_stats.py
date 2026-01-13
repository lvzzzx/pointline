"""Schema and utilities for dim_asset_stats in Polars.

This module provides schema definition and normalization for the dim_asset_stats table,
which tracks daily asset-level statistics (circulating supply, market cap, etc.) from CoinGecko.

Example:
    import polars as pl
    from pointline.dim_asset_stats import normalize_dim_asset_stats_schema

    df = pl.DataFrame({
        "base_asset": ["BTC"],
        "date": [pl.date(2024, 1, 15)],
        "coingecko_coin_id": ["bitcoin"],
        "circulating_supply": [19600000.0],
        "total_supply": [19600000.0],
        "max_supply": [21000000.0],
        "market_cap_usd": [850000000000.0],
        "fully_diluted_valuation_usd": [910000000000.0],
        "updated_at_ts": [1705276800000000],
        "fetched_at_ts": [1705280000000000],
        "source": ["coingecko"],
    })

    normalized = normalize_dim_asset_stats_schema(df)
"""

from __future__ import annotations

import polars as pl

SCHEMA: dict[str, pl.DataType] = {
    "base_asset": pl.Utf8,
    "date": pl.Date,
    "coingecko_coin_id": pl.Utf8,
    "circulating_supply": pl.Float64,
    "total_supply": pl.Float64,  # Nullable
    "max_supply": pl.Float64,  # Nullable (null for uncapped assets)
    "market_cap_usd": pl.Float64,  # Nullable
    "fully_diluted_valuation_usd": pl.Float64,  # Nullable
    "updated_at_ts": pl.Int64,  # CoinGecko's last update timestamp (µs)
    "fetched_at_ts": pl.Int64,  # When we fetched from API (µs)
    "source": pl.Utf8,
}


def normalize_dim_asset_stats_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical dim_asset_stats schema.

    Args:
        df: DataFrame with dim_asset_stats columns

    Returns:
        DataFrame with normalized schema

    Raises:
        ValueError: If required columns are missing
    """
    missing = [col for col in SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"dim_asset_stats missing required columns: {missing}")

    # Cast columns to schema types
    # Handle nullable columns (total_supply, max_supply, market_cap_usd, fully_diluted_valuation_usd)
    result = df.with_columns(
        [
            pl.col(col).cast(dtype) if col not in ("total_supply", "max_supply", "market_cap_usd", "fully_diluted_valuation_usd")
            else pl.col(col).cast(dtype, strict=False)  # Allow nulls for these columns
            for col, dtype in SCHEMA.items()
        ]
    )

    return result


def required_dim_asset_stats_columns() -> tuple[str, ...]:
    """Columns required for a dim_asset_stats DataFrame."""
    return tuple(SCHEMA.keys())
