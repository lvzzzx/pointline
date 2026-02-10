"""Satellite dimension table for equity listing attributes.

Extracts sparse equity-specific columns from dim_symbol into a dedicated table
keyed by symbol_id. This avoids 90%+ null values in the main dim_symbol table.

Schema:
    symbol_id (PK), isin, cusip, figi, listing_exchange, sector, industry
"""

from __future__ import annotations

import polars as pl

DIM_EQUITY_LISTING_SCHEMA: dict[str, pl.DataType] = {
    "symbol_id": pl.Int64,
    "isin": pl.Utf8,
    "cusip": pl.Utf8,
    "figi": pl.Utf8,
    "listing_exchange": pl.Utf8,
    "sector": pl.Utf8,
    "industry": pl.Utf8,
}


def normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to canonical schema, filling missing nullable columns with null."""
    for col, dtype in DIM_EQUITY_LISTING_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    return df.with_columns(
        [pl.col(col).cast(dtype) for col, dtype in DIM_EQUITY_LISTING_SCHEMA.items()]
    ).select(list(DIM_EQUITY_LISTING_SCHEMA.keys()))


def canonical_columns() -> tuple[str, ...]:
    """Return ordered tuple of canonical column names."""
    return tuple(DIM_EQUITY_LISTING_SCHEMA.keys())


def bootstrap_from_dim_symbol(dim_symbol: pl.DataFrame) -> pl.DataFrame:
    """Extract equity listing rows from dim_symbol.

    Filters for rows where isin is not null and selects satellite-exclusive columns.

    Args:
        dim_symbol: Full dim_symbol DataFrame.

    Returns:
        DataFrame conforming to DIM_EQUITY_LISTING_SCHEMA.
    """
    if "isin" not in dim_symbol.columns:
        return pl.DataFrame(schema=DIM_EQUITY_LISTING_SCHEMA)

    equities = dim_symbol.filter(pl.col("isin").is_not_null())

    if equities.is_empty():
        return pl.DataFrame(schema=DIM_EQUITY_LISTING_SCHEMA)

    # Select columns that exist in dim_symbol
    select_cols = ["symbol_id"]
    satellite_cols = ["isin"]
    for col in satellite_cols:
        if col in equities.columns:
            select_cols.append(col)

    result = equities.select(select_cols)
    return normalize_schema(result)


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("dim_equity_listing", DIM_EQUITY_LISTING_SCHEMA)
