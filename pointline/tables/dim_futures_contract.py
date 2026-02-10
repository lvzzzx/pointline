"""Satellite dimension table for futures contract attributes.

Extracts sparse futures-specific columns from dim_symbol into a dedicated table
keyed by symbol_id. This avoids 90%+ null values in the main dim_symbol table.

Schema:
    symbol_id (PK), expiry_ts_us, underlying_symbol_id, settlement_type,
    contract_month, multiplier
"""

from __future__ import annotations

import polars as pl

DIM_FUTURES_CONTRACT_SCHEMA: dict[str, pl.DataType] = {
    "symbol_id": pl.Int64,
    "expiry_ts_us": pl.Int64,
    "underlying_symbol_id": pl.Int64,
    "settlement_type": pl.Utf8,
    "contract_month": pl.Utf8,  # e.g., "2024-06"
    "multiplier": pl.Float64,
}


def normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to canonical schema, filling missing nullable columns with null."""
    for col, dtype in DIM_FUTURES_CONTRACT_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    return df.with_columns(
        [pl.col(col).cast(dtype) for col, dtype in DIM_FUTURES_CONTRACT_SCHEMA.items()]
    ).select(list(DIM_FUTURES_CONTRACT_SCHEMA.keys()))


def canonical_columns() -> tuple[str, ...]:
    """Return ordered tuple of canonical column names."""
    return tuple(DIM_FUTURES_CONTRACT_SCHEMA.keys())


def bootstrap_from_dim_symbol(dim_symbol: pl.DataFrame) -> pl.DataFrame:
    """Extract futures contract rows from dim_symbol.

    Filters for asset_type == 2 (future) and selects satellite-exclusive columns.
    Only includes rows where at least one satellite column is non-null.

    Args:
        dim_symbol: Full dim_symbol DataFrame.

    Returns:
        DataFrame conforming to DIM_FUTURES_CONTRACT_SCHEMA.
    """
    futures = dim_symbol.filter(pl.col("asset_type") == 2)

    if futures.is_empty():
        return pl.DataFrame(schema=DIM_FUTURES_CONTRACT_SCHEMA)

    # Select columns that exist in dim_symbol
    select_cols = ["symbol_id"]
    satellite_cols = ["expiry_ts_us", "underlying_symbol_id", "settlement_type"]
    for col in satellite_cols:
        if col in futures.columns:
            select_cols.append(col)

    result = futures.select(select_cols)

    # Filter: keep only rows where at least one satellite col is non-null
    satellite_present = [col for col in satellite_cols if col in result.columns]
    if satellite_present:
        result = result.filter(
            pl.any_horizontal([pl.col(c).is_not_null() for c in satellite_present])
        )

    return normalize_schema(result)


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("dim_futures_contract", DIM_FUTURES_CONTRACT_SCHEMA)
