"""Satellite dimension table for options contract attributes.

Extracts sparse options-specific columns from dim_symbol into a dedicated table
keyed by symbol_id. This avoids 90%+ null values in the main dim_symbol table.

Schema:
    symbol_id (PK), strike, put_call, exercise_style, expiry_ts_us,
    underlying_symbol_id
"""

from __future__ import annotations

import polars as pl

DIM_OPTIONS_CONTRACT_SCHEMA: dict[str, pl.DataType] = {
    "symbol_id": pl.Int64,
    "strike": pl.Float64,
    "put_call": pl.Utf8,  # "put" / "call"
    "exercise_style": pl.Utf8,  # "american" / "european" (nullable)
    "expiry_ts_us": pl.Int64,
    "underlying_symbol_id": pl.Int64,
}


def normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to canonical schema, filling missing nullable columns with null."""
    for col, dtype in DIM_OPTIONS_CONTRACT_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    return df.with_columns(
        [pl.col(col).cast(dtype) for col, dtype in DIM_OPTIONS_CONTRACT_SCHEMA.items()]
    ).select(list(DIM_OPTIONS_CONTRACT_SCHEMA.keys()))


def canonical_columns() -> tuple[str, ...]:
    """Return ordered tuple of canonical column names."""
    return tuple(DIM_OPTIONS_CONTRACT_SCHEMA.keys())


def bootstrap_from_dim_symbol(dim_symbol: pl.DataFrame) -> pl.DataFrame:
    """Extract options contract rows from dim_symbol.

    Filters for asset_type == 3 (option) and selects satellite-exclusive columns.
    Only includes rows where at least one satellite column is non-null.

    Args:
        dim_symbol: Full dim_symbol DataFrame.

    Returns:
        DataFrame conforming to DIM_OPTIONS_CONTRACT_SCHEMA.
    """
    options = dim_symbol.filter(pl.col("asset_type") == 3)

    if options.is_empty():
        return pl.DataFrame(schema=DIM_OPTIONS_CONTRACT_SCHEMA)

    # Select columns that exist in dim_symbol
    select_cols = ["symbol_id"]
    satellite_cols = ["strike", "put_call", "expiry_ts_us", "underlying_symbol_id"]
    for col in satellite_cols:
        if col in options.columns:
            select_cols.append(col)

    result = options.select(select_cols)

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

_register_schema("dim_options_contract", DIM_OPTIONS_CONTRACT_SCHEMA)
