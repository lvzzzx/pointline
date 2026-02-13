"""Schema normalization helpers for v2 ingestion."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import TableSpec

_INTEGER_DTYPES: tuple[pl.DataType, ...] = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
)


def normalize_to_table_spec(df: pl.DataFrame, spec: TableSpec) -> pl.DataFrame:
    missing_required = [col for col in spec.required_columns() if col not in df.columns]
    if missing_required:
        raise ValueError(
            f"Cannot normalize '{spec.name}': missing required columns {sorted(missing_required)}"
        )

    normalized = df
    for col in spec.nullable_columns():
        if col not in normalized.columns:
            normalized = normalized.with_columns(
                pl.lit(None, dtype=spec.to_polars()[col]).alias(col)
            )

    for col in spec.scaled_columns():
        if col not in normalized.columns:
            continue
        dtype = normalized.schema[col]
        if dtype == pl.Null:
            continue
        if dtype not in _INTEGER_DTYPES:
            scale = spec.scale_for(col)
            raise ValueError(
                f"Column '{col}' in '{spec.name}' must be pre-scaled Int64 (scale={scale}); "
                f"got {dtype}. Convert before normalize_to_table_spec()."
            )

    casts = [pl.col(col).cast(dtype) for col, dtype in spec.to_polars().items()]
    return normalized.with_columns(casts).select(spec.columns())
