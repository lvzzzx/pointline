"""Schema normalization helpers for v2 ingestion."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import TableSpec


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

    casts = [pl.col(col).cast(dtype) for col, dtype in spec.to_polars().items()]
    return normalized.with_columns(casts).select(spec.columns())
