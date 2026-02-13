"""Explicit v2 research primitives."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.schemas.registry import get_table_spec
from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore


def decode_scaled_columns(
    df: pl.DataFrame,
    table: str,
    *,
    columns: list[str] | None = None,
    in_place: bool = False,
    suffix: str = "_decoded",
) -> pl.DataFrame:
    """Decode fixed-point scaled integer columns to floating values.

    By default, decoded values are added as ``<column>_decoded`` and original
    scaled integer columns are preserved.
    """
    spec = get_table_spec(table)
    scaled_cols = spec.scaled_columns()
    scaled_set = set(scaled_cols)

    if columns is None:
        target_cols = [name for name in scaled_cols if name in df.columns]
    else:
        requested = list(dict.fromkeys(columns))
        unknown = sorted(set(requested) - scaled_set)
        if unknown:
            raise ValueError(f"Requested non-scaled columns for decode: {unknown}")

        missing = [name for name in requested if name not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in frame for decode: {missing}")
        target_cols = requested

    if not target_cols:
        return df

    if not in_place and not suffix:
        raise ValueError("suffix must be non-empty when in_place=False")

    exprs: list[pl.Expr] = []
    for col in target_cols:
        scale = spec.scale_for(col)
        if scale is None:
            continue

        decoded = pl.col(col).cast(pl.Float64).truediv(float(scale))
        digits = _power_of_ten_digits(scale)
        if digits is not None:
            decoded = decoded.round(digits)

        out_col = col if in_place else f"{col}{suffix}"
        exprs.append(decoded.alias(out_col))

    if not exprs:
        return df
    return df.with_columns(exprs)


def join_symbol_meta(
    df: pl.DataFrame,
    *,
    silver_root: Path,
    columns: list[str],
    ts_col: str = "ts_event_us",
) -> pl.DataFrame:
    """Attach PIT symbol metadata from dim_symbol to event rows.

    Joins on ``exchange`` + ``symbol_id`` and applies interval semantics:
    ``valid_from_ts_us <= ts_col < valid_until_ts_us``.
    """
    meta_cols = _resolve_meta_columns(columns)
    if not meta_cols:
        return df

    for required in ("exchange", "symbol_id", ts_col):
        if required not in df.columns:
            raise ValueError(f"events missing required column for meta join: {required}")
    overlap = sorted(set(meta_cols) & set(df.columns))
    if overlap:
        raise ValueError(f"event frame already contains requested metadata columns: {overlap}")

    dim_schema = DIM_SYMBOL.to_polars()
    if df.is_empty():
        return df.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        )

    dim = DeltaDimensionStore(silver_root=silver_root).load_dim_symbol()
    if dim.is_empty():
        return df.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        )

    pairs = df.select(["exchange", "symbol_id"]).unique()
    dim = dim.join(pairs, on=["exchange", "symbol_id"], how="inner")
    if dim.is_empty():
        return df.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        )

    dim_cols = ["exchange", "symbol_id", "valid_from_ts_us", "valid_until_ts_us", *meta_cols]
    rows = df.with_row_index("_row_id")
    matches = rows.join(
        dim.select(dim_cols),
        on=["exchange", "symbol_id"],
        how="left",
    ).filter(
        (pl.col("valid_from_ts_us") <= pl.col(ts_col))
        & (pl.col(ts_col) < pl.col("valid_until_ts_us"))
    )

    if matches.is_empty():
        return rows.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        ).drop("_row_id")

    per_row_meta = matches.group_by("_row_id").agg(
        [pl.col(col).first().alias(col) for col in meta_cols]
    )
    return rows.join(per_row_meta, on="_row_id", how="left").drop("_row_id")


def _resolve_meta_columns(columns: list[str]) -> list[str]:
    dim_schema = DIM_SYMBOL.to_polars()
    forbidden = {"exchange", "symbol_id", "valid_from_ts_us", "valid_until_ts_us"}
    allowed = set(dim_schema) - forbidden
    deduped = list(dict.fromkeys(columns))
    unknown = sorted(set(deduped) - allowed)
    if unknown:
        raise ValueError(f"Unknown symbol metadata columns requested: {unknown}")
    return deduped


def _power_of_ten_digits(scale: int) -> int | None:
    """Return decimal digits if scale is a power of 10; otherwise None."""
    if scale <= 0:
        return None
    n = scale
    digits = 0
    while n % 10 == 0:
        n //= 10
        digits += 1
    if n == 1:
        return digits
    return None
