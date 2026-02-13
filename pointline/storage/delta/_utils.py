"""Internal helpers shared by v2 Delta storage adapters."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from pointline.schemas.types import TableSpec


def empty_frame_for_spec(spec: TableSpec) -> pl.DataFrame:
    return pl.DataFrame(schema=spec.to_polars())


def validate_against_spec(df: pl.DataFrame, spec: TableSpec) -> None:
    expected_schema = spec.to_polars()
    expected_cols = set(expected_schema)
    actual_cols = set(df.columns)

    missing = sorted(expected_cols - actual_cols)
    if missing:
        raise ValueError(f"{spec.name}: missing columns {missing}")

    unexpected = sorted(actual_cols - expected_cols)
    if unexpected:
        raise ValueError(f"{spec.name}: unexpected columns {unexpected}")

    dtype_errors: list[str] = []
    for column, expected_dtype in expected_schema.items():
        actual_dtype = df.schema[column]
        if actual_dtype != expected_dtype:
            dtype_errors.append(f"{column}: expected {expected_dtype}, got {actual_dtype}")
    if dtype_errors:
        detail = "; ".join(dtype_errors)
        raise ValueError(f"{spec.name}: dtype mismatch ({detail})")


def normalize_to_spec(df: pl.DataFrame, spec: TableSpec) -> pl.DataFrame:
    casts = [pl.col(name).cast(dtype) for name, dtype in spec.to_polars().items()]
    return df.with_columns(casts).select(spec.columns())


def read_delta_or_empty(path: Path, *, spec: TableSpec) -> pl.DataFrame:
    if not path.exists():
        return empty_frame_for_spec(spec)
    try:
        df = pl.read_delta(str(path))
    except Exception:
        return empty_frame_for_spec(spec)
    return normalize_to_spec(df, spec)


def append_delta(path: Path, *, df: pl.DataFrame, partition_by: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "append" if path.exists() else "overwrite"
    kwargs: dict[str, object] = {}
    if partition_by:
        kwargs["partition_by"] = list(partition_by)
    write_deltalake(str(path), df.to_arrow(), mode=mode, **kwargs)


def overwrite_delta(path: Path, *, df: pl.DataFrame, partition_by: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, object] = {}
    if partition_by:
        kwargs["partition_by"] = list(partition_by)
    write_deltalake(str(path), df.to_arrow(), mode="overwrite", **kwargs)
