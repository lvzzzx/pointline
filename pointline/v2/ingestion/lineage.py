"""Deterministic lineage assignment helpers."""

from __future__ import annotations

import polars as pl


def assign_lineage(
    df: pl.DataFrame,
    file_id: int,
    *,
    file_seq_col: str = "file_seq",
    line_number_col: str = "file_line_number",
) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(
            pl.lit(file_id, dtype=pl.Int64).alias("file_id"),
            pl.lit(None, dtype=pl.Int64).alias(file_seq_col),
        )

    if file_seq_col in df.columns:
        seq_expr = pl.col(file_seq_col).cast(pl.Int64)
    elif line_number_col in df.columns:
        seq_expr = pl.col(line_number_col).cast(pl.Int64)
    else:
        seq_expr = (pl.int_range(0, pl.len(), eager=False) + 1).cast(pl.Int64)

    return df.with_columns(
        pl.lit(file_id, dtype=pl.Int64).alias("file_id"),
        seq_expr.alias(file_seq_col),
    )
