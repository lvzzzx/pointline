"""PIT symbol coverage checks for v2 ingestion."""

from __future__ import annotations

import polars as pl


def check_pit_coverage(
    df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    *,
    exchange_col: str = "exchange",
    symbol_col: str = "symbol",
    ts_col: str = "ts_event_us",
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    required_event_cols = {exchange_col, symbol_col, ts_col}
    missing_event = required_event_cols - set(df.columns)
    if missing_event:
        raise ValueError(f"Event DataFrame missing PIT columns: {sorted(missing_event)}")

    required_dim_cols = {
        "exchange",
        "exchange_symbol",
        "symbol_id",
        "valid_from_ts_us",
        "valid_until_ts_us",
    }
    missing_dim = required_dim_cols - set(dim_symbol_df.columns)
    if missing_dim:
        raise ValueError(f"dim_symbol DataFrame missing PIT columns: {sorted(missing_dim)}")

    if df.is_empty():
        return df, df, None

    if dim_symbol_df.is_empty():
        empty_valid = df.head(0).with_columns(pl.lit(None, dtype=pl.Int64).alias("symbol_id"))
        return empty_valid, df, "missing_pit_symbol_coverage"

    staged = df.with_row_index(name="_row_id")

    joined = staged.join(
        dim_symbol_df,
        left_on=[exchange_col, symbol_col],
        right_on=["exchange", "exchange_symbol"],
        how="left",
        suffix="_dim",
    )

    matches = (
        joined.filter(
            pl.col("symbol_id").is_not_null()
            & (pl.col(ts_col) >= pl.col("valid_from_ts_us"))
            & (pl.col(ts_col) < pl.col("valid_until_ts_us"))
        )
        .sort(["_row_id", "valid_from_ts_us"])
        .group_by("_row_id")
        .head(1)
        .select(["_row_id", "symbol_id"])
    )

    valid = (
        staged.join(matches, on="_row_id", how="inner")
        .drop("_row_id")
        .with_columns(pl.col("symbol_id").cast(pl.Int64))
    )
    quarantined = staged.join(matches.select("_row_id"), on="_row_id", how="anti").drop("_row_id")

    reason = None if quarantined.is_empty() else "missing_pit_symbol_coverage"
    return valid, quarantined, reason
