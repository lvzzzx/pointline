"""CN exchange-specific ingestion validation rules."""

from __future__ import annotations

import polars as pl


def apply_cn_exchange_validations(
    df: pl.DataFrame, *, table_name: str
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    """Apply row-level CN rules and return (valid, quarantined, reason)."""
    if df.is_empty():
        return df, df, None

    if table_name == "cn_order_events":
        return _quarantine_missing_sse_indices(
            df,
            required_cols=("source_exchange_seq", "source_exchange_order_index"),
            reason="missing_sse_order_sequence_fields",
        )
    if table_name == "cn_tick_events":
        return _quarantine_missing_sse_indices(
            df,
            required_cols=("source_exchange_seq", "source_exchange_trade_index"),
            reason="missing_sse_tick_sequence_fields",
        )

    return df, df.head(0), None


def _quarantine_missing_sse_indices(
    df: pl.DataFrame, *, required_cols: tuple[str, ...], reason: str
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    missing = [col for col in ("exchange", *required_cols) if col not in df.columns]
    if missing:
        raise ValueError(f"CN validation requires columns {sorted(missing)}")

    is_sse = pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().eq("sse")
    missing_any_required = pl.any_horizontal([pl.col(col).is_null() for col in required_cols])
    quarantine_mask = is_sse & missing_any_required

    quarantined = df.filter(quarantine_mask)
    valid = df.filter(~quarantine_mask)
    return valid, quarantined, None if quarantined.is_empty() else reason
