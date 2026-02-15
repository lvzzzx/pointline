"""Generic event-table semantic validation rules."""

from __future__ import annotations

import polars as pl


def apply_event_validations(
    df: pl.DataFrame, *, table_name: str
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    """Apply generic row-level rules and return (valid, quarantined, reason)."""
    if df.is_empty():
        return df, df, None

    if table_name == "trades":
        return _quarantine_invalid_trades(df)
    if table_name == "quotes":
        return _quarantine_invalid_quotes(df)
    if table_name == "orderbook_updates":
        return _quarantine_invalid_orderbook_updates(df)
    if table_name == "derivative_ticker":
        return _quarantine_invalid_derivative_ticker(df)
    if table_name == "liquidations":
        return _quarantine_invalid_liquidations(df)
    if table_name == "options_chain":
        return _quarantine_invalid_options_chain(df)
    return df, df.head(0), None


def _quarantine_invalid_trades(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("side", "price", "qty"))

    side_norm = pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    invalid_mask = pl.any_horizontal(
        [
            (~side_norm.is_in(["buy", "sell", "unknown"])).fill_null(True),
            (pl.col("price") <= 0).fill_null(True),
            (pl.col("qty") <= 0).fill_null(True),
        ]
    )
    return _split(df, invalid_mask, reason="invalid_trade_side_or_values")


def _quarantine_invalid_quotes(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("bid_price", "bid_qty", "ask_price", "ask_qty"))

    invalid_mask = pl.any_horizontal(
        [
            (pl.col("bid_price") <= 0).fill_null(True),
            (pl.col("ask_price") <= 0).fill_null(True),
            (pl.col("bid_qty") < 0).fill_null(True),
            (pl.col("ask_qty") < 0).fill_null(True),
            (pl.col("bid_price") > pl.col("ask_price")).fill_null(False),
        ]
    )
    return _split(df, invalid_mask, reason="invalid_quote_top_of_book")


def _quarantine_invalid_orderbook_updates(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("side", "price", "qty", "is_snapshot"))

    side_norm = pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    invalid_mask = pl.any_horizontal(
        [
            (~side_norm.is_in(["bid", "ask"])).fill_null(True),
            (pl.col("price") <= 0).fill_null(True),
            (pl.col("qty") < 0).fill_null(True),
            pl.col("is_snapshot").is_null(),
        ]
    )
    return _split(df, invalid_mask, reason="invalid_orderbook_update_values")


def _quarantine_invalid_derivative_ticker(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("mark_price",))

    invalid_mask = (pl.col("mark_price") <= 0).fill_null(True)
    return _split(df, invalid_mask, reason="invalid_derivative_ticker_mark_price")


def _quarantine_invalid_liquidations(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("side", "price", "qty"))

    side_norm = pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    invalid_mask = pl.any_horizontal(
        [
            (~side_norm.is_in(["buy", "sell"])).fill_null(True),
            (pl.col("price") <= 0).fill_null(True),
            (pl.col("qty") <= 0).fill_null(True),
        ]
    )
    return _split(df, invalid_mask, reason="invalid_liquidation_side_or_values")


def _quarantine_invalid_options_chain(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    _require_columns(df, ("option_type", "strike", "expiration_ts_us"))

    type_norm = pl.col("option_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    invalid_mask = pl.any_horizontal(
        [
            (~type_norm.is_in(["call", "put"])).fill_null(True),
            (pl.col("strike") <= 0).fill_null(True),
            (pl.col("expiration_ts_us") <= 0).fill_null(True),
        ]
    )
    return _split(df, invalid_mask, reason="invalid_options_chain_contract")


def _require_columns(df: pl.DataFrame, required_cols: tuple[str, ...]) -> None:
    missing = sorted(set(required_cols) - set(df.columns))
    if missing:
        raise ValueError(f"Generic event validation requires columns {missing}")


def _split(
    df: pl.DataFrame, invalid_mask: pl.Expr, *, reason: str
) -> tuple[pl.DataFrame, pl.DataFrame, str | None]:
    quarantined = df.filter(invalid_mask)
    valid = df.filter(~invalid_mask)
    return valid, quarantined, None if quarantined.is_empty() else reason
