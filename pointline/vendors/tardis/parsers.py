"""Tardis CSV parsers for v2 canonical event frames."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE


def _require_columns(df: pl.DataFrame, required: list[str], *, context: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns: {missing}")


def _resolve_ts_event_expr(df: pl.DataFrame, *, context: str) -> pl.Expr:
    has_timestamp = "timestamp" in df.columns
    has_local_timestamp = "local_timestamp" in df.columns
    if not has_timestamp and not has_local_timestamp:
        raise ValueError(
            f"{context}: missing required timestamp columns; expected 'timestamp' "
            "or 'local_timestamp'"
        )
    if has_timestamp and has_local_timestamp:
        return pl.coalesce(
            [
                pl.col("timestamp").cast(pl.Int64),
                pl.col("local_timestamp").cast(pl.Int64),
            ]
        )
    if has_timestamp:
        return pl.col("timestamp").cast(pl.Int64)
    return pl.col("local_timestamp").cast(pl.Int64)


def _resolve_ts_local_expr(df: pl.DataFrame) -> pl.Expr:
    if "local_timestamp" in df.columns:
        return pl.col("local_timestamp").cast(pl.Int64)
    return pl.lit(None, dtype=pl.Int64)


def _scaled_expr(column: str, *, scale: int) -> pl.Expr:
    return pl.col(column).cast(pl.Float64).mul(scale).round().cast(pl.Int64)


def _optional_utf8(df: pl.DataFrame, *, column: str) -> pl.Expr:
    if column in df.columns:
        return pl.col(column).cast(pl.Utf8)
    return pl.lit(None, dtype=pl.Utf8)


def _first_present_int64(df: pl.DataFrame, *, candidates: tuple[str, ...]) -> pl.Expr:
    for candidate in candidates:
        if candidate in df.columns:
            return pl.col(candidate).cast(pl.Int64)
    return pl.lit(None, dtype=pl.Int64)


def _optional_float64(df: pl.DataFrame, *, column: str) -> pl.Expr:
    if column in df.columns:
        return pl.col(column).cast(pl.Float64)
    return pl.lit(None, dtype=pl.Float64)


def _optional_scaled(df: pl.DataFrame, *, column: str, scale: int) -> pl.Expr:
    if column in df.columns:
        return _scaled_expr(column, scale=scale)
    return pl.lit(None, dtype=pl.Int64)


def _optional_int64(df: pl.DataFrame, *, column: str) -> pl.Expr:
    if column in df.columns:
        return pl.col(column).cast(pl.Int64)
    return pl.lit(None, dtype=pl.Int64)


def _require_non_null_ts_event(df: pl.DataFrame, *, context: str) -> None:
    if "ts_event_us" not in df.columns:
        raise ValueError(f"{context}: ts_event_us missing after parsing")
    if df.get_column("ts_event_us").null_count() > 0:
        raise ValueError(f"{context}: ts_event_us cannot be null")


def parse_tardis_trades(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis trades CSV rows into canonical `trades` columns.

    Exchange and symbol are read from CSV row data (self-contained).
    Supports grouped-symbol files containing multiple instruments.
    """

    context = "parse_tardis_trades"
    _require_columns(df, ["exchange", "symbol", "side", "price", "amount"], context=context)

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            _optional_utf8(df, column="id").alias("trade_id"),
            pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("side"),
            pl.lit(None, dtype=pl.Boolean).alias("is_buyer_maker"),
            _scaled_expr("price", scale=PRICE_SCALE).alias("price"),
            _scaled_expr("amount", scale=QTY_SCALE).alias("qty"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "trade_id",
            "side",
            "is_buyer_maker",
            "price",
            "qty",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed


def parse_tardis_quotes(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis quotes CSV rows into canonical `quotes` columns."""

    context = "parse_tardis_quotes"
    _require_columns(
        df,
        ["exchange", "symbol", "bid_price", "bid_amount", "ask_price", "ask_amount"],
        context=context,
    )

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            _scaled_expr("bid_price", scale=PRICE_SCALE).alias("bid_price"),
            _scaled_expr("bid_amount", scale=QTY_SCALE).alias("bid_qty"),
            _scaled_expr("ask_price", scale=PRICE_SCALE).alias("ask_price"),
            _scaled_expr("ask_amount", scale=QTY_SCALE).alias("ask_qty"),
            _first_present_int64(
                df,
                candidates=("seq_num", "sequence_number", "last_update_id", "update_id"),
            ).alias("seq_num"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "bid_price",
            "bid_qty",
            "ask_price",
            "ask_qty",
            "seq_num",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed


def parse_tardis_incremental_l2(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis incremental book rows into canonical `orderbook_updates` columns.

    Exchange and symbol are read from CSV row data (self-contained).
    Supports grouped-symbol files containing multiple instruments.
    """

    context = "parse_tardis_incremental_l2"
    _require_columns(
        df, ["exchange", "symbol", "is_snapshot", "side", "price", "amount"], context=context
    )

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            _first_present_int64(
                df,
                candidates=("book_seq", "sequence_number", "seq_num", "update_id"),
            ).alias("book_seq"),
            pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("side"),
            _scaled_expr("price", scale=PRICE_SCALE).alias("price"),
            _scaled_expr("amount", scale=QTY_SCALE).alias("qty"),
            pl.col("is_snapshot").cast(pl.Boolean).alias("is_snapshot"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "book_seq",
            "side",
            "price",
            "qty",
            "is_snapshot",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed


def parse_tardis_derivative_ticker(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis derivative_ticker CSV rows into canonical columns.

    Scales price fields via PRICE_SCALE and open_interest via QTY_SCALE.
    Keeps funding_rate / predicted_funding_rate as raw Float64.
    """

    context = "parse_tardis_derivative_ticker"
    _require_columns(df, ["exchange", "symbol", "mark_price"], context=context)

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            _scaled_expr("mark_price", scale=PRICE_SCALE).alias("mark_price"),
            _optional_scaled(df, column="index_price", scale=PRICE_SCALE).alias("index_price"),
            _optional_scaled(df, column="last_price", scale=PRICE_SCALE).alias("last_price"),
            _optional_scaled(df, column="open_interest", scale=QTY_SCALE).alias("open_interest"),
            _optional_float64(df, column="funding_rate").alias("funding_rate"),
            _optional_float64(df, column="predicted_funding_rate").alias("predicted_funding_rate"),
            _optional_int64(df, column="funding_timestamp").alias("funding_timestamp"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "mark_price",
            "index_price",
            "last_price",
            "open_interest",
            "funding_rate",
            "predicted_funding_rate",
            "funding_timestamp",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed


def parse_tardis_liquidations(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis liquidations CSV rows into canonical columns.

    Nearly identical to trades: scales price/amount, maps side, optional id.
    """

    context = "parse_tardis_liquidations"
    _require_columns(df, ["exchange", "symbol", "side", "price", "amount"], context=context)

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            _optional_utf8(df, column="id").alias("liquidation_id"),
            pl.col("side").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("side"),
            _scaled_expr("price", scale=PRICE_SCALE).alias("price"),
            _scaled_expr("amount", scale=QTY_SCALE).alias("qty"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "liquidation_id",
            "side",
            "price",
            "qty",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed


def parse_tardis_options_chain(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Tardis options_chain CSV rows into canonical columns.

    Scales strike and price columns via PRICE_SCALE, quantities via QTY_SCALE.
    Keeps IV and Greeks as raw Float64. Maps ``type`` → ``option_type``.
    """

    context = "parse_tardis_options_chain"
    _require_columns(
        df,
        ["exchange", "symbol", "type", "strike_price", "expiration"],
        context=context,
    )

    parsed = df.with_columns(
        [
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
            _resolve_ts_event_expr(df, context=context).alias("ts_event_us"),
            _resolve_ts_local_expr(df).alias("ts_local_us"),
            pl.col("type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("option_type"),
            _scaled_expr("strike_price", scale=PRICE_SCALE).alias("strike"),
            pl.col("expiration").cast(pl.Int64).alias("expiration_ts_us"),
            _optional_scaled(df, column="open_interest", scale=QTY_SCALE).alias("open_interest"),
            _optional_scaled(df, column="last_price", scale=PRICE_SCALE).alias("last_price"),
            _optional_scaled(df, column="bid_price", scale=PRICE_SCALE).alias("bid_price"),
            _optional_scaled(df, column="bid_amount", scale=QTY_SCALE).alias("bid_qty"),
            _optional_float64(df, column="bid_iv").alias("bid_iv"),
            _optional_scaled(df, column="ask_price", scale=PRICE_SCALE).alias("ask_price"),
            _optional_scaled(df, column="ask_amount", scale=QTY_SCALE).alias("ask_qty"),
            _optional_float64(df, column="ask_iv").alias("ask_iv"),
            _optional_scaled(df, column="mark_price", scale=PRICE_SCALE).alias("mark_price"),
            _optional_float64(df, column="mark_iv").alias("mark_iv"),
            _optional_utf8(df, column="underlying_index").alias("underlying_index"),
            _optional_scaled(df, column="underlying_price", scale=PRICE_SCALE).alias(
                "underlying_price"
            ),
            _optional_float64(df, column="delta").alias("delta"),
            _optional_float64(df, column="gamma").alias("gamma"),
            _optional_float64(df, column="vega").alias("vega"),
            _optional_float64(df, column="theta").alias("theta"),
            _optional_float64(df, column="rho").alias("rho"),
        ]
    ).select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "option_type",
            "strike",
            "expiration_ts_us",
            "open_interest",
            "last_price",
            "bid_price",
            "bid_qty",
            "bid_iv",
            "ask_price",
            "ask_qty",
            "ask_iv",
            "mark_price",
            "mark_iv",
            "underlying_index",
            "underlying_price",
            "delta",
            "gamma",
            "vega",
            "theta",
            "rho",
        ]
    )

    _require_non_null_ts_event(parsed, context=context)
    return parsed
