"""Canonicalization helpers from Quant360 parser frames to v2 table-ready frames."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE

_VALID_EXEC_TYPES = {"F", "4"}


def canonicalize_quant360_frame(df: pl.DataFrame, *, table_name: str) -> pl.DataFrame:
    if table_name == "cn_order_events":
        return _canonicalize_order_events(df)
    if table_name == "cn_tick_events":
        return _canonicalize_tick_events(df)
    if table_name == "cn_l2_snapshots":
        return _canonicalize_l2_snapshots(df)
    return df


def _canonicalize_order_events(df: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        df,
        [
            "exchange",
            "symbol",
            "ts_event_us",
            "appl_seq_num",
            "channel_no",
            "side_raw",
            "ord_type_raw",
            "order_action_raw",
            "price_raw",
            "qty_raw",
            "biz_index_raw",
            "order_index_raw",
        ],
        context="cn_order_events",
    )

    prepared = df.with_columns(
        [
            pl.col("appl_seq_num").cast(pl.Int64).alias("event_seq"),
            pl.col("channel_no").cast(pl.Int32).alias("channel_id"),
            pl.col("appl_seq_num").cast(pl.Int64).alias("order_ref"),
            pl.col("price_raw")
            .cast(pl.Float64)
            .mul(PRICE_SCALE)
            .round()
            .cast(pl.Int64)
            .alias("price"),
            pl.col("qty_raw").cast(pl.Float64).mul(QTY_SCALE).round().cast(pl.Int64).alias("qty"),
            pl.col("biz_index_raw").cast(pl.Int64).alias("exchange_seq"),
            pl.col("order_index_raw").cast(pl.Int64).alias("exchange_order_index"),
            pl.col("side_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__side_code"),
            pl.col("ord_type_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__ord_type_code"),
            pl.col("order_action_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__order_action_code"),
        ]
    )
    return prepared.with_columns(
        [
            _canonical_side_expr("__side_code").alias("side"),
            _canonical_order_kind_expr(
                exchange_col="exchange",
                source_order_action_col="__order_action_code",
                source_ord_type_col="__ord_type_code",
            ).alias("event_kind"),
            _canonical_order_type_expr(
                exchange_col="exchange", source_ord_type_col="__ord_type_code"
            ).alias("order_type"),
        ]
    )


def _canonicalize_tick_events(df: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        df,
        [
            "exchange",
            "symbol",
            "ts_event_us",
            "appl_seq_num",
            "channel_no",
            "bid_appl_seq_num",
            "offer_appl_seq_num",
            "exec_type_raw",
            "trade_bs_flag_raw",
            "price_raw",
            "qty_raw",
            "biz_index_raw",
            "trade_index_raw",
        ],
        context="cn_tick_events",
    )

    exec_types = (
        df.select(
            pl.col("exec_type_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__exec")
        )
        .get_column("__exec")
        .unique()
        .to_list()
    )
    invalid = sorted(v for v in exec_types if v is not None and v not in _VALID_EXEC_TYPES)
    if invalid:
        raise ValueError(f"cn_tick_events: unsupported exec_type_raw values: {invalid}")

    prepared = df.with_columns(
        [
            pl.col("appl_seq_num").cast(pl.Int64).alias("event_seq"),
            pl.col("channel_no").cast(pl.Int32).alias("channel_id"),
            pl.col("bid_appl_seq_num").cast(pl.Int64).alias("bid_order_ref"),
            pl.col("offer_appl_seq_num").cast(pl.Int64).alias("ask_order_ref"),
            pl.col("exec_type_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__exec_type_code"),
            pl.col("trade_bs_flag_raw")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__trade_side_code"),
            pl.col("price_raw")
            .cast(pl.Float64)
            .mul(PRICE_SCALE)
            .round()
            .cast(pl.Int64)
            .alias("price"),
            pl.col("qty_raw").cast(pl.Float64).mul(QTY_SCALE).round().cast(pl.Int64).alias("qty"),
            pl.col("biz_index_raw").cast(pl.Int64).alias("exchange_seq"),
            pl.col("trade_index_raw").cast(pl.Int64).alias("exchange_trade_index"),
        ]
    )
    return prepared.with_columns(
        [
            pl.when(pl.col("__exec_type_code").eq("F"))
            .then(pl.lit("TRADE"))
            .when(pl.col("__exec_type_code").eq("4"))
            .then(pl.lit("CANCEL"))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("event_kind"),
            pl.when(pl.col("__trade_side_code").is_null())
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(pl.col("__trade_side_code").eq("B"))
            .then(pl.lit("BUY"))
            .when(pl.col("__trade_side_code").eq("S"))
            .then(pl.lit("SELL"))
            .when(pl.col("__trade_side_code").eq("N"))
            .then(pl.lit("UNKNOWN"))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("aggressor_side"),
        ]
    )


def _canonicalize_l2_snapshots(df: pl.DataFrame) -> pl.DataFrame:
    _require_columns(
        df,
        [
            "exchange",
            "symbol",
            "ts_event_us",
            "ts_local_us",
            "msg_seq_num",
            "image_status",
            "trading_phase_code_raw",
            "bid_price_levels",
            "bid_qty_levels",
            "ask_price_levels",
            "ask_qty_levels",
        ],
        context="cn_l2_snapshots",
    )

    return df.with_columns(
        [
            pl.col("msg_seq_num").cast(pl.Int64).alias("snapshot_seq"),
            pl.col("image_status").cast(pl.Utf8).alias("image_status"),
            pl.col("trading_phase_code_raw").cast(pl.Utf8).alias("trading_phase_code"),
            _optional_col(df, name="bid_order_count_levels", dtype=pl.List(pl.Int64)),
            _optional_col(df, name="ask_order_count_levels", dtype=pl.List(pl.Int64)),
            _optional_col(df, name="total_ask_qty", dtype=pl.Int64),
            pl.col("bid_price_levels")
            .map_elements(
                lambda xs: [int(round(float(x) * PRICE_SCALE)) for x in xs],
                return_dtype=pl.List(pl.Int64),
            )
            .alias("bid_price_levels"),
            pl.col("ask_price_levels")
            .map_elements(
                lambda xs: [int(round(float(x) * PRICE_SCALE)) for x in xs],
                return_dtype=pl.List(pl.Int64),
            )
            .alias("ask_price_levels"),
            pl.col("bid_qty_levels")
            .map_elements(lambda xs: [int(x) for x in xs], return_dtype=pl.List(pl.Int64))
            .alias("bid_qty_levels"),
            pl.col("ask_qty_levels")
            .map_elements(lambda xs: [int(x) for x in xs], return_dtype=pl.List(pl.Int64))
            .alias("ask_qty_levels"),
        ]
    )


def _require_columns(df: pl.DataFrame, required: list[str], *, context: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns: {missing}")


def _canonical_side_expr(source_side_col: str) -> pl.Expr:
    normalized = pl.col(source_side_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    return (
        pl.when(normalized.eq("1") | normalized.eq("B"))
        .then(pl.lit("BUY"))
        .when(normalized.eq("2") | normalized.eq("S"))
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("UNKNOWN"))
    )


def _canonical_order_kind_expr(
    *, exchange_col: str, source_order_action_col: str, source_ord_type_col: str
) -> pl.Expr:
    source_code = (
        pl.coalesce(
            [
                pl.col(source_order_action_col).cast(pl.Utf8),
                pl.col(source_ord_type_col).cast(pl.Utf8),
            ]
        )
        .str.strip_chars()
        .str.to_uppercase()
    )
    exchange = pl.col(exchange_col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return (
        pl.when(exchange.eq("sse"))
        .then(
            pl.when(source_code.eq("A"))
            .then(pl.lit("ADD"))
            .when(source_code.eq("D"))
            .then(pl.lit("CANCEL"))
            .otherwise(pl.lit("UNKNOWN"))
        )
        .when(exchange.eq("szse"))
        .then(pl.lit("ADD"))
        .otherwise(pl.lit("UNKNOWN"))
    )


def _canonical_order_type_expr(*, exchange_col: str, source_ord_type_col: str) -> pl.Expr:
    source_code = pl.col(source_ord_type_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    exchange = pl.col(exchange_col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return (
        pl.when(exchange.eq("szse"))
        .then(
            pl.when(source_code.eq("1"))
            .then(pl.lit("MARKET"))
            .when(source_code.eq("2"))
            .then(pl.lit("LIMIT"))
            .otherwise(pl.lit("UNKNOWN"))
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def _optional_col(df: pl.DataFrame, *, name: str, dtype: pl.DataType) -> pl.Expr:
    if name in df.columns:
        return pl.col(name).cast(dtype).alias(name)
    return pl.lit(None, dtype=dtype).alias(name)
