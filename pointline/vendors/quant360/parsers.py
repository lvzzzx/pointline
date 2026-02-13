"""Quant360 stream parsers for v2 canonical intermediate frames."""

from __future__ import annotations

import json
from collections.abc import Callable

import polars as pl

from pointline.vendors.quant360.timestamps import parse_quant360_timestamp


def _normalize_exchange(exchange: str) -> str:
    normalized = exchange.strip().lower()
    if normalized not in {"sse", "szse"}:
        raise ValueError(f"Unsupported Quant360 exchange: {exchange!r}")
    return normalized


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not normalized:
        raise ValueError("Symbol cannot be empty")
    if normalized.isdigit():
        return normalized.zfill(6)
    return normalized


def _parse_ts_expr(column: str) -> pl.Expr:
    return pl.col(column).map_elements(parse_quant360_timestamp, return_dtype=pl.Int64)


def _require_columns(df: pl.DataFrame, required: list[str], *, context: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns: {missing}")


def _check_symbol_match(df: pl.DataFrame, *, symbol: str, column: str, context: str) -> None:
    observed = (
        df.select(pl.col(column).cast(pl.Utf8).str.strip_chars().str.zfill(6).alias("__sym"))
        .get_column("__sym")
        .unique()
        .to_list()
    )
    observed_non_null = [value for value in observed if value is not None]
    mismatch = [value for value in observed_non_null if value != symbol]
    if mismatch:
        raise ValueError(
            f"{context}: Symbol mismatch. path symbol={symbol}, payload values={sorted(set(mismatch))}"
        )


def _with_null_column(df: pl.DataFrame, name: str, dtype: pl.DataType) -> pl.DataFrame:
    if name in df.columns:
        return df
    return df.with_columns(pl.lit(None, dtype=dtype).alias(name))


def _parse_fixed_depth_array(
    value: str | list[float] | list[int],
    *,
    column: str,
    expected_len: int = 10,
    cast_item: Callable[[object], int | float],
) -> list[int | float]:
    parsed: list[object]
    if isinstance(value, list):
        parsed = value
    else:
        try:
            loaded = json.loads(str(value))
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise ValueError(f"{column}: invalid array encoding {value!r}") from exc
        if not isinstance(loaded, list):
            raise ValueError(f"{column}: expected array payload")
        parsed = loaded

    if len(parsed) != expected_len:
        raise ValueError(f"{column}: expected 10 levels, got {len(parsed)}")

    return [cast_item(item) for item in parsed]


def parse_order_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame:
    exchange = _normalize_exchange(exchange)
    symbol = _normalize_symbol(symbol)

    if exchange == "sse":
        _require_columns(
            df,
            [
                "SecurityID",
                "TransactTime",
                "OrderNo",
                "Price",
                "Balance",
                "OrderBSFlag",
                "OrdType",
                "ChannelNo",
            ],
            context="parse_order_stream(sse)",
        )
        _check_symbol_match(
            df, symbol=symbol, column="SecurityID", context="parse_order_stream(sse)"
        )

        parsed = df.with_columns(
            [
                pl.lit(exchange).alias("exchange"),
                pl.lit(symbol).alias("symbol"),
                _parse_ts_expr("TransactTime").alias("ts_event_us"),
                pl.col("OrderNo").cast(pl.Int64).alias("appl_seq_num"),
                pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
                pl.col("OrderBSFlag").cast(pl.Utf8).alias("side_raw"),
                pl.col("OrdType").cast(pl.Utf8).alias("ord_type_raw"),
                pl.col("OrdType").cast(pl.Utf8).alias("order_action_raw"),
                pl.col("Price").cast(pl.Float64).alias("price_raw"),
                pl.col("Balance").cast(pl.Int64).alias("qty_raw"),
            ]
        )
        parsed = _with_null_column(parsed, "BizIndex", pl.Int64)
        parsed = _with_null_column(parsed, "OrderIndex", pl.Int64)
        return parsed.with_columns(
            [
                pl.col("BizIndex").cast(pl.Int64).alias("biz_index_raw"),
                pl.col("OrderIndex").cast(pl.Int64).alias("order_index_raw"),
            ]
        ).select(
            [
                "symbol",
                "exchange",
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
            ]
        )

    _require_columns(
        df,
        ["ApplSeqNum", "Side", "OrdType", "Price", "OrderQty", "TransactTime", "ChannelNo"],
        context="parse_order_stream(szse)",
    )
    parsed = df.with_columns(
        [
            pl.lit(exchange).alias("exchange"),
            pl.lit(symbol).alias("symbol"),
            _parse_ts_expr("TransactTime").alias("ts_event_us"),
            pl.col("ApplSeqNum").cast(pl.Int64).alias("appl_seq_num"),
            pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
            pl.col("Side").cast(pl.Utf8).alias("side_raw"),
            pl.col("OrdType").cast(pl.Utf8).alias("ord_type_raw"),
            pl.lit(None, dtype=pl.Utf8).alias("order_action_raw"),
            pl.col("Price").cast(pl.Float64).alias("price_raw"),
            pl.col("OrderQty").cast(pl.Int64).alias("qty_raw"),
            pl.lit(None, dtype=pl.Int64).alias("biz_index_raw"),
            pl.lit(None, dtype=pl.Int64).alias("order_index_raw"),
        ]
    )
    return parsed.select(
        [
            "symbol",
            "exchange",
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
        ]
    )


def parse_tick_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame:
    exchange = _normalize_exchange(exchange)
    symbol = _normalize_symbol(symbol)

    if exchange == "sse":
        _require_columns(
            df,
            [
                "SecurityID",
                "TradeTime",
                "TradePrice",
                "TradeQty",
                "BuyNo",
                "SellNo",
                "ChannelNo",
            ],
            context="parse_tick_stream(sse)",
        )
        _check_symbol_match(
            df, symbol=symbol, column="SecurityID", context="parse_tick_stream(sse)"
        )

        parsed = df.with_columns(
            [
                pl.lit(exchange).alias("exchange"),
                pl.lit(symbol).alias("symbol"),
                _parse_ts_expr("TradeTime").alias("ts_event_us"),
                (
                    pl.col("ApplSeqNum")
                    .cast(pl.Int64)
                    .fill_null(pl.col("TradeIndex").cast(pl.Int64))
                    if "ApplSeqNum" in df.columns
                    else pl.col("TradeIndex").cast(pl.Int64)
                ).alias("appl_seq_num"),
                pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
                pl.col("BuyNo").cast(pl.Int64).alias("bid_appl_seq_num"),
                pl.col("SellNo").cast(pl.Int64).alias("offer_appl_seq_num"),
                pl.lit("F", dtype=pl.Utf8).alias("exec_type_raw"),
                pl.col("TradeBSFlag").cast(pl.Utf8).alias("trade_bs_flag_raw"),
                pl.col("TradePrice").cast(pl.Float64).alias("price_raw"),
                pl.col("TradeQty").cast(pl.Int64).alias("qty_raw"),
            ]
        )
        parsed = _with_null_column(parsed, "BizIndex", pl.Int64)
        parsed = _with_null_column(parsed, "TradeIndex", pl.Int64)
        return parsed.with_columns(
            [
                pl.col("BizIndex").cast(pl.Int64).alias("biz_index_raw"),
                pl.col("TradeIndex").cast(pl.Int64).alias("trade_index_raw"),
            ]
        ).select(
            [
                "symbol",
                "exchange",
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
            ]
        )

    _require_columns(
        df,
        [
            "ApplSeqNum",
            "BidApplSeqNum",
            "OfferApplSeqNum",
            "Price",
            "Qty",
            "ExecType",
            "TransactTime",
            "ChannelNo",
        ],
        context="parse_tick_stream(szse)",
    )
    parsed = df.with_columns(
        [
            pl.lit(exchange).alias("exchange"),
            pl.lit(symbol).alias("symbol"),
            _parse_ts_expr("TransactTime").alias("ts_event_us"),
            pl.col("ApplSeqNum").cast(pl.Int64).alias("appl_seq_num"),
            pl.col("ChannelNo").cast(pl.Int32).alias("channel_no"),
            pl.col("BidApplSeqNum").cast(pl.Int64).alias("bid_appl_seq_num"),
            pl.col("OfferApplSeqNum").cast(pl.Int64).alias("offer_appl_seq_num"),
            pl.col("ExecType").cast(pl.Utf8).alias("exec_type_raw"),
            pl.lit(None, dtype=pl.Utf8).alias("trade_bs_flag_raw"),
            pl.col("Price").cast(pl.Float64).alias("price_raw"),
            pl.col("Qty").cast(pl.Int64).alias("qty_raw"),
            pl.lit(None, dtype=pl.Int64).alias("biz_index_raw"),
            pl.lit(None, dtype=pl.Int64).alias("trade_index_raw"),
        ]
    )
    return parsed.select(
        [
            "symbol",
            "exchange",
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
        ]
    )


def parse_l2_snapshot_stream(df: pl.DataFrame, *, exchange: str, symbol: str) -> pl.DataFrame:
    exchange = _normalize_exchange(exchange)
    if exchange != "szse":
        raise ValueError("parse_l2_snapshot_stream currently supports szse only")
    symbol = _normalize_symbol(symbol)

    _require_columns(
        df,
        ["MsgSeqNum", "QuotTime", "BidPrice", "BidOrderQty", "OfferPrice", "OfferOrderQty"],
        context="parse_l2_snapshot_stream(szse)",
    )

    parsed = df.with_columns(
        [
            pl.lit(exchange).alias("exchange"),
            pl.lit(symbol).alias("symbol"),
            _parse_ts_expr("QuotTime").alias("ts_event_us"),
            pl.col("SendingTime")
            .map_elements(parse_quant360_timestamp, return_dtype=pl.Int64, skip_nulls=True)
            .alias("ts_local_us")
            if "SendingTime" in df.columns
            else pl.lit(None, dtype=pl.Int64).alias("ts_local_us"),
            pl.col("MsgSeqNum").cast(pl.Int64).alias("msg_seq_num"),
            pl.col("ImageStatus").cast(pl.Utf8).alias("image_status")
            if "ImageStatus" in df.columns
            else pl.lit(None, dtype=pl.Utf8).alias("image_status"),
            pl.col("TradingPhaseCode").cast(pl.Utf8).alias("trading_phase_code_raw")
            if "TradingPhaseCode" in df.columns
            else pl.lit(None, dtype=pl.Utf8).alias("trading_phase_code_raw"),
            pl.col("BidPrice")
            .map_elements(
                lambda v: _parse_fixed_depth_array(
                    v, column="BidPrice", cast_item=float, expected_len=10
                ),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("bid_price_levels"),
            pl.col("BidOrderQty")
            .map_elements(
                lambda v: _parse_fixed_depth_array(
                    v, column="BidOrderQty", cast_item=int, expected_len=10
                ),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("bid_qty_levels"),
            pl.col("OfferPrice")
            .map_elements(
                lambda v: _parse_fixed_depth_array(
                    v, column="OfferPrice", cast_item=float, expected_len=10
                ),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("ask_price_levels"),
            pl.col("OfferOrderQty")
            .map_elements(
                lambda v: _parse_fixed_depth_array(
                    v, column="OfferOrderQty", cast_item=int, expected_len=10
                ),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("ask_qty_levels"),
        ]
    )
    return parsed.select(
        [
            "symbol",
            "exchange",
            "ts_event_us",
            "ts_local_us",
            "msg_seq_num",
            "image_status",
            "trading_phase_code_raw",
            "bid_price_levels",
            "bid_qty_levels",
            "ask_price_levels",
            "ask_qty_levels",
        ]
    )
