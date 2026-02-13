"""Canonical v2 China A-share event table specs (vendor-agnostic semantics)."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, ColumnSpec, TableSpec


def _common_cn_event_columns() -> tuple[ColumnSpec, ...]:
    return (
        ColumnSpec("exchange", pl.Utf8),
        ColumnSpec("trading_date", pl.Date),
        ColumnSpec("symbol", pl.Utf8),
        ColumnSpec("symbol_id", pl.Int64),
        ColumnSpec("ts_event_us", pl.Int64),
        ColumnSpec("ts_local_us", pl.Int64, nullable=True),
        ColumnSpec("file_id", pl.Int64),
        ColumnSpec("file_seq", pl.Int64),
    )


CN_ORDER_EVENTS = TableSpec(
    name="cn_order_events",
    kind="event",
    column_specs=(
        *_common_cn_event_columns(),
        # Sequences: different scopes and semantics
        ColumnSpec("channel_id", pl.Int32),  # Feed channel (1-6)
        ColumnSpec("channel_seq", pl.Int64),  # Per-channel feed sequence (ApplSeqNum)
        ColumnSpec(
            "channel_biz_seq", pl.Int64, nullable=True
        ),  # Per-channel business seq (BizIndex)
        ColumnSpec("symbol_order_seq", pl.Int64, nullable=True),  # Per-symbol order counter
        ColumnSpec("order_ref", pl.Int64),  # Order reference (maps to channel_seq)
        # Event details
        ColumnSpec("event_kind", pl.Utf8),
        ColumnSpec("side", pl.Utf8),
        ColumnSpec("order_type", pl.Utf8, nullable=True),
        ColumnSpec("price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("qty", pl.Int64, scale=QTY_SCALE),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=(
        "exchange",
        "symbol_id",
        "trading_date",
        "channel_id",
        "channel_seq",
        "file_id",
        "file_seq",
    ),
    schema_version="v2",
)


CN_TICK_EVENTS = TableSpec(
    name="cn_tick_events",
    kind="event",
    column_specs=(
        *_common_cn_event_columns(),
        # Sequences: different scopes and semantics
        ColumnSpec("channel_id", pl.Int32),  # Feed channel (1-6)
        ColumnSpec("channel_seq", pl.Int64),  # Per-channel feed sequence (ApplSeqNum)
        ColumnSpec(
            "channel_biz_seq", pl.Int64, nullable=True
        ),  # Per-channel business seq (BizIndex)
        ColumnSpec("symbol_trade_seq", pl.Int64, nullable=True),  # Per-symbol trade counter
        # Order references
        ColumnSpec("bid_order_ref", pl.Int64, nullable=True),
        ColumnSpec("ask_order_ref", pl.Int64, nullable=True),
        # Trade details
        ColumnSpec("event_kind", pl.Utf8),
        ColumnSpec("aggressor_side", pl.Utf8, nullable=True),
        ColumnSpec("price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("qty", pl.Int64, scale=QTY_SCALE),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=(
        "exchange",
        "symbol_id",
        "trading_date",
        "channel_id",
        "channel_seq",
        "file_id",
        "file_seq",
    ),
    schema_version="v2",
)


CN_L2_SNAPSHOTS = TableSpec(
    name="cn_l2_snapshots",
    kind="event",
    column_specs=(
        *_common_cn_event_columns(),
        ColumnSpec("snapshot_seq", pl.Int64),
        ColumnSpec("image_status", pl.Utf8, nullable=True),
        ColumnSpec("trading_phase_code", pl.Utf8, nullable=True),
        ColumnSpec("bid_price_levels", pl.List(pl.Int64)),
        ColumnSpec("bid_qty_levels", pl.List(pl.Int64)),
        ColumnSpec("ask_price_levels", pl.List(pl.Int64)),
        ColumnSpec("ask_qty_levels", pl.List(pl.Int64)),
        ColumnSpec("bid_order_count_levels", pl.List(pl.Int64), nullable=True),
        ColumnSpec("ask_order_count_levels", pl.List(pl.Int64), nullable=True),
        ColumnSpec("pre_close_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("open_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("high_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("low_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("last_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("volume", pl.Int64, nullable=True),
        ColumnSpec("amount", pl.Int64, nullable=True),
        ColumnSpec("num_trades", pl.Int64, nullable=True),
        ColumnSpec("total_bid_qty", pl.Int64, nullable=True),
        ColumnSpec("total_ask_qty", pl.Int64, nullable=True),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=(
        "exchange",
        "symbol_id",
        "ts_event_us",
        "snapshot_seq",
        "file_id",
        "file_seq",
    ),
    schema_version="v2",
)


CN_EVENT_SPECS: tuple[TableSpec, ...] = (CN_ORDER_EVENTS, CN_TICK_EVENTS, CN_L2_SNAPSHOTS)
