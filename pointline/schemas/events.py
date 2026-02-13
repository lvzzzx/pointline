"""Canonical v2 event table specs."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, ColumnSpec, TableSpec


def _common_event_columns() -> tuple[ColumnSpec, ...]:
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


TRADES = TableSpec(
    name="trades",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("trade_id", pl.Utf8, nullable=True),
        ColumnSpec("side", pl.Utf8),
        ColumnSpec("is_buyer_maker", pl.Boolean, nullable=True),
        ColumnSpec("price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("qty", pl.Int64, scale=QTY_SCALE),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=("exchange", "symbol_id", "ts_event_us", "file_id", "file_seq"),
    schema_version="v2",
)


QUOTES = TableSpec(
    name="quotes",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("bid_price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("bid_qty", pl.Int64, scale=QTY_SCALE),
        ColumnSpec("ask_price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("ask_qty", pl.Int64, scale=QTY_SCALE),
        ColumnSpec("seq_num", pl.Int64, nullable=True),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=("exchange", "symbol_id", "ts_event_us", "file_id", "file_seq"),
    schema_version="v2",
)


ORDERBOOK_UPDATES = TableSpec(
    name="orderbook_updates",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("book_seq", pl.Int64, nullable=True),
        ColumnSpec("side", pl.Utf8),
        ColumnSpec("price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("qty", pl.Int64, scale=QTY_SCALE),
        ColumnSpec("is_snapshot", pl.Boolean),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=(
        "exchange",
        "symbol_id",
        "ts_event_us",
        "book_seq",
        "file_id",
        "file_seq",
    ),
    schema_version="v2",
)


EVENT_SPECS: tuple[TableSpec, ...] = (TRADES, QUOTES, ORDERBOOK_UPDATES)
