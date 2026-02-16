"""Canonical v2 crypto-specific event table specs."""

from __future__ import annotations

import polars as pl

from pointline.schemas.events import _common_event_columns
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, ColumnSpec, TableSpec

DERIVATIVE_TICKER = TableSpec(
    name="derivative_ticker",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("mark_price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("index_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("last_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("open_interest", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("funding_rate", pl.Float64, nullable=True),
        ColumnSpec("predicted_funding_rate", pl.Float64, nullable=True),
        ColumnSpec("funding_ts_us", pl.Int64, nullable=True),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=("exchange", "symbol_id", "ts_event_us", "file_id", "file_seq"),
    schema_version="v2",
)

LIQUIDATIONS = TableSpec(
    name="liquidations",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("liquidation_id", pl.Utf8, nullable=True),
        ColumnSpec("side", pl.Utf8),
        ColumnSpec("price", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("qty", pl.Int64, scale=QTY_SCALE),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=("exchange", "symbol_id", "ts_event_us", "file_id", "file_seq"),
    schema_version="v2",
)

OPTIONS_CHAIN = TableSpec(
    name="options_chain",
    kind="event",
    column_specs=(
        *_common_event_columns(),
        ColumnSpec("option_type", pl.Utf8),
        ColumnSpec("strike", pl.Int64, scale=PRICE_SCALE),
        ColumnSpec("expiration_ts_us", pl.Int64),
        ColumnSpec("open_interest", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("last_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("bid_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("bid_qty", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("bid_iv", pl.Float64, nullable=True),
        ColumnSpec("ask_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("ask_qty", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("ask_iv", pl.Float64, nullable=True),
        ColumnSpec("mark_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("mark_iv", pl.Float64, nullable=True),
        ColumnSpec("underlying_index", pl.Utf8, nullable=True),
        ColumnSpec("underlying_price", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("delta", pl.Float64, nullable=True),
        ColumnSpec("gamma", pl.Float64, nullable=True),
        ColumnSpec("vega", pl.Float64, nullable=True),
        ColumnSpec("theta", pl.Float64, nullable=True),
        ColumnSpec("rho", pl.Float64, nullable=True),
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=(),
    tie_break_keys=("exchange", "symbol_id", "ts_event_us", "file_id", "file_seq"),
    schema_version="v2",
)

CRYPTO_EVENT_SPECS: tuple[TableSpec, ...] = (DERIVATIVE_TICKER, LIQUIDATIONS, OPTIONS_CHAIN)
