"""Canonical v2 dimension table specs."""

from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE, ColumnSpec, TableSpec

DIM_SYMBOL = TableSpec(
    name="dim_symbol",
    kind="dimension",
    column_specs=(
        ColumnSpec("symbol_id", pl.Int64),
        ColumnSpec("exchange", pl.Utf8),
        ColumnSpec("exchange_symbol", pl.Utf8),
        ColumnSpec("canonical_symbol", pl.Utf8),
        ColumnSpec("market_type", pl.Utf8),
        ColumnSpec("base_asset", pl.Utf8),
        ColumnSpec("quote_asset", pl.Utf8),
        ColumnSpec("valid_from_ts_us", pl.Int64),
        ColumnSpec("valid_until_ts_us", pl.Int64),
        ColumnSpec("is_current", pl.Boolean),
        ColumnSpec("tick_size", pl.Int64, nullable=True, scale=PRICE_SCALE),
        ColumnSpec("lot_size", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("contract_size", pl.Int64, nullable=True, scale=QTY_SCALE),
        ColumnSpec("updated_at_ts_us", pl.Int64),
    ),
    partition_by=(),
    business_keys=("exchange", "exchange_symbol", "valid_from_ts_us"),
    tie_break_keys=("exchange", "exchange_symbol", "valid_from_ts_us"),
    schema_version="v2",
)


DIMENSION_SPECS: tuple[TableSpec, ...] = (DIM_SYMBOL,)
