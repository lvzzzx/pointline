from __future__ import annotations

import polars as pl

from pointline.schemas.registry import get_table_spec, list_table_specs
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE


def test_quant360_cn_tables_registered() -> None:
    tables = set(list_table_specs())
    assert {"cn_order_events", "cn_tick_events", "cn_l2_snapshots"} <= tables


def test_cn_order_events_contract() -> None:
    spec = get_table_spec("cn_order_events")
    required = set(spec.required_columns())
    schema = spec.to_polars()

    assert spec.partition_by == ("exchange", "trading_date")
    assert {"file_id", "file_seq", "symbol_id", "ts_event_us"} <= required
    assert {"channel_id", "event_seq", "order_ref", "event_kind", "side"} <= required
    assert spec.tie_break_keys == (
        "exchange",
        "symbol_id",
        "trading_date",
        "channel_id",
        "event_seq",
        "file_id",
        "file_seq",
    )
    assert spec.scale_for("price") == PRICE_SCALE
    assert spec.scale_for("qty") == QTY_SCALE
    assert "exchange_seq" in schema
    assert "exchange_order_index" in schema
    assert "source_exchange_seq" not in schema
    assert "source_exchange_order_index" not in schema


def test_cn_tick_events_contract() -> None:
    spec = get_table_spec("cn_tick_events")
    required = set(spec.required_columns())
    schema = spec.to_polars()

    assert spec.partition_by == ("exchange", "trading_date")
    assert {"file_id", "file_seq", "symbol_id", "ts_event_us"} <= required
    assert {"channel_id", "event_seq", "event_kind"} <= required
    assert spec.tie_break_keys == (
        "exchange",
        "symbol_id",
        "trading_date",
        "channel_id",
        "event_seq",
        "file_id",
        "file_seq",
    )
    assert spec.scale_for("price") == PRICE_SCALE
    assert spec.scale_for("qty") == QTY_SCALE
    assert "exchange_seq" in schema
    assert "exchange_trade_index" in schema
    assert "source_exchange_seq" not in schema
    assert "source_exchange_trade_index" not in schema


def test_cn_l2_snapshots_contract() -> None:
    spec = get_table_spec("cn_l2_snapshots")
    required = set(spec.required_columns())
    schema = spec.to_polars()

    assert spec.partition_by == ("exchange", "trading_date")
    assert {"file_id", "file_seq", "symbol_id", "ts_event_us"} <= required
    assert {"snapshot_seq"} <= required
    assert spec.tie_break_keys == (
        "exchange",
        "symbol_id",
        "ts_event_us",
        "snapshot_seq",
        "file_id",
        "file_seq",
    )
    assert schema["bid_price_levels"] == pl.List(pl.Int64)
    assert schema["bid_qty_levels"] == pl.List(pl.Int64)
    assert schema["ask_price_levels"] == pl.List(pl.Int64)
    assert schema["ask_qty_levels"] == pl.List(pl.Int64)
    assert "image_status" in schema
    assert "trading_phase_code" in schema
    assert "source_image_status_raw" not in schema
    assert "source_trading_phase_raw" not in schema
