from __future__ import annotations

import polars as pl

from pointline.schemas.registry import get_table_spec, list_table_specs
from pointline.schemas.types import INGEST_STATUS_VALUES, PRICE_SCALE, QTY_SCALE


def test_registry_contains_v2_core_tables() -> None:
    tables = set(list_table_specs())
    assert {"trades", "quotes", "orderbook_updates", "dim_symbol", "ingest_manifest"} <= tables


def test_event_specs_have_partition_and_lineage_contracts() -> None:
    for table_name in ("trades", "quotes", "orderbook_updates"):
        spec = get_table_spec(table_name)
        required = set(spec.required_columns())

        assert spec.partition_by == ("exchange", "trading_date")
        assert {"file_id", "file_seq", "symbol_id", "ts_event_us"} <= required
        assert spec.tie_break_keys[:3] == ("exchange", "symbol_id", "ts_event_us")


def test_ingest_manifest_identity_and_status_contracts() -> None:
    spec = get_table_spec("ingest_manifest")
    assert spec.business_keys == ("vendor", "data_type", "bronze_path", "file_hash")
    assert set(INGEST_STATUS_VALUES) == {"pending", "success", "failed", "quarantined"}


def test_table_spec_to_polars_schema() -> None:
    schema = get_table_spec("quotes").to_polars()
    assert schema["bid_price"] == pl.Int64
    assert schema["trading_date"] == pl.Date
    assert schema["file_seq"] == pl.Int64


def test_scaled_numeric_columns_are_explicitly_annotated() -> None:
    trades = get_table_spec("trades")
    assert trades.scale_for("price") == PRICE_SCALE
    assert trades.scale_for("qty") == QTY_SCALE

    quotes = get_table_spec("quotes")
    assert quotes.scale_for("bid_price") == PRICE_SCALE
    assert quotes.scale_for("bid_qty") == QTY_SCALE
    assert quotes.scale_for("ask_price") == PRICE_SCALE
    assert quotes.scale_for("ask_qty") == QTY_SCALE


def test_tardis_compat_nullable_fields() -> None:
    trades = get_table_spec("trades")
    assert trades.get_column("trade_id").nullable is True
    assert trades.get_column("is_buyer_maker").nullable is True

    orderbook = get_table_spec("orderbook_updates")
    assert orderbook.get_column("book_seq").nullable is True
