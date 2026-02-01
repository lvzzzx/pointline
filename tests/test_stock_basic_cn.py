from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from pointline.config import get_exchange_id
from pointline.io.vendor.tushare.stock_basic_cn import (
    build_dim_symbol_updates_from_stock_basic_cn,
    build_stock_basic_cn_snapshot,
)


def _ts_us(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1_000_000)


def test_build_stock_basic_cn_snapshot_parses_dates_and_exchange():
    raw = pl.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "symbol": ["000001"],
            "name": ["Ping An Bank"],
            "exchange": ["SZSE"],
            "list_status": ["L"],
            "list_date": ["20240102"],
        }
    )

    snapshot = build_stock_basic_cn_snapshot(
        raw, as_of_date=date(2025, 1, 3), ingest_ts_us=1_700_000_000_000_000
    )

    assert snapshot.height == 1
    assert snapshot["exchange"][0] == "szse"
    assert snapshot["exchange_id"][0] == get_exchange_id("szse")
    assert snapshot["exchange_symbol"][0] == "000001"
    assert snapshot["list_date"][0] == date(2024, 1, 2)
    assert snapshot["as_of_date"][0] == date(2025, 1, 3)


def test_build_dim_symbol_updates_from_stock_basic_cn():
    raw = pl.DataFrame(
        {
            "ts_code": ["000002.SZ"],
            "symbol": ["000002"],
            "name": ["Vanke"],
            "exchange": ["SZSE"],
            "list_status": ["L"],
            "list_date": ["20200115"],
        }
    )

    snapshot = build_stock_basic_cn_snapshot(raw, as_of_date=date(2025, 1, 3))
    updates = build_dim_symbol_updates_from_stock_basic_cn(snapshot)

    assert updates.height == 1
    assert updates["exchange_symbol"][0] == "000002"
    assert updates["quote_asset"][0] == "CNY"
    assert updates["valid_from_ts"][0] == _ts_us(date(2020, 1, 15))
