from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from pointline.v2.storage.delta.event_store import DeltaEventStore


def _trades_row(*, symbol_id: int = 7, file_seq: int = 1) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "trading_date": [date(2024, 1, 1)],
            "symbol": ["BTCUSDT"],
            "symbol_id": [symbol_id],
            "ts_event_us": [1_700_000_000_000_000],
            "ts_local_us": [1_700_000_000_000_000],
            "file_id": [1],
            "file_seq": [file_seq],
            "side": ["buy"],
            "is_buyer_maker": [False],
            "price": [123_000_000_000],
            "qty": [5_000_000_000],
        }
    )


def test_event_store_appends_event_rows(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    store = DeltaEventStore(silver_root=silver_root)

    store.append("trades", _trades_row(file_seq=1))
    store.append("trades", _trades_row(file_seq=2))

    table_path = silver_root / "trades"
    df = pl.read_delta(str(table_path))
    assert df.height == 2
    assert df.sort("file_seq")["file_seq"].to_list() == [1, 2]


def test_event_store_rejects_wrong_schema(tmp_path: Path) -> None:
    store = DeltaEventStore(silver_root=tmp_path / "silver")
    bad = _trades_row().drop("qty")

    try:
        store.append("trades", bad)
    except ValueError as exc:
        assert "missing columns" in str(exc)
    else:
        raise AssertionError("Expected schema validation failure")
