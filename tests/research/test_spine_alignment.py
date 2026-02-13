from __future__ import annotations

import polars as pl

from pointline.research.spine import align_to_spine


def test_align_to_spine_boundary_semantics() -> None:
    events = pl.DataFrame(
        {
            "exchange": ["binance-futures"] * 4,
            "symbol": ["BTCUSDT"] * 4,
            "ts_event_us": [59, 60, 119, 120],
            "value": [1, 2, 3, 4],
        }
    )
    spine = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "symbol_id": [10, 10],
            "ts_spine_us": [60, 120],
        }
    )

    out = align_to_spine(events=events, spine=spine)
    assert out["ts_spine_us"].to_list() == [60, 120, 120, None]


def test_align_to_spine_keeps_input_order() -> None:
    events = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "ts_event_us": [80, 20],
            "value": [8, 2],
        }
    )
    spine = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "symbol_id": [10, 10],
            "ts_spine_us": [30, 90],
        }
    )

    out = align_to_spine(events=events, spine=spine)
    assert out["value"].to_list() == [8, 2]
    assert out["ts_spine_us"].to_list() == [90, 30]
