from datetime import date

import polars as pl

from pointline.l2_state_checkpoint import build_state_checkpoints


def _sample_updates() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["deribit"] * 5,
            "exchange_id": [21] * 5,
            "symbol_id": [1] * 5,
            "date": [date(2025, 1, 1)] * 5,
            "ts_local_us": [1, 1, 2, 3, 4],
            "ingest_seq": [1, 2, 3, 4, 5],
            "file_id": [10] * 5,
            "file_line_number": [1, 2, 3, 4, 5],
            "is_snapshot": [True, True, False, False, False],
            "side": [0, 1, 0, 1, 0],
            "price_int": [100, 101, 100, 102, 99],
            "size_int": [10, 5, 0, 3, 7],
        }
    )


def test_build_state_checkpoints_update_cadence() -> None:
    df = _sample_updates()
    result = build_state_checkpoints(df.lazy(), checkpoint_every_updates=2)

    assert result.height == 2

    first = result.row(0, named=True)
    assert first["ts_local_us"] == 1
    assert first["bids"] == [{"price_int": 100, "size_int": 10}]
    assert first["asks"] == [{"price_int": 101, "size_int": 5}]

    second = result.row(1, named=True)
    assert second["ts_local_us"] == 3
    assert second["bids"] == []
    assert second["asks"] == [
        {"price_int": 101, "size_int": 5},
        {"price_int": 102, "size_int": 3},
    ]


def test_build_state_checkpoints_time_cadence() -> None:
    df = _sample_updates()
    result = build_state_checkpoints(df.lazy(), checkpoint_every_us=2)

    assert result.height == 2
    assert result["ts_local_us"].to_list() == [2, 4]
