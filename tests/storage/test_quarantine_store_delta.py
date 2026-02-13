from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.storage.delta.quarantine_store import DeltaQuarantineStore


def _rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "symbol_id": [1, 2],
            "ts_event_us": [1_700_000_000_000_000, 1_700_000_000_000_100],
            "file_seq": [10, 11],
        }
    )


def test_quarantine_store_writes_validation_log_rows(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    store = DeltaQuarantineStore(silver_root=silver_root)

    store.append("trades", _rows(), reason="missing_pit_symbol_coverage", file_id=9)

    log_path = silver_root / "validation_log"
    df = pl.read_delta(str(log_path)).sort("file_seq")
    assert df.height == 2
    assert df["file_id"].to_list() == [9, 9]
    assert df["rule_name"].to_list() == ["missing_pit_symbol_coverage"] * 2
    assert df["symbol"].to_list() == ["BTCUSDT", "ETHUSDT"]
    assert df["file_seq"].to_list() == [10, 11]


def test_quarantine_store_ignores_empty_frames(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    store = DeltaQuarantineStore(silver_root=silver_root)
    empty = pl.DataFrame({"symbol": [], "symbol_id": [], "ts_event_us": []})
    store.append("trades", empty, reason="x", file_id=1)
    assert not (silver_root / "validation_log").exists()
