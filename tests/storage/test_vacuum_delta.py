from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pointline.storage.delta import DeltaEventStore
from pointline.storage.delta.optimizer_store import DeltaPartitionOptimizer


def _trades_row(*, trading_date: date, file_id: int, file_seq: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "trading_date": [trading_date],
            "symbol": ["BTCUSDT"],
            "symbol_id": [7],
            "ts_event_us": [1_700_000_000_000_000 + file_seq],
            "ts_local_us": [1_700_000_000_000_000 + file_seq],
            "file_id": [file_id],
            "file_seq": [file_seq],
            "trade_id": [""],
            "side": ["buy"],
            "is_buyer_maker": [False],
            "price": [123_000_000_000],
            "qty": [5_000_000_000],
        }
    )


def _seed_small_files(silver_root: Path) -> None:
    writer = DeltaEventStore(silver_root=silver_root)
    day1 = date(2024, 1, 1)
    for seq in range(1, 6):
        writer.append("trades", _trades_row(trading_date=day1, file_id=1, file_seq=seq))


def _create_tombstones_for_vacuum(optimizer: DeltaPartitionOptimizer) -> None:
    optimizer.compact_partitions(
        table_name="trades",
        partitions=[{"exchange": "binance-futures", "trading_date": date(2024, 1, 1)}],
        min_small_files=2,
    )


def test_vacuum_table_dry_run_reports_candidates(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)
    _create_tombstones_for_vacuum(optimizer)

    report = optimizer.vacuum_table(
        table_name="trades",
        retention_hours=0,
        dry_run=True,
        enforce_retention_duration=False,
    )

    assert report.table_name == "trades"
    assert report.dry_run is True
    assert report.deleted_count > 0


def test_vacuum_table_real_delete_then_idempotent_rerun(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)
    _create_tombstones_for_vacuum(optimizer)

    first = optimizer.vacuum_table(
        table_name="trades",
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
    )
    second = optimizer.vacuum_table(
        table_name="trades",
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
    )

    assert first.deleted_count > 0
    assert second.deleted_count == 0


def test_vacuum_table_rejects_negative_retention(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)

    with pytest.raises(ValueError, match="retention_hours"):
        optimizer.vacuum_table(table_name="trades", retention_hours=-1)


def test_vacuum_table_missing_table_fails(tmp_path: Path) -> None:
    optimizer = DeltaPartitionOptimizer(silver_root=tmp_path / "silver")

    with pytest.raises(FileNotFoundError, match="Delta table does not exist"):
        optimizer.vacuum_table(table_name="cn_order_events")
