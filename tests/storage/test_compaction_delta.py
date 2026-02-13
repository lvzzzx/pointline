from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable

from pointline.storage.delta import DeltaEventStore
from pointline.storage.delta.layout import table_path
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
            "side": ["buy"],
            "is_buyer_maker": [False],
            "price": [123_000_000_000],
            "qty": [5_000_000_000],
        }
    )


def _seed_small_files(silver_root: Path) -> None:
    writer = DeltaEventStore(silver_root=silver_root)
    day1 = date(2024, 1, 1)
    day2 = date(2024, 1, 2)

    for seq in range(1, 6):
        writer.append("trades", _trades_row(trading_date=day1, file_id=1, file_seq=seq))
    for seq in range(6, 9):
        writer.append("trades", _trades_row(trading_date=day2, file_id=2, file_seq=seq))


def _partition_filters(*, trading_date: date) -> list[tuple[str, str, object]]:
    return [("exchange", "=", "binance-futures"), ("trading_date", "=", trading_date)]


def _count_partition_files(*, table: DeltaTable, trading_date: date) -> int:
    return len(table.file_uris(partition_filters=_partition_filters(trading_date=trading_date)))


def test_compact_partitions_targets_only_requested_partition(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)

    path = table_path(silver_root=silver_root, table_name="trades")
    table_before = DeltaTable(str(path))
    day1 = date(2024, 1, 1)
    day2 = date(2024, 1, 2)

    day1_before = _count_partition_files(table=table_before, trading_date=day1)
    day2_before = _count_partition_files(table=table_before, trading_date=day2)
    rows_before = pl.read_delta(str(path)).height

    report = optimizer.compact_partitions(
        table_name="trades",
        partitions=[{"exchange": "binance-futures", "trading_date": day1}],
        target_file_size_bytes=1_048_576,
        min_small_files=2,
    )

    table_after = DeltaTable(str(path))
    day1_after = _count_partition_files(table=table_after, trading_date=day1)
    day2_after = _count_partition_files(table=table_after, trading_date=day2)
    rows_after = pl.read_delta(str(path)).height

    assert day1_after < day1_before
    assert day2_after == day2_before
    assert rows_after == rows_before
    assert report.succeeded_partitions == 1
    assert report.failed_partitions == 0


def test_compact_partitions_dry_run_does_not_create_new_version(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)

    path = table_path(silver_root=silver_root, table_name="trades")
    day1 = date(2024, 1, 1)
    before = DeltaTable(str(path))
    version_before = before.version()
    files_before = _count_partition_files(table=before, trading_date=day1)

    report = optimizer.compact_partitions(
        table_name="trades",
        partitions=[{"exchange": "binance-futures", "trading_date": day1}],
        dry_run=True,
    )

    after = DeltaTable(str(path))
    assert after.version() == version_before
    assert _count_partition_files(table=after, trading_date=day1) == files_before
    assert report.attempted_partitions == 0
    assert report.skipped_partitions == 1
    assert report.partitions[0].skipped_reason == "dry_run"


def test_compact_partitions_rejects_invalid_partition_keys(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)

    with pytest.raises(ValueError, match="partition keys"):
        optimizer.compact_partitions(
            table_name="trades",
            partitions=[{"exchange": "binance-futures"}],
        )


def test_compact_partitions_continue_on_error_isolates_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)
    day1 = date(2024, 1, 1)
    day2 = date(2024, 1, 2)

    original = optimizer._compact_partition

    def flaky_compact(
        table: DeltaTable,
        *,
        partition_filters: list[tuple[str, str, object]],
        target_file_size_bytes: int | None,
    ) -> dict[str, object]:
        for key, _, value in partition_filters:
            if key == "trading_date" and value == day2:
                raise RuntimeError("injected failure")
        return original(
            table,
            partition_filters=partition_filters,
            target_file_size_bytes=target_file_size_bytes,
        )

    monkeypatch.setattr(optimizer, "_compact_partition", flaky_compact)

    report = optimizer.compact_partitions(
        table_name="trades",
        partitions=[
            {"exchange": "binance-futures", "trading_date": day1},
            {"exchange": "binance-futures", "trading_date": day2},
        ],
        min_small_files=2,
        continue_on_error=True,
    )

    assert report.succeeded_partitions == 1
    assert report.failed_partitions == 1
    assert any(item.error == "injected failure" for item in report.partitions)


def test_compact_partitions_idempotent_rerun_is_skipped(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_small_files(silver_root)
    optimizer = DeltaPartitionOptimizer(silver_root=silver_root)
    day1 = date(2024, 1, 1)
    path = table_path(silver_root=silver_root, table_name="trades")

    first = optimizer.compact_partitions(
        table_name="trades",
        partitions=[{"exchange": "binance-futures", "trading_date": day1}],
        min_small_files=2,
    )
    assert first.succeeded_partitions == 1

    table_before_second = DeltaTable(str(path))
    version_before_second = table_before_second.version()

    second = optimizer.compact_partitions(
        table_name="trades",
        partitions=[{"exchange": "binance-futures", "trading_date": day1}],
        min_small_files=2,
    )

    table_after_second = DeltaTable(str(path))
    assert table_after_second.version() == version_before_second
    assert second.succeeded_partitions == 0
    assert second.skipped_partitions == 1
    assert second.partitions[0].skipped_reason == "below_min_small_files"
