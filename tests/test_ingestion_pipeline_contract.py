from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata


@dataclass
class _ManifestCall:
    file_id: int
    status: str


class FakeManifestRepo:
    def __init__(self) -> None:
        self._next_file_id = 1
        self.success_identities: set[tuple[str, str, str, str]] = set()
        self.updated: list[_ManifestCall] = []

    def _identity(self, meta: BronzeFileMetadata) -> tuple[str, str, str, str]:
        return (meta.vendor, meta.data_type, meta.bronze_file_path, meta.sha256)

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        file_id = self._next_file_id
        self._next_file_id += 1
        return file_id

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        return [c for c in candidates if self._identity(c) not in self.success_identities]

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: Any | None = None,
    ) -> None:
        if status == "success":
            self.success_identities.add(self._identity(meta))
        self.updated.append(_ManifestCall(file_id=file_id, status=status))


class CapturingWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, pl.DataFrame]] = []

    def __call__(self, table_name: str, df: pl.DataFrame) -> None:
        self.calls.append((table_name, df))


def _meta() -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=szse/type=trades/date=2024-09-30/file.csv.gz",
        file_size_bytes=123,
        last_modified_ts=1,
        sha256="a" * 64,
        date=date(2024, 9, 30),
    )


def _ts_us(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def test_ingest_file_success_derives_partition_assigns_lineage_and_writes() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    # 2024-09-29 16:30 UTC == 2024-09-30 00:30 Asia/Shanghai
    event_ts = _ts_us(datetime(2024, 9, 29, 16, 30, tzinfo=ZoneInfo("UTC")))

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["szse"],
                "symbol": ["000001.SZ"],
                "ts_event_us": [event_ts],
                "ts_local_us": [event_ts],
                "side": ["buy"],
                "is_buyer_maker": [False],
                "price": [123_450_000],
                "qty": [100_000_000],
            }
        )

    dim_symbol = pl.DataFrame(
        {
            "exchange": ["szse"],
            "exchange_symbol": ["000001.SZ"],
            "symbol_id": [42],
            "valid_from_ts_us": [event_ts - 1],
            "valid_until_ts_us": [event_ts + 1],
        }
    )

    result = ingest_file(
        _meta(),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=dim_symbol,
    )

    assert result.status == "success"
    assert result.row_count == 1
    assert result.rows_quarantined == 0
    assert len(writer.calls) == 1

    table_name, written = writer.calls[0]
    assert table_name == "trades"
    assert written["trading_date"][0].isoformat() == "2024-09-30"
    assert written["file_id"][0] == 1
    assert written["file_seq"][0] == 1
    assert written["symbol_id"][0] == 42


def test_ingest_file_quarantines_when_pit_coverage_missing() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "ts_event_us": [1_700_000_000_000_000],
                "side": ["buy"],
                "is_buyer_maker": [False],
                "price": [10],
                "qty": [20],
            }
        )

    result = ingest_file(
        _meta(),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=pl.DataFrame(
            schema={
                "exchange": pl.Utf8,
                "exchange_symbol": pl.Utf8,
                "symbol_id": pl.Int64,
                "valid_from_ts_us": pl.Int64,
                "valid_until_ts_us": pl.Int64,
            }
        ),
    )

    assert result.status == "quarantined"
    assert result.row_count == 1
    assert result.rows_quarantined == 1
    assert writer.calls == []


def test_ingest_file_skips_when_manifest_already_success_and_not_forced() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    meta = _meta()
    manifest.success_identities.add(
        (meta.vendor, meta.data_type, meta.bronze_file_path, meta.sha256)
    )

    called = {"parser": False}

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        called["parser"] = True
        return pl.DataFrame()

    result = ingest_file(
        meta,
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=pl.DataFrame(
            schema={
                "exchange": pl.Utf8,
                "exchange_symbol": pl.Utf8,
                "symbol_id": pl.Int64,
                "valid_from_ts_us": pl.Int64,
                "valid_until_ts_us": pl.Int64,
            }
        ),
    )

    assert result.status == "success"
    assert result.skipped is True
    assert called["parser"] is False
    assert writer.calls == []


def test_ingest_file_fails_when_scaled_numeric_fields_are_not_pre_scaled() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    event_ts = _ts_us(datetime(2024, 9, 29, 16, 30, tzinfo=ZoneInfo("UTC")))

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["szse"],
                "symbol": ["000001.SZ"],
                "ts_event_us": [event_ts],
                "ts_local_us": [event_ts],
                "side": ["buy"],
                "is_buyer_maker": [False],
                # Raw decimal values must be encoded before schema normalization.
                "price": [12.345],
                "qty": [100.0],
            }
        )

    dim_symbol = pl.DataFrame(
        {
            "exchange": ["szse"],
            "exchange_symbol": ["000001.SZ"],
            "symbol_id": [42],
            "valid_from_ts_us": [event_ts - 1],
            "valid_until_ts_us": [event_ts + 1],
        }
    )

    result = ingest_file(
        _meta(),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=dim_symbol,
    )

    assert result.status == "failed"
    assert result.failure_reason == "pipeline_error"
    assert result.error_message is not None
    assert "pre-scaled Int64" in result.error_message
    assert writer.calls == []
