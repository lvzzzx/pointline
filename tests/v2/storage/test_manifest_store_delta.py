from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.v2.ingestion.models import IngestionResult
from pointline.v2.storage.delta.manifest_store import DeltaManifestStore


def _meta(*, sha256: str = "a" * 64) -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=binance-futures/type=trades/date=2024-01-01/file.csv.gz",
        file_size_bytes=100,
        last_modified_ts=1000,
        sha256=sha256,
        date=date(2024, 1, 1),
    )


def _load(path: Path) -> pl.DataFrame:
    return pl.read_delta(str(path))


def test_manifest_store_resolve_and_skip_semantics(tmp_path: Path) -> None:
    manifest_path = tmp_path / "silver" / "ingest_manifest"
    store = DeltaManifestStore(manifest_path)
    meta = _meta()

    assert store.filter_pending([meta]) == [meta]

    file_id = store.resolve_file_id(meta)
    assert file_id == 1
    assert store.resolve_file_id(meta) == 1

    success = IngestionResult(
        status="success",
        row_count=10,
        rows_written=8,
        rows_quarantined=2,
        file_id=file_id,
        trading_date_min=date(2024, 1, 1),
        trading_date_max=date(2024, 1, 1),
    )
    store.update_status(file_id, "success", meta, success)

    assert store.filter_pending([meta]) == []

    df = _load(manifest_path)
    assert df.height == 1
    assert df.item(0, "file_id") == 1
    assert df.item(0, "status") == "success"
    assert df.item(0, "rows_total") == 10
    assert df.item(0, "rows_written") == 8
    assert df.item(0, "rows_quarantined") == 2
    assert df.item(0, "bronze_path") == meta.bronze_file_path
    assert df.item(0, "file_hash") == meta.sha256


def test_manifest_store_only_success_is_skippable(tmp_path: Path) -> None:
    manifest_path = tmp_path / "silver" / "ingest_manifest"
    store = DeltaManifestStore(manifest_path)
    failed_meta = _meta(sha256="b" * 64)

    file_id = store.resolve_file_id(failed_meta)
    failed = IngestionResult(
        status="failed",
        row_count=0,
        rows_written=0,
        rows_quarantined=0,
        file_id=file_id,
        failure_reason="parser_error",
        error_message="boom",
    )
    store.update_status(file_id, "failed", failed_meta, failed)

    pending = store.filter_pending([failed_meta])
    assert pending == [failed_meta]

    df = _load(manifest_path).sort("file_id")
    assert df.height == 1
    assert df.item(0, "status") == "failed"
    assert df.item(0, "status_reason") == "parser_error"
