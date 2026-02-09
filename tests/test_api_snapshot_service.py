from __future__ import annotations

import gzip
import json
from datetime import date

import polars as pl

from pointline.io.protocols import (
    ApiCaptureRequest,
    ApiSnapshotSpec,
    BronzeFileMetadata,
    IngestionResult,
)
from pointline.services import api_snapshot_service as snapshot_module
from pointline.services.api_snapshot_service import ApiSnapshotService


class _FakePlugin:
    supports_api_snapshots = True

    def get_api_snapshot_specs(self):
        return {
            "dim_symbol": ApiSnapshotSpec(
                dataset="dim_symbol",
                data_type="dim_symbol_metadata",
                target_table="dim_symbol",
                partition_keys=("exchange",),
                default_glob="type=dim_symbol_metadata/**/*.jsonl.gz",
            )
        }

    def capture_api_snapshot(self, dataset, request):
        return [{"datasetId": "BTCUSDT"}]

    def build_updates_from_snapshot(self, dataset, records, options):
        _ = (dataset, records, options)
        return pl.DataFrame({"valid_from_ts": [100], "exchange_symbol": ["BTCUSDT"]})


class _FakeManifestRepo:
    def __init__(self, _path):
        self.updated = []

    def filter_pending(self, candidates):
        return candidates

    def resolve_file_id(self, _meta):
        return 1

    def update_status(self, file_id, status, _meta, result):
        self.updated.append((file_id, status, result.row_count, result.error_message))


class _FakeBronzeSource:
    def __init__(self, _root, vendor=None, compute_checksums=True):
        _ = (vendor, compute_checksums)

    def list_files(self, _glob_pattern=None):
        return [
            BronzeFileMetadata(
                vendor="tardis",
                data_type="dim_symbol_metadata",
                bronze_file_path=(
                    "type=dim_symbol_metadata/exchange=binance/date=2026-01-01/"
                    "snapshot_ts=1/tardis_dim_symbol_1.jsonl.gz"
                ),
                file_size_bytes=10,
                last_modified_ts=1,
                sha256="a" * 64,
                date=date(2026, 1, 1),
            )
        ]


def test_capture_writes_canonical_envelope_with_redacted_request(monkeypatch, tmp_path):
    monkeypatch.setattr(snapshot_module, "get_vendor", lambda _vendor: _FakePlugin())

    service = ApiSnapshotService()
    result = service.capture(
        vendor="tardis",
        dataset="dim_symbol",
        request=ApiCaptureRequest(
            params={"exchange": "binance", "api_key": "secret"},
            partitions={"exchange": "binance", "date": "2026-01-01"},
            captured_at_us=1,
        ),
        capture_root=tmp_path,
    )

    assert result.path.exists()
    with gzip.open(result.path, "rt", encoding="utf-8") as handle:
        payload = json.loads(handle.readline())

    assert payload["schema_version"] == 1
    assert payload["vendor"] == "tardis"
    assert payload["dataset"] == "dim_symbol"
    assert payload["request"]["api_key"] == "***"
    assert payload["partitions"]["exchange"] == "binance"


def test_replay_success_updates_manifest(monkeypatch, tmp_path):
    plugin = _FakePlugin()
    fake_manifest = _FakeManifestRepo(tmp_path / "manifest")

    monkeypatch.setattr(snapshot_module, "get_vendor", lambda _vendor: plugin)
    monkeypatch.setattr(snapshot_module, "LocalBronzeSource", _FakeBronzeSource)
    monkeypatch.setattr(snapshot_module, "DeltaManifestRepository", lambda _path: fake_manifest)
    monkeypatch.setattr(
        ApiSnapshotService,
        "_apply_updates",
        lambda self, **kwargs: IngestionResult(
            row_count=kwargs["updates"].height,
            ts_local_min_us=100,
            ts_local_max_us=100,
            error_message=None,
        ),
    )

    replay_file = (
        tmp_path / "type=dim_symbol_metadata/exchange=binance/date=2026-01-01/"
        "snapshot_ts=1/tardis_dim_symbol_1.jsonl.gz"
    )
    replay_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(replay_file, "wt", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "schema_version": 1,
                    "vendor": "tardis",
                    "dataset": "dim_symbol",
                    "captured_at_us": 1,
                    "snapshot_ts_us": 1,
                    "partitions": {"exchange": "binance", "date": "2026-01-01"},
                    "request": {"exchange": "binance"},
                    "record": {"datasetId": "BTCUSDT"},
                }
            )
            + "\n"
        )

    summary = ApiSnapshotService().replay(
        vendor="tardis",
        dataset="dim_symbol",
        bronze_root=tmp_path,
        manifest_path=tmp_path / "manifest",
        table_path=tmp_path / "dim_symbol",
    )

    assert summary.success_count == 1
    assert summary.failed_count == 0
    assert fake_manifest.updated == [(1, "success", 1, None)]


def test_replay_failure_marks_manifest_failed(monkeypatch, tmp_path):
    class FailingPlugin(_FakePlugin):
        def build_updates_from_snapshot(self, dataset, records, options):
            _ = (dataset, records, options)
            raise ValueError("bad snapshot")

    fake_manifest = _FakeManifestRepo(tmp_path / "manifest")
    monkeypatch.setattr(snapshot_module, "get_vendor", lambda _vendor: FailingPlugin())
    monkeypatch.setattr(snapshot_module, "LocalBronzeSource", _FakeBronzeSource)
    monkeypatch.setattr(snapshot_module, "DeltaManifestRepository", lambda _path: fake_manifest)

    replay_file = (
        tmp_path / "type=dim_symbol_metadata/exchange=binance/date=2026-01-01/"
        "snapshot_ts=1/tardis_dim_symbol_1.jsonl.gz"
    )
    replay_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(replay_file, "wt", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "schema_version": 1,
                    "vendor": "tardis",
                    "dataset": "dim_symbol",
                    "captured_at_us": 1,
                    "snapshot_ts_us": 1,
                    "partitions": {"exchange": "binance", "date": "2026-01-01"},
                    "request": {"exchange": "binance"},
                    "record": {"datasetId": "BTCUSDT"},
                }
            )
            + "\n"
        )

    summary = ApiSnapshotService().replay(
        vendor="tardis",
        dataset="dim_symbol",
        bronze_root=tmp_path,
        manifest_path=tmp_path / "manifest",
        table_path=tmp_path / "dim_symbol",
    )

    assert summary.success_count == 0
    assert summary.failed_count == 1
    assert fake_manifest.updated == [(1, "failed", 0, "bad snapshot")]


def test_load_snapshot_records_accepts_envelope_and_raw(tmp_path):
    envelope_path = tmp_path / "e.jsonl.gz"
    with gzip.open(envelope_path, "wt", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "record": {"id": 1},
                    "request": {"foo": "bar"},
                    "partitions": {"exchange": "binance"},
                }
            )
            + "\n"
        )

    raw_path = tmp_path / "r.jsonl.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({"id": 2}) + "\n")

    service = ApiSnapshotService()
    records1, request1, partitions1 = service._load_snapshot_records(envelope_path)
    records2, request2, partitions2 = service._load_snapshot_records(raw_path)

    assert records1 == [{"id": 1}]
    assert request1 == {"foo": "bar"}
    assert partitions1 == {"exchange": "binance"}
    assert records2 == [{"id": 2}]
    assert request2 is None
    assert partitions2 == {}
