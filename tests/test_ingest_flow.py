from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from pointline.cli.commands import ingest as ingest_cmd
from pointline.io.protocols import BronzeFileMetadata, IngestionResult


def _make_args(tmp_path: Path, *, dry_run: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        bronze_root=str(tmp_path),
        vendor=None,
        glob="**/*.csv.gz",
        data_type=None,
        force=False,
        retry_quarantined=False,
        validate=False,
        validate_sample_size=1000,
        validate_seed=42,
        dry_run=dry_run,
        optimize_after_ingest=False,
        optimize_target_file_size=None,
        optimize_zorder=None,
    )


def _sample_meta() -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=binance/type=trades/date=2024-05-01/symbol=BTCUSDT/file.csv.gz",
        file_size_bytes=1,
        last_modified_ts=1,
        sha256="",
        date=date(2024, 5, 1),
    )


def test_cmd_ingest_run_dry_run_has_no_manifest_side_effects(monkeypatch, tmp_path):
    meta = _sample_meta()

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [meta]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            self.resolve_calls = 0
            self.update_calls = 0

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            self.resolve_calls += 1
            return 123

        def update_status(self, *_args, **_kwargs):
            self.update_calls += 1

        def read_all(self):
            raise AssertionError("read_all should not be called in this test")

    class FakeService:
        def ingest_file(self, _meta, file_id, **kwargs):
            assert file_id == 0
            assert kwargs["dry_run"] is True
            assert kwargs["idempotent_write"] is False
            return IngestionResult(row_count=5, ts_local_min_us=1, ts_local_max_us=2)

    fake_manifest = FakeManifestRepo()
    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: fake_manifest
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: FakeService()
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", lambda _path: "a" * 64)

    rc = ingest_cmd.cmd_ingest_run(_make_args(tmp_path, dry_run=True))
    assert rc == 0
    assert fake_manifest.resolve_calls == 0
    assert fake_manifest.update_calls == 0


def test_cmd_ingest_run_marks_all_symbols_quarantined(monkeypatch, tmp_path):
    meta = _sample_meta()

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [meta]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            self.updated = []

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 7

        def update_status(self, file_id, status, _meta, result):
            self.updated.append((file_id, status, result.error_message))

        def read_all(self):
            raise AssertionError("read_all should not be called in this test")

    class FakeService:
        def ingest_file(self, _meta, _file_id, **_kwargs):
            return IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message="All symbols quarantined",
                failure_reason="all_symbols_quarantined",
            )

    fake_manifest = FakeManifestRepo()
    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: fake_manifest
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: FakeService()
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", lambda _path: "a" * 64)

    rc = ingest_cmd.cmd_ingest_run(_make_args(tmp_path, dry_run=False))
    assert rc == 0
    assert fake_manifest.updated == [(7, "quarantined", "All symbols quarantined")]


def test_cmd_ingest_run_marks_partial_symbol_quarantine_as_quarantined(monkeypatch, tmp_path):
    meta = _sample_meta()

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [meta]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            self.updated = []

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 11

        def update_status(self, file_id, status, _meta, result):
            self.updated.append((file_id, status, result.error_message))

        def read_all(self):
            raise AssertionError("read_all should not be called in this test")

    class FakeService:
        def ingest_file(self, _meta, _file_id, **_kwargs):
            return IngestionResult(
                row_count=10,
                ts_local_min_us=1,
                ts_local_max_us=2,
                partial_ingestion=True,
                filtered_symbol_count=1,
                filtered_row_count=5,
            )

    fake_manifest = FakeManifestRepo()
    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: fake_manifest
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: FakeService()
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", lambda _path: "a" * 64)

    rc = ingest_cmd.cmd_ingest_run(_make_args(tmp_path, dry_run=False))
    assert rc == 0
    assert len(fake_manifest.updated) == 1
    file_id, status, message = fake_manifest.updated[0]
    assert file_id == 11
    assert status == "quarantined"
    assert message is not None
    assert "Partial ingestion" in message


def test_cmd_ingest_run_passes_idempotent_write_for_real_ingest(monkeypatch, tmp_path):
    meta = _sample_meta()

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [meta]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            self.updated = []

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 9

        def update_status(self, file_id, status, _meta, _result):
            self.updated.append((file_id, status))

        def read_all(self):
            raise AssertionError("read_all should not be called in this test")

    seen = {}

    class FakeService:
        def ingest_file(self, _meta, file_id, **kwargs):
            seen["file_id"] = file_id
            seen["kwargs"] = kwargs
            return IngestionResult(row_count=1, ts_local_min_us=1, ts_local_max_us=2)

    fake_manifest = FakeManifestRepo()
    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: fake_manifest
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: FakeService()
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", lambda _path: "a" * 64)

    rc = ingest_cmd.cmd_ingest_run(_make_args(tmp_path, dry_run=False))
    assert rc == 0
    assert seen["file_id"] == 9
    assert seen["kwargs"]["dry_run"] is False
    assert seen["kwargs"]["idempotent_write"] is True
    assert fake_manifest.updated == [(9, "success")]


def test_cmd_ingest_run_filters_pending_after_hash(monkeypatch, tmp_path):
    meta = _sample_meta()

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [meta]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            self.filter_seen_sha: list[str] = []

        def filter_pending(self, candidates):
            self.filter_seen_sha.extend([c.sha256 for c in candidates])
            return []

        def resolve_file_id(self, _meta):
            raise AssertionError("resolve_file_id should not be called when no pending files")

        def update_status(self, *_args, **_kwargs):
            raise AssertionError("update_status should not be called when no pending files")

        def read_all(self):
            raise AssertionError("read_all should not be called in this test")

    fake_manifest = FakeManifestRepo()
    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: fake_manifest
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: AssertionError("unused")
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", lambda _path: "z" * 64)

    rc = ingest_cmd.cmd_ingest_run(_make_args(tmp_path, dry_run=False))
    assert rc == 0
    assert fake_manifest.filter_seen_sha == ["z" * 64]


def test_cmd_ingest_run_retry_quarantined_prefilters_before_hash(monkeypatch, tmp_path):
    m1 = _sample_meta()
    m2 = BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=binance/type=trades/date=2024-05-01/symbol=ETHUSDT/file.csv.gz",
        file_size_bytes=1,
        last_modified_ts=1,
        sha256="",
        date=date(2024, 5, 1),
    )

    class FakeSource:
        def __init__(self, *_args, **_kwargs):
            pass

        def list_files(self, _glob):
            return [m1, m2]

    class FakeManifestRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 1

        def update_status(self, *_args, **_kwargs):
            pass

        def read_all(self):
            return pl.DataFrame(
                {
                    "status": ["quarantined"],
                    "vendor": ["tardis"],
                    "bronze_file_name": [m2.bronze_file_path],
                }
            )

    class FakeService:
        def ingest_file(self, _meta, _file_id, **_kwargs):
            return IngestionResult(row_count=1, ts_local_min_us=1, ts_local_max_us=2)

    sha_paths: list[str] = []

    def _fake_sha(path):
        sha_paths.append(str(path))
        return "x" * 64

    monkeypatch.setattr(ingest_cmd, "LocalBronzeSource", FakeSource)
    monkeypatch.setattr(
        ingest_cmd, "DeltaManifestRepository", lambda *_args, **_kwargs: FakeManifestRepo()
    )
    monkeypatch.setattr(
        ingest_cmd, "create_ingestion_service", lambda *_args, **_kwargs: FakeService()
    )
    monkeypatch.setattr(ingest_cmd, "compute_sha256", _fake_sha)

    args = _make_args(tmp_path, dry_run=False)
    args.vendor = "tardis"
    args.retry_quarantined = True
    rc = ingest_cmd.cmd_ingest_run(args)
    assert rc == 0
    assert len(sha_paths) == 1
    assert "ETHUSDT" in sha_paths[0]
