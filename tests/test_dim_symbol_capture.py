from __future__ import annotations

import argparse

from pointline.cli.commands import dim_symbol as dim_symbol_cmd
from pointline.cli.parser import build_parser
from pointline.services.api_snapshot_service import (
    ApiCaptureResult,
    ApiReplayFileResult,
    ApiReplaySummary,
)


def _ok_summary(vendor: str, bronze_file_path: str = "type=dim_symbol_metadata/f.jsonl.gz"):
    return ApiReplaySummary(
        vendor=vendor,
        dataset="dim_symbol",
        discovered_files=1,
        processed_files=1,
        success_count=1,
        failed_count=0,
        file_results=[
            ApiReplayFileResult(
                bronze_file_path=bronze_file_path,
                status="success",
                row_count=3,
                error_message=None,
            )
        ],
    )


def _failed_summary(vendor: str, bronze_file_path: str = "type=dim_symbol_metadata/f.jsonl.gz"):
    return ApiReplaySummary(
        vendor=vendor,
        dataset="dim_symbol",
        discovered_files=1,
        processed_files=1,
        success_count=0,
        failed_count=1,
        file_results=[
            ApiReplayFileResult(
                bronze_file_path=bronze_file_path,
                status="failed",
                row_count=0,
                error_message="bad capture",
            )
        ],
    )


def test_symbol_sync_file_source_rejects_capture_flags(tmp_path):
    source = tmp_path / "updates.csv"
    source.write_text("exchange_id,exchange_symbol,valid_from_ts\n1,BTCUSDT,1\n", encoding="utf-8")
    args = argparse.Namespace(
        source=str(source),
        exchange=None,
        symbol=None,
        filter=None,
        api_key="",
        effective_ts="now",
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        capture_api_response=True,
        capture_only=False,
        capture_root=None,
    )
    assert dim_symbol_cmd.cmd_dim_symbol_sync(args) == 2


def test_symbol_sync_parser_accepts_capture_flags_and_ingest_metadata_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "symbol",
            "sync",
            "--source",
            "api",
            "--exchange",
            "binance-futures",
            "--capture-api-response",
            "--capture-root",
            "/tmp/capture",
        ]
    )
    assert args.capture_api_response is True
    assert args.capture_root == "/tmp/capture"

    args_tushare = parser.parse_args(
        [
            "symbol",
            "sync-tushare",
            "--exchange",
            "szse",
            "--capture-only",
            "--rebuild",
        ]
    )
    assert args_tushare.capture_only is True
    assert args_tushare.rebuild is True

    args_ingest = parser.parse_args(
        [
            "symbol",
            "ingest-metadata",
            "--vendor",
            "tardis",
            "--exchange",
            "binance-futures",
            "--manifest-path",
            "/tmp/manifest",
            "--table-path",
            "/tmp/dim_symbol",
            "--effective-ts",
            "1700000000000000",
            "--rebuild",
            "--force",
        ]
    )
    assert args_ingest.vendor == "tardis"
    assert args_ingest.exchange == "binance-futures"
    assert args_ingest.rebuild is True
    assert args_ingest.force is True


def test_cmd_dim_symbol_sync_api_capture_only(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.capture_calls = []
            self.replay_calls = []

        def capture(self, **kwargs):
            self.capture_calls.append(kwargs)
            out = tmp_path / "bronze" / "type=dim_symbol_metadata" / "f.jsonl.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("", encoding="utf-8")
            return ApiCaptureResult(
                vendor="tardis",
                dataset="dim_symbol",
                bronze_root=tmp_path / "bronze",
                path=out,
                snapshot_ts_us=1,
                row_count=0,
            )

        def replay(self, **kwargs):
            self.replay_calls.append(kwargs)
            return _ok_summary("tardis")

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_symbol_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        source="api",
        exchange="binance-futures",
        symbol=None,
        filter=None,
        api_key="k",
        effective_ts="now",
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        capture_api_response=False,
        capture_only=True,
        capture_root=str(tmp_path / "bronze"),
    )

    code = dim_symbol_cmd.cmd_dim_symbol_sync(args)
    assert code == 0
    assert len(fake_service.capture_calls) == 1
    assert len(fake_service.replay_calls) == 0


def test_cmd_dim_symbol_sync_api_replays_from_capture(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.capture_calls = []
            self.replay_calls = []

        def capture(self, **kwargs):
            self.capture_calls.append(kwargs)
            out = tmp_path / "bronze" / "type=dim_symbol_metadata" / "f.jsonl.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("", encoding="utf-8")
            return ApiCaptureResult(
                vendor="tardis",
                dataset="dim_symbol",
                bronze_root=tmp_path / "bronze",
                path=out,
                snapshot_ts_us=1,
                row_count=1,
            )

        def replay(self, **kwargs):
            self.replay_calls.append(kwargs)
            return _ok_summary("tardis")

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_symbol_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        source="api",
        exchange="binance-futures",
        symbol="BTCUSDT",
        filter='{"foo":"bar"}',
        api_key="k",
        effective_ts="1700000000000000",
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=True,
        capture_api_response=False,
        capture_only=False,
        capture_root=str(tmp_path / "bronze"),
    )

    code = dim_symbol_cmd.cmd_dim_symbol_sync(args)
    assert code == 0
    assert len(fake_service.capture_calls) == 1
    assert len(fake_service.replay_calls) == 1
    assert fake_service.replay_calls[0]["rebuild"] is True


def test_cmd_dim_symbol_sync_tushare_capture_and_replay(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.capture_calls = []
            self.replay_calls = []

        def capture(self, **kwargs):
            self.capture_calls.append(kwargs)
            out = tmp_path / "bronze" / "type=dim_symbol_metadata" / "f.jsonl.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("", encoding="utf-8")
            return ApiCaptureResult(
                vendor="tushare",
                dataset="dim_symbol",
                bronze_root=tmp_path / "bronze",
                path=out,
                snapshot_ts_us=1,
                row_count=1,
            )

        def replay(self, **kwargs):
            self.replay_calls.append(kwargs)
            return _ok_summary("tushare")

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_symbol_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        exchange="szse",
        include_delisted=False,
        token="x",
        table_path=str(tmp_path / "custom_dim_symbol"),
        rebuild=False,
        capture_api_response=False,
        capture_only=False,
        capture_root=str(tmp_path / "bronze"),
    )

    code = dim_symbol_cmd.cmd_dim_symbol_sync_tushare(args)
    assert code == 0
    assert len(fake_service.capture_calls) == 1
    assert len(fake_service.replay_calls) == 1


def test_cmd_dim_symbol_ingest_metadata_success_and_failure(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.calls = 0

        def replay(self, **kwargs):
            self.calls += 1
            return _ok_summary("tardis") if self.calls == 1 else _failed_summary("tardis")

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_symbol_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        vendor="tardis",
        bronze_root=str(tmp_path),
        glob="**/*.jsonl.gz",
        exchange=None,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        force=False,
        effective_ts=None,
    )

    code_1 = dim_symbol_cmd.cmd_dim_symbol_ingest_metadata(args)
    code_2 = dim_symbol_cmd.cmd_dim_symbol_ingest_metadata(args)
    assert code_1 == 0
    assert code_2 == 1


def test_parser_accepts_bronze_api_capture_and_replay_flags():
    parser = build_parser()

    args_capture = parser.parse_args(
        [
            "bronze",
            "api-capture",
            "--vendor",
            "tardis",
            "--dataset",
            "dim_symbol",
            "--exchange",
            "binance-futures",
            "--capture-only",
        ]
    )
    assert args_capture.vendor == "tardis"
    assert args_capture.dataset == "dim_symbol"
    assert args_capture.capture_only is True

    args_replay = parser.parse_args(
        [
            "bronze",
            "api-replay",
            "--vendor",
            "coingecko",
            "--dataset",
            "dim_asset_stats",
            "--force",
        ]
    )
    assert args_replay.vendor == "coingecko"
    assert args_replay.dataset == "dim_asset_stats"
    assert args_replay.force is True


def test_parser_accepts_dim_asset_stats_capture_flags():
    parser = build_parser()

    args_sync = parser.parse_args(
        [
            "silver",
            "dim-asset-stats",
            "sync",
            "--date",
            "2026-01-01",
            "--capture-only",
            "--capture-root",
            "/tmp/coingecko",
        ]
    )
    assert args_sync.capture_only is True
    assert args_sync.capture_root == "/tmp/coingecko"

    args_backfill = parser.parse_args(
        [
            "silver",
            "dim-asset-stats",
            "backfill",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--capture-only",
        ]
    )
    assert args_backfill.capture_only is True
