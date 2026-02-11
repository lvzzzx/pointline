from __future__ import annotations

import argparse

from pointline.cli.commands import api_snapshot as api_snapshot_cmd
from pointline.services.api_snapshot_service import (
    ApiCaptureResult,
    ApiReplayFileResult,
    ApiReplaySummary,
)


def _ok_summary(vendor: str, dataset: str) -> ApiReplaySummary:
    return ApiReplaySummary(
        vendor=vendor,
        dataset=dataset,
        discovered_files=1,
        processed_files=1,
        success_count=1,
        failed_count=0,
        file_results=[
            ApiReplayFileResult(
                bronze_file_path=f"type={dataset}_metadata/f.jsonl.gz",
                status="success",
                row_count=1,
                error_message=None,
            )
        ],
    )


def test_bronze_api_capture_capture_only(monkeypatch, tmp_path):
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
            return _ok_summary("tardis", "dim_symbol")

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(api_snapshot_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        vendor="tardis",
        dataset="dim_symbol",
        capture_root=str(tmp_path / "bronze"),
        capture_only=True,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_symbol"),
        force=False,
        rebuild=False,
        effective_ts=None,
        exchange="binance-futures",
        symbol=None,
        filter=None,
        api_key="x",
        token="",
        include_delisted=False,
        mode="daily",
        date=None,
        start_date=None,
        end_date=None,
        base_assets=None,
    )

    code = api_snapshot_cmd.cmd_bronze_api_capture(args)
    assert code == 0
    assert len(fake_service.capture_calls) == 1
    assert len(fake_service.replay_calls) == 0


def test_bronze_api_replay_success(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def replay(self, **kwargs):
            return _ok_summary("coingecko", "dim_asset_stats")

    monkeypatch.setattr(api_snapshot_cmd, "ApiSnapshotService", lambda: FakeSnapshotService())

    args = argparse.Namespace(
        vendor="coingecko",
        dataset="dim_asset_stats",
        bronze_root=str(tmp_path / "bronze"),
        glob="**/*.jsonl.gz",
        exchange=None,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_asset_stats"),
        force=False,
        rebuild=False,
        effective_ts=None,
    )

    code = api_snapshot_cmd.cmd_bronze_api_replay(args)
    assert code == 0


def test_bronze_api_replay_missing_root_returns_2(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def replay(self, **kwargs):
            raise FileNotFoundError("metadata root not found")

    monkeypatch.setattr(api_snapshot_cmd, "ApiSnapshotService", lambda: FakeSnapshotService())

    args = argparse.Namespace(
        vendor="coingecko",
        dataset="dim_asset_stats",
        bronze_root=str(tmp_path / "bronze"),
        glob="**/*.jsonl.gz",
        exchange=None,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_asset_stats"),
        force=False,
        rebuild=False,
        effective_ts=None,
    )

    code = api_snapshot_cmd.cmd_bronze_api_replay(args)
    assert code == 2


def test_build_capture_request_for_coingecko_range():
    args = argparse.Namespace(
        vendor="coingecko",
        dataset="dim_asset_stats",
        mode="range",
        base_assets="BTC,ETH",
        start_date="2026-01-01",
        end_date="2026-01-02",
        date=None,
        exchange=None,
        include_delisted=False,
        token="",
        api_key="k",
        symbol=None,
        filter=None,
    )

    request = api_snapshot_cmd._build_capture_request(args)
    assert request.params["mode"] == "range"
    assert request.params["base_assets"] == ["BTC", "ETH"]
    assert request.partitions == {"date": "2026-01-01"}
