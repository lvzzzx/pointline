from __future__ import annotations

import argparse

from pointline.cli.commands import dim_asset_stats as dim_asset_stats_cmd
from pointline.services.api_snapshot_service import (
    ApiCaptureResult,
    ApiReplayFileResult,
    ApiReplaySummary,
)


def _ok_summary():
    return ApiReplaySummary(
        vendor="coingecko",
        dataset="dim_asset_stats",
        discovered_files=1,
        processed_files=1,
        success_count=1,
        failed_count=0,
        file_results=[
            ApiReplayFileResult(
                bronze_file_path="type=dim_asset_stats_metadata/f.jsonl.gz",
                status="success",
                row_count=5,
                error_message=None,
            )
        ],
    )


def test_dim_asset_stats_sync_capture_only(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.capture_calls = []
            self.replay_calls = []

        def capture(self, **kwargs):
            self.capture_calls.append(kwargs)
            out = tmp_path / "bronze" / "type=dim_asset_stats_metadata" / "f.jsonl.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("", encoding="utf-8")
            return ApiCaptureResult(
                vendor="coingecko",
                dataset="dim_asset_stats",
                bronze_root=tmp_path / "bronze",
                path=out,
                snapshot_ts_us=1,
                row_count=1,
            )

        def replay(self, **kwargs):
            self.replay_calls.append(kwargs)
            return _ok_summary()

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_asset_stats_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        date="2026-01-01",
        base_assets="BTC,ETH",
        table_path=str(tmp_path / "dim_asset_stats"),
        api_key="key",
        provider="coingecko",
        capture_only=True,
        capture_root=str(tmp_path / "bronze"),
    )

    code = dim_asset_stats_cmd.cmd_dim_asset_stats_sync(args)
    assert code == 0
    assert len(fake_service.capture_calls) == 1
    assert len(fake_service.replay_calls) == 0


def test_dim_asset_stats_sync_replay(monkeypatch, tmp_path):
    class FakeSnapshotService:
        def __init__(self):
            self.replay_calls = []

        def capture(self, **kwargs):
            out = tmp_path / "bronze" / "type=dim_asset_stats_metadata" / "f.jsonl.gz"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("", encoding="utf-8")
            return ApiCaptureResult(
                vendor="coingecko",
                dataset="dim_asset_stats",
                bronze_root=tmp_path / "bronze",
                path=out,
                snapshot_ts_us=1,
                row_count=1,
            )

        def replay(self, **kwargs):
            self.replay_calls.append(kwargs)
            return _ok_summary()

    fake_service = FakeSnapshotService()
    monkeypatch.setattr(dim_asset_stats_cmd, "ApiSnapshotService", lambda: fake_service)

    args = argparse.Namespace(
        date="2026-01-01",
        base_assets=None,
        table_path=str(tmp_path / "dim_asset_stats"),
        api_key="key",
        provider="coingecko",
        capture_only=False,
        capture_root=str(tmp_path / "bronze"),
    )

    code = dim_asset_stats_cmd.cmd_dim_asset_stats_sync(args)
    assert code == 0
    assert len(fake_service.replay_calls) == 1


def test_dim_asset_stats_backfill_validation_errors(tmp_path):
    args_provider = argparse.Namespace(
        start_date="2026-01-01",
        end_date="2026-01-02",
        base_assets=None,
        table_path=str(tmp_path / "dim_asset_stats"),
        api_key="",
        provider="coinmarketcap",
        capture_only=False,
        capture_root=None,
    )
    assert dim_asset_stats_cmd.cmd_dim_asset_stats_backfill(args_provider) == 1

    args_order = argparse.Namespace(
        start_date="2026-01-03",
        end_date="2026-01-02",
        base_assets=None,
        table_path=str(tmp_path / "dim_asset_stats"),
        api_key="",
        provider="coingecko",
        capture_only=False,
        capture_root=None,
    )
    assert dim_asset_stats_cmd.cmd_dim_asset_stats_backfill(args_order) == 1
