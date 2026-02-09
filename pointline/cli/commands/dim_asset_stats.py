"""dim_asset_stats commands."""

from __future__ import annotations

import argparse
from datetime import datetime

from pointline.io.protocols import ApiCaptureRequest
from pointline.services.api_snapshot_service import ApiReplaySummary, ApiSnapshotService


def _parse_iso_date(raw: str, *, field: str) -> datetime.date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid {field} format: {raw} (expected YYYY-MM-DD)") from exc


def _parse_base_assets(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [asset.strip().upper() for asset in raw.split(",") if asset.strip()]


def _print_replay_summary(summary: ApiReplaySummary) -> None:
    print(f"Ingesting {summary.processed_files} metadata file(s) for vendor={summary.vendor}...")
    for item in summary.file_results:
        if item.status == "success":
            print(f"✓ {item.bronze_file_path}: {item.row_count} updates")
        else:
            print(f"✗ {item.bronze_file_path}: {item.error_message}")
    print(f"\nSummary: {summary.success_count} succeeded, {summary.failed_count} failed")


def cmd_dim_asset_stats_sync(args: argparse.Namespace) -> int:
    """Sync dim_asset_stats for a single date."""
    if (args.provider or "coingecko").lower() != "coingecko":
        print("Error: Only provider=coingecko supports API snapshot capture/replay")
        return 1

    try:
        target_date = _parse_iso_date(args.date, field="date")
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    base_assets = _parse_base_assets(args.base_assets)

    print(f"Syncing dim_asset_stats for date: {target_date}")
    if base_assets:
        print(f"Base assets: {', '.join(base_assets)}")
    else:
        print("Syncing all assets from dim_symbol")

    snapshot_service = ApiSnapshotService()
    try:
        capture = snapshot_service.capture(
            vendor="coingecko",
            dataset="dim_asset_stats",
            request=ApiCaptureRequest(
                params={
                    "mode": "daily",
                    "date": target_date.isoformat(),
                    "base_assets": base_assets,
                    "api_key": args.api_key,
                },
                partitions={"date": target_date.isoformat()},
            ),
            capture_root=args.capture_root,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    print(f"Captured CoinGecko dim_asset_stats metadata: {capture.path}")
    if args.capture_only:
        return 0

    try:
        summary = snapshot_service.replay(
            vendor="coingecko",
            dataset="dim_asset_stats",
            bronze_root=capture.bronze_root,
            glob_pattern=str(capture.path.relative_to(capture.bronze_root)),
            table_path=args.table_path,
            force=True,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    _print_replay_summary(summary)
    return 0 if summary.failed_count == 0 else 1


def cmd_dim_asset_stats_backfill(args: argparse.Namespace) -> int:
    """Backfill historical dim_asset_stats for a date range."""
    if (args.provider or "coingecko").lower() != "coingecko":
        print("Error: Only provider=coingecko supports API snapshot capture/replay")
        return 1

    try:
        start_date = _parse_iso_date(args.start_date, field="start_date")
        end_date = _parse_iso_date(args.end_date, field="end_date")
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    if start_date > end_date:
        print("Error: start_date must be <= end_date")
        return 1

    base_assets = _parse_base_assets(args.base_assets)
    total_days = (end_date - start_date).days + 1

    print(f"Backfilling dim_asset_stats from {start_date} to {end_date} ({total_days} days)")
    if base_assets:
        print(f"Base assets: {', '.join(base_assets)}")
    else:
        print("Syncing all assets from dim_symbol")

    snapshot_service = ApiSnapshotService()
    try:
        capture = snapshot_service.capture(
            vendor="coingecko",
            dataset="dim_asset_stats",
            request=ApiCaptureRequest(
                params={
                    "mode": "range",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "base_assets": base_assets,
                    "api_key": args.api_key,
                },
                partitions={"date": start_date.isoformat()},
            ),
            capture_root=args.capture_root,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    print(f"Captured CoinGecko dim_asset_stats metadata: {capture.path}")
    if args.capture_only:
        return 0

    try:
        summary = snapshot_service.replay(
            vendor="coingecko",
            dataset="dim_asset_stats",
            bronze_root=capture.bronze_root,
            glob_pattern=str(capture.path.relative_to(capture.bronze_root)),
            table_path=args.table_path,
            force=True,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    _print_replay_summary(summary)
    return 0 if summary.failed_count == 0 else 1
