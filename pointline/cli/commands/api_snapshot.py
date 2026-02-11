"""Generic API snapshot capture/replay commands."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from pointline.cli.utils import parse_effective_ts
from pointline.io.protocols import ApiCaptureRequest
from pointline.services.api_snapshot_service import ApiReplaySummary, ApiSnapshotService


def _print_replay_summary(summary: ApiReplaySummary) -> None:
    print(f"Ingesting {summary.processed_files} metadata file(s) for vendor={summary.vendor}...")
    for item in summary.file_results:
        if item.status == "success":
            print(f"✓ {item.bronze_file_path}: {item.row_count} updates")
        else:
            print(f"✗ {item.bronze_file_path}: {item.error_message}")
    print(f"\nSummary: {summary.success_count} succeeded, {summary.failed_count} failed")


def _parse_iso_date(raw: str, *, field: str) -> datetime.date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid {field} format: {raw} (expected YYYY-MM-DD)") from exc


def _parse_base_assets(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [asset.strip().upper() for asset in raw.split(",") if asset.strip()]


def _build_capture_request(args: argparse.Namespace) -> ApiCaptureRequest:
    if args.vendor == "tardis" and args.dataset == "dim_symbol":
        if not args.exchange:
            raise ValueError("--exchange is required for tardis dim_symbol capture")
        filter_payload = json.loads(args.filter) if args.filter else None
        return ApiCaptureRequest(
            params={
                "exchange": args.exchange,
                "symbol": args.symbol,
                "filter_payload": filter_payload,
                "api_key": args.api_key,
            },
            partitions={"exchange": args.exchange.lower()},
        )

    if args.vendor == "tushare" and args.dataset == "dim_symbol":
        if not args.exchange:
            raise ValueError("--exchange is required for tushare dim_symbol capture")
        return ApiCaptureRequest(
            params={
                "exchange": args.exchange,
                "include_delisted": args.include_delisted,
                "token": args.token,
            },
            partitions={"exchange": args.exchange.lower()},
        )

    if args.vendor == "coingecko" and args.dataset == "dim_asset_stats":
        mode = args.mode
        base_assets = _parse_base_assets(args.base_assets)
        if mode == "daily":
            if not args.date:
                raise ValueError("--date is required when --mode=daily")
            date_value = _parse_iso_date(args.date, field="date")
            return ApiCaptureRequest(
                params={
                    "mode": "daily",
                    "date": date_value.isoformat(),
                    "base_assets": base_assets,
                    "api_key": args.api_key,
                },
                partitions={"date": date_value.isoformat()},
            )

        if mode == "range":
            if not args.start_date or not args.end_date:
                raise ValueError("--start-date and --end-date are required when --mode=range")
            start_date = _parse_iso_date(args.start_date, field="start_date")
            end_date = _parse_iso_date(args.end_date, field="end_date")
            if start_date > end_date:
                raise ValueError("start_date must be <= end_date")
            return ApiCaptureRequest(
                params={
                    "mode": "range",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "base_assets": base_assets,
                    "api_key": args.api_key,
                },
                partitions={"date": start_date.isoformat()},
            )

        raise ValueError(f"Unsupported coingecko mode: {mode}")

    raise ValueError(f"Unsupported vendor/dataset combination: {args.vendor}/{args.dataset}")


def cmd_bronze_api_capture(args: argparse.Namespace) -> int:
    snapshot_service = ApiSnapshotService()

    try:
        capture_request = _build_capture_request(args)
        capture = snapshot_service.capture(
            vendor=args.vendor,
            dataset=args.dataset,
            request=capture_request,
            capture_root=args.capture_root,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    print(f"Captured API metadata: {capture.path}")
    if args.capture_only:
        return 0

    effective_ts_us = parse_effective_ts(args.effective_ts) if args.effective_ts else None
    summary = snapshot_service.replay(
        vendor=args.vendor,
        dataset=args.dataset,
        bronze_root=capture.bronze_root,
        glob_pattern=str(capture.path.relative_to(capture.bronze_root)),
        exchange=args.exchange,
        manifest_path=args.manifest_path,
        table_path=args.table_path,
        force=True,
        rebuild=args.rebuild,
        effective_ts_us=effective_ts_us,
    )
    _print_replay_summary(summary)
    return 0 if summary.failed_count == 0 else 1


def cmd_bronze_api_replay(args: argparse.Namespace) -> int:
    snapshot_service = ApiSnapshotService()
    effective_ts_us = parse_effective_ts(args.effective_ts) if args.effective_ts else None

    try:
        summary = snapshot_service.replay(
            vendor=args.vendor,
            dataset=args.dataset,
            bronze_root=args.bronze_root,
            glob_pattern=args.glob,
            exchange=args.exchange,
            manifest_path=args.manifest_path,
            table_path=args.table_path,
            force=args.force,
            rebuild=args.rebuild,
            effective_ts_us=effective_ts_us,
        )
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        return 2
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if summary.discovered_files == 0:
        print("No captured metadata files found.")
        return 0
    if summary.processed_files == 0:
        print("No metadata files to ingest.")
        return 0

    _print_replay_summary(summary)
    return 0 if summary.failed_count == 0 else 1
