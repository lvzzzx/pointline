"""dim_symbol commands."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import polars as pl

from pointline.cli.utils import parse_effective_ts, read_updates
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.protocols import ApiCaptureRequest
from pointline.services.api_snapshot_service import ApiReplaySummary, ApiSnapshotService
from pointline.services.dim_symbol_service import DimSymbolService


def _print_replay_summary(summary: ApiReplaySummary) -> None:
    print(f"Ingesting {summary.processed_files} metadata file(s) for vendor={summary.vendor}...")
    for item in summary.file_results:
        if item.status == "success":
            print(f"✓ {item.bronze_file_path}: {item.row_count} updates")
        else:
            print(f"✗ {item.bronze_file_path}: {item.error_message}")
    print(f"\nSummary: {summary.success_count} succeeded, {summary.failed_count} failed")


def cmd_dim_symbol_upsert(args: argparse.Namespace) -> int:
    updates = read_updates(Path(args.file))
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)
    service.update(updates)
    print(f"dim_symbol updated: {updates.height} rows")
    return 0


def cmd_dim_symbol_sync(args: argparse.Namespace) -> int:
    """Sync dim_symbol updates from a source."""
    snapshot_service = ApiSnapshotService()

    if args.source == "api":
        if not args.exchange:
            print("Error: --exchange is required when --source=api")
            return 1

        try:
            filter_payload = json.loads(args.filter) if args.filter else None
        except json.JSONDecodeError as exc:
            print(f"Error: invalid --filter JSON: {exc}")
            return 1

        api_key = args.api_key or os.getenv("TARDIS_API_KEY", "")
        effective_ts = parse_effective_ts(args.effective_ts)

        try:
            capture = snapshot_service.capture(
                vendor="tardis",
                dataset="dim_symbol",
                request=ApiCaptureRequest(
                    params={
                        "exchange": args.exchange,
                        "symbol": args.symbol,
                        "filter_payload": filter_payload,
                        "api_key": api_key,
                    },
                    partitions={"exchange": args.exchange.lower()},
                ),
                capture_root=args.capture_root,
            )
        except Exception as exc:
            print(f"Error: {exc}")
            return 1
        print(f"Captured Tardis dim_symbol metadata: {capture.path}")

        if args.capture_only:
            return 0

        try:
            replay_summary = snapshot_service.replay(
                vendor="tardis",
                dataset="dim_symbol",
                bronze_root=capture.bronze_root,
                glob_pattern=str(capture.path.relative_to(capture.bronze_root)),
                exchange=args.exchange,
                table_path=args.table_path,
                force=True,
                rebuild=args.rebuild,
                effective_ts_us=effective_ts,
            )
        except Exception as exc:
            print(f"Error: {exc}")
            return 1
        _print_replay_summary(replay_summary)
        return 0 if replay_summary.failed_count == 0 else 1

    if args.capture_api_response or args.capture_only:
        print("Error: --capture-api-response/--capture-only only supported with --source=api")
        return 2

    source_path = Path(args.source)
    if not source_path.exists():
        print(f"Error: source {source_path} not found")
        return 2

    updates = read_updates(source_path)
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)

    if args.rebuild:
        print(f"Rebuilding history for {updates.select('exchange_symbol').n_unique()} symbols...")
        service.rebuild(updates)
    else:
        print("Applying incremental updates...")
        service.update(updates)

    print("Sync complete.")
    return 0


def cmd_dim_symbol_ingest_metadata(args: argparse.Namespace) -> int:
    """Ingest captured dim_symbol metadata files via manifest semantics."""
    effective_ts_us = parse_effective_ts(args.effective_ts) if args.effective_ts else None
    snapshot_service = ApiSnapshotService()

    try:
        summary = snapshot_service.replay(
            vendor=args.vendor,
            dataset="dim_symbol",
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


def cmd_dim_symbol_sync_tushare(args: argparse.Namespace) -> int:
    """Sync Chinese stock symbols from Tushare to dim_symbol via capture/replay."""
    snapshot_service = ApiSnapshotService()

    try:
        capture = snapshot_service.capture(
            vendor="tushare",
            dataset="dim_symbol",
            request=ApiCaptureRequest(
                params={
                    "exchange": args.exchange,
                    "include_delisted": args.include_delisted,
                    "token": args.token,
                },
                partitions={"exchange": args.exchange.lower()},
            ),
            capture_root=args.capture_root,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    print(f"Captured Tushare dim_symbol metadata: {capture.path}")

    if args.capture_only:
        return 0

    try:
        replay_summary = snapshot_service.replay(
            vendor="tushare",
            dataset="dim_symbol",
            bronze_root=capture.bronze_root,
            glob_pattern=str(capture.path.relative_to(capture.bronze_root)),
            exchange=args.exchange,
            table_path=args.table_path,
            force=True,
            rebuild=args.rebuild,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    _print_replay_summary(replay_summary)
    return 0 if replay_summary.failed_count == 0 else 1


def cmd_dim_symbol_sync_from_stock_basic_cn(args: argparse.Namespace) -> int:
    """Sync dim_symbol from silver.stock_basic_cn snapshot."""
    from pointline.io.vendors.tushare.stock_basic_cn import (
        build_dim_symbol_updates_from_stock_basic_cn,
    )

    try:
        stock_basic = pl.read_delta(str(args.stock_basic_path))
    except Exception as exc:
        print(f"Error reading stock_basic_cn table: {exc}")
        return 1

    if stock_basic.is_empty():
        print("Warning: stock_basic_cn is empty.")
        return 0

    updates = build_dim_symbol_updates_from_stock_basic_cn(stock_basic)
    if updates.is_empty():
        print("Warning: No valid symbols after transformation.")
        return 0

    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)

    if args.rebuild:
        print(f"Rebuilding history for {updates.select('exchange_symbol').n_unique()} symbols...")
        service.rebuild(updates)
    else:
        print("Applying incremental updates...")
        service.update(updates)

    print("Sync complete.")
    return 0
