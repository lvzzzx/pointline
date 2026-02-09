"""dim_symbol commands."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from pointline.cli.utils import compute_sha256, parse_effective_ts, read_updates
from pointline.config import get_bronze_root
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.io.vendors.tardis import TardisClient, build_updates_from_instruments
from pointline.services.dim_symbol_service import DimSymbolService


def _sanitize_component(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return clean or "unknown"


def _capture_dim_symbol_api_response(
    *,
    vendor: str,
    exchange: str,
    records: list[dict[str, Any]],
    source_name: str,
    capture_root: str | None = None,
    snapshot_ts_us: int | None = None,
) -> Path:
    if snapshot_ts_us is None:
        snapshot_ts_us = int(time.time() * 1_000_000)
    snapshot_date = time.strftime("%Y-%m-%d", time.gmtime(snapshot_ts_us / 1_000_000))

    root = Path(capture_root).expanduser() if capture_root else get_bronze_root(vendor)
    exchange_token = _sanitize_component(exchange.lower())
    source_token = _sanitize_component(source_name)
    out_dir = (
        root
        / "type=dim_symbol_metadata"
        / f"exchange={exchange_token}"
        / f"date={snapshot_date}"
        / f"snapshot_ts={snapshot_ts_us}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{vendor}_{source_token}_{exchange_token}_{snapshot_ts_us}.jsonl.gz"

    with gzip.open(out_path, "wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            handle.write("\n")

    return out_path


def _extract_partition_value(relative_path: str, key: str) -> str | None:
    token = f"{key}="
    for part in Path(relative_path).parts:
        if part.startswith(token):
            value = part.split("=", 1)[1].strip()
            return value or None
    return None


def _load_captured_jsonl(path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    rows: list[dict[str, Any]] = []
    with opener(path, "rt", encoding="utf-8") as handle:  # type: ignore[arg-type]
        for line in handle:
            payload = line.strip()
            if not payload:
                continue
            rows.append(json.loads(payload))
    return rows


def _discover_metadata_files(
    *,
    vendor: str,
    bronze_root: Path,
    glob_pattern: str,
    exchange_filter: str | None = None,
) -> list[BronzeFileMetadata]:
    results: list[BronzeFileMetadata] = []
    exchange_filter_norm = exchange_filter.lower() if exchange_filter else None

    for path in bronze_root.glob(glob_pattern):
        if not path.is_file():
            continue

        rel = path.relative_to(bronze_root)
        rel_str = str(rel)
        exchange = _extract_partition_value(rel_str, "exchange")
        if exchange_filter_norm and (exchange or "").lower() != exchange_filter_norm:
            continue

        date_value: date | None = None
        date_raw = _extract_partition_value(rel_str, "date")
        if date_raw:
            try:
                date_value = date.fromisoformat(date_raw)
            except ValueError:
                date_value = None

        data_type = _extract_partition_value(rel_str, "type") or "dim_symbol_metadata"
        stat = path.stat()
        results.append(
            BronzeFileMetadata(
                vendor=vendor,
                data_type=data_type,
                bronze_file_path=rel_str,
                file_size_bytes=stat.st_size,
                last_modified_ts=int(stat.st_mtime * 1_000_000),
                sha256=compute_sha256(path),
                date=date_value,
            )
        )

    return sorted(results, key=lambda meta: (meta.date or date.min, meta.bronze_file_path))


def _build_updates_from_captured_metadata(
    *,
    vendor: str,
    meta: BronzeFileMetadata,
    bronze_root: Path,
    rebuild: bool,
    effective_ts_us: int | None,
) -> pl.DataFrame:
    file_path = bronze_root / meta.bronze_file_path
    records = _load_captured_jsonl(file_path)
    if not records:
        return pl.DataFrame()

    if vendor == "tardis":
        exchange = _extract_partition_value(meta.bronze_file_path, "exchange")
        if not exchange:
            raise ValueError(
                f"Captured metadata path missing exchange partition: {meta.bronze_file_path}"
            )
        return build_updates_from_instruments(
            records,
            exchange=exchange,
            effective_ts=effective_ts_us,
            rebuild=rebuild,
        )

    if vendor == "tushare":
        from pointline.io.vendors.tushare.stock_basic_cn import (
            build_dim_symbol_updates_from_stock_basic_cn,
        )

        return build_dim_symbol_updates_from_stock_basic_cn(pl.DataFrame(records))

    raise ValueError(f"Unsupported vendor for metadata ingest: {vendor}")


def cmd_dim_symbol_upsert(args: argparse.Namespace) -> int:
    updates = read_updates(Path(args.file))
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)
    service.update(updates)
    print(f"dim_symbol updated: {updates.height} rows")
    return 0


def cmd_dim_symbol_sync(args: argparse.Namespace) -> int:
    """Sync dim_symbol updates from a source."""
    capture_requested = args.capture_api_response or args.capture_only

    if args.source == "api":
        if not args.exchange:
            print("Error: --exchange is required when --source=api")
            return 1

        try:
            filter_payload = json.loads(args.filter) if args.filter else None
        except json.JSONDecodeError as exc:
            print(f"Error: invalid --filter JSON: {exc}")
            return 1

        effective_ts = parse_effective_ts(args.effective_ts)
        api_key = args.api_key or os.getenv("TARDIS_API_KEY", "")

        client = TardisClient(api_key=api_key)
        instruments = client.fetch_instruments(
            args.exchange,
            symbol=args.symbol,
            filter_payload=filter_payload,
        )

        if capture_requested:
            capture_path = _capture_dim_symbol_api_response(
                vendor="tardis",
                exchange=args.exchange,
                records=instruments,
                source_name="instruments",
                capture_root=args.capture_root,
            )
            print(f"Captured Tardis dim_symbol metadata: {capture_path}")
            if args.capture_only:
                return 0

        updates = build_updates_from_instruments(
            instruments,
            exchange=args.exchange,
            effective_ts=effective_ts,
            rebuild=args.rebuild,
        )
    else:
        if capture_requested:
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
    bronze_root = (
        Path(args.bronze_root).expanduser() if args.bronze_root else get_bronze_root(args.vendor)
    )
    if not bronze_root.exists():
        print(f"Error: metadata root not found: {bronze_root}")
        return 2

    files = _discover_metadata_files(
        vendor=args.vendor,
        bronze_root=bronze_root,
        glob_pattern=args.glob,
        exchange_filter=args.exchange,
    )
    if not files:
        print("No captured metadata files found.")
        return 0

    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
    if not args.force:
        files = manifest_repo.filter_pending(files)
    if not files:
        print("No metadata files to ingest.")
        return 0

    effective_ts_us = parse_effective_ts(args.effective_ts) if args.effective_ts else None
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)

    print(f"Ingesting {len(files)} metadata file(s) for vendor={args.vendor}...")
    success_count = 0
    failed_count = 0

    for meta in files:
        file_id = manifest_repo.resolve_file_id(meta)
        try:
            updates = _build_updates_from_captured_metadata(
                vendor=args.vendor,
                meta=meta,
                bronze_root=bronze_root,
                rebuild=args.rebuild,
                effective_ts_us=effective_ts_us,
            )

            if updates.is_empty():
                result = IngestionResult(
                    row_count=0,
                    ts_local_min_us=0,
                    ts_local_max_us=0,
                    error_message=None,
                )
                status = "success"
                success_count += 1
                print(f"✓ {meta.bronze_file_path}: 0 updates (empty capture)")
            else:
                if args.rebuild:
                    service.rebuild(updates)
                else:
                    service.update(updates)
                min_ts = (
                    int(updates["valid_from_ts"].min()) if "valid_from_ts" in updates.columns else 0
                )
                max_ts = (
                    int(updates["valid_from_ts"].max()) if "valid_from_ts" in updates.columns else 0
                )
                result = IngestionResult(
                    row_count=updates.height,
                    ts_local_min_us=min_ts,
                    ts_local_max_us=max_ts,
                    error_message=None,
                )
                status = "success"
                success_count += 1
                print(f"✓ {meta.bronze_file_path}: {updates.height} updates")
        except Exception as exc:
            status = "failed"
            failed_count += 1
            result = IngestionResult(
                row_count=0,
                ts_local_min_us=0,
                ts_local_max_us=0,
                error_message=str(exc),
            )
            print(f"✗ {meta.bronze_file_path}: {exc}")

        manifest_repo.update_status(file_id, status, meta, result)

    print(f"\nSummary: {success_count} succeeded, {failed_count} failed")
    return 0 if failed_count == 0 else 1


def cmd_dim_symbol_sync_tushare(args: argparse.Namespace) -> int:
    """Sync Chinese stock symbols from Tushare to dim_symbol."""
    from pointline.io.vendors.tushare import TushareClient
    from pointline.io.vendors.tushare.stock_basic_cn import (
        build_dim_symbol_updates_from_stock_basic_cn,
    )

    capture_requested = args.capture_api_response or args.capture_only

    try:
        # Initialize Tushare client
        client = TushareClient(token=args.token)
    except (ValueError, ImportError) as exc:
        print(f"Error: {exc}")
        return 1

    # Fetch stocks based on exchange
    print(f"Fetching {args.exchange.upper()} stocks from Tushare...")
    try:
        if args.exchange.lower() == "szse":
            df = client.get_szse_stocks(include_delisted=args.include_delisted)
        elif args.exchange.lower() == "sse":
            df = client.get_sse_stocks(include_delisted=args.include_delisted)
        elif args.exchange.lower() == "all":
            df = client.get_all_stocks(
                exchanges=["SZSE", "SSE"], include_delisted=args.include_delisted
            )
        else:
            print(f"Error: Invalid exchange '{args.exchange}'. Use 'szse', 'sse', or 'all'.")
            return 1
    except Exception as exc:
        print(f"Error fetching data from Tushare: {exc}")
        return 1

    if df.is_empty():
        print("Warning: No stocks returned from Tushare.")
        return 0

    print(f"Fetched {len(df)} stocks from Tushare")

    if capture_requested:
        capture_path = _capture_dim_symbol_api_response(
            vendor="tushare",
            exchange=args.exchange,
            records=df.to_dicts(),
            source_name="stock_basic",
            capture_root=args.capture_root,
        )
        print(f"Captured Tushare dim_symbol metadata: {capture_path}")
        if args.capture_only:
            return 0

    print("Transforming to dim_symbol schema...")
    updates = build_dim_symbol_updates_from_stock_basic_cn(df)

    if updates.is_empty():
        print("Warning: No valid symbols after transformation.")
        return 0

    print(f"Transformed {len(updates)} symbols")

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
