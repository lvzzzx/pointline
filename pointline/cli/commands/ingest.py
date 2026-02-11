"""Ingestion commands."""

from __future__ import annotations

import argparse
import gc
from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl

from pointline.cli.ingestion_factory import TABLE_PARTITIONS, create_ingestion_service
from pointline.cli.utils import compute_sha256, print_files, sorted_files
from pointline.config import BRONZE_ROOT, get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.io.vendors.registry import resolve_table_name


def _extract_partition_value(bronze_file_path: str, key: str) -> str | None:
    """Extract partition value from bronze file path."""
    token = f"{key}="
    for part in Path(bronze_file_path).parts:
        if part.startswith(token):
            value = part.split("=", 1)[1].strip()
            return value or None
    return None


def _resolve_target_table_name(meta: BronzeFileMetadata) -> str | None:
    """Resolve target table name from bronze file metadata."""
    return resolve_table_name(meta.vendor, meta.data_type, interval=meta.interval)


def _extract_partition_filters(meta: BronzeFileMetadata) -> dict[str, object] | None:
    """Extract partition filters from bronze file metadata."""
    exchange = _extract_partition_value(meta.bronze_file_path, "exchange")

    partition_date = meta.date
    if partition_date is None:
        raw_date = _extract_partition_value(meta.bronze_file_path, "date")
        if raw_date:
            try:
                partition_date = date.fromisoformat(raw_date)
            except ValueError:
                partition_date = None

    if exchange is None or partition_date is None:
        return None

    return {"exchange": exchange, "date": partition_date}


def _run_post_ingest_optimize(
    touched_partitions: dict[str, set[tuple[str, date]]],
    *,
    target_file_size: int | None = None,
    zorder: str | None = None,
) -> int:
    """Run post-ingest optimization on touched partitions."""
    if not touched_partitions:
        print("Post-ingest optimize: no touched partitions to optimize.")
        return 0

    print("\nPost-ingest optimize:")
    z_order_cols = [s.strip() for s in zorder.split(",") if s.strip()] if zorder else None
    optimize_failures = 0

    for table_name in sorted(touched_partitions.keys()):
        repo = BaseDeltaRepository(
            get_table_path(table_name),
            partition_by=TABLE_PARTITIONS.get(table_name),
        )
        touched = sorted(
            touched_partitions[table_name],
            key=lambda item: (item[0], item[1].isoformat()),
        )
        for exchange, partition_date in touched:
            filters = {"exchange": exchange, "date": partition_date}
            try:
                metrics = repo.optimize_partition(
                    filters=filters,
                    target_file_size=target_file_size,
                    z_order=z_order_cols,
                )
                considered = metrics.get("totalConsideredFiles", 0) if metrics else 0
                if considered == 0:
                    print(
                        f"- {table_name} exchange={exchange} date={partition_date}: nothing to optimize"
                    )
                    continue

                print(
                    f"- {table_name} exchange={exchange} date={partition_date}: "
                    f"removed={metrics.get('numFilesRemoved')}, "
                    f"added={metrics.get('numFilesAdded')}, "
                    f"considered={metrics.get('totalConsideredFiles')}"
                )
            except Exception as exc:
                optimize_failures += 1
                print(
                    f"- {table_name} exchange={exchange} date={partition_date}: optimize failed: {exc}"
                )

    return optimize_failures


def cmd_ingest_discover(args: argparse.Namespace) -> int:
    """Discover bronze files for ingestion."""
    # Resolve bronze_root: vendor parameter takes precedence
    if hasattr(args, "vendor") and args.vendor:
        bronze_root = BRONZE_ROOT / args.vendor
    else:
        bronze_root = Path(args.bronze_root)

    # When using --pending-only, we need checksums to match against the manifest
    # Otherwise, skip checksums for faster discovery
    compute_checksums = args.pending_only

    source = LocalBronzeSource(
        bronze_root,
        compute_checksums=compute_checksums,
    )
    files = list(source.list_files(args.glob))

    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]

    if args.pending_only:
        manifest_path = Path(args.manifest_path)
        manifest_repo = DeltaManifestRepository(manifest_path)
        files = manifest_repo.filter_pending(files)

    files = sorted_files(files)
    label = "pending files" if args.pending_only else "files"
    print(f"{label}: {len(files)}")

    # Determine display limit
    limit = getattr(args, "limit", 100)
    if limit == 0:
        limit = None  # Show all files

    print_files(files, limit=limit)
    return 0


def cmd_ingest_run(args: argparse.Namespace) -> int:
    """Run ingestion for pending bronze files."""
    # Resolve bronze_root: vendor parameter takes precedence
    if hasattr(args, "vendor") and args.vendor:
        bronze_root = BRONZE_ROOT / args.vendor
    else:
        bronze_root = Path(args.bronze_root)

    # Discover first, then compute checksums before filtering pending.
    # This avoids false skips when fallback metadata matching is ambiguous.
    source = LocalBronzeSource(
        bronze_root,
        compute_checksums=False,
    )
    manifest_repo = DeltaManifestRepository(get_table_path("ingest_manifest"))

    files = list(source.list_files(args.glob))

    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]

    if not files:
        print("No files to ingest.")
        return 0

    if args.retry_quarantined:
        manifest_df = manifest_repo.read_all()
        quarantined = manifest_df.filter(pl.col("status") == "quarantined")
        if bronze_root.name != "bronze" and "vendor" in quarantined.columns:
            quarantined = quarantined.filter(pl.col("vendor") == bronze_root.name)
        if not quarantined.is_empty():
            quarantined_paths = set(quarantined["bronze_file_name"].to_list())
            files = [f for f in files if f.bronze_file_path in quarantined_paths]
        else:
            print("No quarantined files to retry.")
            return 0

    # Resolve SHA256 for candidate files before manifest filtering.
    hashed_files = []
    for file_meta in files:
        bronze_path = bronze_root / file_meta.bronze_file_path
        sha256 = compute_sha256(bronze_path)
        hashed_files.append(replace(file_meta, sha256=sha256))
    files = hashed_files

    if not args.force:
        files = manifest_repo.filter_pending(files)
        if not files:
            print("No files to ingest.")
            return 0

    files = sorted_files(files)
    print(f"Ingesting {len(files)} file(s)...")

    success_count = 0
    failed_count = 0
    quarantined_count = 0
    touched_partitions: dict[str, set[tuple[str, date]]] = {}

    # Group files by (data_type, interval) for klines, or just data_type for others
    files_by_key: dict[tuple, list] = {}
    for file_meta in files:
        if file_meta.data_type == "klines" and file_meta.interval:
            key = ("klines", file_meta.interval)
        else:
            key = (file_meta.data_type, None)
        files_by_key.setdefault(key, []).append(file_meta)

    for (data_type, interval), type_files in files_by_key.items():
        try:
            service = create_ingestion_service(data_type, manifest_repo, interval=interval)
        except ValueError as exc:
            print(f"Error: {exc}")
            for file_meta in type_files:
                desc = f"{data_type}:{interval}" if interval else data_type
                print(f"✗ {file_meta.bronze_file_path}: Unsupported type {desc}")
                failed_count += 1
            continue

        for file_meta in type_files:
            if args.data_type and file_meta.data_type != args.data_type:
                continue

            dry_run = getattr(args, "dry_run", False)
            file_id = 0 if dry_run else manifest_repo.resolve_file_id(file_meta)
            result = service.ingest_file(
                file_meta,
                file_id,
                bronze_root=bronze_root,
                dry_run=dry_run,
                idempotent_write=not dry_run,
            )

            if (
                args.validate
                and not dry_run
                and result.error_message is None
                and hasattr(service, "validate_ingested")
            ):
                ok, message = service.validate_ingested(
                    file_meta,
                    file_id,
                    bronze_root=bronze_root,
                    sample_size=args.validate_sample_size,
                    seed=args.validate_seed,
                )
                if not ok:
                    result = IngestionResult(
                        row_count=result.row_count,
                        ts_local_min_us=result.ts_local_min_us,
                        ts_local_max_us=result.ts_local_max_us,
                        error_message=message,
                    )

            if result.filtered_symbol_count > 0 and result.error_message is None:
                result = IngestionResult(
                    row_count=result.row_count,
                    ts_local_min_us=result.ts_local_min_us,
                    ts_local_max_us=result.ts_local_max_us,
                    error_message=(
                        "Partial ingestion: "
                        f"{result.filtered_symbol_count} symbol-date pairs quarantined, "
                        f"{result.filtered_row_count} rows filtered"
                    ),
                    failure_reason=result.failure_reason,
                    partial_ingestion=result.partial_ingestion,
                    filtered_symbol_count=result.filtered_symbol_count,
                    filtered_row_count=result.filtered_row_count,
                )

            if (
                result.failure_reason
                in {
                    "missing_symbol",
                    "invalid_validity_window",
                    "all_symbols_quarantined",
                }
                or result.filtered_symbol_count > 0
            ):
                status = "quarantined"
                quarantined_count += 1
            elif result.error_message:
                status = "failed"
                failed_count += 1
            else:
                status = "success"
                success_count += 1
                table_name = _resolve_target_table_name(file_meta)
                partition_filters = _extract_partition_filters(file_meta)
                if table_name in TABLE_PARTITIONS and partition_filters is not None:
                    exchange = partition_filters.get("exchange")
                    partition_date = partition_filters.get("date")
                    if isinstance(exchange, str) and isinstance(partition_date, date):
                        touched_partitions.setdefault(table_name, set()).add(
                            (exchange, partition_date)
                        )

            if not dry_run:
                manifest_repo.update_status(file_id, status, file_meta, result)

            if status == "success":
                prefix = "[DRY RUN] " if dry_run else ""
                print(f"{prefix}✓ {file_meta.bronze_file_path}: {result.row_count} rows")
            elif status == "quarantined":
                print(f"⚠ {file_meta.bronze_file_path}: QUARANTINED - {result.error_message}")
            else:
                print(f"✗ {file_meta.bronze_file_path}: FAILED - {result.error_message}")

            gc.collect()

    summary = (
        f"\nSummary: {success_count} succeeded, "
        f"{failed_count} failed, "
        f"{quarantined_count} quarantined"
    )
    print(summary)

    optimize_failures = 0
    if args.optimize_after_ingest and success_count > 0 and not getattr(args, "dry_run", False):
        optimize_failures = _run_post_ingest_optimize(
            touched_partitions,
            target_file_size=args.optimize_target_file_size,
            zorder=args.optimize_zorder,
        )
        if optimize_failures > 0:
            print(f"Post-ingest optimize completed with {optimize_failures} failure(s).")

    return 0 if failed_count == 0 and optimize_failures == 0 else 1
