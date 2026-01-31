"""Ingestion commands."""

from __future__ import annotations

import argparse
import gc
from pathlib import Path

import polars as pl

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.cli.utils import print_files, sorted_files
from pointline.config import BRONZE_ROOT
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import IngestionResult


def cmd_ingest_discover(args: argparse.Namespace) -> int:
    # Resolve bronze_root: vendor parameter takes precedence
    if hasattr(args, "vendor") and args.vendor:
        bronze_root = BRONZE_ROOT / args.vendor
    else:
        bronze_root = Path(args.bronze_root)

    enable_prehooks = not getattr(args, "no_prehook", False)

    # Discovery doesn't need checksums - skip for performance
    source = LocalBronzeSource(
        bronze_root,
        enable_prehooks=enable_prehooks,
        compute_checksums=False,  # Fast discovery without SHA256
    )
    files = list(source.list_files(args.glob))

    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]

    if args.pending_only:
        manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
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

    enable_prehooks = not getattr(args, "no_prehook", False)

    # Ingestion needs checksums for manifest tracking
    source = LocalBronzeSource(
        bronze_root,
        enable_prehooks=enable_prehooks,
        compute_checksums=True,  # Need SHA256 for manifest
    )
    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))

    files = list(source.list_files(args.glob))

    if args.data_type:
        files = [f for f in files if f.data_type == args.data_type]

    if not args.force:
        files = manifest_repo.filter_pending(files)

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

    files = sorted_files(files)
    print(f"Ingesting {len(files)} file(s)...")

    success_count = 0
    failed_count = 0
    quarantined_count = 0

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

            file_id = manifest_repo.resolve_file_id(file_meta)

            result = service.ingest_file(file_meta, file_id, bronze_root=bronze_root)

            if (
                args.validate
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

            if result.error_message:
                if (
                    "missing_symbol" in result.error_message
                    or "invalid_validity_window" in result.error_message
                ):
                    status = "quarantined"
                    quarantined_count += 1
                else:
                    status = "failed"
                    failed_count += 1
            else:
                status = "success"
                success_count += 1

            manifest_repo.update_status(file_id, status, file_meta, result)

            if status == "success":
                print(f"✓ {file_meta.bronze_file_path}: {result.row_count} rows")
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
    return 0 if failed_count == 0 else 1
