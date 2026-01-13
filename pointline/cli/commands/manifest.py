"""Manifest commands."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from pointline.cli.utils import compute_sha256
from pointline.io.delta_manifest_repo import DeltaManifestRepository


def cmd_manifest_show(args: argparse.Namespace) -> int:
    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
    df = manifest_repo.read_all()

    if df.is_empty():
        print("manifest: empty")
        return 0

    if args.status:
        df = df.filter(pl.col("status") == args.status)
    if args.exchange:
        df = df.filter(pl.col("exchange") == args.exchange)
    if args.data_type:
        df = df.filter(pl.col("data_type") == args.data_type)
    if args.symbol:
        df = df.filter(pl.col("symbol") == args.symbol)

    if df.is_empty():
        print("manifest: no matching records")
        return 0

    if args.detailed:
        if args.limit:
            df = df.head(args.limit)

        display_cols = [
            "file_id",
            "exchange",
            "data_type",
            "symbol",
            "date",
            "status",
            "row_count",
            "bronze_file_name",
        ]
        if args.show_errors:
            display_cols.append("error_message")

        available_cols = [col for col in display_cols if col in df.columns]
        display_df = df.select(available_cols)

        print(f"manifest entries ({df.height} total):")
        print(display_df)
    else:
        summary = df.group_by("status").agg(pl.len().alias("count")).sort("status")
        print("manifest status counts:")
        for row in summary.iter_rows(named=True):
            print(f"  {row['status']}: {row['count']}")

        if "exchange" in df.columns:
            exchange_summary = (
                df.group_by("exchange")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )
            if exchange_summary.height > 0:
                print("\nexchange counts:")
                for row in exchange_summary.iter_rows(named=True):
                    print(f"  {row['exchange']}: {row['count']}")

        print(f"\ntotal rows: {df.height}")

    return 0


def cmd_manifest_backfill_sha256(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest_path)
    bronze_root = Path(args.bronze_root)
    manifest_repo = DeltaManifestRepository(manifest_path)
    df = manifest_repo.read_all()

    if df.is_empty():
        print("manifest: empty")
        return 0

    if "sha256" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("sha256"))

    candidates = df.filter(pl.col("sha256").is_null() | (pl.col("sha256") == ""))
    if candidates.is_empty():
        print("manifest: no rows missing sha256")
        return 0

    if args.limit:
        candidates = candidates.head(args.limit)

    total = candidates.height
    updated_rows: list[dict[str, object]] = []
    missing_files: list[str] = []

    for row in candidates.iter_rows(named=True):
        bronze_name = row.get("bronze_file_name")
        if bronze_name is None:
            missing_files.append("<missing bronze_file_name>")
            continue

        path = bronze_root / str(bronze_name)
        if not path.exists():
            missing_files.append(str(path))
            continue

        sha256 = compute_sha256(path)
        updated = dict(row)
        updated["sha256"] = sha256
        updated_rows.append(updated)

    if args.dry_run:
        print("manifest sha256 backfill dry-run:")
        print(f"  candidates: {total}")
        print(f"  would update: {len(updated_rows)}")
        print(f"  missing files: {len(missing_files)}")
        if missing_files:
            sample = missing_files[: min(10, len(missing_files))]
            print(f"  missing sample: {sample}")
        return 0

    if not updated_rows:
        print("manifest: no rows updated (all missing files)")
        return 0

    batch_size = max(1, args.batch_size)
    for start in range(0, len(updated_rows), batch_size):
        batch = updated_rows[start : start + batch_size]
        batch_df = pl.DataFrame(batch, schema=df.schema)
        manifest_repo.merge(batch_df, keys=["file_id"])

    print("manifest sha256 backfill complete:")
    print(f"  candidates: {total}")
    print(f"  updated: {len(updated_rows)}")
    print(f"  missing files: {len(missing_files)}")
    if missing_files:
        sample = missing_files[: min(10, len(missing_files))]
        print(f"  missing sample: {sample}")
    return 0
