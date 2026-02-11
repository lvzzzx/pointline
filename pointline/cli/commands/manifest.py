"""Manifest commands."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from pointline.cli.utils import compute_sha256
from pointline.io.delta_manifest_repo import DeltaManifestRepository


def _extract_partition_from_path_expr(key: str) -> pl.Expr:
    """Create Polars expression to extract partition value from path."""
    return pl.col("bronze_file_name").cast(pl.Utf8).str.extract(rf"(?:^|/){key}=([^/]+)", 1)


def _filter_partition_column(
    df: pl.DataFrame,
    *,
    key: str,
    value: str | None,
) -> tuple[pl.DataFrame, bool]:
    """Filter DataFrame by partition column value."""
    if value is None:
        return df, True
    if key in df.columns:
        return df.filter(pl.col(key) == value), True
    if "bronze_file_name" in df.columns:
        return df.filter(_extract_partition_from_path_expr(key) == value), True
    return df, False


def cmd_manifest_show(args: argparse.Namespace) -> int:
    """Show manifest information."""
    manifest_repo = DeltaManifestRepository(Path(args.manifest_path))
    df = manifest_repo.read_all()

    if df.is_empty():
        print("manifest: empty")
        return 0

    if args.vendor:
        if "vendor" in df.columns:
            df = df.filter(pl.col("vendor") == args.vendor)
        else:
            print("manifest: vendor filter ignored (vendor column not present)")
    if args.status:
        df = df.filter(pl.col("status") == args.status)
    if args.exchange:
        df, applied = _filter_partition_column(df, key="exchange", value=args.exchange)
        if not applied:
            print("manifest: exchange filter ignored (exchange column not present)")
    if args.data_type:
        df = df.filter(pl.col("data_type") == args.data_type)
    if args.symbol:
        df, applied = _filter_partition_column(df, key="symbol", value=args.symbol)
        if not applied:
            print("manifest: symbol filter ignored (symbol column not present)")

    if df.is_empty():
        print("manifest: no matching records")
        return 0

    if args.detailed:
        if args.limit:
            df = df.head(args.limit)

        display_cols = [
            "file_id",
            "vendor",
            "data_type",
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

        exchange_df = df
        exchange_col = "exchange"
        if exchange_col not in exchange_df.columns and "bronze_file_name" in exchange_df.columns:
            exchange_col = "_exchange_from_path"
            exchange_df = exchange_df.with_columns(
                _extract_partition_from_path_expr("exchange").alias(exchange_col)
            )
        if exchange_col in exchange_df.columns:
            exchange_summary = (
                exchange_df.filter(pl.col(exchange_col).is_not_null())
                .group_by(exchange_col)
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )
            if exchange_summary.height > 0:
                print("\nexchange counts:")
                for row in exchange_summary.iter_rows(named=True):
                    print(f"  {row[exchange_col]}: {row['count']}")

        print(f"\ntotal rows: {df.height}")

    return 0


def cmd_manifest_backfill_sha256(args: argparse.Namespace) -> int:
    """Backfill SHA256 hashes for manifest entries."""
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
