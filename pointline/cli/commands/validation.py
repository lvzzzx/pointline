"""Validation log query commands."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl

from pointline.config import get_table_path


def cmd_validation_show(args: argparse.Namespace) -> int:
    """Show validation log information."""
    validation_log_path = Path(args.validation_log_path)

    if not validation_log_path.exists():
        print(f"Validation log not found at: {validation_log_path}")
        print("No validations have been run yet.")
        return 0

    # Read validation log
    df = pl.read_delta(str(validation_log_path))

    if df.is_empty():
        print("Validation log is empty. No validations have been recorded.")
        return 0

    # Apply filters
    if args.status:
        df = df.filter(pl.col("validation_status") == args.status)

    if args.table:
        df = df.filter(pl.col("table_name") == args.table)

    if args.failed_only:
        df = df.filter(pl.col("validation_status") == "failed")

    if args.passed_only:
        df = df.filter(pl.col("validation_status") == "passed")

    if df.is_empty():
        print("No validation records match the specified filters.")
        return 0

    if args.detailed:
        # Show detailed validation records
        # Convert timestamps to readable format
        display_df = df.with_columns(
            [
                pl.from_epoch(pl.col("validated_at"), time_unit="us")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .alias("validated_at_readable"),
            ]
        )

        # Select columns to display
        columns = [
            "validation_id",
            "file_id",
            "table_name",
            "validated_at_readable",
            "validation_status",
            "expected_rows",
            "ingested_rows",
            "missing_rows",
            "extra_rows",
            "mismatched_rows",
            "validation_duration_ms",
        ]

        display_df = display_df.select(columns).sort("validated_at", descending=True)

        if args.limit:
            display_df = display_df.head(args.limit)

        print(display_df)

        # Show mismatch samples if requested
        if args.show_mismatches:
            failed_df = df.filter(
                (pl.col("validation_status") == "failed")
                & (pl.col("mismatch_sample").is_not_null())
            )
            if not failed_df.is_empty():
                print("\n--- Mismatch Samples ---")
                for row in failed_df.iter_rows(named=True):
                    print(
                        f"\nValidation ID: {row['validation_id']} | File ID: {row['file_id']} | Table: {row['table_name']}"
                    )
                    if row["mismatch_sample"]:
                        print(row["mismatch_sample"][:1000])  # Truncate to 1000 chars

    else:
        # Show summary statistics
        summary = df.group_by(["table_name", "validation_status"]).agg(
            [
                pl.len().alias("count"),
                pl.col("expected_rows").sum().alias("total_expected_rows"),
                pl.col("ingested_rows").sum().alias("total_ingested_rows"),
                pl.col("missing_rows").sum().alias("total_missing_rows"),
                pl.col("extra_rows").sum().alias("total_extra_rows"),
                pl.col("mismatched_rows").sum().alias("total_mismatched_rows"),
            ]
        ).sort(["table_name", "validation_status"])

        print("Validation Summary by Table and Status:")
        print(summary)

        # Overall statistics
        print("\nOverall Statistics:")
        total_validations = df.height
        total_passed = df.filter(pl.col("validation_status") == "passed").height
        total_failed = df.filter(pl.col("validation_status") == "failed").height
        pass_rate = (total_passed / total_validations * 100) if total_validations > 0 else 0

        print(f"  Total validations: {total_validations}")
        print(f"  Passed: {total_passed}")
        print(f"  Failed: {total_failed}")
        print(f"  Pass rate: {pass_rate:.2f}%")

        # Most recent validation
        most_recent = df.sort("validated_at", descending=True).head(1)
        if not most_recent.is_empty():
            latest_ts = most_recent.item(0, "validated_at")
            latest_readable = datetime.fromtimestamp(latest_ts / 1_000_000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            print(f"  Most recent validation: {latest_readable}")

    return 0


def cmd_validation_stats(args: argparse.Namespace) -> int:
    """Show validation statistics and pass rates."""
    validation_log_path = Path(args.validation_log_path)

    if not validation_log_path.exists():
        print("Validation log not found. No validations have been run yet.")
        return 0

    df = pl.read_delta(str(validation_log_path))

    if df.is_empty():
        print("Validation log is empty.")
        return 0

    # Join with ingest_manifest to get exchange information
    manifest_path = get_table_path("ingest_manifest")
    if Path(manifest_path).exists():
        manifest_df = pl.read_delta(str(manifest_path)).select(
            ["file_id", "exchange", "data_type", "symbol"]
        )
        df = df.join(manifest_df, on="file_id", how="left")

        # Pass rate by exchange
        if "exchange" in df.columns:
            exchange_stats = (
                df.group_by("exchange")
                .agg(
                    [
                        pl.len().alias("total"),
                        (pl.col("validation_status") == "passed").sum().alias("passed"),
                    ]
                )
                .with_columns(
                    ((pl.col("passed") / pl.col("total")) * 100).alias("pass_rate")
                )
                .sort("pass_rate", descending=False)
            )

            print("Validation Pass Rate by Exchange:")
            print(exchange_stats)

    # Pass rate by table
    table_stats = (
        df.group_by("table_name")
        .agg(
            [
                pl.len().alias("total"),
                (pl.col("validation_status") == "passed").sum().alias("passed"),
            ]
        )
        .with_columns(((pl.col("passed") / pl.col("total")) * 100).alias("pass_rate"))
        .sort("pass_rate", descending=False)
    )

    print("\nValidation Pass Rate by Table:")
    print(table_stats)

    return 0
