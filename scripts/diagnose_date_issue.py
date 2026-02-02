#!/usr/bin/env python3
"""Diagnose date_integrity check failures."""

from datetime import date

import polars as pl

from pointline.config import get_table_path


def diagnose_date_mismatch(table_name: str = "trades", check_date: str = "2024-05-01"):
    """Diagnose why date_integrity check is failing."""

    dt = date.fromisoformat(check_date)
    table_path = get_table_path(table_name)

    print(f"Diagnosing {table_name} for date {check_date}")
    print(f"Table path: {table_path}")
    print("=" * 60)

    # Load data for the partition
    lf = pl.scan_delta(str(table_path))
    lf = lf.filter(pl.col("date") == pl.lit(dt))

    # Get sample of timestamps
    sample = (
        lf.select(
            [
                pl.col("ts_local_us"),
                pl.col("date"),
                pl.col("exchange"),
            ]
        )
        .head(10)
        .collect()
    )

    print("\nSample rows from partition:")
    print(sample)

    # Convert ts_local_us to datetime
    print("\nConverting timestamps to dates:")
    for row in sample.iter_rows(named=True):
        ts_us = row["ts_local_us"]
        exchange = row["exchange"]

        # Convert µs to seconds
        ts_sec = ts_us / 1_000_000

        # To UTC datetime
        dt_utc = pl.from_epoch(ts_sec, time_unit="s").dt.replace_time_zone("UTC")
        date_utc = dt_utc.dt.date()

        print(f"  ts_local_us: {ts_us}")
        print(f"  exchange: {exchange}")
        print(f"  partition date: {row['date']}")
        print(f"  UTC date from ts: {date_utc}")
        print()

    # Check min/max timestamps
    print("Timestamp range in partition:")
    range_df = lf.select(
        [
            pl.col("ts_local_us").min().alias("min_ts"),
            pl.col("ts_local_us").max().alias("max_ts"),
        ]
    ).collect()

    min_ts = range_df["min_ts"][0]
    max_ts = range_df["max_ts"][0]

    print(f"  Min ts_local_us: {min_ts}")
    print(f"  Max ts_local_us: {max_ts}")

    # Convert to readable dates
    min_dt = pl.from_epoch(min_ts / 1_000_000, time_unit="s").dt.replace_time_zone("UTC")
    max_dt = pl.from_epoch(max_ts / 1_000_000, time_unit="s").dt.replace_time_zone("UTC")

    print(f"  Min UTC datetime: {min_dt}")
    print(f"  Max UTC datetime: {max_dt}")
    print(f"  Min UTC date: {min_dt.dt.date()}")
    print(f"  Max UTC date: {max_dt.dt.date()}")

    # Check if dates span midnight
    print(f"\nPartition date: {dt}")
    if min_dt.dt.date() != dt:
        print(f"  ⚠️  Min timestamp date ({min_dt.dt.date()}) != partition date!")
    if max_dt.dt.date() != dt:
        print(f"  ⚠️  Max timestamp date ({max_dt.dt.date()}) != partition date!")

    # Count how many timestamps map to different dates
    print("\nDate distribution from timestamps:")
    ts_to_date_expr = (
        (pl.col("ts_local_us") / 1_000_000)
        .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
        .dt.date()
    )

    date_dist = (
        lf.with_columns(ts_to_date_expr.alias("ts_date"))
        .group_by("ts_date")
        .agg(pl.len().alias("count"))
        .sort("ts_date")
        .collect()
    )

    print(date_dist)

    # Summary
    total = lf.select(pl.len()).collect().item()
    mismatched = (
        lf.with_columns(ts_to_date_expr.alias("ts_date"))
        .filter(pl.col("ts_date") != pl.col("date"))
        .select(pl.len())
        .collect()
        .item()
    )

    print("\nSummary:")
    print(f"  Total rows: {total:,}")
    print(f"  Mismatched: {mismatched:,}")
    print(f"  Match rate: {(total - mismatched) / total:.2%}")


if __name__ == "__main__":
    import sys

    table = sys.argv[1] if len(sys.argv) > 1 else "trades"
    check_dt = sys.argv[2] if len(sys.argv) > 2 else "2024-05-01"

    diagnose_date_mismatch(table, check_dt)
