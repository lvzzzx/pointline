"""Data quality commands."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date as date_type
from pathlib import Path

import polars as pl

from pointline.cli.utils import parse_date_arg
from pointline.dq.registry import list_dq_tables
from pointline.dq.runner import (
    run_dq_for_all_tables,
    run_dq_for_all_tables_partitioned,
    run_dq_for_table,
    run_dq_partitioned,
)
from pointline.tables.dq_summary import DQ_SUMMARY_SCHEMA, normalize_dq_summary_schema

logger = logging.getLogger(__name__)


def _parse_date(value: str | None) -> date_type | None:
    if not value:
        return None
    parsed = parse_date_arg(value)
    if parsed is None:
        raise ValueError(f"Invalid date: {value}")
    return parsed


def _write_dq_summary(df: pl.DataFrame, *, table_path: Path) -> None:
    if df.is_empty():
        return
    normalized = normalize_dq_summary_schema(df)
    if not table_path.exists():
        normalized.write_delta(str(table_path), mode="overwrite")
    else:
        normalized.write_delta(str(table_path), mode="append")


def _progress_printer(*, table_name: str, index: int, total: int, date_partition) -> None:
    print(f"[dq] {table_name}: {index}/{total} date={date_partition}")


def cmd_dq_run(args: argparse.Namespace) -> int:
    date_partition = _parse_date(args.date)
    dq_summary_path = Path(args.dq_summary_path)
    progress_cb = _progress_printer if args.progress else None

    if args.table == "all":
        if args.partitioned:
            summary_df = run_dq_for_all_tables_partitioned(
                start_date=_parse_date(args.start_date),
                end_date=_parse_date(args.end_date),
                max_dates=args.max_dates,
                include_rollup=not args.no_rollup,
                progress_cb=progress_cb,
            )
        else:
            summary_df = run_dq_for_all_tables(date_partition=date_partition)
    else:
        if args.partitioned and date_partition is None:
            summary_df = run_dq_partitioned(
                args.table,
                start_date=_parse_date(args.start_date),
                end_date=_parse_date(args.end_date),
                max_dates=args.max_dates,
                include_rollup=not args.no_rollup,
                progress_cb=progress_cb,
            )
        else:
            summary_df = run_dq_for_table(args.table, date_partition=date_partition)

    if not args.no_write:
        _write_dq_summary(summary_df, table_path=dq_summary_path)

    print("DQ run complete.")
    print(summary_df.select(list(DQ_SUMMARY_SCHEMA.keys())))
    return 0


def cmd_dq_report(args: argparse.Namespace) -> int:
    dq_summary_path = Path(args.dq_summary_path)
    if not dq_summary_path.exists():
        print(f"dq_summary not found at: {dq_summary_path}. Running DQ now...")
        date_partition = _parse_date(args.date)
        if date_partition is None:
            summary_df = run_dq_partitioned(
                args.table,
                max_dates=1,
                include_rollup=False,
            )
        else:
            summary_df = run_dq_for_table(args.table, date_partition=date_partition)
        _write_dq_summary(summary_df, table_path=dq_summary_path)

    df = pl.read_delta(str(dq_summary_path))
    if df.is_empty():
        print("dq_summary is empty.")
        return 0

    df = df.filter(pl.col("table_name") == args.table)
    date_partition = _parse_date(args.date)
    if date_partition is not None:
        df = df.filter(pl.col("date") == pl.lit(date_partition))

    if df.is_empty():
        print("No dq_summary records match the specified filters.")
        return 0

    if args.latest:
        df = df.sort("validated_at", descending=True).head(1)
    else:
        df = df.sort("validated_at", descending=True).head(args.limit)

    print("DQ summary:")
    print(df.select(list(DQ_SUMMARY_SCHEMA.keys())))
    return 0


def _aggregate_issue_counts(rows: list[dict]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in rows:
        raw = row.get("issue_counts")
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("Malformed issue_counts JSON: %s - %s", raw, exc)
            continue
        for key, value in parsed.items():
            if value is None:
                continue
            totals[key] = totals.get(key, 0) + int(value)
    return totals


def _extract_partition_stats(row: dict) -> tuple[int | None, int | None]:
    raw = row.get("profile_stats")
    if not raw:
        return None, None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Malformed profile_stats JSON: %s - %s", raw, exc)
        return None, None
    partition = parsed.get("_partition")
    if not isinstance(partition, dict):
        return None, None
    file_count = partition.get("file_count")
    total_bytes = partition.get("total_bytes")
    return (int(file_count) if file_count is not None else None,
            int(total_bytes) if total_bytes is not None else None)


def cmd_dq_summary(args: argparse.Namespace) -> int:
    dq_summary_path = Path(args.dq_summary_path)
    if not dq_summary_path.exists():
        print(f"dq_summary not found at: {dq_summary_path}")
        return 0

    df = pl.read_delta(str(dq_summary_path))
    if df.is_empty():
        print("dq_summary is empty.")
        return 0

    df = df.filter(pl.col("table_name") == args.table)
    if df.is_empty():
        print("No dq_summary records found for table.")
        return 0

    rollup = df.filter(pl.col("date").is_null())
    partitions = df.filter(pl.col("date").is_not_null())

    if args.recent:
        partitions = partitions.sort("validated_at", descending=True).head(args.recent)

    partition_rows = partitions.sort("date", descending=True).to_dicts()
    total_partitions = len(partition_rows)
    failed_partitions = sum(1 for row in partition_rows if row.get("status") == "failed")
    latest = partition_rows[0] if partition_rows else None

    max_duration_row = None
    if partition_rows:
        max_duration_row = max(partition_rows, key=lambda r: r.get("validation_duration_ms") or 0)

    issue_totals = _aggregate_issue_counts(partition_rows)

    print(f"DQ health summary for {args.table}:")
    print(f"  partitions scanned: {total_partitions}")
    print(f"  failed partitions: {failed_partitions}")
    if latest:
        print(f"  latest partition: {latest.get('date')} status={latest.get('status')}")
    if max_duration_row:
        file_count, total_bytes = _extract_partition_stats(max_duration_row)
        size_info = ""
        if file_count is not None:
            size_info += f" files={file_count}"
        if total_bytes is not None:
            size_info += f" bytes={total_bytes}"
        print(
            "  slowest partition: "
            f"{max_duration_row.get('date')} "
            f"{max_duration_row.get('validation_duration_ms')}ms{size_info}"
        )
    if issue_totals:
        issue_summary = ", ".join(f"{k}={v}" for k, v in sorted(issue_totals.items()))
        print(f"  issues: {issue_summary}")
    else:
        print("  issues: none")

    if not rollup.is_empty():
        rollup_row = rollup.sort("validated_at", descending=True).row(0, named=True)
        print("  rollup: present")
        print(
            f"  rollup rows: {rollup_row.get('row_count')} "
            f"status={rollup_row.get('status')}"
        )
    else:
        print("  rollup: none")

    return 0


def dq_table_choices() -> list[str]:
    return ["all", *list_dq_tables()]
