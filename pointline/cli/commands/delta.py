"""Delta maintenance commands."""

from __future__ import annotations

import argparse

from pointline.cli.ingestion_factory import TABLE_PARTITIONS
from pointline.cli.utils import parse_partition_filters
from pointline.config import get_table_path
from pointline.io.base_repository import BaseDeltaRepository


def cmd_delta_optimize(args: argparse.Namespace) -> int:
    """Optimize Delta table partitions."""
    try:
        filters = parse_partition_filters(args.partition)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 2

    if not filters:
        print("Error: at least one --partition KEY=VALUE is required")
        return 2

    partition_by = TABLE_PARTITIONS.get(args.table)
    repo = BaseDeltaRepository(get_table_path(args.table), partition_by=partition_by)
    z_order = [s.strip() for s in args.zorder.split(",")] if args.zorder else None
    try:
        metrics = repo.optimize_partition(
            filters=filters,
            target_file_size=args.target_file_size,
            z_order=z_order,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if not metrics or metrics.get("totalConsideredFiles", 0) == 0:
        print("No rows matched the partition filters; nothing to optimize.")
        return 0

    predicate = " AND ".join(f"{k}={v}" for k, v in filters.items())
    print(f"Optimized {args.table} where {predicate}")
    print(
        f"filesRemoved={metrics.get('numFilesRemoved')}, "
        f"filesAdded={metrics.get('numFilesAdded')}, "
        f"totalConsideredFiles={metrics.get('totalConsideredFiles')}"
    )
    return 0


def cmd_delta_vacuum(args: argparse.Namespace) -> int:
    """Vacuum old files from Delta tables."""
    partition_by = TABLE_PARTITIONS.get(args.table)
    repo = BaseDeltaRepository(get_table_path(args.table), partition_by=partition_by)
    dry_run = not args.execute
    try:
        removed = repo.vacuum(
            retention_hours=args.retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=not args.no_retention_check,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if dry_run:
        print(f"Vacuum dry run: {len(removed)} files would be removed.")
    else:
        print(f"Vacuum complete: {len(removed)} files removed.")
    return 0
