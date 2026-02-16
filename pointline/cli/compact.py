"""``pointline compact`` — small-file compaction for Delta tables."""

from __future__ import annotations

import argparse
import json


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("compact", help="Compact small files in a Delta table partition")
    p.add_argument("table", help="Table name (e.g. trades, cn_order_events)")
    p.add_argument(
        "--partitions",
        required=True,
        help='Partition filters as JSON list of dicts, e.g. \'[{"exchange":"deribit","trading_date":"2024-01-01"}]\'',
    )
    p.add_argument("--silver-root", default=None, help="Silver data root directory")
    p.add_argument("--dry-run", action="store_true", help="Report without compacting")
    p.add_argument("--target-size", type=int, default=None, help="Target file size in bytes")
    p.add_argument(
        "--min-small-files", type=int, default=8, help="Min small files to trigger compaction"
    )
    p.set_defaults(handler=_handle)


def _handle(args: argparse.Namespace) -> int:
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._stores import build_stores

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)

    try:
        partitions = json.loads(args.partitions)
    except json.JSONDecodeError as exc:
        print(f"error: invalid --partitions JSON: {exc}")
        return 1

    if not isinstance(partitions, list):
        print("error: --partitions must be a JSON array of dicts")
        return 1

    stores = build_stores(silver_root)
    optimizer = stores["optimizer"]

    report = optimizer.compact_partitions(
        table_name=args.table,
        partitions=partitions,
        target_file_size_bytes=args.target_size,
        min_small_files=args.min_small_files,
        dry_run=args.dry_run,
    )

    print(f"Table:     {report.table_name}")
    print(f"Planned:   {report.planned_partitions}")
    print(f"Attempted: {report.attempted_partitions}")
    print(f"Succeeded: {report.succeeded_partitions}")
    print(f"Skipped:   {report.skipped_partitions}")
    print(f"Failed:    {report.failed_partitions}")

    for pr in report.partitions:
        status = "SKIP" if pr.skipped_reason else ("ERR" if pr.error else "OK")
        detail = pr.skipped_reason or pr.error or f"{pr.before_file_count}->{pr.after_file_count}"
        print(f"  {dict(pr.partition)}: {status} ({detail})")

    return 0 if report.failed_partitions == 0 else 2
