"""``pointline vacuum`` — tombstone cleanup for Delta tables."""

from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("vacuum", help="Clean up tombstoned files from a Delta table")
    p.add_argument("table", help="Table name (e.g. trades, cn_order_events)")
    p.add_argument("--silver-root", default=None, help="Silver data root directory")
    p.add_argument(
        "--retention-hours", type=int, default=168, help="Retention period in hours (default: 168)"
    )
    p.add_argument(
        "--no-dry-run", action="store_true", help="Actually delete files (default is dry-run)"
    )
    p.add_argument("--full", action="store_true", help="Full vacuum (all versions)")
    p.set_defaults(handler=_handle)


def _handle(args: argparse.Namespace) -> int:
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._stores import build_stores

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)
    stores = build_stores(silver_root)
    optimizer = stores["optimizer"]

    dry_run = not args.no_dry_run

    report = optimizer.vacuum_table(
        table_name=args.table,
        retention_hours=args.retention_hours,
        dry_run=dry_run,
        full=args.full,
    )

    mode = "DRY RUN" if report.dry_run else "LIVE"
    print(f"Vacuum [{mode}]: {report.table_name}")
    print(f"Retention:     {report.retention_hours}h")
    print(f"Files deleted: {report.deleted_count}")
    if report.deleted_files and report.deleted_count <= 20:
        for f in report.deleted_files:
            print(f"  {f}")
    return 0
