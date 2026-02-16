"""``pointline manifest`` — inspect ingestion manifest history."""

from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("manifest", help="Inspect ingestion manifest")
    sub = p.add_subparsers(dest="manifest_command")

    # manifest list
    list_p = sub.add_parser("list", help="List manifest entries")
    list_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    list_p.add_argument(
        "--status",
        choices=["pending", "success", "failed", "quarantined"],
        default=None,
        help="Filter by ingestion status",
    )
    list_p.add_argument("--vendor", default=None, help="Filter by vendor")
    list_p.add_argument("--data-type", default=None, help="Filter by data type")
    list_p.add_argument("--limit", type=int, default=50, help="Max rows to display (default: 50)")
    list_p.set_defaults(handler=_handle_list)

    # manifest show <file_id>
    show_p = sub.add_parser("show", help="Show full details for one manifest entry")
    show_p.add_argument("file_id", type=int, help="File ID to look up")
    show_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    show_p.set_defaults(handler=_handle_show)

    # manifest summary
    summary_p = sub.add_parser("summary", help="Aggregated status counts")
    summary_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    summary_p.set_defaults(handler=_handle_summary)

    # manifest diff
    diff_p = sub.add_parser(
        "diff",
        help="Find Bronze files not yet in the manifest",
    )
    diff_p.add_argument(
        "--bronze-root",
        default=None,
        help="Vendor bronze root (e.g. /data/lake/bronze/quant360)",
    )
    diff_p.add_argument("--vendor", required=True, help="Vendor name (must match manifest)")
    diff_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    diff_p.add_argument("--data-type", default=None, help="Filter by data type partition")
    diff_p.add_argument("--exchange", default=None, help="Filter by exchange partition")
    diff_p.add_argument(
        "--glob",
        default="**/*.csv.gz",
        dest="glob_pattern",
        help="File glob pattern (default: **/*.csv.gz)",
    )
    diff_p.add_argument("--limit", type=int, default=50, help="Max results (default: 50)")
    diff_p.set_defaults(handler=_handle_diff)


def _load_manifest(args):
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.schemas.control import INGEST_MANIFEST
    from pointline.storage.delta._utils import read_delta_or_empty

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(
        getattr(args, "silver_root", None),
        root=root,
    )
    table_path = silver_root / "ingest_manifest"
    return read_delta_or_empty(table_path, spec=INGEST_MANIFEST)


def _handle_list(args: argparse.Namespace) -> int:
    import polars as pl

    from pointline.cli._output import print_table

    df = _load_manifest(args)
    if df.is_empty():
        print("Manifest is empty — no files have been ingested.")
        return 0

    # Apply filters
    if args.status:
        df = df.filter(pl.col("status") == args.status)
    if args.vendor:
        df = df.filter(pl.col("vendor") == args.vendor)
    if args.data_type:
        df = df.filter(pl.col("data_type") == args.data_type)

    if df.is_empty():
        print("No manifest entries match the given filters.")
        return 0

    # Sort by most recent first
    df = df.sort("file_id", descending=True).head(args.limit)

    display_cols = [
        "file_id",
        "status",
        "vendor",
        "data_type",
        "rows_written",
        "rows_quarantined",
        "trading_date_min",
        "trading_date_max",
        "bronze_path",
    ]
    # Only show columns that exist
    display_cols = [c for c in display_cols if c in df.columns]

    rows = []
    for row in df.select(display_cols).iter_rows(named=True):
        rows.append({k: str(v) if v is not None else "" for k, v in row.items()})

    print_table(rows, columns=display_cols)
    print(f"\n({len(rows)} of {df.height} entries shown)")
    return 0


def _handle_show(args: argparse.Namespace) -> int:
    import polars as pl

    df = _load_manifest(args)
    if df.is_empty():
        print("Manifest is empty.")
        return 0

    entry = df.filter(pl.col("file_id") == args.file_id)
    if entry.is_empty():
        print(f"No manifest entry with file_id={args.file_id}")
        return 1

    row = entry.row(0, named=True)
    max_key_len = max(len(k) for k in row)
    for key, val in row.items():
        print(f"  {key:<{max_key_len}}  {val}")
    return 0


def _handle_summary(args: argparse.Namespace) -> int:
    import polars as pl

    from pointline.cli._output import print_table

    df = _load_manifest(args)
    if df.is_empty():
        print("Manifest is empty.")
        return 0

    summary = (
        df.group_by("vendor", "data_type", "status")
        .agg(
            pl.len().alias("files"),
            pl.col("rows_written").sum().alias("total_written"),
            pl.col("rows_quarantined").sum().alias("total_quarantined"),
        )
        .sort(["vendor", "data_type", "status"])
    )

    print_table(
        summary.to_dicts(),
        columns=["vendor", "data_type", "status", "files", "total_written", "total_quarantined"],
    )
    print(f"\nTotal files: {df.height}")
    return 0


def _handle_diff(args: argparse.Namespace) -> int:
    import polars as pl

    from pointline.cli._config import resolve_bronze_root, resolve_root
    from pointline.cli._output import print_table

    root = resolve_root(getattr(args, "root", None))
    bronze_root = resolve_bronze_root(args.bronze_root, root=root, vendor=args.vendor)
    if not bronze_root.exists():
        print(f"error: bronze root does not exist: {bronze_root}")
        return 1

    # Narrow the scan directory when exchange/data-type filters are given.
    # Bronze layout: <bronze_root>/exchange=X/type=Y/date=Z/symbol=W/*.csv.gz
    scan_dir = bronze_root
    if args.exchange:
        scan_dir = scan_dir / f"exchange={args.exchange}"
    if args.data_type:
        scan_dir = scan_dir / f"type={args.data_type}"

    if not scan_dir.exists():
        print(f"error: scan directory does not exist: {scan_dir}")
        return 1

    # Discover files on disk
    disk_files = sorted(scan_dir.glob(args.glob_pattern))
    if not disk_files:
        print(f"No files found under {scan_dir} matching '{args.glob_pattern}'")
        return 0

    # Compute relative paths (same format as manifest bronze_path)
    disk_rel = {}
    for f in disk_files:
        try:
            rel = f.relative_to(bronze_root).as_posix()
        except ValueError:
            continue
        disk_rel[rel] = f

    # Load manifest and extract known bronze_paths for this vendor
    manifest = _load_manifest(args)
    if manifest.is_empty():
        known_paths: set[str] = set()
    else:
        vendor_manifest = manifest.filter(pl.col("vendor") == args.vendor)
        known_paths = set(vendor_manifest["bronze_path"].to_list())

    # Diff
    missing = sorted(rel for rel in disk_rel if rel not in known_paths)
    if not missing:
        print(f"All {len(disk_rel)} Bronze files are in the manifest.")
        return 0

    display = missing[: args.limit]
    rows = [
        {"bronze_path": p, "size_mb": f"{disk_rel[p].stat().st_size / 1_048_576:.1f}"}
        for p in display
    ]
    print_table(rows, columns=["bronze_path", "size_mb"])
    print(
        f"\n{len(missing)} files not in manifest (of {len(disk_rel)} on disk), showing {len(display)}"
    )
    return 0
