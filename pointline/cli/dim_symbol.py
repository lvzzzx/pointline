"""``pointline dim-symbol`` — SCD Type 2 symbol management."""

from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("dim-symbol", help="Manage dim_symbol SCD2 table")
    sub = p.add_subparsers(dest="dim_symbol_command")

    # dim-symbol sync
    sync_p = sub.add_parser("sync", help="Sync symbols from a vendor into dim_symbol")
    sync_p.add_argument(
        "--vendor",
        required=True,
        choices=["tushare"],
        help="Vendor to sync from",
    )
    sync_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    sync_p.add_argument("--token", default=None, help="Vendor API token")
    sync_p.add_argument(
        "--http-url",
        default="https://api.tushare.pro",
        help="Tushare API endpoint URL",
    )
    sync_p.add_argument(
        "--exchange",
        choices=["SSE", "SZSE"],
        default=None,
        help="Specific exchange to sync (default: both)",
    )
    sync_p.set_defaults(handler=_handle_sync)

    # dim-symbol validate
    val_p = sub.add_parser("validate", help="Run SCD2 invariant checks on dim_symbol")
    val_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    val_p.set_defaults(handler=_handle_validate)

    # dim-symbol show
    show_p = sub.add_parser("show", help="Show dim_symbol summary statistics")
    show_p.add_argument("--silver-root", default=None, help="Silver data root directory")
    show_p.add_argument("--exchange", default=None, help="Filter by exchange")
    show_p.set_defaults(handler=_handle_show)


def _handle_sync(args: argparse.Namespace) -> int:
    from datetime import datetime, timezone
    from typing import Literal

    import polars as pl

    from pointline.cli._config import resolve_root, resolve_silver_root, resolve_tushare_token
    from pointline.dim_symbol import assign_symbol_ids, validate
    from pointline.storage.delta.dimension_store import DeltaDimensionStore
    from pointline.vendors.tushare.symbols import stock_basic_to_snapshot

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)
    token = resolve_tushare_token(args.token)

    # Configure Tushare
    print(f"Configuring Tushare API: {args.http_url}")
    try:
        import tushare as ts
    except ImportError:
        print("error: tushare not installed (pip install tushare)")
        return 1

    ts.set_token(token)
    pro = ts.pro_api()
    pro._DataApi__token = token
    pro._DataApi__http_url = args.http_url

    # Load existing
    store = DeltaDimensionStore(silver_root=silver_root)
    dim = store.load_dim_symbol()
    existing_version = store.current_version()
    print(
        f"Current dim_symbol: version={existing_version}, "
        f"rows={dim.height if not dim.is_empty() else 0}"
    )

    # Fetch stocks
    exchange_filter: Literal["SSE", "SZSE"] | None = args.exchange
    print(f"Fetching stock_basic (exchange={exchange_filter or 'all'})")
    fields = "ts_code,symbol,name,market,exchange,list_date,delist_date,list_status"

    listed_raw = pl.from_pandas(
        pro.stock_basic(exchange=exchange_filter, list_status="L", fields=fields)
    )
    delisted_raw = pl.from_pandas(
        pro.stock_basic(exchange=exchange_filter, list_status="D", fields=fields)
    )
    print(f"  Listed: {len(listed_raw)}, Delisted: {len(delisted_raw)}")

    all_raw = pl.concat([listed_raw, delisted_raw], how="diagonal")
    print(f"  Total for snapshot: {len(all_raw)}")

    # Transform
    effective_ts_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    snapshot = stock_basic_to_snapshot(all_raw, effective_ts_us=effective_ts_us)
    print(f"  Snapshot rows: {len(snapshot)}")

    # Merge with existing (preserve other exchanges)
    snapshot_exchanges = snapshot["exchange"].unique().to_list()
    if dim.is_empty():
        merged = snapshot
        print("  Existing dim_symbol is empty, using snapshot only")
    else:
        preserved = dim.filter(~pl.col("exchange").is_in(snapshot_exchanges))
        print(f"  Preserving {len(preserved)} rows from other exchanges")
        merged = pl.concat([preserved, snapshot], how="diagonal")
        print(f"  Combined: {len(merged)} rows")

    # Assign IDs + validate
    result = assign_symbol_ids(merged)
    try:
        validate(result)
    except ValueError as exc:
        print(f"FAILED: {exc}")
        return 2
    print(f"  Final: {result.height} rows")

    # Summary
    exch_summary = (
        result.group_by("exchange").agg(pl.len().alias("count")).sort("count", descending=True)
    )
    for row in exch_summary.iter_rows(named=True):
        print(f"    {row['exchange']}: {row['count']}")

    # Save
    new_version = store.save_dim_symbol(result, expected_version=existing_version)
    print(f"Saved: version {existing_version} -> {new_version}")
    return 0


def _handle_validate(args: argparse.Namespace) -> int:
    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.dim_symbol import validate
    from pointline.storage.delta.dimension_store import DeltaDimensionStore

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)
    store = DeltaDimensionStore(silver_root=silver_root)
    dim = store.load_dim_symbol()

    if dim.is_empty():
        print("dim_symbol is empty — nothing to validate")
        return 0

    try:
        validate(dim)
        print(f"OK: {dim.height} rows pass all SCD2 invariant checks")
        return 0
    except ValueError as exc:
        print(f"FAILED: {exc}")
        return 2


def _handle_show(args: argparse.Namespace) -> int:
    import polars as pl

    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._output import print_table
    from pointline.storage.delta.dimension_store import DeltaDimensionStore

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)
    store = DeltaDimensionStore(silver_root=silver_root)
    dim = store.load_dim_symbol()

    if dim.is_empty():
        print("dim_symbol is empty")
        return 0

    if args.exchange:
        exchange_filter = args.exchange.strip().upper()
        dim = dim.filter(pl.col("exchange").str.to_uppercase() == exchange_filter)
        if dim.is_empty():
            print(f"No symbols found for exchange '{args.exchange}'")
            return 0

    print(f"Version: {store.current_version()}")
    print(f"Total rows: {dim.height}")
    print()

    summary = (
        dim.group_by("exchange")
        .agg(
            pl.len().alias("rows"),
            pl.col("is_current").sum().alias("current"),
            pl.col("exchange_symbol").n_unique().alias("symbols"),
        )
        .sort("exchange")
    )
    print_table(summary.to_dicts(), columns=["exchange", "symbols", "current", "rows"])
    return 0
