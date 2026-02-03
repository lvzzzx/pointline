"""dim_symbol commands."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import polars as pl

from pointline.cli.utils import parse_effective_ts, read_updates
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendors.tardis import TardisClient, build_updates_from_instruments
from pointline.services.dim_symbol_service import DimSymbolService


def cmd_dim_symbol_upsert(args: argparse.Namespace) -> int:
    updates = read_updates(Path(args.file))
    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)
    service.update(updates)
    print(f"dim_symbol updated: {updates.height} rows")
    return 0


def cmd_dim_symbol_sync(args: argparse.Namespace) -> int:
    """Sync dim_symbol updates from a source."""
    if args.source == "api":
        if not args.exchange:
            print("Error: --exchange is required when --source=api")
            return 1

        try:
            filter_payload = json.loads(args.filter) if args.filter else None
        except json.JSONDecodeError as exc:
            print(f"Error: invalid --filter JSON: {exc}")
            return 1

        effective_ts = parse_effective_ts(args.effective_ts)
        api_key = args.api_key or os.getenv("TARDIS_API_KEY", "")

        client = TardisClient(api_key=api_key)
        instruments = client.fetch_instruments(
            args.exchange,
            symbol=args.symbol,
            filter_payload=filter_payload,
        )
        updates = build_updates_from_instruments(
            instruments,
            exchange=args.exchange,
            effective_ts=effective_ts,
            rebuild=args.rebuild,
        )
    else:
        source_path = Path(args.source)
        if not source_path.exists():
            print(f"Error: source {source_path} not found")
            return 2

        updates = read_updates(source_path)

    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)

    if args.rebuild:
        print(f"Rebuilding history for {updates.select('exchange_symbol').n_unique()} symbols...")
        service.rebuild(updates)
    else:
        print("Applying incremental updates...")
        service.update(updates)

    print("Sync complete.")
    return 0


def cmd_dim_symbol_sync_tushare(args: argparse.Namespace) -> int:
    """Sync Chinese stock symbols from Tushare to dim_symbol."""
    from pointline.dim_symbol import read_dim_symbol_table, scd2_bootstrap, scd2_upsert
    from pointline.io.vendors.tushare import TushareClient
    from pointline.io.vendors.tushare.stock_basic_cn import (
        build_dim_symbol_updates_from_stock_basic_cn,
    )

    try:
        # Initialize Tushare client
        client = TushareClient(token=args.token)
    except (ValueError, ImportError) as exc:
        print(f"Error: {exc}")
        return 1

    # Fetch stocks based on exchange
    print(f"Fetching {args.exchange.upper()} stocks from Tushare...")
    try:
        if args.exchange.lower() == "szse":
            df = client.get_szse_stocks(include_delisted=args.include_delisted)
        elif args.exchange.lower() == "sse":
            df = client.get_sse_stocks(include_delisted=args.include_delisted)
        elif args.exchange.lower() == "all":
            df = client.get_all_stocks(
                exchanges=["SZSE", "SSE"], include_delisted=args.include_delisted
            )
        else:
            print(f"Error: Invalid exchange '{args.exchange}'. Use 'szse', 'sse', or 'all'.")
            return 1
    except Exception as exc:
        print(f"Error fetching data from Tushare: {exc}")
        return 1

    if df.is_empty():
        print("Warning: No stocks returned from Tushare.")
        return 0

    print(f"Fetched {len(df)} stocks from Tushare")

    print("Transforming to dim_symbol schema...")
    updates = build_dim_symbol_updates_from_stock_basic_cn(df)

    if updates.is_empty():
        print("Warning: No valid symbols after transformation.")
        return 0

    print(f"Transformed {len(updates)} symbols")

    # Load or initialize dim_symbol
    repo = BaseDeltaRepository(Path(args.table_path))

    try:
        print("Loading existing dim_symbol...")
        current_dim = read_dim_symbol_table()
        print(f"Found {len(current_dim)} existing symbols")

        # Upsert
        print("Upserting symbols...")
        updated_dim = scd2_upsert(current_dim, updates)

    except Exception as e:
        # dim_symbol doesn't exist yet, bootstrap
        print(f"dim_symbol table not found ({e}), bootstrapping...")
        updated_dim = scd2_bootstrap(updates)

    # Write back
    print("Writing to Delta Lake...")
    repo.write_full(updated_dim)

    print(f"✓ Successfully synced {len(updates)} symbols to dim_symbol")
    print(f"  Total symbols in dim_symbol: {len(updated_dim)}")

    # Show summary by exchange
    summary = (
        updated_dim.filter(pl.col("is_current"))
        .groupby("exchange")
        .agg(pl.count().alias("count"))
        .sort("exchange")
    )

    print("\nCurrent symbols by exchange:")
    for row in summary.iter_rows(named=True):
        print(f"  {row['exchange']}: {row['count']}")

    return 0


def cmd_dim_symbol_sync_from_stock_basic_cn(args: argparse.Namespace) -> int:
    """Sync dim_symbol from silver.stock_basic_cn snapshot."""
    from pointline.io.vendors.tushare.stock_basic_cn import (
        build_dim_symbol_updates_from_stock_basic_cn,
    )

    try:
        stock_basic = pl.read_delta(str(args.stock_basic_path))
    except Exception as exc:
        print(f"Error reading stock_basic_cn table: {exc}")
        return 1

    if stock_basic.is_empty():
        print("Warning: stock_basic_cn is empty.")
        return 0

    updates = build_dim_symbol_updates_from_stock_basic_cn(stock_basic)
    if updates.is_empty():
        print("Warning: No valid symbols after transformation.")
        return 0

    repo = BaseDeltaRepository(Path(args.table_path))
    service = DimSymbolService(repo)

    if args.rebuild:
        print(f"Rebuilding history for {updates.select('exchange_symbol').n_unique()} symbols...")
        service.rebuild(updates)
    else:
        print("Applying incremental updates...")
        service.update(updates)

    print("Sync complete.")
    return 0
