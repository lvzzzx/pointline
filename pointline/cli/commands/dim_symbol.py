"""dim_symbol commands."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import polars as pl

from pointline.cli.utils import parse_effective_ts, read_updates
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.tardis import TardisClient, build_updates_from_instruments
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
    from pointline.config import get_table_path
    from pointline.dim_symbol import scd2_upsert, read_dim_symbol_table, scd2_bootstrap
    from pointline.io.vendor.tushare import TushareClient

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
                exchanges=["SZSE", "SSE"],
                include_delisted=args.include_delisted
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

    # Transform to dim_symbol schema
    def parse_tushare_date(date_str: str | None) -> int:
        """Parse YYYYMMDD string to microseconds timestamp."""
        if not date_str or date_str == "" or date_str is None:
            return 0  # Use 0 for missing dates
        try:
            dt = datetime.strptime(str(date_str), "%Y%m%d")
            return int(dt.timestamp() * 1_000_000)
        except (ValueError, TypeError):
            return 0

    print("Transforming to dim_symbol schema...")

    # Map exchange_id
    from pointline.config import EXCHANGE_MAP
    szse_id = EXCHANGE_MAP.get("szse", 30)
    sse_id = EXCHANGE_MAP.get("sse", 31)

    updates = df.with_columns([
        # Remove exchange suffix from ts_code: "000001.SZ" -> "000001"
        pl.col("symbol").alias("exchange_symbol"),

        # Map exchange name
        pl.when(pl.col("exchange") == "SZSE")
            .then(pl.lit("szse"))
            .when(pl.col("exchange") == "SSE")
            .then(pl.lit("sse"))
            .otherwise(pl.lit("unknown"))
            .alias("exchange"),

        # Map exchange_id
        pl.when(pl.col("exchange") == "SZSE")
            .then(pl.lit(szse_id))
            .when(pl.col("exchange") == "SSE")
            .then(pl.lit(sse_id))
            .otherwise(pl.lit(0))
            .cast(pl.Int16)
            .alias("exchange_id"),

        # Use name as base_asset
        pl.col("name").alias("base_asset"),

        # Fixed fields for Chinese A-shares
        pl.lit("CNY").alias("quote_asset"),
        pl.lit(0).cast(pl.UInt8).alias("asset_type"),  # spot stocks
        pl.lit(0.01).alias("tick_size"),  # 1 fen
        pl.lit(100.0).alias("lot_size"),  # 1 lot = 100 shares
        pl.lit(0.01).alias("price_increment"),  # tick-based encoding
        pl.lit(100.0).alias("amount_increment"),  # lot-based encoding
        pl.lit(1.0).alias("contract_size"),

        # Parse dates
        pl.col("list_date").map_elements(
            parse_tushare_date, return_dtype=pl.Int64
        ).alias("valid_from_ts"),
    ]).select([
        "exchange_id",
        "exchange",
        "exchange_symbol",
        "base_asset",
        "quote_asset",
        "asset_type",
        "tick_size",
        "lot_size",
        "price_increment",
        "amount_increment",
        "contract_size",
        "valid_from_ts",
    ])

    # Filter out unknown exchanges
    updates = updates.filter(pl.col("exchange_id") != 0)

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
    summary = updated_dim.filter(
        pl.col("is_current")
    ).groupby("exchange").agg(
        pl.count().alias("count")
    ).sort("exchange")

    print("\nCurrent symbols by exchange:")
    for row in summary.iter_rows(named=True):
        print(f"  {row['exchange']}: {row['count']}")

    return 0
