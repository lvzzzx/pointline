"""dim_symbol commands."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

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
