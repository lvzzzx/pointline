"""dim_asset_stats commands."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.coingecko import CoinGeckoClient
from pointline.services.asset_stats_providers import (
    CoinGeckoAssetStatsProvider,
    CoinMarketCapAssetStatsProvider,
)
from pointline.services.dim_asset_stats_service import DimAssetStatsService


def cmd_dim_asset_stats_sync(args: argparse.Namespace) -> int:
    """Sync dim_asset_stats for a single date."""
    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        print(f"Error: Invalid date format: {args.date} (expected YYYY-MM-DD)")
        return 1

    base_assets = None
    if args.base_assets:
        base_assets = [asset.strip().upper() for asset in args.base_assets.split(",")]

    repo = BaseDeltaRepository(Path(args.table_path))
    provider = _build_provider(args)
    service = DimAssetStatsService(repo, provider=provider)

    print(f"Syncing dim_asset_stats for date: {target_date}")
    if base_assets:
        print(f"Base assets: {', '.join(base_assets)}")
    else:
        print("Syncing all assets from dim_symbol")

    try:
        service.sync_daily(target_date, base_assets)
        print(f"✓ Sync complete for {target_date}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


def cmd_dim_asset_stats_backfill(args: argparse.Namespace) -> int:
    """Backfill historical dim_asset_stats for a date range."""
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        print("Error: Invalid date format (expected YYYY-MM-DD)")
        return 1

    if start_date > end_date:
        print("Error: start_date must be <= end_date")
        return 1

    base_assets = None
    if args.base_assets:
        base_assets = [asset.strip().upper() for asset in args.base_assets.split(",")]

    repo = BaseDeltaRepository(Path(args.table_path))
    provider = _build_provider(args)
    service = DimAssetStatsService(repo, provider=provider)

    total_days = (end_date - start_date).days + 1
    print(f"Backfilling dim_asset_stats from {start_date} to {end_date} ({total_days} days)")
    if base_assets:
        print(f"Base assets: {', '.join(base_assets)}")
    else:
        print("Syncing all assets from dim_symbol")

    try:
        service.sync_date_range(start_date, end_date, base_assets)
        print(f"✓ Backfill complete: {total_days} days")
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


def _build_provider(args: argparse.Namespace):
    provider = (args.provider or "coingecko").lower()
    if provider == "coingecko":
        client = CoinGeckoClient(api_key=args.api_key if args.api_key else None)
        return CoinGeckoAssetStatsProvider(client=client)
    if provider == "coinmarketcap":
        return CoinMarketCapAssetStatsProvider()
    raise ValueError(f"Unknown provider: {provider}")
