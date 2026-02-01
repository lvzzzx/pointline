"""stock_basic_cn commands."""

from __future__ import annotations

import argparse

from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.tushare import TushareClient
from pointline.io.vendor.tushare.stock_basic_cn import build_stock_basic_cn_snapshot


def cmd_stock_basic_cn_sync(args: argparse.Namespace) -> int:
    """Sync Tushare stock_basic into silver.stock_basic_cn (full overwrite)."""
    try:
        client = TushareClient(token=args.token)
    except (ValueError, ImportError) as exc:
        print(f"Error: {exc}")
        return 1

    print("Fetching stock_basic from Tushare...")
    try:
        if args.exchange == "szse":
            df = client.get_szse_stocks(include_delisted=args.include_delisted)
        elif args.exchange == "sse":
            df = client.get_sse_stocks(include_delisted=args.include_delisted)
        else:
            df = client.get_all_stocks(
                exchanges=["SZSE", "SSE"], include_delisted=args.include_delisted
            )
    except Exception as exc:
        print(f"Error fetching data from Tushare: {exc}")
        return 1

    if df.is_empty():
        print("Warning: No stocks returned from Tushare.")
        return 0

    print(f"Fetched {len(df)} rows from Tushare")

    snapshot = build_stock_basic_cn_snapshot(df)
    if snapshot.is_empty():
        print("Warning: No valid rows after normalization.")
        return 0

    repo = BaseDeltaRepository(args.table_path)
    print("Writing stock_basic_cn snapshot...")
    repo.write_full(snapshot)

    print(f"✓ stock_basic_cn updated: {snapshot.height} rows")
    return 0
