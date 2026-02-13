"""Sync SSE/SZSE symbol info from Tushare to dim_symbol.

Usage:
    uv run python scripts/sync_tushare_dim_symbol.py --silver-root /data/silver
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from pointline.v2.dim_symbol import assign_symbol_ids, validate
from pointline.v2.vendors.tushare.symbols import stock_basic_to_snapshot


def configure_tushare(token: str, http_url: str = "http://lianghua.nanyangqiankun.top"):
    """Configure Tushare with custom API endpoint."""
    try:
        import tushare as ts
    except ImportError:
        raise RuntimeError("tushare not installed: pip install tushare")

    ts.set_token(token)
    pro = ts.pro_api()
    pro._DataApi__token = token
    pro._DataApi__http_url = http_url
    return pro


def fetch_stock_basic(
    pro,
    exchange: Literal["SSE", "SZSE"] | None = None,
    list_status: str = "L",
) -> pl.DataFrame:
    """Fetch stock_basic from Tushare."""
    fields = "ts_code,symbol,name,market,exchange,list_date,delist_date,list_status"

    pdf = pro.stock_basic(exchange=exchange, list_status=list_status, fields=fields)
    return pl.from_pandas(pdf)


def main():
    parser = argparse.ArgumentParser(description="Sync Tushare symbols to dim_symbol")
    parser.add_argument(
        "--silver-root",
        type=Path,
        required=True,
        help="Path to silver data root",
    )
    parser.add_argument(
        "--token",
        type=str,
        default="c5fda5a8cab4c35343db26199fdd04e43bf21e1ca9e566e9224d45dc1823",
        help="Tushare API token",
    )
    parser.add_argument(
        "--http-url",
        type=str,
        default="http://lianghua.nanyangqiankun.top",
        help="Tushare API endpoint URL",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        choices=["SSE", "SZSE"],
        default=None,
        help="Specific exchange to sync (default: both)",
    )
    args = parser.parse_args()

    print(f"Configuring Tushare API: {args.http_url}")
    pro = configure_tushare(args.token, args.http_url)

    print(f"Loading existing dim_symbol from {args.silver_root}")
    from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore

    store = DeltaDimensionStore(silver_root=args.silver_root)
    dim = store.load_dim_symbol()
    existing_version = store.current_version()
    print(f"  Current version: {existing_version}, rows: {dim.height if not dim.is_empty() else 0}")

    # Fetch all stocks (listed + paused + delisted) for complete history
    print(f"Fetching stock_basic from Tushare (exchange={args.exchange or 'all'})")

    # Fetch listed/paused
    listed_raw = fetch_stock_basic(pro, exchange=args.exchange, list_status="L")
    print(f"  Listed stocks: {len(listed_raw)}")

    # Fetch delisted for historical PIT
    delisted_raw = fetch_stock_basic(pro, exchange=args.exchange, list_status="D")
    print(f"  Delisted stocks: {len(delisted_raw)}")

    # Combine for historical load
    all_raw = pl.concat([listed_raw, delisted_raw], how="diagonal")
    print(f"  Total for snapshot: {len(all_raw)}")

    # Transform with SCD2 metadata (valid_from = list_date, valid_until = delist_date for D)
    effective_ts_us = int(datetime.now().timestamp() * 1_000_000)
    snapshot = stock_basic_to_snapshot(all_raw, effective_ts_us=effective_ts_us)
    print(f"  Snapshot rows: {len(snapshot)}")

    # Merge with existing dim_symbol (preserve other vendors)
    print("Merging with existing dim_symbol")
    tushare_exchanges = ["sse", "szse"]

    if dim.is_empty():
        # First run - just use Tushare data
        merged = snapshot
        print("  Existing dim_symbol is empty, using Tushare data only")
    else:
        # Preserve non-Tushare exchanges
        other_exchanges = dim.filter(~pl.col("exchange").is_in(tushare_exchanges))
        print(f"  Preserving {len(other_exchanges)} rows from other exchanges")
        print(f"    Exchanges: {other_exchanges['exchange'].unique().to_list()}")

        # Combine: other exchanges + new Tushare data
        merged = pl.concat([other_exchanges, snapshot], how="diagonal")
        print(f"  Combined: {len(merged)} rows")

    # Assign symbol_ids
    print("Assigning symbol_ids")
    result = assign_symbol_ids(merged)

    # Validate
    print("Validating")
    validate(result)
    print(f"  Final result: {result.height} rows")

    # Print summary
    print("  By exchange:")
    exch_summary = (
        result.group_by("exchange").agg(pl.len().alias("count")).sort("count", descending=True)
    )
    for row in exch_summary.iter_rows(named=True):
        print(f"    {row['exchange']}: {row['count']}")

    print("  By market type:")
    market_summary = (
        result.filter(pl.col("exchange").is_in(tushare_exchanges))
        .group_by("market_type")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    for row in market_summary.iter_rows(named=True):
        print(f"    {row['market_type']}: {row['count']}")

    # Save
    print(f"Saving to {store.dim_symbol_path}")
    new_version = store.save_dim_symbol(result, expected_version=existing_version)
    print(f"  New version: {new_version}")
    print("Done!")


if __name__ == "__main__":
    main()
