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


# Configure Tushare with custom endpoint
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


# Exchange rules for Chinese A-shares
CN_EXCHANGE_RULES: dict[tuple[str, str], dict[str, int]] = {
    ("SSE", "main_board"): {"lot_size": 100, "tick_size": 1},
    ("SSE", "star_board"): {"lot_size": 200, "tick_size": 1},
    ("SZSE", "main_board"): {"lot_size": 100, "tick_size": 1},
    ("SZSE", "growth_board"): {"lot_size": 100, "tick_size": 1},
}

MARKET_MAP: dict[str, str] = {
    "主板": "main_board",
    "创业板": "growth_board",
    "科创板": "star_board",
    "北交所": "bse_board",
}


def derive_contract_specs(exchange: str, exchange_symbol: str, market_type: str) -> dict[str, int]:
    """Derive lot_size and tick_size from exchange rules."""
    key = (exchange, market_type)
    if key in CN_EXCHANGE_RULES:
        return CN_EXCHANGE_RULES[key]
    # Fallback: infer STAR market by symbol prefix
    if exchange == "SSE" and exchange_symbol.startswith("688"):
        return CN_EXCHANGE_RULES[("SSE", "star_board")]
    return {"lot_size": 100, "tick_size": 1}


def fetch_stock_basic(
    pro,
    exchange: Literal["SSE", "SZSE"] | None = None,
    list_status: str = "L",
) -> pl.DataFrame:
    """Fetch stock_basic from Tushare."""
    fields = "ts_code,symbol,name,market,exchange,list_date,delist_date,list_status"

    pdf = pro.stock_basic(exchange=exchange, list_status=list_status, fields=fields)
    return pl.from_pandas(pdf)


def transform_to_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    """Transform Tushare stock_basic to dim_symbol snapshot format."""
    # Map market types
    df = df.with_columns(
        [
            pl.col("symbol").alias("exchange_symbol"),
            pl.col("ts_code").alias("canonical_symbol"),
            pl.col("market")
            .map_elements(lambda x: MARKET_MAP.get(x, "main_board"), return_dtype=pl.Utf8)
            .alias("market_type"),
        ]
    )

    # Derive contract specs
    def get_lot_size(row: dict) -> int:
        return derive_contract_specs(row["exchange"], row["exchange_symbol"], row["market_type"])[
            "lot_size"
        ]

    def get_tick_size(row: dict) -> int:
        return derive_contract_specs(row["exchange"], row["exchange_symbol"], row["market_type"])[
            "tick_size"
        ]

    df = df.with_columns(
        [
            pl.struct(["exchange", "exchange_symbol", "market_type"])
            .map_elements(get_lot_size, return_dtype=pl.Int64)
            .alias("lot_size"),
            pl.struct(["exchange", "exchange_symbol", "market_type"])
            .map_elements(get_tick_size, return_dtype=pl.Int64)
            .alias("tick_size"),
            pl.lit(None).cast(pl.Utf8).alias("base_asset"),
            pl.lit(None).cast(pl.Utf8).alias("quote_asset"),
            pl.lit(None).cast(pl.Int64).alias("contract_size"),
        ]
    )

    return df.select(
        [
            "exchange",
            "exchange_symbol",
            "canonical_symbol",
            "market_type",
            "base_asset",
            "quote_asset",
            "lot_size",
            "tick_size",
            "contract_size",
        ]
    )


def transform_delistings(df: pl.DataFrame) -> pl.DataFrame | None:
    """Transform delisted stocks to delistings format."""
    if df.is_empty():
        return None

    def yyyymmdd_to_micros(date_str: str | None) -> int:
        if not date_str:
            return int(datetime.now().timestamp() * 1_000_000)
        dt = datetime.strptime(str(date_str), "%Y%m%d")
        return int(dt.timestamp() * 1_000_000)

    return df.select(
        [
            pl.col("exchange"),
            pl.col("symbol").alias("exchange_symbol"),
            pl.col("delist_date")
            .map_elements(yyyymmdd_to_micros, return_dtype=pl.Int64)
            .alias("delisted_at_ts_us"),
        ]
    )


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

    # Fetch listed stocks
    print(f"Fetching stock_basic from Tushare (exchange={args.exchange or 'all'})")
    listed_df = fetch_stock_basic(pro, exchange=args.exchange, list_status="L")
    print(f"  Listed stocks: {len(listed_df)}")

    snapshot = transform_to_snapshot(listed_df)

    # Fetch delistings
    print("Fetching delisted stocks")
    delisted_df = fetch_stock_basic(pro, exchange=args.exchange, list_status="D")
    delistings = transform_delistings(delisted_df)
    print(f"  Delisted stocks: {len(delisted_df)}")

    # Perform SCD2 upsert
    print("Performing SCD2 upsert")
    from pointline.v2.dim_symbol import bootstrap, upsert, validate

    effective_ts_us = int(datetime.now().timestamp() * 1_000_000)

    if dim.is_empty():
        print("  Bootstrapping new dim_symbol table")
        result = bootstrap(snapshot, effective_ts_us)
    else:
        print("  Upserting into existing dim_symbol table")
        result = upsert(dim, snapshot, effective_ts_us, delistings=delistings)

    validate(result)
    print(f"  Result: {result.height} rows")

    # Save
    print(f"Saving to {store.dim_symbol_path}")
    new_version = store.save_dim_symbol(result, expected_version=existing_version)
    print(f"  New version: {new_version}")
    print("Done!")


if __name__ == "__main__":
    main()
