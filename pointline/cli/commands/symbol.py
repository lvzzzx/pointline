"""Symbol registry commands."""

from __future__ import annotations

import argparse

import polars as pl

from pointline.tables.dim_symbol import find_symbol


def cmd_symbol_search(args: argparse.Namespace) -> int:
    """Search for symbols in the registry."""
    df = find_symbol(
        query=args.query,
        exchange=args.exchange,
        base_asset=args.base_asset,
        quote_asset=args.quote_asset,
    )

    if df.is_empty():
        print("No matching symbols found.")
        return 0

    print(f"Found {df.height} matching symbols:")

    with pl.Config(tbl_rows=100, tbl_cols=20, fmt_float="full"):
        cols = [
            "symbol_id",
            "exchange",
            "exchange_symbol",
            "base_asset",
            "quote_asset",
            "asset_type",
            "tick_size",
            "lot_size",
            "contract_size",
            "valid_from_ts",
            "valid_until_ts",
        ]
        display_cols = [c for c in cols if c in df.columns]
        print(df.select(display_cols))

    return 0
