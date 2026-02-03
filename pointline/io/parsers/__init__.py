"""Vendor-specific data parsers.

This package provides a registry-based system for vendor-specific data parsing.
Each vendor has its own subpackage with parsers that are automatically registered
using the @register_parser decorator.

Usage:
    from pointline.io.parsers import get_parser, list_supported_combinations

    # Get a parser for a specific vendor and data type
    parser = get_parser("tardis", "trades")
    parsed_df = parser(raw_df)

    # List all available parsers
    combos = list_supported_combinations()
    # [("tardis", "trades"), ("tardis", "quotes"), ...]

Vendor packages:
    - tardis: Tardis market data (trades, quotes, book_snapshots, derivative_ticker)
    - quant360: Quant360 SZSE Level 3 data (l3_orders, l3_ticks)
    - binance: Binance data (klines)
"""

import pointline.io.parsers.binance  # noqa: F401
import pointline.io.parsers.quant360  # noqa: F401

# Import vendor packages to trigger parser registration
import pointline.io.parsers.tardis  # noqa: F401
from pointline.io.parsers.registry import (
    get_parser,
    is_parser_registered,
    list_supported_combinations,
    register_parser,
)

__all__ = [
    "get_parser",
    "list_supported_combinations",
    "register_parser",
    "is_parser_registered",
]
