"""Binance Vision vendor parsers.

This package contains parsers for Binance Vision data files (data.binance.vision).
All parsers are automatically registered via the @register_parser decorator.
"""

# Import all parsers to trigger registration
from pointline.io.parsers.binance.klines import parse_binance_klines_csv

__all__ = [
    "parse_binance_klines_csv",
]
