"""Binance Vision vendor plugin package.

This package provides Binance Vision integration including:
- Historical kline data download
- Kline CSV parser
- Symbol aliasing utilities
"""

# Import plugin class
# Import client and utilities
from pointline.io.vendors.binance_vision.aliases import (
    SYMBOL_ALIAS_MAP,
    normalize_symbol,
)
from pointline.io.vendors.binance_vision.datasets import (
    BINANCE_PUBLIC_BASE_URL,
    BinanceDownloadResult,
    download_binance_klines,
)

# Import parsers (triggers registration)
from pointline.io.vendors.binance_vision.parsers import parse_binance_klines_csv
from pointline.io.vendors.binance_vision.plugin import BinanceVisionVendor

# Register plugin
from pointline.io.vendors.registry import register_vendor

register_vendor(BinanceVisionVendor())

__all__ = [
    # Plugin
    "BinanceVisionVendor",
    # Client
    "BINANCE_PUBLIC_BASE_URL",
    "BinanceDownloadResult",
    "download_binance_klines",
    # Utilities
    "SYMBOL_ALIAS_MAP",
    "normalize_symbol",
    # Parsers
    "parse_binance_klines_csv",
]
