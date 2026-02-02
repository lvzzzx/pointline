"""Vendor-specific data source clients and utilities.

This package provides vendor-specific implementations for fetching market data
from various exchanges and data providers.

Available vendors:
    - binance: Public kline data from Binance Vision
    - coingecko: Asset statistics and market cap data
    - quant360: SZSE/SSE Level 3 data reorganization
    - tardis: Historical cryptocurrency data
    - tushare: Chinese stock market data
"""

# Import main APIs for convenient access
from pointline.io.vendor.binance import (
    BINANCE_PUBLIC_BASE_URL,
    BinanceDownloadResult,
    download_binance_klines,
    normalize_symbol,
)
from pointline.io.vendor.coingecko import CoinGeckoClient
from pointline.io.vendor.quant360 import reorganize_quant360_archives
from pointline.io.vendor.tardis import (
    TardisClient,
    build_updates_from_instruments,
    download_tardis_datasets,
)
from pointline.io.vendor.tushare import TushareClient

__all__ = [
    # Binance
    "BINANCE_PUBLIC_BASE_URL",
    "BinanceDownloadResult",
    "download_binance_klines",
    "normalize_symbol",
    # CoinGecko
    "CoinGeckoClient",
    # Quant360
    "reorganize_quant360_archives",
    # Tardis
    "TardisClient",
    "build_updates_from_instruments",
    "download_tardis_datasets",
    # Tushare
    "TushareClient",
]
