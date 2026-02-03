"""CoinGecko vendor plugin package.

This package provides CoinGecko API client integration for market data.
"""

# Import plugin class
# Import client
from pointline.io.vendors.coingecko.client import CoinGeckoClient
from pointline.io.vendors.coingecko.plugin import CoingeckoVendor

# Register plugin
from pointline.io.vendors.registry import register_vendor

register_vendor(CoingeckoVendor())

__all__ = [
    # Plugin
    "CoingeckoVendor",
    # Client
    "CoinGeckoClient",
]
