"""Vendor plugin system.

This package provides a plugin-based architecture for vendor integrations.
Each vendor is self-contained with:
- Plugin class (implements VendorPlugin protocol)
- Client code (download/API clients)
- Parsers (vendor-specific data format parsers)
- Utilities (prehooks, helpers)

Auto-discovery: All vendor plugins auto-register on import.

Usage:
    from pointline.io.vendors.registry import get_vendor, list_vendors

    # List all available vendors
    vendors = list_vendors()

    # Get a specific vendor
    tardis = get_vendor("tardis")

    # Get parsers from a vendor
    parsers = tardis.get_parsers()
    parse_trades = parsers["trades"]
"""

# Import registry functions for public API
from pointline.io.vendors.binance_vision import BinanceVisionVendor
from pointline.io.vendors.coingecko import CoingeckoVendor
from pointline.io.vendors.quant360 import Quant360Vendor
from pointline.io.vendors.registry import (
    get_parser,
    get_vendor,
    list_supported_combinations,
    list_vendors,
    register_vendor,
)

# Import all vendor plugins (triggers auto-registration)
from pointline.io.vendors.tardis import TardisVendor
from pointline.io.vendors.tushare import TushareVendor

__all__ = [
    # Registry functions
    "register_vendor",
    "get_vendor",
    "list_vendors",
    "get_parser",
    "list_supported_combinations",
    # Vendor plugins
    "TardisVendor",
    "BinanceVisionVendor",
    "Quant360Vendor",
    "CoingeckoVendor",
    "TushareVendor",
]
