"""Tushare vendor plugin package.

This package provides Tushare API client integration for Chinese stock market data.
"""

# Import plugin class
# Register plugin
from pointline.io.vendors.registry import register_vendor

# Import client
from pointline.io.vendors.tushare.client import TushareClient
from pointline.io.vendors.tushare.plugin import TushareVendor

register_vendor(TushareVendor())

__all__ = [
    # Plugin
    "TushareVendor",
    # Client
    "TushareClient",
]
