"""Quant360 vendor plugin package.

This package provides Quant360 integration including:
- Archive reorganization (prehook)
- SZSE/SSE Level 3 order and tick parsers
"""

# Import plugin class
# Import parsers (triggers registration)
from pointline.io.vendors.quant360.parsers import (
    parse_quant360_orders_csv,
    parse_quant360_ticks_csv,
    parse_quant360_timestamp,
)
from pointline.io.vendors.quant360.plugin import Quant360Vendor

# Import reorganization utilities
from pointline.io.vendors.quant360.reorganize import reorganize_quant360_archives

# Register plugin
from pointline.io.vendors.registry import register_vendor

register_vendor(Quant360Vendor())

__all__ = [
    # Plugin
    "Quant360Vendor",
    # Prehook
    "reorganize_quant360_archives",
    # Parsers
    "parse_quant360_orders_csv",
    "parse_quant360_ticks_csv",
    "parse_quant360_timestamp",
]
