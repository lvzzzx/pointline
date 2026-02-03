"""Tardis vendor plugin package.

This package provides complete Tardis.dev integration including:
- API client for downloading data
- Parsers for all Tardis data formats
- Instrument mapping utilities
"""

# Import plugin class
from pointline.io.vendors.tardis.plugin import TardisVendor

# Import client and utilities
from pointline.io.vendors.tardis.client import TardisClient
from pointline.io.vendors.tardis.datasets import download_tardis_datasets
from pointline.io.vendors.tardis.mapper import build_updates_from_instruments

# Import parsers (triggers registration)
from pointline.io.vendors.tardis.parsers import (
    parse_tardis_book_snapshots_csv,
    parse_tardis_derivative_ticker_csv,
    parse_tardis_quotes_csv,
    parse_tardis_trades_csv,
)

# Register plugin
from pointline.io.vendors.registry import register_vendor

register_vendor(TardisVendor())

__all__ = [
    # Plugin
    "TardisVendor",
    # Client
    "TardisClient",
    "download_tardis_datasets",
    # Mapper
    "build_updates_from_instruments",
    # Parsers
    "parse_tardis_trades_csv",
    "parse_tardis_quotes_csv",
    "parse_tardis_book_snapshots_csv",
    "parse_tardis_derivative_ticker_csv",
]
