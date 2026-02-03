"""Binance Vision vendor plugin.

This module provides the vendor plugin implementation for Binance Vision (historical data).
"""

from collections.abc import Callable
from typing import Any

import polars as pl


class BinanceVisionVendor:
    """Binance Vision vendor plugin for historical kline data."""

    name = "binance_vision"
    display_name = "Binance Vision (Historical Data)"
    supports_parsers = True
    supports_download = True
    supports_prehooks = False

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor.

        Returns:
            Dictionary mapping data_type to parser function
        """
        from pointline.io.vendors.binance_vision.parsers import parse_binance_klines_csv

        return {
            "klines": parse_binance_klines_csv,
        }

    def get_download_client(self) -> Any:
        """Get download client for this vendor.

        Returns:
            Module or class with download functions
        """
        from pointline.io.vendors.binance_vision import datasets

        return datasets

    def run_prehook(self, bronze_root) -> None:
        """Run prehook for this vendor (not supported)."""
        raise NotImplementedError(f"{self.name} does not support prehooks")
