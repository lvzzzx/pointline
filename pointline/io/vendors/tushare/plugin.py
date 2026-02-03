"""Tushare vendor plugin.

This module provides the vendor plugin implementation for Tushare (Chinese stock data API).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl


class TushareVendor:
    """Tushare vendor plugin for Chinese stock market data."""

    name = "tushare"
    display_name = "Tushare"
    supports_parsers = False  # API client only, no file parsing
    supports_download = True
    supports_prehooks = False

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor (none).

        Raises:
            NotImplementedError: Tushare doesn't provide parsers
        """
        raise NotImplementedError(f"{self.name} does not provide parsers")

    def get_download_client(self) -> Any:
        """Get download client for this vendor.

        Returns:
            TushareClient class
        """
        from pointline.io.vendors.tushare.client import TushareClient

        return TushareClient

    def run_prehook(self, bronze_root, source_dir=None) -> None:
        """Run prehook for this vendor (not supported)."""
        raise NotImplementedError(f"{self.name} does not support prehooks")

    def can_handle(self, path: Path) -> bool:
        """Detect Tushare data by directory name.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains "tushare" in directory name

        Note:
            Tushare is primarily an API-based vendor, so bronze files are rare.
            This detection is mainly for completeness.
        """
        # Check if directory name is "tushare"
        return "tushare" in path.name.lower()
