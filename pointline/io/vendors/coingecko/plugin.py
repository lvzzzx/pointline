"""CoinGecko vendor plugin.

This module provides the vendor plugin implementation for CoinGecko (market data API).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl


class CoingeckoVendor:
    """CoinGecko vendor plugin for market data and asset statistics."""

    name = "coingecko"
    display_name = "CoinGecko"
    supports_parsers = False  # API client only, no file parsing
    supports_download = True
    supports_prehooks = False

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor (none).

        Raises:
            NotImplementedError: CoinGecko doesn't provide parsers
        """
        raise NotImplementedError(f"{self.name} does not provide parsers")

    def get_download_client(self) -> Any:
        """Get download client for this vendor.

        Returns:
            CoinGeckoClient class
        """
        from pointline.io.vendors.coingecko.client import CoinGeckoClient

        return CoinGeckoClient

    def run_prehook(self, bronze_root) -> None:
        """Run prehook for this vendor (not supported)."""
        raise NotImplementedError(f"{self.name} does not support prehooks")

    def can_handle(self, path: Path) -> bool:
        """Detect CoinGecko data by directory name.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains "coingecko" in directory name

        Note:
            CoinGecko is primarily an API-based vendor, so bronze files are rare.
            This detection is mainly for completeness.
        """
        # Check if directory name is "coingecko"
        return "coingecko" in path.name.lower()
