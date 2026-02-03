"""Base plugin protocol for vendor plugins.

This module defines the interface that all vendor plugins must implement.
Each vendor (Tardis, Binance, Quant360, etc.) is packaged as a self-contained
plugin with its own parsers, client code, and utilities.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Protocol

import polars as pl


class VendorPlugin(Protocol):
    """Protocol defining the interface for vendor plugins.

    Each vendor plugin is a self-contained module that provides:
    - Parser functions for vendor-specific data formats
    - Optional download client for fetching data from vendor APIs
    - Optional prehooks for vendor-specific preprocessing (e.g., archive extraction)

    Example:
        class TardisVendor:
            name = "tardis"
            display_name = "Tardis.dev"
            supports_parsers = True
            supports_download = True
            supports_prehooks = False

            def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
                return {
                    "trades": parse_tardis_trades_csv,
                    "quotes": parse_tardis_quotes_csv,
                }

            def get_download_client(self) -> TardisClient:
                return TardisClient()
    """

    name: str
    """Vendor identifier (lowercase, no spaces). Example: 'tardis', 'binance_vision'"""

    display_name: str
    """Human-readable vendor name. Example: 'Tardis.dev', 'Binance Vision'"""

    supports_parsers: bool
    """Whether this vendor provides data parsers"""

    supports_download: bool
    """Whether this vendor provides a download client"""

    supports_prehooks: bool
    """Whether this vendor requires preprocessing before ingestion"""

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor.

        Returns:
            Dictionary mapping data_type to parser function.
            Example: {"trades": parse_tardis_trades_csv, "quotes": parse_tardis_quotes_csv}
        """
        ...

    def get_download_client(self) -> Any:
        """Get the download client for this vendor.

        Returns:
            Client instance (e.g., TardisClient, BinanceDownloader)

        Raises:
            NotImplementedError: If vendor doesn't support downloads
        """
        ...

    def run_prehook(self, bronze_root: Path) -> None:
        """Run vendor-specific preprocessing before ingestion.

        Some vendors (e.g., Quant360) deliver data in archives that must be
        reorganized before ingestion. This hook allows vendor-specific preprocessing.

        Args:
            bronze_root: Root directory for bronze files

        Raises:
            NotImplementedError: If vendor doesn't support prehooks
        """
        ...
