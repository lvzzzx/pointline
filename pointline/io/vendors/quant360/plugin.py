"""Quant360 vendor plugin.

This module provides the vendor plugin implementation for Quant360 (SZSE/SSE Level 3 data).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl


class Quant360Vendor:
    """Quant360 vendor plugin for Chinese stock exchange Level 3 data."""

    name = "quant360"
    display_name = "Quant360 (SZSE/SSE Level 3)"
    supports_parsers = True
    supports_download = False  # No download API, data delivered as archives
    supports_prehooks = True  # Needs reorganization from .7z archives

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor.

        Returns:
            Dictionary mapping data_type to parser function
        """
        from pointline.io.vendors.quant360.parsers import (
            parse_quant360_orders_csv,
            parse_quant360_ticks_csv,
        )

        return {
            "l3_orders": parse_quant360_orders_csv,
            "l3_ticks": parse_quant360_ticks_csv,
        }

    def get_download_client(self) -> Any:
        """Get download client for this vendor (not supported).

        Raises:
            NotImplementedError: Quant360 doesn't provide download API
        """
        raise NotImplementedError(f"{self.name} does not support downloads")

    def run_prehook(self, bronze_root: Path) -> None:
        """Run prehook to reorganize .7z archives into Hive-partitioned structure.

        Args:
            bronze_root: Bronze layer root directory
        """
        from pointline.io.vendors.quant360.reorganize import reorganize_quant360_archives

        reorganize_quant360_archives(bronze_root)
