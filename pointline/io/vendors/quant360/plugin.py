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

    def run_prehook(self, bronze_root: Path, source_dir: Path | None = None) -> None:
        """Run prehook to reorganize .7z archives into Hive-partitioned structure.

        Args:
            bronze_root: Bronze layer root directory (output location)
            source_dir: Directory containing .7z archives (input location).
                       If None, uses bronze_root as source.
        """
        from pointline.io.vendors.quant360.reorganize import reorganize_quant360_archives

        if source_dir is None:
            source_dir = bronze_root

        reorganize_quant360_archives(source_dir, bronze_root)

    def can_handle(self, path: Path) -> bool:
        """Detect Quant360 data by archive patterns or directory structure.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains Quant360 archives or reorganized data
        """
        # Check for directory name
        if path.name in ["quant360", "data.quant360.com"]:
            return True

        # Check for Quant360 .7z archives (order_new_STK_SZ_*.7z, tick_new_STK_SH_*.7z)
        if list(path.glob("*_new_STK_*.7z")):
            return True

        # Check for reorganized Quant360 structure (l3_orders or l3_ticks data types)
        # These are unique to Quant360's SZSE/SSE Level 3 data
        if list(path.glob("exchange=*/type=l3_orders/date=*/symbol=*/*.csv.gz")):
            return True

        return bool(list(path.glob("exchange=*/type=l3_ticks/date=*/symbol=*/*.csv.gz")))
