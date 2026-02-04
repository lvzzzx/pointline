"""Quant360 vendor plugin.

This module provides the vendor plugin implementation for Quant360 (SZSE/SSE Level 3 data).
"""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pointline.io.protocols import BronzeFileMetadata, BronzeLayoutSpec


class Quant360Vendor:
    """Quant360 vendor plugin for Chinese stock exchange Level 3 data."""

    name = "quant360"
    display_name = "Quant360 (SZSE/SSE Level 3)"
    supports_parsers = True
    supports_download = False  # No download API, data delivered as archives
    supports_prehooks = True  # Needs reorganization from .7z archives

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout specification for Quant360.

        Quant360 uses Hive-style partitioning for SZSE L3 data:
        exchange={exchange}/type=l3_{orders|ticks}/date={date}/symbol={symbol}/*.csv.gz

        Returns:
            BronzeLayoutSpec for Quant360 vendor
        """
        return BronzeLayoutSpec(
            glob_patterns=["exchange=*/type=l3_*/date=*/symbol=*/*.csv.gz"],
            required_fields={"vendor", "data_type", "exchange", "symbol", "date"},
            extract_metadata=self._extract_hive_metadata,
            normalize_metadata=self._normalize_hive_metadata,
        )

    def _extract_hive_metadata(self, path: Path) -> dict[str, Any]:
        """Parse Hive-style partitions from Quant360 path.

        Args:
            path: File path to parse

        Returns:
            Dictionary with extracted metadata fields
        """
        meta: dict[str, Any] = {"vendor": "quant360"}

        for part in path.parts:
            if "=" in part:
                key, val = part.split("=", 1)
                if key == "type":
                    meta["data_type"] = val
                elif key == "date":
                    meta["date"] = datetime.strptime(val, "%Y-%m-%d").date()
                else:
                    meta[key] = val

        return meta

    def _normalize_hive_metadata(
        self, partial: dict[str, Any], file_stats: dict[str, Any]
    ) -> BronzeFileMetadata:
        """Combine partial metadata + file stats into BronzeFileMetadata.

        Args:
            partial: Partial metadata extracted from path
            file_stats: File statistics (rel_path, size, mtime_us, sha256)

        Returns:
            Complete BronzeFileMetadata instance
        """
        return BronzeFileMetadata(
            vendor=partial["vendor"],
            data_type=partial["data_type"],
            bronze_file_path=str(file_stats["rel_path"]),
            file_size_bytes=file_stats["size"],
            last_modified_ts=file_stats["mtime_us"],
            sha256=file_stats["sha256"],
            exchange=partial.get("exchange"),
            symbol=partial.get("symbol"),
            date=partial.get("date"),
            interval=partial.get("interval"),
            extra=None,
        )

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
