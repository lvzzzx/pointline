"""Tardis vendor plugin.

This plugin provides parsers and client for Tardis.dev historical crypto market data.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pointline.io.protocols import BronzeFileMetadata, BronzeLayoutSpec


class TardisVendor:
    """Tardis.dev vendor plugin.

    Provides parsers for:
    - trades
    - quotes
    - book_snapshot_25
    - derivative_ticker
    """

    name = "tardis"
    display_name = "Tardis.dev"
    supports_parsers = True
    supports_download = True
    supports_prehooks = False

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout specification for Tardis.

        Tardis uses Hive-style partitioning:
        exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/*.csv.gz

        Returns:
            BronzeLayoutSpec for Tardis vendor
        """
        return BronzeLayoutSpec(
            glob_patterns=["exchange=*/type=*/date=*/symbol=*/*.csv.gz"],
            required_fields={"vendor", "data_type", "exchange", "symbol", "date"},
            extract_metadata=self._extract_hive_metadata,
            normalize_metadata=self._normalize_hive_metadata,
        )

    def _extract_hive_metadata(self, path: Path) -> dict[str, Any]:
        """Parse Hive-style partitions from path.

        Args:
            path: File path to parse (e.g., exchange=binance/type=trades/date=2024-05-01/symbol=BTCUSDT/file.csv.gz)

        Returns:
            Dictionary with extracted metadata fields
        """
        meta: dict[str, Any] = {"vendor": "tardis"}

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
        """Get all Tardis parsers.

        Returns:
            Dictionary mapping data_type to parser function
        """
        # Import parsers locally to avoid circular imports
        from pointline.io.vendors.tardis.parsers import (
            parse_tardis_book_snapshots_csv,
            parse_tardis_derivative_ticker_csv,
            parse_tardis_quotes_csv,
            parse_tardis_trades_csv,
        )

        return {
            "trades": parse_tardis_trades_csv,
            "quotes": parse_tardis_quotes_csv,
            "book_snapshot_25": parse_tardis_book_snapshots_csv,
            "derivative_ticker": parse_tardis_derivative_ticker_csv,
        }

    def get_download_client(self) -> Any:
        """Get Tardis API client.

        Returns:
            TardisClient instance
        """
        from pointline.io.vendors.tardis.client import TardisClient

        return TardisClient()

    def run_prehook(self, bronze_root: Path, source_dir: Path | None = None) -> None:
        """Tardis doesn't need prehooks."""
        raise NotImplementedError("Tardis vendor doesn't support prehooks")

    def can_handle(self, path: Path) -> bool:
        """Detect Tardis data by directory name.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains "tardis" in directory name or path components
        """
        # Check if directory name is "tardis"
        if path.name == "tardis":
            return True

        # Check if "tardis" appears anywhere in the path
        return "tardis" in path.parts
