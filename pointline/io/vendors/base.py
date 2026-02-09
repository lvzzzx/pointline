"""Base plugin protocol for vendor plugins.

This module defines the interface that all vendor plugins must implement.
Each vendor (Tardis, Binance, Quant360, etc.) is packaged as a self-contained
plugin with its own parsers, client code, and utilities.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import polars as pl

if TYPE_CHECKING:
    from pointline.io.protocols import (
        ApiCaptureRequest,
        ApiReplayOptions,
        ApiSnapshotSpec,
        BronzeFileMetadata,
        BronzeLayoutSpec,
    )


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

    supports_api_snapshots: bool
    """Whether this vendor can capture/replay API metadata snapshots."""

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get the bronze layout specification for this vendor.

        Returns:
            BronzeLayoutSpec defining glob patterns, required fields,
            and metadata extraction functions for this vendor's bronze layer.

        Example:
            def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
                return BronzeLayoutSpec(
                    glob_patterns=["exchange=*/type=*/date=*/symbol=*/*.csv.gz"],
                    required_fields={"vendor", "data_type", "date"},
                    extract_metadata=self._extract_hive_metadata,
                    normalize_metadata=self._normalize_hive_metadata,
                )
        """
        ...

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor.

        Returns:
            Dictionary mapping data_type to parser function.
            Example: {"trades": parse_tardis_trades_csv, "quotes": parse_tardis_quotes_csv}
        """
        ...

    def get_api_snapshot_specs(self) -> dict[str, ApiSnapshotSpec]:
        """Get API snapshot dataset specs by dataset name."""
        ...

    def capture_api_snapshot(
        self, dataset: str, request: ApiCaptureRequest
    ) -> list[dict[str, Any]]:
        """Capture raw API records for a dataset."""
        ...

    def build_updates_from_snapshot(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        options: ApiReplayOptions,
    ) -> pl.DataFrame:
        """Build table updates from captured snapshot records."""
        ...

    def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """Read bronze file and return parsed DataFrame with metadata columns."""
        ...

    def normalize_exchange(self, exchange: str) -> str:
        """Normalize vendor-specific exchange name to canonical format."""
        ...

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize vendor-specific symbol format for dim_symbol matching."""
        ...

    def get_download_client(self) -> Any:
        """Get the download client for this vendor.

        Returns:
            Client instance (e.g., TardisClient, BinanceDownloader)

        Raises:
            NotImplementedError: If vendor doesn't support downloads
        """
        ...

    def run_prehook(self, bronze_root: Path, source_dir: Path | None = None) -> None:
        """Run vendor-specific preprocessing before ingestion.

        Some vendors (e.g., Quant360) deliver data in archives that must be
        reorganized before ingestion. This hook allows vendor-specific preprocessing.

        Args:
            bronze_root: Root directory for bronze files (output location)
            source_dir: Source directory containing raw archives (input location).
                       If None, assumes archives are in bronze_root.

        Raises:
            NotImplementedError: If vendor doesn't support prehooks
        """
        ...

    def can_handle(self, path: Path) -> bool:
        """Detect if this vendor can handle the given directory structure.

        This method allows vendors to "claim" a directory by examining:
        - Directory names
        - File patterns (e.g., *.7z archives)
        - Directory structure
        - File contents (if needed)

        Args:
            path: Bronze root path to check

        Returns:
            True if this vendor recognizes the structure, False otherwise

        Examples:
            # Simple directory name detection
            def can_handle(self, path: Path) -> bool:
                return path.name == "tardis"

            # Archive pattern detection
            def can_handle(self, path: Path) -> bool:
                return bool(list(path.glob("*_new_STK_*.7z")))

            # Structure-based detection
            def can_handle(self, path: Path) -> bool:
                return (path / "data" / "spot").exists()
        """
        ...
