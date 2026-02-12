"""Tushare vendor plugin.

This module provides the vendor plugin implementation for Tushare (Chinese stock data API).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pointline.io.protocols import (
    ApiCaptureRequest,
    ApiReplayOptions,
    ApiSnapshotSpec,
    BronzeFileMetadata,
    BronzeLayoutSpec,
)


class TushareVendor:
    """Tushare vendor plugin for Chinese stock market data."""

    name = "tushare"
    display_name = "Tushare"
    supports_parsers = False  # API client only, no file parsing
    supports_download = True
    supports_prehooks = False
    supports_api_snapshots = True

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout spec for Tushare API metadata snapshots."""
        return BronzeLayoutSpec(
            glob_patterns=[
                "type=*metadata/date=*/snapshot_ts=*/*.jsonl.gz",
                "type=*metadata/exchange=*/date=*/snapshot_ts=*/*.jsonl.gz",
            ],
            required_fields={"vendor", "data_type", "date"},
            extract_metadata=self._extract_hive_metadata,
            normalize_metadata=self._normalize_hive_metadata,
        )

    def _extract_hive_metadata(self, path: Path) -> dict[str, Any]:
        meta: dict[str, Any] = {"vendor": self.name}
        for part in path.parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key == "type":
                meta["data_type"] = value
            elif key == "date":
                meta["date"] = datetime.strptime(value, "%Y-%m-%d").date()
            else:
                meta[key] = value
        return meta

    def _normalize_hive_metadata(
        self, partial: dict[str, Any], file_stats: dict[str, Any]
    ) -> BronzeFileMetadata:
        return BronzeFileMetadata(
            vendor=partial["vendor"],
            data_type=partial["data_type"],
            bronze_file_path=str(file_stats["rel_path"]),
            file_size_bytes=file_stats["size"],
            last_modified_ts=file_stats["mtime_us"],
            sha256=file_stats["sha256"],
            date=partial.get("date"),
            interval=partial.get("interval"),
            extra=None,
        )

    def get_table_mapping(self) -> dict[str, str]:
        return {}

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor (none).

        Raises:
            NotImplementedError: Tushare doesn't provide parsers
        """
        raise NotImplementedError(f"{self.name} does not provide parsers")

    def get_api_snapshot_specs(self) -> dict[str, ApiSnapshotSpec]:
        return {
            "dim_symbol": ApiSnapshotSpec(
                dataset="dim_symbol",
                data_type="dim_symbol_metadata",
                target_table="dim_symbol",
                partition_keys=("exchange",),
                default_glob="type=dim_symbol_metadata/**/*.jsonl.gz",
            )
        }

    def capture_api_snapshot(
        self, dataset: str, request: ApiCaptureRequest
    ) -> list[dict[str, Any]]:
        if dataset != "dim_symbol":
            raise ValueError(f"{self.name} does not support API snapshot dataset '{dataset}'")

        exchange = str(request.params.get("exchange", "")).lower().strip()
        include_delisted = bool(request.params.get("include_delisted", False))
        token = str(request.params.get("token", ""))

        if exchange not in {"szse", "sse", "all"}:
            raise ValueError("exchange must be one of: szse, sse, all")

        from pointline.io.vendors.tushare.client import TushareClient

        client = TushareClient(token=token)
        if exchange == "szse":
            df = client.get_szse_stocks(include_delisted=include_delisted)
        elif exchange == "sse":
            df = client.get_sse_stocks(include_delisted=include_delisted)
        else:
            df = client.get_all_stocks(exchanges=["SZSE", "SSE"], include_delisted=include_delisted)
        return df.to_dicts()

    def build_updates_from_snapshot(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        options: ApiReplayOptions,
    ) -> pl.DataFrame:
        if dataset != "dim_symbol":
            raise ValueError(f"{self.name} does not support API snapshot dataset '{dataset}'")
        if not records:
            return pl.DataFrame()

        from pointline.io.vendors.tushare.stock_basic_cn import (
            build_dim_symbol_updates_from_stock_basic_cn,
        )

        return build_dim_symbol_updates_from_stock_basic_cn(pl.DataFrame(records))

    def get_scd2_tracked_columns(self, dataset: str) -> list[str] | None:
        if dataset == "dim_symbol":
            from pointline.tables.dim_symbol import TRACKED_COLS

            return list(TRACKED_COLS)
        return None

    def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """Tushare does not support file parsing."""
        raise NotImplementedError(f"{self.name} does not support read_and_parse")

    def normalize_exchange(self, exchange: str) -> str:
        """Normalize exchange names (unused for Tushare)."""
        return exchange.lower().strip()

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize symbols (unused for Tushare)."""
        return symbol

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
            Tushare is primarily an API-based vendor.
        """
        # Check if directory name is "tushare"
        return "tushare" in path.name.lower()
