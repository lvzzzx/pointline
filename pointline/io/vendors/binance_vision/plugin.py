"""Binance Vision vendor plugin.

This module provides the vendor plugin implementation for Binance Vision (historical data).
"""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from pointline.config import normalize_exchange
from pointline.io.protocols import (
    ApiCaptureRequest,
    ApiReplayOptions,
    ApiSnapshotSpec,
    BronzeFileMetadata,
    BronzeLayoutSpec,
)


class BinanceVisionVendor:
    """Binance Vision vendor plugin for historical kline data."""

    name = "binance_vision"
    display_name = "Binance Vision (Historical Data)"
    supports_parsers = True
    supports_download = True
    supports_prehooks = False
    supports_api_snapshots = False

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout specification for Binance Vision.

        Binance Vision uses Hive-style partitioning with optional interval:
        - With interval: */exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/interval={interval}/*.csv
        - Without interval: */exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/*.csv

        Returns:
            BronzeLayoutSpec for Binance Vision vendor
        """
        return BronzeLayoutSpec(
            glob_patterns=[
                "*/exchange=*/type=*/date=*/symbol=*/interval=*/*.csv",  # With interval
                "*/exchange=*/type=*/date=*/symbol=*/*.csv",  # Without interval
            ],
            required_fields={"vendor", "data_type", "date"},
            extract_metadata=self._extract_binance_metadata,
            normalize_metadata=self._normalize_binance_metadata,
        )

    def _extract_binance_metadata(self, path: Path) -> dict[str, Any]:
        """Parse Hive-style partitions from Binance Vision path.

        Args:
            path: File path to parse

        Returns:
            Dictionary with extracted metadata fields
        """
        meta: dict[str, Any] = {"vendor": "binance_vision"}

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

    def _normalize_binance_metadata(
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
            date=partial.get("date"),
            interval=partial.get("interval"),
            extra=None,
        )

    def get_table_mapping(self) -> dict[str, str]:
        """Return mapping from vendor data types to table names."""
        return {
            "klines": "kline_{interval}",
        }

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all parsers provided by this vendor.

        Returns:
            Dictionary mapping data_type to parser function
        """
        from pointline.io.vendors.binance_vision.parsers import parse_binance_klines_csv

        return {
            "klines": parse_binance_klines_csv,
        }

    def get_api_snapshot_specs(self) -> dict[str, ApiSnapshotSpec]:
        """Return API snapshot specifications (not supported by Binance Vision)."""
        return {}

    def capture_api_snapshot(
        self, dataset: str, request: ApiCaptureRequest
    ) -> list[dict[str, Any]]:
        """Capture API snapshot (not supported by Binance Vision)."""
        raise NotImplementedError(f"{self.name} does not support API snapshots")

    def build_updates_from_snapshot(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        options: ApiReplayOptions,
    ) -> pl.DataFrame:
        """Build updates from snapshot records (not supported by Binance Vision)."""
        raise NotImplementedError(f"{self.name} does not support API snapshots")

    def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """Read bronze file and return parsed DataFrame with metadata columns."""
        from pointline.io.vendors.utils import read_csv_with_lineage

        if meta.data_type == "klines":
            columns = [
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trade_count",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
                "ignore",
            ]
            df = read_csv_with_lineage(path, has_header=False, columns=columns)
        else:
            df = read_csv_with_lineage(path, has_header=True)

        if df.is_empty():
            return df

        parser = self.get_parsers().get(meta.data_type)
        if parser is None:
            raise ValueError(f"No parser for data_type={meta.data_type}")

        parsed_df = parser(df)

        path_meta = self._extract_binance_metadata(path)
        exchange_raw = path_meta.get("exchange")
        symbol_raw = path_meta.get("symbol")
        trading_date = path_meta.get("date")

        if exchange_raw is None or symbol_raw is None or trading_date is None:
            raise ValueError(f"Missing exchange/symbol/date in path: {path}")

        return parsed_df.with_columns(
            [
                pl.lit(self.normalize_exchange(exchange_raw)).alias("exchange"),
                pl.lit(self.normalize_symbol(symbol_raw, exchange_raw)).alias("exchange_symbol"),
                pl.lit(trading_date).alias("date"),
            ]
        )

    def normalize_exchange(self, exchange: str) -> str:
        """Normalize vendor-specific exchange name."""
        return normalize_exchange(exchange)

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize Binance symbols (uppercase, remove separators)."""
        return symbol.upper().replace("-", "").replace("/", "")

    def get_download_client(self) -> Any:
        """Get download client for this vendor.

        Returns:
            Module or class with download functions
        """
        from pointline.io.vendors.binance_vision import datasets

        return datasets

    def run_prehook(self, bronze_root, source_dir=None) -> None:
        """Run prehook for this vendor (not supported)."""
        raise NotImplementedError(f"{self.name} does not support prehooks")

    def can_handle(self, path: Path) -> bool:
        """Detect Binance Vision data by directory name.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains "binance" in directory name
        """
        # Check if directory name contains "binance"
        if "binance" in path.name.lower():
            return True

        # Check if "binance_vision" or "binance" appears in path
        return bool("binance_vision" in path.parts or "binance" in path.parts)
