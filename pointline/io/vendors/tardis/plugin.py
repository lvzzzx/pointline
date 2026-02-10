"""Tardis vendor plugin.

This plugin provides parsers and client for Tardis.dev historical crypto market data.
"""

from __future__ import annotations

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
from pointline.io.vendors.tardis.mapper import build_updates_from_instruments


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
    supports_api_snapshots = True

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout specification for Tardis.

        Tardis uses Hive-style partitioning:
        exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/*.csv.gz

        Returns:
            BronzeLayoutSpec for Tardis vendor
        """
        return BronzeLayoutSpec(
            glob_patterns=["exchange=*/type=*/date=*/symbol=*/*.csv.gz"],
            required_fields={"vendor", "data_type", "date"},
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
            date=partial.get("date"),
            interval=partial.get("interval"),
            extra=None,
        )

    def get_table_mapping(self) -> dict[str, str]:
        return {
            "trades": "trades",
            "quotes": "quotes",
            "book_snapshot_25": "book_snapshot_25",
            "derivative_ticker": "derivative_ticker",
        }

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

        exchange = request.params.get("exchange")
        if not exchange:
            raise ValueError("exchange is required for tardis dim_symbol capture")

        api_key = str(request.params.get("api_key", ""))
        symbol = request.params.get("symbol")
        filter_payload = request.params.get("filter_payload")

        from pointline.io.vendors.tardis.client import TardisClient

        client = TardisClient(api_key=api_key)
        return client.fetch_instruments(exchange, symbol=symbol, filter_payload=filter_payload)

    def build_updates_from_snapshot(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        options: ApiReplayOptions,
    ) -> pl.DataFrame:
        if dataset != "dim_symbol":
            raise ValueError(f"{self.name} does not support API snapshot dataset '{dataset}'")

        exchange = (options.partitions or {}).get("exchange")
        if not exchange:
            raise ValueError("exchange partition is required to replay tardis dim_symbol metadata")

        return build_updates_from_instruments(
            records,
            exchange=exchange,
            effective_ts=options.effective_ts_us,
            rebuild=options.rebuild,
        )

    def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """Read bronze file and return parsed DataFrame with metadata columns."""
        from pointline.io.vendors.utils import read_csv_with_lineage

        df = read_csv_with_lineage(path, has_header=True)
        if df.is_empty():
            return df

        parser = self.get_parsers().get(meta.data_type)
        if parser is None:
            raise ValueError(f"No parser for data_type={meta.data_type}")

        parsed_df = parser(df)

        path_meta = self._extract_hive_metadata(path)
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
        """Tardis symbols already normalized (uppercase, no separator)."""
        return symbol

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
