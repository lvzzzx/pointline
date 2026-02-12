"""CoinGecko vendor plugin.

This module provides the vendor plugin implementation for CoinGecko (market data API).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
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
from pointline.tables.dim_asset_stats import normalize_dim_asset_stats_schema


class CoingeckoVendor:
    """CoinGecko vendor plugin for market data and asset statistics."""

    name = "coingecko"
    display_name = "CoinGecko"
    supports_parsers = False  # API client only, no file parsing
    supports_download = True
    supports_prehooks = False
    supports_api_snapshots = True

    def get_bronze_layout_spec(self) -> BronzeLayoutSpec:
        """Get bronze layout spec for CoinGecko API metadata snapshots."""
        return BronzeLayoutSpec(
            glob_patterns=["type=*metadata/date=*/snapshot_ts=*/*.jsonl.gz"],
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
            NotImplementedError: CoinGecko doesn't provide parsers
        """
        raise NotImplementedError(f"{self.name} does not provide parsers")

    def get_api_snapshot_specs(self) -> dict[str, ApiSnapshotSpec]:
        return {
            "dim_asset_stats": ApiSnapshotSpec(
                dataset="dim_asset_stats",
                data_type="dim_asset_stats_metadata",
                target_table="dim_asset_stats",
                partition_keys=(),
                default_glob="type=dim_asset_stats_metadata/**/*.jsonl.gz",
            )
        }

    def capture_api_snapshot(
        self, dataset: str, request: ApiCaptureRequest
    ) -> list[dict[str, Any]]:
        if dataset != "dim_asset_stats":
            raise ValueError(f"{self.name} does not support API snapshot dataset '{dataset}'")

        mode = str(request.params.get("mode", "daily")).lower().strip()
        base_assets = request.params.get("base_assets")
        if isinstance(base_assets, str):
            base_assets = [
                token.strip().upper() for token in base_assets.split(",") if token.strip()
            ]
        elif isinstance(base_assets, list):
            base_assets = [
                str(token).strip().upper() for token in base_assets if str(token).strip()
            ]
        else:
            base_assets = None

        api_key = request.params.get("api_key")

        from pointline.io.vendors.coingecko.client import CoinGeckoClient
        from pointline.services.asset_stats_providers import CoinGeckoAssetStatsProvider

        client = CoinGeckoClient(api_key=str(api_key) if api_key else None)
        provider = CoinGeckoAssetStatsProvider(client=client)

        if mode == "daily":
            date_raw = request.params.get("date")
            if not date_raw:
                raise ValueError("date is required for coingecko daily capture")
            target_date = self._parse_date(str(date_raw), field="date")
            data = provider.fetch_daily(target_date, base_assets)
            return data.to_dicts()

        if mode == "range":
            start_date_raw = request.params.get("start_date")
            end_date_raw = request.params.get("end_date")
            if not start_date_raw or not end_date_raw:
                raise ValueError("start_date and end_date are required for range capture")
            start_date = self._parse_date(str(start_date_raw), field="start_date")
            end_date = self._parse_date(str(end_date_raw), field="end_date")
            if start_date > end_date:
                raise ValueError("start_date must be <= end_date")

            data = provider.fetch_range(start_date, end_date, base_assets)
            if data is None:
                daily_batches: list[pl.DataFrame] = []
                current = start_date
                while current <= end_date:
                    daily_batches.append(provider.fetch_daily(current, base_assets))
                    current = date.fromordinal(current.toordinal() + 1)
                data = (
                    pl.concat(
                        [batch for batch in daily_batches if not batch.is_empty()], how="vertical"
                    )
                    if daily_batches
                    else pl.DataFrame()
                )

            return data.to_dicts()

        raise ValueError(f"Unsupported coingecko capture mode: {mode}")

    def build_updates_from_snapshot(
        self,
        dataset: str,
        records: list[dict[str, Any]],
        options: ApiReplayOptions,
    ) -> pl.DataFrame:
        if dataset != "dim_asset_stats":
            raise ValueError(f"{self.name} does not support API snapshot dataset '{dataset}'")
        if not records:
            return pl.DataFrame()
        return normalize_dim_asset_stats_schema(pl.DataFrame(records))

    def get_scd2_tracked_columns(self, dataset: str) -> list[str] | None:
        # dim_asset_stats is not SCD2-managed
        return None

    @staticmethod
    def _parse_date(raw: str, field: str) -> date:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid {field} format: {raw} (expected YYYY-MM-DD)") from exc

    def read_and_parse(self, path: Path, meta: BronzeFileMetadata) -> pl.DataFrame:
        """CoinGecko does not support file parsing."""
        raise NotImplementedError(f"{self.name} does not support read_and_parse")

    def normalize_exchange(self, exchange: str) -> str:
        """Normalize exchange names (unused for CoinGecko)."""
        return exchange.lower().strip()

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """Normalize symbols (unused for CoinGecko)."""
        return symbol

    def get_download_client(self) -> Any:
        """Get download client for this vendor.

        Returns:
            CoinGeckoClient class
        """
        from pointline.io.vendors.coingecko.client import CoinGeckoClient

        return CoinGeckoClient

    def run_prehook(self, bronze_root, source_dir=None) -> None:
        """Run prehook for this vendor (not supported)."""
        raise NotImplementedError(f"{self.name} does not support prehooks")

    def can_handle(self, path: Path) -> bool:
        """Detect CoinGecko data by directory name.

        Args:
            path: Bronze root path to check

        Returns:
            True if path contains "coingecko" in directory name
        """
        # Check if directory name is "coingecko"
        return "coingecko" in path.name.lower()
