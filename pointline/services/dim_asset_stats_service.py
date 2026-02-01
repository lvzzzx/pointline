"""Service for managing dim_asset_stats table updates."""

import logging
import time
from datetime import date, timedelta

import polars as pl
from deltalake.exceptions import CommitFailedError

from pointline.io.base_repository import BaseDeltaRepository
from pointline.services.asset_stats_providers import (
    AssetStatsProvider,
    CoinGeckoAssetStatsProvider,
)
from pointline.services.base_service import BaseService
from pointline.tables.dim_asset_stats import (
    normalize_dim_asset_stats_schema,
    required_dim_asset_stats_columns,
)

logger = logging.getLogger(__name__)


class DimAssetStatsService(BaseService):
    """
    Orchestrates daily updates for the dim_asset_stats table from CoinGecko API.

    Uses MERGE operations on (base_asset, date) key for idempotent updates.
    """

    def __init__(
        self,
        repo: BaseDeltaRepository,
        provider: AssetStatsProvider | None = None,
        max_retries: int = 3,
    ):
        """
        Initialize service.

        Args:
            repo: Repository for dim_asset_stats table
            provider: Optional asset stats provider (defaults to CoinGecko)
            max_retries: Maximum retry attempts for optimistic concurrency conflicts
        """
        self.repo = repo
        self.provider = provider or CoinGeckoAssetStatsProvider()
        self.max_retries = max_retries

    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Validate and clean incoming data.

        Args:
            data: DataFrame with dim_asset_stats columns

        Returns:
            Cleaned DataFrame

        Raises:
            ValueError: If required columns are missing
        """
        req = required_dim_asset_stats_columns()
        missing = [c for c in req if c not in data.columns]
        if missing:
            raise ValueError(f"dim_asset_stats missing required columns: {missing}")

        # Deduplicate by natural key (base_asset, date)
        # Keep the last row if duplicates exist
        return data.unique(subset=["base_asset", "date"], keep="last")

    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Compute new state by merging with existing data.

        For daily snapshots, we simply merge new data with existing.
        If a (base_asset, date) already exists, it gets updated.

        Args:
            valid_data: Validated new data

        Returns:
            Complete DataFrame with merged data
        """
        try:
            existing = self.repo.read_all()
        except Exception:
            # Table doesn't exist yet, start fresh
            existing = pl.DataFrame()

        if existing.is_empty():
            return valid_data

        # Merge: remove existing rows that match new data's (base_asset, date) keys
        # Then append new data
        if not valid_data.is_empty():
            keys_to_update = valid_data.select(["base_asset", "date"])
            existing_filtered = existing.join(keys_to_update, on=["base_asset", "date"], how="anti")
            result = pl.concat([existing_filtered, valid_data], how="vertical")
        else:
            result = existing

        return result

    def write(self, result: pl.DataFrame) -> None:
        """
        Persist the new state to the repository.

        Args:
            result: Complete DataFrame to write
        """
        # Normalize schema before writing
        result = normalize_dim_asset_stats_schema(result)
        self.repo.write_full(result)

    def update(self, data: pl.DataFrame) -> None:
        """
        Orchestrates the update lifecycle with optimistic concurrency retries.

        Args:
            data: DataFrame with dim_asset_stats data to merge
        """
        valid_data = self.validate(data)

        attempt = 0
        while attempt <= self.max_retries:
            try:
                result = self.compute_state(valid_data)
                self.write(result)

                logger.info(
                    f"DimAssetStats update complete. Processed {valid_data.height} rows. "
                    f"Resulting table height: {result.height}."
                )
                return
            except CommitFailedError as e:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error(
                        f"Failed to update DimAssetStats after {self.max_retries} retries due to conflict: {e}"
                    )
                    raise

                logger.warning(
                    f"Conflict detected (attempt {attempt}/{self.max_retries}). Retrying..."
                )
                time.sleep(0.1 * attempt)  # Simple backoff

    def sync_daily(self, target_date: date, base_assets: list[str] | None = None) -> None:
        """
        Fetch and sync asset stats for a single date.

        Args:
            target_date: Date to sync
            base_assets: Optional list of base assets to sync. If None, fetches all assets
                        that exist in dim_symbol and have CoinGecko mappings.
        """
        data = self.provider.fetch_daily(target_date, base_assets)
        if data.is_empty():
            logger.warning(f"No data fetched for date {target_date}")
            return

        self.update(data)

    def sync_date_range(
        self, start_date: date, end_date: date, base_assets: list[str] | None = None
    ) -> None:
        """
        Sync asset stats for a date range.

        Uses CoinGecko's circulating_supply_chart endpoint if API key is available
        (much more efficient - one call per asset), otherwise falls back to daily syncs.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            base_assets: Optional list of base assets to sync
        """
        data = self.provider.fetch_range(start_date, end_date, base_assets)
        if data is not None:
            if data.is_empty():
                logger.warning(f"No data fetched for date range {start_date} to {end_date}")
                return
            self.update(data)
            logger.info(f"Synced {data.height} rows for date range {start_date} to {end_date}")
            return

        # Fallback to daily syncs (for free tier)
        logger.info(
            f"Using daily sync method for date range {start_date} to {end_date} "
            "(provider does not support range fetch)"
        )
        current = start_date
        total_days = (end_date - start_date).days + 1

        day = 0
        while current <= end_date:
            day += 1
            logger.info(f"Syncing day {day}/{total_days}: {current}")
            self.sync_daily(current, base_assets)
            current += timedelta(days=1)

        logger.info(f"Completed syncing {total_days} days")
