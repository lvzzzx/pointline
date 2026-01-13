"""Service for managing dim_asset_stats table updates from CoinGecko API."""

import logging
import time
from datetime import date, datetime

import polars as pl
from deltalake.exceptions import CommitFailedError

from pointline.config import get_coingecko_coin_id, get_table_path
from pointline.dim_asset_stats import normalize_dim_asset_stats_schema, required_dim_asset_stats_columns
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.vendor.coingecko import CoinGeckoClient
from pointline.services.base_service import BaseService

logger = logging.getLogger(__name__)


class DimAssetStatsService(BaseService):
    """
    Orchestrates daily updates for the dim_asset_stats table from CoinGecko API.
    
    Uses MERGE operations on (base_asset, date) key for idempotent updates.
    """

    def __init__(
        self,
        repo: BaseDeltaRepository,
        coingecko_client: CoinGeckoClient | None = None,
        max_retries: int = 3,
    ):
        """
        Initialize service.

        Args:
            repo: Repository for dim_asset_stats table
            coingecko_client: Optional CoinGecko client (creates default if None)
            max_retries: Maximum retry attempts for optimistic concurrency conflicts
        """
        self.repo = repo
        self.coingecko_client = coingecko_client or CoinGeckoClient()
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

                logger.warning(f"Conflict detected (attempt {attempt}/{self.max_retries}). Retrying...")
                time.sleep(0.1 * attempt)  # Simple backoff

    def sync_daily(self, target_date: date, base_assets: list[str] | None = None) -> None:
        """
        Fetch and sync asset stats for a single date.

        Args:
            target_date: Date to sync
            base_assets: Optional list of base assets to sync. If None, fetches all assets
                        that exist in dim_symbol and have CoinGecko mappings.
        """
        if base_assets is None:
            # Get all unique base_assets from dim_symbol that have CoinGecko mappings
            from pointline.config import get_table_path

            try:
                dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
                # Get current symbols only
                current_symbols = dim_symbol.filter(pl.col("is_current") == True)
                base_assets = current_symbols["base_asset"].unique().to_list()
                # Filter to only those with CoinGecko mappings
                base_assets = [asset for asset in base_assets if get_coingecko_coin_id(asset) is not None]
            except Exception as e:
                logger.warning(f"Could not read dim_symbol to get base_assets: {e}")
                base_assets = []

        if not base_assets:
            logger.warning(f"No base_assets to sync for date {target_date}")
            return

        # Fetch data from CoinGecko
        stats_data = []
        fetched_at_ts = int(datetime.now().timestamp() * 1_000_000)  # Current time in µs

        for base_asset in base_assets:
            coin_id = get_coingecko_coin_id(base_asset)
            if coin_id is None:
                logger.warning(f"No CoinGecko mapping for base_asset: {base_asset}, skipping")
                continue

            try:
                stats = self.coingecko_client.fetch_asset_stats(coin_id)
                updated_at_ts = self.coingecko_client.parse_timestamp(stats["last_updated"])

                # Convert to float to handle both int and float from API
                def to_float_or_none(val):
                    if val is None:
                        return None
                    return float(val)

                stats_data.append({
                    "base_asset": base_asset,
                    "date": target_date,
                    "coingecko_coin_id": coin_id,
                    "circulating_supply": to_float_or_none(stats.get("circulating_supply")),
                    "total_supply": to_float_or_none(stats.get("total_supply")),
                    "max_supply": to_float_or_none(stats.get("max_supply")),  # None for uncapped
                    "market_cap_usd": to_float_or_none(stats.get("market_cap_usd")),
                    "fully_diluted_valuation_usd": to_float_or_none(stats.get("fully_diluted_valuation_usd")),
                    "updated_at_ts": updated_at_ts,
                    "fetched_at_ts": fetched_at_ts,
                    "source": "coingecko",
                })
            except Exception as e:
                logger.error(f"Failed to fetch stats for {base_asset} ({coin_id}): {e}")
                continue

        if not stats_data:
            logger.warning(f"No data fetched for date {target_date}")
            return

        # Convert to DataFrame and update
        df = pl.DataFrame(stats_data)
        self.update(df)

    def sync_date_range(self, start_date: date, end_date: date, base_assets: list[str] | None = None) -> None:
        """
        Sync asset stats for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            base_assets: Optional list of base assets to sync
        """
        current = start_date
        total_days = (end_date - start_date).days + 1
        logger.info(f"Syncing dim_asset_stats for {total_days} days from {start_date} to {end_date}")

        from datetime import timedelta

        day = 0
        while current <= end_date:
            day += 1
            logger.info(f"Syncing day {day}/{total_days}: {current}")
            self.sync_daily(current, base_assets)
            current += timedelta(days=1)

        logger.info(f"Completed syncing {total_days} days")
