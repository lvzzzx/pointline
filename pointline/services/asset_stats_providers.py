"""Asset stats providers for dim_asset_stats."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone

import polars as pl

from pointline.config import get_coingecko_coin_id, get_table_path
from pointline.io.vendor.coingecko import CoinGeckoClient

logger = logging.getLogger(__name__)


class AssetStatsProvider(ABC):
    """Provider interface for asset stats."""

    name: str

    @abstractmethod
    def fetch_daily(self, target_date: date, base_assets: list[str] | None = None) -> pl.DataFrame:
        """Fetch stats for a single date."""

    def fetch_range(
        self, start_date: date, end_date: date, base_assets: list[str] | None = None
    ) -> pl.DataFrame | None:
        """Fetch stats for a date range; return None to use daily fallback."""
        return None


class CoinGeckoAssetStatsProvider(AssetStatsProvider):
    """CoinGecko-backed asset stats provider."""

    name = "coingecko"

    def __init__(self, client: CoinGeckoClient | None = None) -> None:
        self.client = client or CoinGeckoClient()

    def fetch_daily(self, target_date: date, base_assets: list[str] | None = None) -> pl.DataFrame:
        base_assets = self._resolve_base_assets(base_assets)
        if not base_assets:
            logger.warning(f"No base_assets to sync for date {target_date}")
            return pl.DataFrame()

        stats_data = []
        fetched_at_ts = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        for base_asset in base_assets:
            coin_id = get_coingecko_coin_id(base_asset)
            if coin_id is None:
                logger.warning(f"No CoinGecko mapping for base_asset: {base_asset}, skipping")
                continue

            try:
                stats = self.client.fetch_asset_stats(coin_id)
                updated_at_ts = self.client.parse_timestamp(stats["last_updated"])

                stats_data.append(
                    {
                        "base_asset": base_asset,
                        "date": target_date,
                        "coingecko_coin_id": coin_id,
                        "circulating_supply": _to_float_or_none(
                            stats.get("circulating_supply")
                        ),
                        "total_supply": _to_float_or_none(stats.get("total_supply")),
                        "max_supply": _to_float_or_none(stats.get("max_supply")),
                        "market_cap_usd": _to_float_or_none(stats.get("market_cap_usd")),
                        "fully_diluted_valuation_usd": _to_float_or_none(
                            stats.get("fully_diluted_valuation_usd")
                        ),
                        "updated_at_ts": updated_at_ts,
                        "fetched_at_ts": fetched_at_ts,
                        "source": "coingecko",
                    }
                )
            except Exception as exc:
                logger.error(f"Failed to fetch stats for {base_asset} ({coin_id}): {exc}")
                continue

        return pl.DataFrame(stats_data)

    def fetch_range(
        self, start_date: date, end_date: date, base_assets: list[str] | None = None
    ) -> pl.DataFrame | None:
        if not self.client.api_key:
            return None

        base_assets = self._resolve_base_assets(base_assets)
        if not base_assets:
            logger.warning(f"No base_assets to sync for date range {start_date} to {end_date}")
            return pl.DataFrame()

        days_ago = (datetime.now(timezone.utc).date() - start_date).days + 30
        days_param = "max" if days_ago > 365 else str(days_ago)

        start_ts_ms = int(
            datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc).timestamp()
            * 1000
        )
        end_ts_ms = int(
            (
                datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)
                + timedelta(days=1)
            ).timestamp()
            * 1000
        )

        stats_data = []
        fetched_at_ts = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        for base_asset in base_assets:
            coin_id = get_coingecko_coin_id(base_asset)
            if coin_id is None:
                logger.warning(f"No CoinGecko mapping for base_asset: {base_asset}, skipping")
                continue

            try:
                logger.info(f"Fetching chart data for {base_asset} ({coin_id})...")
                chart_data = self.client.fetch_circulating_supply_chart(
                    coin_id, days=days_param, interval="daily"
                )

                daily_supplies: dict[date, float] = {}
                for timestamp_ms, supply_value in chart_data:
                    if start_ts_ms <= timestamp_ms < end_ts_ms:
                        dt = datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)
                        day = dt.date()
                        if day not in daily_supplies:
                            daily_supplies[day] = supply_value

                current_stats = None
                try:
                    current_stats = self.client.fetch_asset_stats(coin_id)
                except Exception as exc:
                    logger.warning(f"Could not fetch current stats for {base_asset}: {exc}")

                for day, supply in daily_supplies.items():
                    if start_date <= day <= end_date:
                        updated_at_ts = int(
                            datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                            .timestamp()
                            * 1_000_000
                        )

                        stats_data.append(
                            {
                                "base_asset": base_asset,
                                "date": day,
                                "coingecko_coin_id": coin_id,
                                "circulating_supply": supply,
                                "total_supply": _to_float_or_none(
                                    current_stats.get("total_supply")
                                )
                                if current_stats
                                else None,
                                "max_supply": _to_float_or_none(current_stats.get("max_supply"))
                                if current_stats
                                else None,
                                "market_cap_usd": None,
                                "fully_diluted_valuation_usd": None,
                                "updated_at_ts": updated_at_ts,
                                "fetched_at_ts": fetched_at_ts,
                                "source": "coingecko",
                            }
                        )

                logger.info(f"Processed {len(daily_supplies)} days for {base_asset}")

            except ValueError as exc:
                if "requires Pro/Enterprise API key" in str(exc):
                    logger.warning(
                        f"Chart endpoint requires API key for {base_asset}. "
                        "Falling back to daily sync method."
                    )
                    return None
                logger.error(f"Failed to fetch chart for {base_asset} ({coin_id}): {exc}")
            except Exception as exc:
                logger.error(f"Failed to process {base_asset} ({coin_id}): {exc}")
                continue

        return pl.DataFrame(stats_data)

    def _resolve_base_assets(self, base_assets: list[str] | None) -> list[str]:
        if base_assets is not None:
            return [asset.strip().upper() for asset in base_assets if asset.strip()]

        try:
            dim_symbol = pl.read_delta(str(get_table_path("dim_symbol")))
            current_symbols = dim_symbol.filter(pl.col("is_current"))
            assets = current_symbols["base_asset"].unique().to_list()
            return [asset for asset in assets if get_coingecko_coin_id(asset) is not None]
        except Exception as exc:
            logger.warning(f"Could not read dim_symbol to get base_assets: {exc}")
            return []


class CoinMarketCapAssetStatsProvider(AssetStatsProvider):
    """Placeholder for CoinMarketCap-backed provider (CSV or API)."""

    name = "coinmarketcap"

    def fetch_daily(self, target_date: date, base_assets: list[str] | None = None) -> pl.DataFrame:
        raise NotImplementedError(
            "CoinMarketCap provider not configured yet. Provide an offline CSV source first."
        )

    def fetch_range(
        self, start_date: date, end_date: date, base_assets: list[str] | None = None
    ) -> pl.DataFrame | None:
        raise NotImplementedError(
            "CoinMarketCap provider not configured yet. Provide an offline CSV source first."
        )


def _to_float_or_none(val: float | None) -> float | None:
    if val is None:
        return None
    return float(val)
