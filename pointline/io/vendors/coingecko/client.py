"""CoinGecko API client for fetching asset statistics."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Rate limiting: very conservative limit for free tier
# CoinGecko free tier: ~10-50 calls/minute (varies, can be strict)
MAX_REQUESTS_PER_MINUTE = 10
REQUEST_DELAY_SECONDS = 7.0  # ~8-9 requests/minute (very conservative to avoid 429 errors)


class CoinGeckoClient:
    """Client for CoinGecko API v3."""

    BASE_URL_FREE = "https://api.coingecko.com/api/v3"
    BASE_URL_PRO = "https://pro-api.coingecko.com/api/v3"

    def __init__(self, api_key: str | None = None, rate_limit_delay: float = REQUEST_DELAY_SECONDS):
        """
        Initialize CoinGecko client.

        Args:
            api_key: Optional API key for Pro/Enterprise tier (enables chart endpoints)
            rate_limit_delay: Delay between requests in seconds (default: 7.0s for free tier)
        """
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time: float = 0.0
        self.base_url = self.BASE_URL_PRO if api_key else self.BASE_URL_FREE

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _make_request(
        self, endpoint: str, params: dict[str, Any] | None = None, max_retries: int = 3
    ) -> dict[str, Any]:
        """
        Make HTTP request to CoinGecko API with rate limiting and error handling.

        Args:
            endpoint: API endpoint (e.g., "/coins/bitcoin")
            params: Optional query parameters
            max_retries: Maximum retry attempts for rate limit errors

        Returns:
            JSON response as dict

        Raises:
            requests.RequestException: If request fails after retries
            ValueError: If coin not found (404)
        """
        url = f"{self.base_url}{endpoint}"
        headers = {}
        if self.api_key:
            # Use Pro API header for Pro/Enterprise endpoints
            headers["x-cg-pro-api-key"] = self.api_key

        for attempt in range(max_retries):
            self._rate_limit()

            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    raise ValueError(f"Coin not found: {endpoint}") from e
                elif e.response.status_code == 429:
                    # Rate limit exceeded - wait longer and retry
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 60  # Exponential backoff: 60s, 120s, 180s
                        logger.warning(
                            f"Rate limit exceeded (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        raise
                raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed: {e}. Retrying {attempt + 1}/{max_retries}")
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                    continue
                logger.error(f"CoinGecko API request failed after {max_retries} retries: {e}")
                raise

    def fetch_asset_stats(self, coin_id: str) -> dict[str, Any]:
        """
        Fetch asset statistics for a single coin.

        Args:
            coin_id: CoinGecko coin ID (e.g., "bitcoin", "ethereum")

        Returns:
            Dictionary with asset statistics:
            {
                "coin_id": str,
                "symbol": str,
                "circulating_supply": float | None,
                "total_supply": float | None,
                "max_supply": float | None,
                "market_cap_usd": float | None,
                "fully_diluted_valuation_usd": float | None,
                "last_updated": str,  # ISO 8601 timestamp
            }

        Raises:
            ValueError: If coin_id not found
            requests.RequestException: If API request fails
        """
        data = self._make_request(f"/coins/{coin_id}")

        market_data = data.get("market_data", {})
        market_cap = market_data.get("market_cap", {})
        fdv = market_data.get("fully_diluted_valuation", {})

        return {
            "coin_id": data.get("id"),
            "symbol": data.get("symbol", "").upper(),
            "circulating_supply": market_data.get("circulating_supply"),
            "total_supply": market_data.get("total_supply"),
            "max_supply": market_data.get("max_supply"),  # None for uncapped assets
            "market_cap_usd": market_cap.get("usd"),
            "fully_diluted_valuation_usd": fdv.get("usd"),
            "last_updated": data.get("last_updated"),  # ISO 8601 format
        }

    def fetch_batch_asset_stats(self, coin_ids: list[str]) -> list[dict[str, Any]]:
        """
        Fetch asset statistics for multiple coins in one request.

        Uses the markets endpoint which is more efficient for batch operations.

        Args:
            coin_ids: List of CoinGecko coin IDs

        Returns:
            List of dictionaries with asset statistics (same format as fetch_asset_stats)

        Note:
            CoinGecko markets endpoint has a limit of 250 coins per request.
            This method will split into batches if needed.
        """
        BATCH_SIZE = 250
        results = []

        for i in range(0, len(coin_ids), BATCH_SIZE):
            batch = coin_ids[i : i + BATCH_SIZE]
            ids_param = ",".join(batch)

            data = self._make_request(
                "/coins/markets", params={"vs_currency": "usd", "ids": ids_param}
            )

            for item in data:
                results.append(
                    {
                        "coin_id": item.get("id"),
                        "symbol": item.get("symbol", "").upper(),
                        "circulating_supply": item.get("circulating_supply"),
                        "total_supply": item.get("total_supply"),
                        "max_supply": item.get("max_supply"),
                        "market_cap_usd": item.get("market_cap"),
                        "fully_diluted_valuation_usd": item.get("fully_diluted_valuation"),
                        "last_updated": item.get("last_updated"),
                    }
                )

        return results

    def fetch_circulating_supply_chart(
        self, coin_id: str, days: str | int = "max", interval: str | None = None
    ) -> list[tuple[int, float]]:
        """
        Fetch historical circulating supply chart data.

        This is much more efficient for historical backfills than fetching individual dates.
        Returns data points as (timestamp_ms, supply_value) tuples.

        Args:
            coin_id: CoinGecko coin ID (e.g., "bitcoin")
            days: Number of days of history ("max" for all available, or integer)
            interval: Optional interval ("5m", "hourly", "daily"). If None, auto-selected:
                     - 1 day = 5-minutely
                     - 2-90 days = hourly
                     - 91+ days = daily

        Returns:
            List of (timestamp_ms, supply_value) tuples
            timestamp_ms is in milliseconds since epoch

        Note:
            This endpoint requires Pro/Enterprise API key.
            Data available from June 22, 2019.
            For free tier, falls back to individual date fetches.

        Raises:
            ValueError: If coin_id not found
            requests.RequestException: If API request fails
        """
        if not self.api_key:
            raise ValueError(
                "Circulating supply chart endpoint requires Pro/Enterprise API key. "
                "Use fetch_asset_stats() for free tier or provide --api-key"
            )

        params: dict[str, Any] = {"days": str(days)}
        if interval:
            params["interval"] = interval

        data = self._make_request(f"/coins/{coin_id}/circulating_supply_chart", params=params)

        # Parse response: [[timestamp_ms, "supply_value"], ...]
        chart_data = data.get("circulating_supply", [])
        result = []
        for point in chart_data:
            timestamp_ms = int(point[0])
            supply_value = float(point[1]) if point[1] else None
            if supply_value is not None:
                result.append((timestamp_ms, supply_value))

        return result

    @staticmethod
    def parse_timestamp(iso_str: str) -> int:
        """
        Convert ISO 8601 timestamp to microseconds timestamp.

        Args:
            iso_str: ISO 8601 timestamp (e.g., "2026-01-12T14:52:42.485Z")

        Returns:
            Timestamp in microseconds (i64)
        """
        # Parse ISO 8601 with timezone
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1_000_000)  # Convert to microseconds
