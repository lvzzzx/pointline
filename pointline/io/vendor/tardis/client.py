from __future__ import annotations

import json
import time
import urllib.parse
from typing import Any

import requests


class TardisClient:
    """Client for interacting with the Tardis.dev HTTP API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.tardis.dev/v1",
        timeout: int = 30,
        max_retries: int = 5,
    ):
        if not api_key:
            raise ValueError("Tardis API key is required.")
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        if params:
            # Tardis expects JSON filter to be stringified in the query string
            if "filter" in params and isinstance(params["filter"], dict):
                params["filter"] = json.dumps(params["filter"], separators=(",", ":"))
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "User-Agent": "pointline/0.1 (tardis-vendor-client)",
        }

        retryable_http = {429, 500, 502, 503, 504}
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException:
                if attempt >= self.max_retries - 1:
                    raise
                time.sleep(0.5 * (2**attempt))
                continue

            if resp.status_code in retryable_http and attempt < self.max_retries - 1:
                time.sleep(0.5 * (2**attempt))
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"Tardis API error {resp.status_code}: {resp.text}"
                ) from exc

            return resp.json()

        raise RuntimeError("Tardis API request failed after retries.")

    def fetch_instruments(
        self,
        exchange: str,
        symbol: str | None = None,
        filter_payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch instrument metadata for an exchange."""
        if symbol:
            endpoint = f"instruments/{exchange}/{urllib.parse.quote(symbol)}"
            result = self._get(endpoint)
            return [result] if isinstance(result, dict) else result

        endpoint = f"instruments/{exchange}"
        params = {"filter": filter_payload} if filter_payload else {}
        result = self._get(endpoint, params=params)
        return result if isinstance(result, list) else [result]
