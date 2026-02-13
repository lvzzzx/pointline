"""Minimal exchange metadata for v2 core ingestion."""

from __future__ import annotations

EXCHANGE_TIMEZONE_MAP: dict[str, str] = {
    "binance": "UTC",
    "binance-futures": "UTC",
    "binance-coin-futures": "UTC",
    "binance-us": "UTC",
    "coinbase": "UTC",
    "coinbase-pro": "UTC",
    "kraken": "UTC",
    "okx": "UTC",
    "okx-futures": "UTC",
    "huobi": "UTC",
    "gate": "UTC",
    "bitfinex": "UTC",
    "bitstamp": "UTC",
    "gemini": "UTC",
    "crypto-com": "UTC",
    "kucoin": "UTC",
    "deribit": "UTC",
    "bybit": "UTC",
    "bitmex": "UTC",
    "ftx": "UTC",
    "dydx": "UTC",
    "sse": "Asia/Shanghai",
    "szse": "Asia/Shanghai",
}


def get_exchange_timezone(exchange: str) -> str:
    normalized = exchange.strip().lower()
    try:
        return EXCHANGE_TIMEZONE_MAP[normalized]
    except KeyError as exc:
        known = ", ".join(sorted(EXCHANGE_TIMEZONE_MAP))
        raise ValueError(
            f"Unknown exchange '{exchange}'. No v2 timezone mapping configured. Known: {known}"
        ) from exc
