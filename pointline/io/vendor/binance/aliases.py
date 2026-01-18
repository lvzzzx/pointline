"""Binance symbol aliasing for source-to-dim_symbol alignment."""

from __future__ import annotations

from collections.abc import Mapping

# Keep this map small and explicit.
# Keys: normalized exchange name (e.g., "binance", "binance-futures")
# Values: {source_symbol_normalized: canonical_exchange_symbol}
SYMBOL_ALIAS_MAP: dict[str, dict[str, str]] = {
    "binance": {},
    "binance-futures": {},
}


def normalize_symbol(exchange: str, raw_symbol: str) -> str:
    """
    Normalize a vendor symbol and apply explicit aliasing.

    - Uppercase and trim whitespace.
    - Remove common separators ("/", "-") but keep underscores (used in some futures).
    - Apply exchange-scoped alias map.
    """
    normalized_exchange = exchange.lower().strip()
    normalized_symbol = raw_symbol.strip().upper().replace("/", "").replace("-", "")
    alias_map: Mapping[str, str] = SYMBOL_ALIAS_MAP.get(normalized_exchange, {})
    return alias_map.get(normalized_symbol, normalized_symbol)
