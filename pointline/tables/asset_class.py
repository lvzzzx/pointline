"""Asset class taxonomy: hierarchical classification of exchanges and markets.

This module provides the canonical asset class hierarchy for data discovery
and filtering. It is designed for extensibility — adding new asset classes
(US equities, futures, forex) requires only adding entries here.

The taxonomy uses a prefix convention for parent/child relationships:
  - "crypto" is the parent of "crypto-spot" and "crypto-derivatives"
  - "stocks" is the parent of "stocks-cn", "stocks-us", etc.

When ``dim_exchange`` is available, exchange lists are derived from its
``asset_class`` column.  The ``exchanges`` lists here serve as bootstrap
fallbacks before dim_exchange is seeded.
"""

from __future__ import annotations

ASSET_CLASS_TAXONOMY: dict[str, dict] = {
    "crypto": {
        "description": "Cryptocurrency spot and derivatives",
        "children": ["crypto-spot", "crypto-derivatives"],
    },
    "crypto-spot": {
        "description": "Cryptocurrency spot trading",
        "parent": "crypto",
        "exchanges": [
            "binance",
            "coinbase",
            "kraken",
            "okx",
            "huobi",
            "gate",
            "bitfinex",
            "bitstamp",
            "gemini",
            "crypto-com",
            "kucoin",
            "binance-us",
            "coinbase-pro",
        ],
    },
    "crypto-derivatives": {
        "description": "Cryptocurrency futures, perpetuals, and options",
        "parent": "crypto",
        "exchanges": [
            "binance-futures",
            "binance-coin-futures",
            "deribit",
            "bybit",
            "okx-futures",
            "bitmex",
            "ftx",
            "dydx",
        ],
    },
    "stocks": {
        "description": "Equity markets",
        "children": ["stocks-cn"],
    },
    "stocks-cn": {
        "description": "Chinese stock exchanges with Level 3 order book data",
        "parent": "stocks",
        "exchanges": ["szse", "sse"],
    },
    # Future asset classes (placeholder for extensibility):
    # "stocks-us": {"description": "US stock exchanges", "parent": "stocks", "exchanges": []},
    # "futures": {"description": "Traditional futures (CME, ICE, etc.)", "exchanges": []},
    # "options": {"description": "Listed options markets", "exchanges": []},
    # "forex": {"description": "Foreign exchange spot and derivatives", "exchanges": []},
}


def taxonomy_exchanges(asset_class: str) -> list[str]:
    """Get exchanges for an asset class from the static taxonomy (no dim_exchange).

    Handles parent classes by recursing into children.

    Args:
        asset_class: Asset class name (e.g., "crypto", "crypto-spot", "stocks-cn")

    Returns:
        List of exchange names from the static taxonomy.
    """
    if asset_class not in ASSET_CLASS_TAXONOMY:
        return []

    entry = ASSET_CLASS_TAXONOMY[asset_class]

    if "children" in entry:
        exchanges: list[str] = []
        for child in entry["children"]:
            exchanges.extend(taxonomy_exchanges(child))
        return exchanges

    return list(entry.get("exchanges", []))


def taxonomy_description(asset_class: str) -> str | None:
    """Get human-readable description for an asset class.

    Args:
        asset_class: Asset class name

    Returns:
        Description string or None if not found.
    """
    entry = ASSET_CLASS_TAXONOMY.get(asset_class)
    return entry["description"] if entry else None


def taxonomy_children(asset_class: str) -> list[str]:
    """Get direct children of a parent asset class.

    Args:
        asset_class: Parent asset class name (e.g., "crypto")

    Returns:
        List of child asset class names, or empty list if leaf or not found.
    """
    entry = ASSET_CLASS_TAXONOMY.get(asset_class)
    if entry is None:
        return []
    return list(entry.get("children", []))
