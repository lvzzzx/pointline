"""Symbol normalization utilities for vendor-specific symbol formats.

Different vendors use different symbol formats (BTCUSDT vs BTC-USDT vs BTC/USDT).
These utilities normalize exchange symbols for dim_symbol matching.
"""


def normalize_binance_symbol(symbol: str, exchange: str) -> str:
    """Normalize Binance Vision symbol format.

    Binance uses format like "BTCUSDT" (no separator).
    This function is an identity transform by default, but can be customized
    if dim_symbol uses a different format (e.g., "BTC-USDT").

    Args:
        symbol: Raw exchange symbol (e.g., "BTCUSDT")
        exchange: Exchange name (e.g., "binance", "binance-futures")

    Returns:
        Normalized symbol for dim_symbol matching

    Examples:
        >>> normalize_binance_symbol("BTCUSDT", "binance")
        "BTCUSDT"

        # If dim_symbol uses hyphenated format:
        # return symbol[:3] + "-" + symbol[3:]  # "BTCUSDT" -> "BTC-USDT"
    """
    # Identity transform by default - adjust if dim_symbol uses different format
    return symbol


def no_normalization(symbol: str, exchange: str) -> str:
    """Default: no symbol normalization (identity function).

    Args:
        symbol: Raw exchange symbol
        exchange: Exchange name

    Returns:
        Unchanged symbol
    """
    return symbol
