"""Error message templates for the Pointline research API.

This module provides reusable error message templates with helpful examples
and suggestions. Error messages include:
- Clear problem description
- Suggested fixes with example code
- Fuzzy matching for common typos ("Did you mean...?")
"""

from __future__ import annotations

from difflib import get_close_matches


def symbol_id_required_error() -> str:
    """Error message when symbol_id is not provided to research functions."""
    return (
        "symbol_id is required for partition pruning.\n"
        "\n"
        "To resolve symbol IDs from exchange symbols, use:\n"
        "  from pointline import registry\n"
        "  symbols = registry.find_symbol('BTC-PERPETUAL', exchange='deribit')\n"
        "  symbol_ids = symbols['symbol_id'].to_list()\n"
        "\n"
        "Then pass to your query:\n"
        "  research.load_trades(symbol_id=symbol_ids, start_ts_us=..., end_ts_us=...)"
    )


def timestamp_required_error() -> str:
    """Error message when timestamps are not provided to research functions."""
    return (
        "start_ts_us and end_ts_us are required.\n"
        "\n"
        "You can provide timestamps as:\n"
        "  1. Integer microseconds since epoch:\n"
        "     start_ts_us=1700000000000000\n"
        "  2. datetime objects (timezone-aware preferred):\n"
        "     from datetime import datetime, timezone\n"
        "     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc)\n"
        "     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc)"
    )


def exchange_not_found_error(exchange: str, available_exchanges: list[str]) -> str:
    """Error message when exchange is not found in EXCHANGE_MAP.

    Includes fuzzy matching suggestions for common typos.

    Args:
        exchange: The exchange name that was not found
        available_exchanges: List of valid exchange names

    Returns:
        Formatted error message with suggestions
    """
    normalized_available = [e.lower() for e in available_exchanges]
    suggestions = get_close_matches(exchange.lower(), normalized_available, n=3, cutoff=0.6)

    # Map back to original case
    suggestions_original_case = []
    for suggestion in suggestions:
        for orig in available_exchanges:
            if orig.lower() == suggestion:
                suggestions_original_case.append(orig)
                break

    msg = f"Exchange '{exchange}' not found in EXCHANGE_MAP.\n"

    if suggestions_original_case:
        msg += "\nDid you mean one of these?\n"
        for suggestion in suggestions_original_case:
            msg += f"  - {suggestion}\n"

    msg += "\nAvailable exchanges:\n"
    sorted_exchanges = sorted(available_exchanges)
    for exch in sorted_exchanges[:10]:
        msg += f"  - {exch}\n"

    if len(available_exchanges) > 10:
        msg += f"  ... and {len(available_exchanges) - 10} more\n"

    msg += "\nUse `from pointline.config import EXCHANGE_MAP` to see all exchanges."

    return msg


def symbol_not_found_error(symbol_id: int) -> str:
    """Error message when symbol_id is not found in dim_symbol registry.

    Args:
        symbol_id: The symbol ID that was not found

    Returns:
        Formatted error message with troubleshooting suggestions
    """
    return (
        f"Symbol ID {symbol_id} not found in dim_symbol registry.\n"
        "\n"
        "Possible causes:\n"
        "  1. Symbol ID is incorrect or from a different data lake\n"
        "  2. dim_symbol table needs to be populated\n"
        "  3. Symbol was deactivated or never activated\n"
        "\n"
        "To search for symbols, use:\n"
        "  from pointline import registry\n"
        "  symbols = registry.find_symbol('BTC', exchange='binance-futures')\n"
        "  print(symbols[['symbol_id', 'exchange_symbol', 'base_asset', 'quote_asset']])"
    )


def table_not_found_error(table_name: str, available_tables: list[str]) -> str:
    """Error message when table name is not found in TABLE_PATHS registry.

    Includes fuzzy matching suggestions for common typos.

    Args:
        table_name: The table name that was not found
        available_tables: List of valid table names

    Returns:
        Formatted error message with suggestions
    """
    suggestions = get_close_matches(table_name, available_tables, n=3, cutoff=0.6)

    msg = f"Table '{table_name}' not found in TABLE_PATHS registry.\n"

    if suggestions:
        msg += "\nDid you mean one of these?\n"
        for suggestion in suggestions:
            msg += f"  - {suggestion}\n"

    msg += "\nAvailable tables:\n"
    for table in sorted(available_tables):
        msg += f"  - {table}\n"

    msg += "\nUse `research.list_tables()` to see all registered tables."

    return msg


def invalid_timestamp_range_error(start_ts_us: int, end_ts_us: int) -> str:
    """Error message when timestamp range is invalid (end <= start).

    Args:
        start_ts_us: Start timestamp in microseconds
        end_ts_us: End timestamp in microseconds

    Returns:
        Formatted error message
    """
    return (
        f"Invalid timestamp range: end_ts_us ({end_ts_us}) must be greater than "
        f"start_ts_us ({start_ts_us}).\n"
        "\n"
        "Ensure your time range is correct:\n"
        "  start_ts_us should be the earlier timestamp\n"
        "  end_ts_us should be the later timestamp"
    )


__all__ = [
    "symbol_id_required_error",
    "timestamp_required_error",
    "exchange_not_found_error",
    "symbol_not_found_error",
    "table_not_found_error",
    "invalid_timestamp_range_error",
]
