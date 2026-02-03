"""Centralized parser registry for vendor-specific data parsing.

This module provides a decorator-based registry pattern for registering
and retrieving vendor-specific parsers. This enables orthogonal vendor/table
combinations without hardcoding dependencies.

Example:
    Register a parser:

    >>> @register_parser(vendor="tardis", data_type="trades")
    >>> def parse_tardis_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
    ...     return df.select([...])

    Retrieve and use a parser:

    >>> parser = get_parser("tardis", "trades")
    >>> parsed_df = parser(raw_df)

    List all supported combinations:

    >>> combos = list_supported_combinations()
    >>> print(combos)  # [("tardis", "trades"), ("tardis", "quotes"), ...]
"""

from collections.abc import Callable

import polars as pl

# Registry: (vendor, data_type) → parser function
_PARSER_REGISTRY: dict[tuple[str, str], Callable[[pl.DataFrame], pl.DataFrame]] = {}


def register_parser(vendor: str, data_type: str):
    """Decorator to register a parser function.

    Args:
        vendor: Vendor name (e.g., "tardis", "quant360", "binance")
        data_type: Data type (e.g., "trades", "quotes", "book_snapshots")

    Returns:
        Decorator function that registers the parser

    Raises:
        ValueError: If a parser is already registered for (vendor, data_type)

    Example:
        @register_parser(vendor="tardis", data_type="trades")
        def parse_tardis_trades_csv(df: pl.DataFrame) -> pl.DataFrame:
            return df.select([
                pl.col("timestamp").alias("ts_local_us"),
                pl.col("price").alias("price"),
                ...
            ])
    """

    def decorator(func: Callable[[pl.DataFrame], pl.DataFrame]):
        key = (vendor.lower(), data_type.lower())
        if key in _PARSER_REGISTRY:
            raise ValueError(
                f"Parser already registered for vendor={vendor}, data_type={data_type}. "
                f"Existing parser: {_PARSER_REGISTRY[key].__name__}"
            )
        _PARSER_REGISTRY[key] = func
        return func

    return decorator


def get_parser(vendor: str, data_type: str) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """Get parser function for vendor and data type.

    Args:
        vendor: Vendor name (case-insensitive)
        data_type: Data type (case-insensitive)

    Returns:
        Parser function that transforms raw DataFrame to normalized format

    Raises:
        KeyError: If no parser registered for (vendor, data_type)

    Example:
        >>> parser = get_parser("tardis", "trades")
        >>> parsed_df = parser(raw_df)
    """
    key = (vendor.lower(), data_type.lower())
    if key not in _PARSER_REGISTRY:
        available = list_supported_combinations()
        raise KeyError(
            f"No parser registered for vendor={vendor}, data_type={data_type}.\n"
            f"Available combinations: {available}"
        )
    return _PARSER_REGISTRY[key]


def list_supported_combinations() -> list[tuple[str, str]]:
    """List all supported (vendor, data_type) combinations.

    Returns:
        Sorted list of (vendor, data_type) tuples

    Example:
        >>> combos = list_supported_combinations()
        >>> for vendor, data_type in combos:
        ...     print(f"{vendor}: {data_type}")
        tardis: trades
        tardis: quotes
        quant360: l3_orders
        ...
    """
    return sorted(_PARSER_REGISTRY.keys())


def is_parser_registered(vendor: str, data_type: str) -> bool:
    """Check if a parser is registered for vendor and data type.

    Args:
        vendor: Vendor name (case-insensitive)
        data_type: Data type (case-insensitive)

    Returns:
        True if parser is registered, False otherwise

    Example:
        >>> is_parser_registered("tardis", "trades")
        True
        >>> is_parser_registered("unknown", "trades")
        False
    """
    key = (vendor.lower(), data_type.lower())
    return key in _PARSER_REGISTRY
