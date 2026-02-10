"""High-level convenience API for research queries with automatic symbol resolution.

This module provides a simplified API for quick exploration and prototyping.
For production research, use the explicit API in pointline.research.core.

Key differences from core API:
- Accepts exchange_symbol instead of symbol_id (automatic resolution)
- Handles SCD Type 2 filtering automatically
- Warns if symbol metadata changed during query period
- More flexible datetime parsing

When to use this module:
- Prototyping and exploration
- Jupyter notebooks
- Quick data checks
- LLM agents

When to use core API instead:
- Production research code
- When you need to inspect/validate symbol_ids
- Performance-critical queries
- Reproducible research requiring explicit symbol_ids
"""

from __future__ import annotations

import warnings

import polars as pl

from pointline import registry
from pointline.research import core
from pointline.types import TimestampInput


def _resolve_symbols_with_warning(
    exchange: str,
    symbol: str,
    start_ts_us: int,
    end_ts_us: int,
) -> list[int]:
    """Resolve symbol_ids for a time range and warn if metadata changed.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "SOLUSDT")
        start_ts_us: Start timestamp in microseconds
        end_ts_us: End timestamp in microseconds

    Returns:
        List of symbol_ids valid during the time range

    Raises:
        ValueError: If no symbols found for the time range
    """
    # Find all symbols matching the exchange_symbol
    symbols_df = registry.find_symbol(symbol, exchange=exchange)

    if symbols_df.height == 0:
        raise ValueError(
            f"No symbols found for exchange='{exchange}', symbol='{symbol}'.\n"
            "\n"
            "Possible causes:\n"
            "  1. Exchange name is incorrect (check available exchanges)\n"
            "  2. Symbol not yet loaded into dim_symbol\n"
            "  3. Symbol name typo\n"
            "\n"
            "Use registry.find_symbol() to search available symbols."
        )

    # Filter by SCD Type 2 validity window
    active_symbols = symbols_df.filter(
        (pl.col("valid_from_ts") < end_ts_us) & (pl.col("valid_until_ts") > start_ts_us)
    )

    symbol_ids = active_symbols["symbol_id"].to_list()

    if not symbol_ids:
        raise ValueError(
            f"No active symbols found for {exchange}:{symbol} "
            f"in the specified time range.\n"
            "\n"
            f"Found {symbols_df.height} total symbol(s), but none are valid "
            f"during the query period.\n"
            "\n"
            "This may indicate:\n"
            "  1. Symbol was not active during this time range\n"
            "  2. Time range is outside symbol's validity window\n"
            "\n"
            "Check symbol validity:\n"
            f"  symbols = registry.find_symbol('{symbol}', exchange='{exchange}')\n"
            "  print(symbols[['symbol_id', 'valid_from_ts', 'valid_until_ts']])"
        )

    # Warn if metadata changed (multiple symbol_ids)
    if len(symbol_ids) > 1:
        # Get details for warning message
        details = active_symbols.select(
            ["symbol_id", "valid_from_ts", "valid_until_ts", "price_increment", "tick_size"]
        ).sort("valid_from_ts")

        warning_msg = (
            f"Symbol metadata changed during query period:\n"
            f"  Exchange: {exchange}\n"
            f"  Symbol: {symbol}\n"
            f"  Symbol IDs: {symbol_ids}\n"
            f"\n"
            f"Found {len(symbol_ids)} versions:\n"
        )

        for row in details.iter_rows(named=True):
            warning_msg += (
                f"  - symbol_id={row['symbol_id']}: "
                f"valid_from_ts={row['valid_from_ts']}, "
                f"tick_size={row.get('tick_size', 'N/A')}\n"
            )

        warning_msg += (
            f"\n"
            f"This may affect your analysis if:\n"
            f"  - Tick size or lot size changed (affects price precision)\n"
            f"  - Contract parameters changed (affects calculations)\n"
            f"\n"
            f"For production use, consider using the explicit API:\n"
            f"  from pointline import research, registry\n"
            f"  symbol_ids = {symbol_ids}\n"
            f"  data = research.load_trades(symbol_id=symbol_ids, ...)"
        )

        warnings.warn(warning_msg, UserWarning, stacklevel=3)

    return symbol_ids


def trades(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    keep_ints: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load trades with automatic symbol resolution.

    This is a convenience function for exploration. For production use,
    prefer research.load_trades() with explicit symbol_id.

    Args:
        exchange: Exchange name (e.g., "binance-futures", "deribit")
        symbol: Exchange symbol (e.g., "SOLUSDT", "BTC-PERPETUAL")
        start: Start time (datetime, ISO string, or int microseconds)
        end: End time (datetime, ISO string, or int microseconds)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        decoded: Decode fixed-point integer columns into floats (default: False)
        keep_ints: Keep fixed-point integer columns when decoded=True (default: False)
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Trades data (LazyFrame or DataFrame)

    Raises:
        ValueError: If no symbols found or invalid time range

    Examples:
        >>> from pointline.research import query
        >>> from datetime import datetime, timezone
        >>>
        >>> # Quick exploration with datetime
        >>> trades = query.trades(
        ...     exchange="binance-futures",
        ...     symbol="SOLUSDT",
        ...     start=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ...     end=datetime(2024, 5, 2, tzinfo=timezone.utc),
        ... )
        >>>
        >>> # Or with ISO strings
        >>> trades = query.trades(
        ...     "binance-futures",
        ...     "SOLUSDT",
        ...     "2024-05-01",
        ...     "2024-05-02",
        ...     lazy=False,
        ... )
    """
    # Normalize timestamps
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    # Auto-resolve symbol_ids
    symbol_ids = _resolve_symbols_with_warning(exchange, symbol, start_ts_us, end_ts_us)

    # Delegate to low-level API
    if decoded:
        return core.load_trades_decoded(
            symbol_id=symbol_ids,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    return core.load_trades(
        symbol_id=symbol_ids,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        lazy=lazy,
    )


def quotes(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    keep_ints: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load quotes with automatic symbol resolution.

    This is a convenience function for exploration. For production use,
    prefer research.load_quotes() with explicit symbol_id.

    Args:
        exchange: Exchange name
        symbol: Exchange symbol
        start: Start time
        end: End time
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        decoded: Decode fixed-point integer columns into floats (default: False)
        keep_ints: Keep fixed-point integer columns when decoded=True (default: False)
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Quotes data (LazyFrame or DataFrame)

    Examples:
        >>> quotes = query.quotes(
        ...     "binance-futures",
        ...     "SOLUSDT",
        ...     "2024-05-01",
        ...     "2024-05-02",
        ... )
    """
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    symbol_ids = _resolve_symbols_with_warning(exchange, symbol, start_ts_us, end_ts_us)

    if decoded:
        return core.load_quotes_decoded(
            symbol_id=symbol_ids,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    return core.load_quotes(
        symbol_id=symbol_ids,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        lazy=lazy,
    )


def book_snapshot_25(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load book snapshots with automatic symbol resolution.

    This is a convenience function for exploration. For production use,
    prefer research.load_book_snapshot_25() with explicit symbol_id.

    Args:
        exchange: Exchange name
        symbol: Exchange symbol
        start: Start time
        end: End time
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        decoded: Decode fixed-point list columns into floats (default: False)
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Book snapshot data (LazyFrame or DataFrame)

    Examples:
        >>> # Large date range - use lazy
        >>> book = query.book_snapshot_25(
        ...     "binance-futures",
        ...     "SOLUSDT",
        ...     "2024-05-01",
        ...     "2024-09-30",
        ...     decoded=True,
        ...     lazy=True,
        ... )
        >>>
        >>> # Feature engineering before collecting
        >>> features = book.select([
        ...     "ts_local_us",
        ...     pl.col("bids_px").list.first().alias("best_bid_px"),
        ...     pl.col("asks_px").list.first().alias("best_ask_px"),
        ... ])
        >>> df = features.collect()
    """
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    symbol_ids = _resolve_symbols_with_warning(exchange, symbol, start_ts_us, end_ts_us)

    if decoded:
        return core.load_book_snapshot_25_decoded(
            symbol_id=symbol_ids,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            lazy=lazy,
        )

    return core.load_book_snapshot_25(
        symbol_id=symbol_ids,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        lazy=lazy,
    )


def derivative_ticker(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_local_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load derivative ticker data with automatic symbol resolution.

    This is a convenience function for exploration. For production use,
    prefer research.load_derivative_ticker() with explicit symbol_id.

    Derivative ticker data includes funding rates, open interest, mark price,
    index price, and last price for perpetual futures and other derivatives.

    Args:
        exchange: Exchange name (e.g., "binance-futures", "deribit")
        symbol: Exchange symbol (e.g., "SOLUSDT", "BTC-PERPETUAL")
        start: Start time (datetime, ISO string, or int microseconds)
        end: End time (datetime, ISO string, or int microseconds)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        decoded: Ignored for derivative_ticker (data is already float, default: False)
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Derivative ticker data (LazyFrame or DataFrame)

    Examples:
        >>> from pointline.research import query
        >>> from datetime import datetime, timezone
        >>>
        >>> # Quick exploration with datetime
        >>> ticker = query.derivative_ticker(
        ...     exchange="binance-futures",
        ...     symbol="SOLUSDT",
        ...     start=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ...     end=datetime(2024, 5, 2, tzinfo=timezone.utc),
        ... )
        >>>
        >>> # Or with ISO strings
        >>> ticker = query.derivative_ticker(
        ...     "binance-futures",
        ...     "SOLUSDT",
        ...     "2024-05-01",
        ...     "2024-05-02",
        ...     lazy=False,
        ... )
    """
    # Note: `decoded` is accepted for API consistency with trades/quotes/book_snapshot_25,
    # but derivative_ticker data is stored as Float64 (not fixed-point), so no decoding is needed.
    del decoded  # Unused - derivative_ticker is already float

    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    symbol_ids = _resolve_symbols_with_warning(exchange, symbol, start_ts_us, end_ts_us)

    return core.load_derivative_ticker(
        symbol_id=symbol_ids,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        lazy=lazy,
    )


def kline_1d(
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    *,
    ts_col: str = "ts_bucket_start_us",
    columns: list[str] | tuple[str, ...] | None = None,
    decoded: bool = False,
    keep_ints: bool = False,
    lazy: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """Load daily klines with automatic symbol resolution.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "BTCUSDT")
        start: Start time (datetime, ISO string, or int microseconds)
        end: End time (datetime, ISO string, or int microseconds)
        ts_col: Timestamp column to filter on (default: "ts_bucket_start_us")
        columns: List of columns to select (default: all)
        decoded: Decode fixed-point integer columns into floats (default: False)
        keep_ints: Keep fixed-point integer columns when decoded=True (default: False)
        lazy: Return LazyFrame (True) or DataFrame (False)

    Returns:
        Daily kline data (LazyFrame or DataFrame)

    Examples:
        >>> klines = query.kline_1d(
        ...     "binance-futures",
        ...     "BTCUSDT",
        ...     "2024-01-01",
        ...     "2024-12-31",
        ...     decoded=True,
        ...     lazy=False,
        ... )
    """
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    symbol_ids = _resolve_symbols_with_warning(exchange, symbol, start_ts_us, end_ts_us)

    if decoded:
        return core.load_kline_1d_decoded(
            symbol_id=symbol_ids,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    lf = core.scan_table(
        "kline_1d",
        symbol_id=symbol_ids,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


__all__ = [
    "trades",
    "quotes",
    "book_snapshot_25",
    "derivative_ticker",
    "kline_1d",
]
