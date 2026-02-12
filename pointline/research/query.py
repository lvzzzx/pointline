"""High-level convenience API for research queries.

This module provides a simplified API for loading market data by exchange and
symbol name. Timestamps can be passed as datetime objects, ISO strings, or
integer microseconds -- the module normalises them before delegating to the
core layer.

When to use this module:
- Prototyping and exploration
- Jupyter notebooks
- Quick data checks
- LLM agents

When to use core API instead:
- Fine-grained control over scan/load behaviour
- Performance-critical queries where you want to manage LazyFrames directly
"""

from __future__ import annotations

import polars as pl

from pointline.research import core
from pointline.types import TimestampInput


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
    """Load trades for an exchange and symbol.

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
        ValueError: If no data found or invalid time range

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
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    if decoded:
        return core.load_trades_decoded(
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    return core.load_trades(
        exchange=exchange,
        symbol=symbol,
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
    """Load quotes for an exchange and symbol.

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

    if decoded:
        return core.load_quotes_decoded(
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    return core.load_quotes(
        exchange=exchange,
        symbol=symbol,
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
    """Load book snapshots for an exchange and symbol.

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

    if decoded:
        return core.load_book_snapshot_25_decoded(
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            lazy=lazy,
        )

    return core.load_book_snapshot_25(
        exchange=exchange,
        symbol=symbol,
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
    """Load derivative ticker data for an exchange and symbol.

    Derivative ticker data includes funding rates, open interest, mark price,
    index price, and last price for perpetual futures and other derivatives.

    Args:
        exchange: Exchange name (e.g., "binance-futures", "deribit")
        symbol: Exchange symbol (e.g., "SOLUSDT", "BTC-PERPETUAL")
        start: Start time (datetime, ISO string, or int microseconds)
        end: End time (datetime, ISO string, or int microseconds)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        decoded: Decode fixed-point integer columns into floats (default: False)
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
        ...     decoded=True,
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
    start_ts_us = core._normalize_timestamp(start, "start")
    end_ts_us = core._normalize_timestamp(end, "end")

    if decoded:
        return core.load_derivative_ticker_decoded(
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            lazy=lazy,
        )

    lf = core.scan_table(
        "derivative_ticker",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )

    return lf if lazy else lf.collect()


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
    """Load daily klines for an exchange and symbol.

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

    if decoded:
        return core.load_kline_1d_decoded(
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=columns,
            keep_ints=keep_ints,
            lazy=lazy,
        )

    lf = core.scan_table(
        "kline_1d",
        exchange=exchange,
        symbol=symbol,
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
