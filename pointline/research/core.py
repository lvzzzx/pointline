"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from pointline._error_messages import (
    invalid_timestamp_range_error,
    timestamp_required_error,
)
from pointline.config import TABLE_HAS_DATE, TABLE_PATHS, get_table_path
from pointline.tables.domain_registry import get_domain
from pointline.types import TableName, TimestampInput


def list_tables() -> list[str]:
    """Return registered table names."""
    return sorted(TABLE_PATHS.keys())


def table_path(table_name: str) -> Path:
    """Return the resolved filesystem path for a table."""
    return get_table_path(table_name)


def _normalize_timestamp(ts: TimestampInput | None, param_name: str) -> int | None:
    """Convert datetime or ISO string to microseconds since epoch, or pass through int.

    Accepts int (microseconds since epoch), datetime objects, or ISO 8601 date/datetime strings.
    Naive datetimes and date-only strings are interpreted as UTC with a warning.

    Args:
        ts: int (microseconds since epoch), datetime object, or ISO string (e.g., "2024-05-01")
        param_name: Parameter name for error messages

    Returns:
        Microseconds since epoch (int) or None if input is None

    Raises:
        TypeError: If ts is not int, datetime, str, or None
        ValueError: If ISO string cannot be parsed
    """
    if ts is None:
        return None

    if isinstance(ts, int):
        return ts

    # Parse ISO string to datetime
    if isinstance(ts, str):
        try:
            # Handle Z suffix (common UTC indicator not supported by fromisoformat in Python 3.10)
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            ts = datetime.fromisoformat(ts)
        except ValueError as e:
            raise ValueError(
                f"{param_name}: Invalid ISO datetime string. "
                f"Expected formats: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS', "
                f"'YYYY-MM-DDTHH:MM:SSZ', or 'YYYY-MM-DDTHH:MM:SS+00:00'. "
                f"Error: {e}"
            ) from e

    if isinstance(ts, datetime):
        # Warn if naive datetime (ambiguous, but accept as UTC)
        if ts.tzinfo is None:
            warnings.warn(
                f"{param_name}: naive datetime interpreted as UTC. "
                f"Use timezone-aware datetime for clarity: "
                f"datetime(..., tzinfo=timezone.utc) or ISO string with timezone '2024-05-01T00:00:00+00:00'",
                UserWarning,
                stacklevel=4,
            )
            ts = ts.replace(tzinfo=timezone.utc)

        # Convert to UTC then to microseconds
        return int(ts.timestamp() * 1_000_000)

    raise TypeError(
        f"{param_name} must be int (microseconds), datetime object, or ISO string, got {type(ts).__name__}"
    )


def scan_table(
    table_name: TableName,
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """Return a filtered LazyFrame for a Delta table.

    Requires exchange + time range for partition pruning. Symbol is optional
    (can query all symbols for an exchange).

    Args:
        table_name: Name of the table to scan (e.g., "trades", "quotes")
        exchange: Exchange name for partition pruning (e.g., "binance-futures").
            Required.
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)

    Returns:
        Filtered LazyFrame

    Raises:
        ValueError: If required parameters are missing or invalid
        TypeError: If timestamp types are invalid

    Examples:
        >>> from datetime import datetime, timezone
        >>> from pointline import research
        >>>
        >>> # Using int timestamps
        >>> lf = research.scan_table(
        ...     "trades",
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=1700000000000000,
        ...     end_ts_us=1700003600000000,
        ... )
        >>>
        >>> # Using datetime objects
        >>> lf = research.scan_table(
        ...     "trades",
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    # Convert timestamps early (supports both int and datetime)
    start_ts_us_int = _normalize_timestamp(start_ts_us, "start_ts_us")
    end_ts_us_int = _normalize_timestamp(end_ts_us, "end_ts_us")

    # Validation with enhanced error messages
    if exchange is None:
        raise ValueError("exchange is required for partition pruning.")
    if start_ts_us_int is None or end_ts_us_int is None:
        raise ValueError(timestamp_required_error())

    _validate_ts_range(start_ts_us_int, end_ts_us_int)
    _validate_ts_col(ts_col)

    start_date, end_date = _derive_date_bounds_from_ts(start_ts_us_int, end_ts_us_int)
    date_filter_is_implicit = True

    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        table_name=table_name,
        exchange=exchange,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        start_ts_us=start_ts_us_int,
        end_ts_us=end_ts_us_int,
        ts_col=ts_col,
        date_filter_is_implicit=date_filter_is_implicit,
    )
    if columns:
        lf = lf.select(list(columns))
    return lf


def read_table(
    table_name: TableName,
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table.

    This is an eager version of scan_table() that immediately collects results.
    Requires exchange + time range.

    Args:
        table_name: Name of the table to read
        exchange: Exchange name for partition pruning (e.g., "binance-futures").
            Required.
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)

    Returns:
        Filtered DataFrame (eager evaluation)

    Examples:
        >>> from datetime import datetime, timezone
        >>> df = research.read_table(
        ...     "trades",
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    return scan_table(
        table_name,
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    ).collect()


def load_trades(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied.

    Requires exchange + time range.

    Args:
        exchange: Exchange name for partition pruning (e.g., "binance-futures")
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        lazy: If True, return LazyFrame; if False, return DataFrame

    Returns:
        Filtered trades data (DataFrame or LazyFrame based on lazy parameter)

    Examples:
        >>> from datetime import datetime, timezone
        >>> trades = research.load_trades(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "trades",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_quotes(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied.

    Requires exchange + time range.

    Args:
        exchange: Exchange name for partition pruning (e.g., "binance-futures")
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        lazy: If True, return LazyFrame; if False, return DataFrame

    Returns:
        Filtered quotes data (DataFrame or LazyFrame based on lazy parameter)

    Examples:
        >>> from datetime import datetime, timezone
        >>> quotes = research.load_quotes(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "quotes",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_book_snapshot_25(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied.

    Requires exchange + time range.

    Args:
        exchange: Exchange name for partition pruning (e.g., "binance-futures")
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        lazy: If True, return LazyFrame; if False, return DataFrame

    Returns:
        Filtered book snapshot data (DataFrame or LazyFrame based on lazy parameter)

    Examples:
        >>> from datetime import datetime, timezone
        >>> book = research.load_book_snapshot_25(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "book_snapshot_25",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_derivative_ticker(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load derivative ticker data with common filters applied.

    Requires exchange + time range.

    Derivative ticker data includes funding rates, open interest, mark price,
    index price, and last price for perpetual futures and other derivatives.

    Args:
        exchange: Exchange name for partition pruning (e.g., "binance-futures")
        symbol: Symbol name(s) to filter (e.g., "BTCUSDT"). Optional.
        start_ts_us: Start timestamp (microseconds since epoch or datetime object)
        end_ts_us: End timestamp (microseconds since epoch or datetime object)
        ts_col: Timestamp column to filter on (default: "ts_local_us")
        columns: List of columns to select (default: all)
        lazy: If True, return LazyFrame; if False, return DataFrame

    Returns:
        Filtered derivative ticker data (DataFrame or LazyFrame based on lazy parameter)

    Examples:
        >>> from datetime import datetime, timezone
        >>> ticker = research.load_derivative_ticker(
        ...     exchange="binance-futures",
        ...     symbol="BTCUSDT",
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "derivative_ticker",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_trades_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades and decode fixed-point integers into float price/qty columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    return _load_decoded_table(
        table_name="trades",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=keep_ints,
        lazy=lazy,
    )


def load_quotes_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes and decode fixed-point integers into float bid/ask columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    return _load_decoded_table(
        table_name="quotes",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=keep_ints,
        lazy=lazy,
    )


def load_book_snapshot_25_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots and decode fixed-point list columns into floats.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    return _load_decoded_table(
        table_name="book_snapshot_25",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=False,
        lazy=lazy,
    )


def load_derivative_ticker_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load derivative ticker and decode fixed-point integers into float columns."""
    return _load_decoded_table(
        table_name="derivative_ticker",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=keep_ints,
        lazy=lazy,
    )


def load_kline_1h_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_bucket_start_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load 1h klines and decode fixed-point integers into float OHLC/volume columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    return _load_decoded_table(
        table_name="kline_1h",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=keep_ints,
        lazy=lazy,
    )


def load_kline_1d_decoded(
    *,
    exchange: str | None = None,
    symbol: str | Iterable[str] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_bucket_start_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load 1d klines and decode fixed-point integers into float OHLC/volume columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    return _load_decoded_table(
        table_name="kline_1d",
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        keep_ints=keep_ints,
        lazy=lazy,
    )


def _apply_filters(
    lf: pl.LazyFrame,
    *,
    table_name: str | None,
    exchange: str | None,
    symbol: str | Iterable[str] | None,
    start_date: date | None,
    end_date: date | None,
    start_ts_us: int | None,
    end_ts_us: int | None,
    ts_col: str,
    date_filter_is_implicit: bool = False,
) -> pl.LazyFrame:
    # 1. Apply exchange filter (partition pruning)
    if exchange is not None:
        lf = lf.filter(pl.col("exchange") == exchange)

    # 2. Apply symbol filter
    if symbol is not None:
        symbols = [symbol] if isinstance(symbol, str) else list(symbol)
        lf = lf.filter(pl.col("symbol").is_in(symbols))

    # 3. Apply Date Filters
    if start_date is not None or end_date is not None:
        table_has_date = TABLE_HAS_DATE.get(table_name) if table_name is not None else None
        if table_has_date is False and not date_filter_is_implicit:
            raise ValueError(
                "scan_table: start_date/end_date provided but table has no 'date' column"
            )
        if table_has_date is None and "date" not in lf.schema:
            if not date_filter_is_implicit:
                raise ValueError(
                    "scan_table: start_date/end_date provided but table has no 'date' column"
                )
        if table_has_date is not False and (table_has_date is not None or "date" in lf.schema):
            start = start_date
            end = end_date
            if start and end:
                lf = lf.filter((pl.col("date") >= start) & (pl.col("date") <= end))
            elif start:
                lf = lf.filter(pl.col("date") >= start)
            elif end:
                lf = lf.filter(pl.col("date") <= end)

    # 4. Apply Time Filters
    if start_ts_us is not None or end_ts_us is not None:
        if start_ts_us is not None and end_ts_us is not None:
            lf = lf.filter((pl.col(ts_col) >= start_ts_us) & (pl.col(ts_col) < end_ts_us))
        elif start_ts_us is not None:
            lf = lf.filter(pl.col(ts_col) >= start_ts_us)
        else:
            lf = lf.filter(pl.col(ts_col) < end_ts_us)

    return lf


def _merge_decode_columns(
    columns: Sequence[str] | None, required: Sequence[str]
) -> Sequence[str] | None:
    if columns is None:
        return None
    merged = list(columns)
    for col in required:
        if col not in merged:
            merged.append(col)
    return merged


def _load_decoded_table(
    *,
    table_name: TableName,
    exchange: str | None,
    symbol: str | Iterable[str] | None,
    start_ts_us: TimestampInput | None,
    end_ts_us: TimestampInput | None,
    ts_col: str,
    columns: Sequence[str] | None,
    keep_ints: bool,
    lazy: bool,
) -> pl.DataFrame | pl.LazyFrame:
    domain = get_domain(table_name)
    requested_columns = list(columns) if columns is not None else None
    decode_required = list(domain.required_decode_columns())
    int_like_cols = [col for col in decode_required if col.endswith("_int")]
    effective_keep_ints = keep_ints or (
        requested_columns is not None and any(col in int_like_cols for col in requested_columns)
    )
    scan_columns = _merge_decode_columns(columns, decode_required)

    if lazy:
        lf = scan_table(
            table_name,
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=scan_columns,
        )
        decoded = domain.decode_storage_lazy(lf, keep_ints=effective_keep_ints)
        return decoded.select(requested_columns) if requested_columns is not None else decoded

    df = read_table(
        table_name,
        exchange=exchange,
        symbol=symbol,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=scan_columns,
    )
    decoded = domain.decode_storage(df, keep_ints=effective_keep_ints)
    return decoded.select(requested_columns) if requested_columns is not None else decoded


def _derive_date_bounds_from_ts(
    start_ts_us: int | None, end_ts_us: int | None
) -> tuple[date | None, date | None]:
    start_date = _ts_us_to_date(start_ts_us) if start_ts_us is not None else None
    end_date = None if end_ts_us is None else _ts_us_to_date(max(end_ts_us - 1, 0))
    return start_date, end_date


def _ts_us_to_date(value: int) -> date:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).date()


def _validate_ts_col(ts_col: str) -> None:
    if ts_col not in {"ts_local_us", "ts_exch_us"}:
        raise ValueError("scan_table: ts_col must be 'ts_local_us' or 'ts_exch_us'")


def _validate_ts_range(start_ts_us: int | None, end_ts_us: int | None) -> None:
    """Validate that end_ts_us is greater than start_ts_us."""
    if start_ts_us is not None and end_ts_us is not None and end_ts_us <= start_ts_us:
        raise ValueError(invalid_timestamp_range_error(start_ts_us, end_ts_us))
