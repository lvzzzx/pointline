"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from pointline._error_messages import (
    invalid_timestamp_range_error,
    symbol_id_required_error,
    timestamp_required_error,
)
from pointline.config import TABLE_HAS_DATE, TABLE_PATHS, get_table_path
from pointline.dim_symbol import read_dim_symbol_table
from pointline.registry import resolve_symbols
from pointline.tables.book_snapshots import decode_fixed_point as decode_book_snapshots
from pointline.tables.klines import decode_fixed_point as decode_klines
from pointline.tables.quotes import _max_decimal_places as _max_quote_decimals
from pointline.tables.quotes import decode_fixed_point as decode_quotes
from pointline.tables.trades import decode_fixed_point as decode_trades
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
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """Return a filtered LazyFrame for a Delta table.

    Requires symbol_id + time range. Exchange partitions are derived from symbol_id
    for pruning. Timestamps are applied to the selected time column and implicitly
    prune date partitions when available.

    Args:
        table_name: Name of the table to scan (e.g., "trades", "quotes")
        symbol_id: Symbol ID(s) to filter. Required for partition pruning.
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
        >>> # Using int timestamps (existing API)
        >>> lf = research.scan_table(
        ...     "trades",
        ...     symbol_id=101,
        ...     start_ts_us=1700000000000000,
        ...     end_ts_us=1700003600000000,
        ... )
        >>>
        >>> # Using datetime objects (new convenience)
        >>> lf = research.scan_table(
        ...     "trades",
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    # Convert timestamps early (supports both int and datetime)
    start_ts_us_int = _normalize_timestamp(start_ts_us, "start_ts_us")
    end_ts_us_int = _normalize_timestamp(end_ts_us, "end_ts_us")

    # Validation with enhanced error messages
    if symbol_id is None:
        raise ValueError(symbol_id_required_error())
    if start_ts_us_int is None or end_ts_us_int is None:
        raise ValueError(timestamp_required_error())

    _validate_ts_range(start_ts_us_int, end_ts_us_int)
    _validate_ts_col(ts_col)

    resolved_symbol_ids = symbol_id

    start_date, end_date = _derive_date_bounds_from_ts(start_ts_us_int, end_ts_us_int)
    date_filter_is_implicit = True

    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        table_name=table_name,
        symbol_id=resolved_symbol_ids,
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
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table.

    This is an eager version of scan_table() that immediately collects results.
    Requires symbol_id + time range.

    Args:
        table_name: Name of the table to read
        symbol_id: Symbol ID(s) to filter
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
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    return scan_table(
        table_name,
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    ).collect()


def load_trades(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied.

    Requires symbol_id + time range.

    Args:
        symbol_id: Symbol ID(s) to filter
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
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "trades",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_quotes(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied.

    Requires symbol_id + time range.

    Args:
        symbol_id: Symbol ID(s) to filter
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
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "quotes",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_book_snapshot_25(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied.

    Requires symbol_id + time range.

    Args:
        symbol_id: Symbol ID(s) to filter
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
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_derivative_ticker(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load derivative ticker data with common filters applied.

    Requires symbol_id + time range.

    Derivative ticker data includes funding rates, open interest, mark price,
    index price, and last price for perpetual futures and other derivatives.

    Args:
        symbol_id: Symbol ID(s) to filter
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
        ...     symbol_id=101,
        ...     start_ts_us=datetime(2023, 11, 14, 12, 0, tzinfo=timezone.utc),
        ...     end_ts_us=datetime(2023, 11, 14, 13, 0, tzinfo=timezone.utc),
        ... )
    """
    lf = scan_table(
        "derivative_ticker",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_trades_decoded(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    dim_symbol: pl.DataFrame | pl.LazyFrame | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades and decode fixed-point integers into float price/qty columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    required_ints = ["price_int", "qty_int"]
    requested_columns = list(columns) if columns is not None else None
    effective_keep_ints = keep_ints or (
        requested_columns is not None and any(col in required_ints for col in requested_columns)
    )

    if lazy:
        lf = scan_table(
            "trades",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
        )
        dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
        decoded = _decode_trades_lazy(lf, dim_symbol_df.lazy(), keep_ints=effective_keep_ints)
        return decoded.select(requested_columns) if requested_columns is not None else decoded

    df = read_table(
        "trades",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
    )
    dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
    decoded = decode_trades(df, dim_symbol_df, keep_ints=effective_keep_ints)
    return decoded.select(requested_columns) if requested_columns is not None else decoded


def load_quotes_decoded(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    dim_symbol: pl.DataFrame | pl.LazyFrame | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes and decode fixed-point integers into float bid/ask columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    required_ints = ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    requested_columns = list(columns) if columns is not None else None
    effective_keep_ints = keep_ints or (
        requested_columns is not None and any(col in required_ints for col in requested_columns)
    )

    if lazy:
        lf = scan_table(
            "quotes",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
        )
        dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
        decoded = _decode_quotes_lazy(
            lf, dim_symbol_df.lazy(), dim_symbol_df, keep_ints=effective_keep_ints
        )
        return decoded.select(requested_columns) if requested_columns is not None else decoded

    df = read_table(
        "quotes",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
    )
    dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
    decoded = decode_quotes(df, dim_symbol_df, keep_ints=effective_keep_ints)
    return decoded.select(requested_columns) if requested_columns is not None else decoded


def load_book_snapshot_25_decoded(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    dim_symbol: pl.DataFrame | pl.LazyFrame | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots and decode fixed-point list columns into floats.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    required_lists = ["bids_px", "bids_sz", "asks_px", "asks_sz"]
    requested_columns = list(columns) if columns is not None else None

    if lazy:
        lf = scan_table(
            "book_snapshot_25",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=_merge_decode_columns(columns, ["symbol_id", *required_lists]),
        )
        dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
        decoded = _decode_book_snapshot_25_lazy(lf, dim_symbol_df.lazy())
        return decoded.select(requested_columns) if requested_columns is not None else decoded

    df = read_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=_merge_decode_columns(columns, ["symbol_id", *required_lists]),
    )
    dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
    decoded = decode_book_snapshots(df, dim_symbol_df)
    return decoded.select(requested_columns) if requested_columns is not None else decoded


def load_kline_1h_decoded(
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: TimestampInput | None = None,
    end_ts_us: TimestampInput | None = None,
    ts_col: str = "ts_bucket_start_us",
    columns: Sequence[str] | None = None,
    keep_ints: bool = False,
    dim_symbol: pl.DataFrame | pl.LazyFrame | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load 1h klines and decode fixed-point integers into float OHLC/volume columns.

    If lazy=True, returns a LazyFrame with decode expressions applied.
    """
    required_ints = [
        "open_px_int",
        "high_px_int",
        "low_px_int",
        "close_px_int",
        "volume_qty_int",
        "quote_volume_int",
        "taker_buy_base_qty_int",
        "taker_buy_quote_qty_int",
    ]
    requested_columns = list(columns) if columns is not None else None
    effective_keep_ints = keep_ints or (
        requested_columns is not None and any(col in required_ints for col in requested_columns)
    )

    if lazy:
        lf = scan_table(
            "kline_1h",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            ts_col=ts_col,
            columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
        )
        dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
        decoded = _decode_klines_lazy(lf, dim_symbol_df.lazy(), keep_ints=effective_keep_ints)
        return decoded.select(requested_columns) if requested_columns is not None else decoded

    df = read_table(
        "kline_1h",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=_merge_decode_columns(columns, ["symbol_id", *required_ints]),
    )
    dim_symbol_df = _ensure_dim_symbol_increments(dim_symbol)
    decoded = decode_klines(df, dim_symbol_df, keep_ints=effective_keep_ints)
    return decoded.select(requested_columns) if requested_columns is not None else decoded


def _apply_filters(
    lf: pl.LazyFrame,
    *,
    table_name: str | None,
    symbol_id: int | Iterable[int] | None,
    start_date: date | None,
    end_date: date | None,
    start_ts_us: int | None,
    end_ts_us: int | None,
    ts_col: str,
    date_filter_is_implicit: bool = False,
) -> pl.LazyFrame:
    # 1. Resolve Exchange from Symbol ID (for partition pruning)
    exchanges_to_filter: list[str] = []

    if symbol_id is not None:
        ids = [symbol_id] if isinstance(symbol_id, int) else list(symbol_id)
        resolved = resolve_symbols(ids)
        if not resolved:
            raise ValueError("scan_table: symbol_id values not found in dim_symbol registry")
        exchanges_to_filter.extend(resolved)

        # Apply symbol_id filter
        lf = lf.filter(pl.col("symbol_id").is_in(ids))

    # 2. Apply unique exchange filters
    if exchanges_to_filter:
        unique_exchanges = sorted(set(exchanges_to_filter))
        lf = lf.filter(pl.col("exchange").is_in(unique_exchanges))

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


def _ensure_dim_symbol_increments(
    dim_symbol: pl.DataFrame | pl.LazyFrame | None,
) -> pl.DataFrame:
    if dim_symbol is None:
        return _read_dim_symbol_increments()
    if isinstance(dim_symbol, pl.LazyFrame):
        return dim_symbol.collect()
    return dim_symbol


def _read_dim_symbol_increments() -> pl.DataFrame:
    return read_dim_symbol_table(
        columns=["symbol_id", "price_increment", "amount_increment"],
        unique_by=["symbol_id"],
    )


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


def _decode_trades_lazy(
    lf: pl.LazyFrame,
    dim_symbol_lf: pl.LazyFrame,
    *,
    keep_ints: bool,
) -> pl.LazyFrame:
    joined = lf.join(
        dim_symbol_lf.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    result = joined.with_columns(
        [
            (pl.col("price_int") * pl.col("price_increment")).cast(pl.Float64).alias("price"),
            (pl.col("qty_int") * pl.col("amount_increment")).cast(pl.Float64).alias("qty"),
        ]
    )
    drop_cols = ["price_increment", "amount_increment"]
    if not keep_ints:
        drop_cols += ["price_int", "qty_int"]
    return result.drop(drop_cols)


def _decode_quotes_lazy(
    lf: pl.LazyFrame,
    dim_symbol_lf: pl.LazyFrame,
    dim_symbol_df: pl.DataFrame,
    *,
    keep_ints: bool,
) -> pl.LazyFrame:
    price_decimals = _max_quote_decimals(dim_symbol_df["price_increment"].to_list())
    amount_decimals = _max_quote_decimals(dim_symbol_df["amount_increment"].to_list())
    joined = lf.join(
        dim_symbol_lf.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    result = joined.with_columns(
        [
            (pl.col("bid_px_int") * pl.col("price_increment"))
            .round(price_decimals)
            .cast(pl.Float64)
            .alias("bid_price"),
            (pl.col("bid_sz_int") * pl.col("amount_increment"))
            .round(amount_decimals)
            .cast(pl.Float64)
            .alias("bid_amount"),
            (pl.col("ask_px_int") * pl.col("price_increment"))
            .round(price_decimals)
            .cast(pl.Float64)
            .alias("ask_price"),
            (pl.col("ask_sz_int") * pl.col("amount_increment"))
            .round(amount_decimals)
            .cast(pl.Float64)
            .alias("ask_amount"),
        ]
    )
    drop_cols = ["price_increment", "amount_increment"]
    if not keep_ints:
        drop_cols += ["bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"]
    return result.drop(drop_cols)


def _decode_book_snapshot_25_lazy(
    lf: pl.LazyFrame,
    dim_symbol_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    joined = lf.join(
        dim_symbol_lf.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    result = joined.with_columns(
        [
            (pl.col("bids_px") * pl.col("price_increment")).alias("bids_px"),
            (pl.col("bids_sz") * pl.col("amount_increment")).alias("bids_sz"),
            (pl.col("asks_px") * pl.col("price_increment")).alias("asks_px"),
            (pl.col("asks_sz") * pl.col("amount_increment")).alias("asks_sz"),
        ]
    )
    return result.drop(["price_increment", "amount_increment"])


def _decode_klines_lazy(
    lf: pl.LazyFrame,
    dim_symbol_lf: pl.LazyFrame,
    *,
    keep_ints: bool,
) -> pl.LazyFrame:
    joined = lf.join(
        dim_symbol_lf.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )
    result = joined.with_columns(
        (pl.col("price_increment") * pl.col("amount_increment")).alias("quote_increment")
    )
    result = result.with_columns(
        [
            (pl.col("open_px_int") * pl.col("price_increment")).cast(pl.Float64).alias("open"),
            (pl.col("high_px_int") * pl.col("price_increment")).cast(pl.Float64).alias("high"),
            (pl.col("low_px_int") * pl.col("price_increment")).cast(pl.Float64).alias("low"),
            (pl.col("close_px_int") * pl.col("price_increment")).cast(pl.Float64).alias("close"),
            (pl.col("volume_qty_int") * pl.col("amount_increment"))
            .cast(pl.Float64)
            .alias("volume"),
            (pl.col("quote_volume_int") * pl.col("quote_increment"))
            .cast(pl.Float64)
            .alias("quote_volume"),
            (pl.col("taker_buy_base_qty_int") * pl.col("amount_increment"))
            .cast(pl.Float64)
            .alias("taker_buy_base_volume"),
            (pl.col("taker_buy_quote_qty_int") * pl.col("quote_increment"))
            .cast(pl.Float64)
            .alias("taker_buy_quote_volume"),
        ]
    )
    drop_cols = ["price_increment", "amount_increment", "quote_increment"]
    if not keep_ints:
        drop_cols += [
            "open_px_int",
            "high_px_int",
            "low_px_int",
            "close_px_int",
            "volume_qty_int",
            "quote_volume_int",
            "taker_buy_base_qty_int",
            "taker_buy_quote_qty_int",
        ]
    return result.drop(drop_cols)


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
