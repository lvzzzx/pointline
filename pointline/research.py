"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence
import warnings

import polars as pl

from pointline.config import TABLE_HAS_DATE, TABLE_PATHS, get_table_path, normalize_exchange
from pointline.dim_symbol import DEFAULT_VALID_UNTIL_TS_US, read_dim_symbol_table
from pointline.registry import resolve_symbols


def list_tables() -> list[str]:
    """Return registered table names."""
    return sorted(TABLE_PATHS.keys())


def table_path(table_name: str) -> Path:
    """Return the resolved filesystem path for a table."""
    return get_table_path(table_name)


def scan_table(
    table_name: str,
    *,
    symbol_id: int | Iterable[int] | None = None,
    symbol: str | Iterable[str] | None = None,
    exchange: str | Iterable[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """
    Return a filtered LazyFrame for a Delta table.
    
    Prefers symbol_id as the primary filter. If symbol_id is provided,
    the exchange filter is automatically resolved for partition pruning.
    If symbol is provided with exchange, all matching symbol_id values are included.
    """
    if symbol is not None and symbol_id is not None:
        raise ValueError("scan_table: provide only one of symbol_id or symbol")

    resolved_symbol_ids = symbol_id
    resolved_exchanges: list[str] | None = None
    if symbol is not None:
        resolved_symbol_ids, resolved_exchanges = _resolve_symbol_ids_by_name(
            symbol,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
        )

    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        table_name=table_name,
        symbol_id=resolved_symbol_ids,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        resolved_exchanges=resolved_exchanges,
    )
    if columns:
        lf = lf.select(list(columns))
    return lf


def read_table(
    table_name: str,
    *,
    symbol_id: int | Iterable[int] | None = None,
    symbol: str | Iterable[str] | None = None,
    exchange: str | Iterable[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table."""
    return scan_table(
        table_name,
        symbol_id=symbol_id,
        symbol=symbol,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    ).collect()


def load_trades(
    *,
    symbol_id: int | Iterable[int] | None = None,
    symbol: str | Iterable[str] | None = None,
    exchange: str | Iterable[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied."""
    lf = scan_table(
        "trades",
        symbol_id=symbol_id,
        symbol=symbol,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_quotes(
    *,
    symbol_id: int | Iterable[int] | None = None,
    symbol: str | Iterable[str] | None = None,
    exchange: str | Iterable[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied."""
    lf = scan_table(
        "quotes",
        symbol_id=symbol_id,
        symbol=symbol,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_book_snapshot_25(
    *,
    symbol_id: int | Iterable[int] | None = None,
    symbol: str | Iterable[str] | None = None,
    exchange: str | Iterable[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied."""
    lf = scan_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        symbol=symbol,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def _apply_filters(
    lf: pl.LazyFrame,
    *,
    table_name: str | None,
    symbol_id: int | Iterable[int] | None,
    exchange: str | Iterable[str] | None,
    start_date: date | str | None,
    end_date: date | str | None,
    resolved_exchanges: Sequence[str] | None = None,
) -> pl.LazyFrame:
    # 1. Resolve Exchange from Symbol ID if provided (for partition pruning)
    exchanges_to_filter = []
    if exchange:
        exchanges_to_filter.extend(_normalize_exchanges(exchange))
        
    if symbol_id is not None:
        ids = [symbol_id] if isinstance(symbol_id, int) else list(symbol_id)
        # Add resolved exchanges to the filter list to ensure we hit partitions
        if resolved_exchanges:
            exchanges_to_filter.extend(resolved_exchanges)
        elif not exchange:
            resolved = resolve_symbols(ids)
            if not resolved:
                raise ValueError(
                    "scan_table: symbol_id values not found in dim_symbol registry"
                )
            exchanges_to_filter.extend(resolved)
        
        # Apply symbol_id filter
        lf = lf.filter(pl.col("symbol_id").is_in(ids))

    # 2. Apply unique exchange filters
    if exchanges_to_filter:
        unique_exchanges = sorted(list(set(exchanges_to_filter)))
        lf = lf.filter(pl.col("exchange").is_in(unique_exchanges))

    # 3. Apply Date Filters
    if start_date is not None or end_date is not None:
        if table_name is not None:
            table_has_date = TABLE_HAS_DATE.get(table_name)
        else:
            table_has_date = None
        if table_has_date is False:
            raise ValueError(
                "scan_table: start_date/end_date provided but table has no 'date' column"
            )
        if table_has_date is None and "date" not in lf.schema:
            raise ValueError(
                "scan_table: start_date/end_date provided but table has no 'date' column"
            )
        start = _to_date(start_date) if start_date is not None else None
        end = _to_date(end_date) if end_date is not None else None
        if start and end:
            lf = lf.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        elif start:
            lf = lf.filter(pl.col("date") >= start)
        elif end:
            lf = lf.filter(pl.col("date") <= end)

    return lf


def _to_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_exchanges(exchange: str | Iterable[str]) -> list[str]:
    exchanges = [exchange] if isinstance(exchange, str) else list(exchange)
    return [normalize_exchange(value) for value in exchanges]


def _date_to_ts_us(value: date | str) -> int:
    date_value = _to_date(value)
    dt = datetime.combine(date_value, time.min, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _resolve_symbol_ids_by_name(
    symbol: str | Iterable[str],
    *,
    exchange: str | Iterable[str] | None,
    start_date: date | str | None,
    end_date: date | str | None,
) -> tuple[list[int], list[str]]:
    symbols = [symbol] if isinstance(symbol, str) else list(symbol)
    symbols_lower = [value.lower() for value in symbols]

    dim_symbol = _read_dim_symbol()
    if dim_symbol.is_empty():
        raise ValueError("scan_table: dim_symbol is empty; cannot resolve symbols")

    if exchange:
        exchanges = _normalize_exchanges(exchange)
        dim_symbol = dim_symbol.filter(pl.col("exchange").is_in(exchanges))

    dim_symbol = dim_symbol.filter(
        pl.col("exchange_symbol").str.to_lowercase().is_in(symbols_lower)
    )

    if start_date is not None or end_date is not None:
        start_ts = _date_to_ts_us(start_date) if start_date is not None else None
        if end_date is not None:
            end_ts = _date_to_ts_us(_to_date(end_date) + timedelta(days=1))
        else:
            end_ts = DEFAULT_VALID_UNTIL_TS_US

        if start_ts is not None:
            dim_symbol = dim_symbol.filter(
                (pl.col("valid_from_ts") < end_ts)
                & (pl.col("valid_until_ts") > start_ts)
            )
        else:
            dim_symbol = dim_symbol.filter(pl.col("valid_from_ts") < end_ts)

    if dim_symbol.is_empty():
        raise ValueError("scan_table: no matching symbol_ids for provided symbol/exchange/date")

    symbol_ids = dim_symbol.select("symbol_id").unique()["symbol_id"].to_list()
    exchanges = dim_symbol.select("exchange").unique()["exchange"].to_list()
    if exchange is None and len(exchanges) > 1:
        warnings.warn(
            "scan_table: exchange not provided; symbol lookup matched multiple exchanges"
        )
    return symbol_ids, exchanges


def _read_dim_symbol() -> pl.DataFrame:
    return read_dim_symbol_table(
        columns=[
            "symbol_id",
            "exchange",
            "exchange_symbol",
            "valid_from_ts",
            "valid_until_ts",
        ]
    )
