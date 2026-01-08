"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import EXCHANGE_MAP, TABLE_PATHS, get_exchange_id, get_table_path, normalize_exchange
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
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """
    Return a filtered LazyFrame for a Delta table.
    
    Prefers symbol_id as the primary filter. If symbol_id is provided,
    the exchange filter is automatically resolved for partition pruning.
    """
    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        symbol_id=symbol_id,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    )
    if columns:
        lf = lf.select(list(columns))
    return lf


def read_table(
    table_name: str,
    *,
    symbol_id: int | Iterable[int] | None = None,
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table."""
    return scan_table(
        table_name,
        symbol_id=symbol_id,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    ).collect()


def load_trades(
    *,
    symbol_id: int | Iterable[int] | None = None,
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied."""
    lf = scan_table(
        "trades",
        symbol_id=symbol_id,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_quotes(
    *,
    symbol_id: int | Iterable[int] | None = None,
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied."""
    lf = scan_table(
        "quotes",
        symbol_id=symbol_id,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_book_snapshot_25(
    *,
    symbol_id: int | Iterable[int] | None = None,
    exchange: str | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied."""
    lf = scan_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def _apply_filters(
    lf: pl.LazyFrame,
    *,
    symbol_id: int | Iterable[int] | None,
    exchange: str | None,
    start_date: date | str | None,
    end_date: date | str | None,
) -> pl.LazyFrame:
    # 1. Resolve Exchange from Symbol ID if provided (for partition pruning)
    exchanges_to_filter = []
    if exchange:
        exchanges_to_filter.append(normalize_exchange(exchange))
        
    if symbol_id is not None:
        ids = [symbol_id] if isinstance(symbol_id, int) else list(symbol_id)
        # Add resolved exchanges to the filter list to ensure we hit partitions
        resolved = resolve_symbols(ids)
        exchanges_to_filter.extend(resolved)
        
        # Apply symbol_id filter
        lf = lf.filter(pl.col("symbol_id").is_in(ids))

    # 2. Apply unique exchange filters
    if exchanges_to_filter:
        unique_exchanges = sorted(list(set(exchanges_to_filter)))
        lf = lf.filter(pl.col("exchange").is_in(unique_exchanges))

    # 3. Apply Date Filters
    if start_date is not None or end_date is not None:
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
