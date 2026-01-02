"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import EXCHANGE_MAP, TABLE_PATHS, get_exchange_id, get_table_path, normalize_exchange


def list_tables() -> list[str]:
    """Return registered table names."""
    return sorted(TABLE_PATHS.keys())


def table_path(table_name: str) -> Path:
    """Return the resolved filesystem path for a table."""
    return get_table_path(table_name)


def scan_table(
    table_name: str,
    *,
    exchange: str | None = None,
    exchange_id: int | Iterable[int] | None = None,
    symbol_id: int | Iterable[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """Return a filtered LazyFrame for a Delta table."""
    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_date,
        end_date=end_date,
    )
    if columns:
        lf = lf.select(list(columns))
    return lf


def read_table(
    table_name: str,
    *,
    exchange: str | None = None,
    exchange_id: int | Iterable[int] | None = None,
    symbol_id: int | Iterable[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table."""
    return scan_table(
        table_name,
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    ).collect()


def load_trades(
    *,
    exchange: str | None = None,
    exchange_id: int | Iterable[int] | None = None,
    symbol_id: int | Iterable[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied."""
    lf = scan_table(
        "trades",
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_quotes(
    *,
    exchange: str | None = None,
    exchange_id: int | Iterable[int] | None = None,
    symbol_id: int | Iterable[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied."""
    lf = scan_table(
        "quotes",
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def load_book_snapshots_top25(
    *,
    exchange: str | None = None,
    exchange_id: int | Iterable[int] | None = None,
    symbol_id: int | Iterable[int] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied."""
    lf = scan_table(
        "book_snapshots_top25",
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
    )
    return lf if lazy else lf.collect()


def _apply_filters(
    lf: pl.LazyFrame,
    *,
    exchange: str | None,
    exchange_id: int | Iterable[int] | None,
    symbol_id: int | Iterable[int] | None,
    start_date: date | str | None,
    end_date: date | str | None,
) -> pl.LazyFrame:
    if exchange:
        lf = lf.filter(pl.col("exchange") == normalize_exchange(exchange))

    if exchange_id is not None:
        if isinstance(exchange_id, Iterable) and not isinstance(exchange_id, (str, bytes)):
            lf = lf.filter(pl.col("exchange_id").is_in(list(exchange_id)))
        else:
            lf = lf.filter(pl.col("exchange_id") == exchange_id)

    if symbol_id is not None:
        if isinstance(symbol_id, Iterable) and not isinstance(symbol_id, (str, bytes)):
            lf = lf.filter(pl.col("symbol_id").is_in(list(symbol_id)))
        else:
            lf = lf.filter(pl.col("symbol_id") == symbol_id)

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
