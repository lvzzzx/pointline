"""Researcher-facing access helpers for the Pointline data lake."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from pointline.config import TABLE_HAS_DATE, TABLE_PATHS, get_table_path
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
    start_ts_us: int | None = None,
    end_ts_us: int | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.LazyFrame:
    """
    Return a filtered LazyFrame for a Delta table.
    
    Requires symbol_id + time range. Exchange partitions are derived from symbol_id
    for pruning.
    Timestamps are applied to the selected time column and implicitly prune
    date partitions when available.
    """
    if symbol_id is None:
        raise ValueError("scan_table: symbol_id is required; resolve IDs first")
    if start_ts_us is None or end_ts_us is None:
        raise ValueError("scan_table: start_ts_us and end_ts_us are required")
    _validate_ts_range(start_ts_us, end_ts_us)
    _validate_ts_col(ts_col)

    resolved_symbol_ids = symbol_id

    start_date, end_date = _derive_date_bounds_from_ts(start_ts_us, end_ts_us)
    date_filter_is_implicit = True

    lf = pl.scan_delta(str(get_table_path(table_name)))
    lf = _apply_filters(
        lf,
        table_name=table_name,
        symbol_id=resolved_symbol_ids,
        start_date=start_date,
        end_date=end_date,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        date_filter_is_implicit=date_filter_is_implicit,
    )
    if columns:
        lf = lf.select(list(columns))
    return lf


def read_table(
    table_name: str,
    *,
    symbol_id: int | Iterable[int] | None = None,
    start_ts_us: int | None = None,
    end_ts_us: int | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Return a filtered DataFrame for a Delta table (symbol_id + time range required)."""
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
    start_ts_us: int | None = None,
    end_ts_us: int | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load trades with common filters applied (symbol_id + time range required)."""
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
    start_ts_us: int | None = None,
    end_ts_us: int | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load quotes with common filters applied (symbol_id + time range required)."""
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
    start_ts_us: int | None = None,
    end_ts_us: int | None = None,
    ts_col: str = "ts_local_us",
    columns: Sequence[str] | None = None,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """Load top-25 book snapshots with common filters applied (symbol_id + time range required)."""
    lf = scan_table(
        "book_snapshot_25",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
    )
    return lf if lazy else lf.collect()


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
            if not date_filter_is_implicit:
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




def _derive_date_bounds_from_ts(
    start_ts_us: int | None, end_ts_us: int | None
) -> tuple[date | None, date | None]:
    start_date = _ts_us_to_date(start_ts_us) if start_ts_us is not None else None
    if end_ts_us is None:
        end_date = None
    else:
        end_date = _ts_us_to_date(max(end_ts_us - 1, 0))
    return start_date, end_date


def _ts_us_to_date(value: int) -> date:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).date()


def _validate_ts_col(ts_col: str) -> None:
    if ts_col not in {"ts_local_us", "ts_exch_us"}:
        raise ValueError(
            "scan_table: ts_col must be 'ts_local_us' or 'ts_exch_us'"
        )


def _validate_ts_range(start_ts_us: int | None, end_ts_us: int | None) -> None:
    if start_ts_us is not None and end_ts_us is not None and end_ts_us <= start_ts_us:
        raise ValueError("scan_table: end_ts_us must be greater than start_ts_us")
