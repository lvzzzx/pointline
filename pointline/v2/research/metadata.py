"""Explicit symbol metadata queries over canonical v2 dim_symbol."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.v2.research._time import TimestampInput, normalize_ts_us
from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore


def load_symbol_meta(
    *,
    silver_root: Path,
    exchange: str,
    symbols: str | list[str] | None = None,
    as_of: TimestampInput | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Load canonical symbol metadata explicitly (no event join side effects).

    Behavior:
    - If ``as_of`` is omitted, returns current rows only.
    - If ``as_of`` is provided, returns rows valid at that timestamp.
    - ``symbols`` filters by exchange_symbol (single or many).
    - ``columns`` projects output columns; default is full DIM_SYMBOL schema.
    """
    exchange_norm = exchange.strip().lower()
    if not exchange_norm:
        raise ValueError("exchange must be non-empty")

    selected_cols = _resolve_columns(columns)

    dim = DeltaDimensionStore(silver_root=silver_root).load_dim_symbol()
    if dim.is_empty():
        return _empty_result(selected_cols)

    frame = dim.filter(pl.col("exchange") == exchange_norm)

    symbol_values = _normalize_symbols(symbols)
    if symbol_values:
        frame = frame.filter(pl.col("exchange_symbol").is_in(symbol_values))

    if as_of is None:
        frame = frame.filter(pl.col("is_current"))
    else:
        ts_us = normalize_ts_us(as_of, param_name="as_of")
        frame = frame.filter(
            (pl.col("valid_from_ts_us") <= ts_us) & (ts_us < pl.col("valid_until_ts_us"))
        )

    frame = frame.sort(
        by=["exchange_symbol", "valid_from_ts_us"],
        descending=[False, True],
    )
    return frame.select(selected_cols)


def _resolve_columns(columns: list[str] | None) -> list[str]:
    schema = DIM_SYMBOL.to_polars()
    if columns is None:
        return list(schema)

    unknown = sorted(set(columns) - set(schema))
    if unknown:
        raise ValueError(f"Unknown symbol metadata columns requested: {unknown}")
    return list(columns)


def _normalize_symbols(symbols: str | list[str] | None) -> list[str]:
    if symbols is None:
        return []
    if isinstance(symbols, str):
        value = symbols.strip()
        return [value] if value else []

    normalized = [symbol.strip() for symbol in symbols if symbol.strip()]
    return list(dict.fromkeys(normalized))


def _empty_result(columns: list[str]) -> pl.DataFrame:
    schema = DIM_SYMBOL.to_polars()
    return pl.DataFrame(schema={col: schema[col] for col in columns})
