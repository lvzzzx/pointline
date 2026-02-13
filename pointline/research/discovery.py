"""Symbol discovery over canonical v2 dim_symbol."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.research._time import TimestampInput, normalize_ts_us
from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.storage.delta.dimension_store import DeltaDimensionStore


def discover_symbols(
    *,
    silver_root: Path,
    exchange: str,
    q: str | None = None,
    as_of: TimestampInput | None = None,
    include_meta: bool = False,
    limit: int = 50,
) -> pl.DataFrame:
    """Discover symbols from dim_symbol with optional as-of filtering.

    Behavior:
    - If ``as_of`` is omitted, returns current rows only.
    - If ``as_of`` is provided, returns rows valid at that timestamp.
    - Metadata columns (tick/lot/contract sizes) are returned only when
      ``include_meta=True``.
    """
    if limit <= 0:
        raise ValueError(f"limit must be > 0, got {limit}")

    exchange_norm = exchange.strip().lower()
    if not exchange_norm:
        raise ValueError("exchange must be non-empty")

    store = DeltaDimensionStore(silver_root=silver_root)
    dim = store.load_dim_symbol()
    if dim.is_empty():
        return _empty_discovery_frame(include_meta=include_meta)

    filtered = dim.filter(pl.col("exchange") == exchange_norm)

    if as_of is None:
        filtered = filtered.filter(pl.col("is_current"))
    else:
        ts = normalize_ts_us(as_of, param_name="as_of")
        filtered = filtered.filter(
            (pl.col("valid_from_ts_us") <= ts) & (ts < pl.col("valid_until_ts_us"))
        )

    needle = (q or "").strip().lower()
    if needle:
        filtered = filtered.filter(
            pl.any_horizontal(
                pl.col("exchange_symbol").str.to_lowercase().str.contains(needle, literal=True),
                pl.col("canonical_symbol").str.to_lowercase().str.contains(needle, literal=True),
                pl.col("base_asset").str.to_lowercase().str.contains(needle, literal=True),
            )
        )

    cols = _discovery_columns(include_meta=include_meta)
    return (
        filtered.select(cols)
        .sort(
            by=["exchange_symbol", "valid_from_ts_us"],
            descending=[False, True],
        )
        .head(limit)
    )


def _discovery_columns(*, include_meta: bool) -> list[str]:
    base = [
        "exchange",
        "exchange_symbol",
        "canonical_symbol",
        "symbol_id",
        "is_current",
        "valid_from_ts_us",
        "valid_until_ts_us",
    ]
    if not include_meta:
        return base
    return base + [
        "market_type",
        "base_asset",
        "quote_asset",
        "tick_size",
        "lot_size",
        "contract_size",
        "updated_at_ts_us",
    ]


def _empty_discovery_frame(*, include_meta: bool) -> pl.DataFrame:
    schema = DIM_SYMBOL.to_polars()
    cols = _discovery_columns(include_meta=include_meta)
    return pl.DataFrame(schema={col: schema[col] for col in cols})
