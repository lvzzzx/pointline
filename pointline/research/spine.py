"""Public v2 spine API (internal builders, explicit alignment)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.research._spine_builders import (
    build_clock_spine,
    build_dollar_spine,
    build_trades_spine,
    build_volume_spine,
    empty_spine_frame,
)
from pointline.research._spine_types import (
    BuilderName,
    ClockSpineConfig,
    DollarSpineConfig,
    SpineConfig,
    TradesSpineConfig,
    VolumeSpineConfig,
)
from pointline.research._time import TimestampInput, normalize_ts_us, validate_time_window


def build_spine(
    *,
    silver_root: Path,
    exchange: str,
    symbol: str | list[str],
    start: TimestampInput,
    end: TimestampInput,
    builder: BuilderName | str,
    config: SpineConfig,
) -> pl.DataFrame:
    """Build a canonical v2 spine for one exchange and one/many symbols."""
    exchange_norm = exchange.strip().lower()
    if not exchange_norm:
        raise ValueError("exchange must be non-empty")

    symbols = _normalize_symbols(symbol)
    start_ts_us = normalize_ts_us(start, param_name="start")
    end_ts_us = normalize_ts_us(end, param_name="end")
    validate_time_window(start_ts_us, end_ts_us)

    builder_name = builder.strip().lower()
    if builder_name == "clock":
        if not isinstance(config, ClockSpineConfig):
            raise TypeError("clock builder requires ClockSpineConfig")
        return build_clock_spine(
            silver_root=silver_root,
            exchange=exchange_norm,
            symbols=symbols,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            config=config,
        )
    if builder_name == "trades":
        if not isinstance(config, TradesSpineConfig):
            raise TypeError("trades builder requires TradesSpineConfig")
        return build_trades_spine(
            silver_root=silver_root,
            exchange=exchange_norm,
            symbols=symbols,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            config=config,
        )
    if builder_name == "volume":
        if not isinstance(config, VolumeSpineConfig):
            raise TypeError("volume builder requires VolumeSpineConfig")
        return build_volume_spine(
            silver_root=silver_root,
            exchange=exchange_norm,
            symbols=symbols,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            config=config,
        )
    if builder_name == "dollar":
        if not isinstance(config, DollarSpineConfig):
            raise TypeError("dollar builder requires DollarSpineConfig")
        return build_dollar_spine(
            silver_root=silver_root,
            exchange=exchange_norm,
            symbols=symbols,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            config=config,
        )
    raise ValueError(
        f"Unknown spine builder {builder!r}. Expected one of: clock, trades, volume, dollar"
    )


def align_to_spine(
    *,
    events: pl.DataFrame,
    spine: pl.DataFrame,
    ts_col: str = "ts_event_us",
    by: tuple[str, str] = ("exchange", "symbol"),
) -> pl.DataFrame:
    """Assign each event to the next spine bar-end using PIT-safe semantics.

    Boundary behavior is explicit: events exactly at spine boundary are aligned
    to the next bar (`event_ts == ts_spine_us` maps forward).
    """
    for key in by:
        if key not in events.columns:
            raise ValueError(f"events missing join key column: {key}")
        if key not in spine.columns:
            raise ValueError(f"spine missing join key column: {key}")
    if ts_col not in events.columns:
        raise ValueError(f"events missing timestamp column: {ts_col}")
    if "ts_spine_us" not in spine.columns:
        raise ValueError("spine missing required column: ts_spine_us")

    if events.is_empty():
        return events.with_columns(pl.lit(None, dtype=pl.Int64).alias("ts_spine_us"))
    if spine.is_empty():
        return events.with_columns(pl.lit(None, dtype=pl.Int64).alias("ts_spine_us"))

    by_cols = list(by)
    left = (
        events.with_row_index("_row_id")
        .with_columns((pl.col(ts_col).cast(pl.Int64) + 1).alias("_ts_join"))
        .sort([*by_cols, "_ts_join", "_row_id"])
    )
    right = spine.sort([*by_cols, "ts_spine_us"]).select([*by_cols, "ts_spine_us"])

    out = left.join_asof(
        right,
        left_on="_ts_join",
        right_on="ts_spine_us",
        by=by_cols,
        strategy="forward",
        check_sortedness=False,
    )
    return out.sort("_row_id").drop(["_row_id", "_ts_join"])


def _normalize_symbols(symbol: str | list[str]) -> list[str]:
    values = [symbol.strip()] if isinstance(symbol, str) else [item.strip() for item in symbol]

    normalized = [value for value in values if value]
    if not normalized:
        raise ValueError("symbol must be non-empty")
    return list(dict.fromkeys(normalized))


__all__ = [
    "ClockSpineConfig",
    "DollarSpineConfig",
    "TradesSpineConfig",
    "VolumeSpineConfig",
    "align_to_spine",
    "build_spine",
    "empty_spine_frame",
]
