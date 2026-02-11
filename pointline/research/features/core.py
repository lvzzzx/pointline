"""Core utilities for PIT-correct feature engineering."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl

from pointline.dim_symbol import resolve_exchange_ids
from pointline.research import core as research_core
from pointline.research.spines import get_builder_by_config
from pointline.research.spines.base import SpineBuilderConfig
from pointline.research.spines.cache import SpineCache
from pointline.research.spines.clock import generate_bar_end_timestamps
from pointline.types import TimestampInput


@dataclass(frozen=True)
class EventSpineConfig:
    """Configuration for event spine construction.

    Usage:
        EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000))
        EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0))

    Args:
        builder_config: Builder-specific configuration (required)
    """

    builder_config: SpineBuilderConfig


def _ensure_symbol_ids(symbol_id: int | Iterable[int]) -> list[int]:
    if isinstance(symbol_id, int):
        return [symbol_id]
    return list(symbol_id)


def _normalize_ts_inputs(start_ts_us: TimestampInput, end_ts_us: TimestampInput) -> tuple[int, int]:
    start_us = research_core._normalize_timestamp(start_ts_us, "start_ts_us")
    end_us = research_core._normalize_timestamp(end_ts_us, "end_ts_us")
    if start_us is None or end_us is None:
        raise ValueError("start_ts_us and end_ts_us are required")
    research_core._validate_ts_range(start_us, end_us)
    return start_us, end_us


def build_clock_spine(
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    step_ms: int = 1000,
    exchange_id: int | Iterable[int] | None = None,
    max_rows: int = 5_000_000,
) -> pl.LazyFrame:
    """Create a deterministic clock spine for PIT feature computation."""
    if step_ms <= 0:
        raise ValueError("step_ms must be positive")

    symbol_ids = _ensure_symbol_ids(symbol_id)
    start_us, end_us = _normalize_ts_inputs(start_ts_us, end_ts_us)
    step_us = step_ms * 1_000

    # Generate bar-end timestamps aligned to grid
    timestamps = generate_bar_end_timestamps(start_us, end_us, step_us)

    total_rows = len(timestamps) * len(symbol_ids)
    if total_rows > max_rows:
        raise ValueError(
            "Clock spine would generate too many rows. "
            f"rows={total_rows:,} > max_rows={max_rows:,}. "
            "Increase step_ms or max_rows."
        )

    time_df = pl.DataFrame({"ts_local_us": timestamps})

    if exchange_id is None:
        exchange_ids = resolve_exchange_ids(symbol_ids)
    else:
        exchange_ids = _ensure_symbol_ids(exchange_id)

    if len(exchange_ids) != len(symbol_ids):
        raise ValueError("exchange_id length must match symbol_id length")

    symbols_df = pl.DataFrame({"symbol_id": symbol_ids, "exchange_id": exchange_ids})
    return symbols_df.lazy().join(time_df.lazy(), how="cross")


def build_trades_spine(
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
) -> pl.LazyFrame:
    """Create an event spine from the trades stream."""
    lf = research_core.scan_table(
        "trades",
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        columns=[
            "ts_local_us",
            "exchange_id",
            "symbol_id",
            "file_id",
            "file_line_number",
        ],
    )
    return lf.sort(["exchange_id", "symbol_id", "ts_local_us", "file_id", "file_line_number"])


def build_event_spine(
    *,
    symbol_id: int | Iterable[int],
    start_ts_us: TimestampInput,
    end_ts_us: TimestampInput,
    config: EventSpineConfig,
    cache: SpineCache | None = None,
) -> pl.LazyFrame:
    """Build an event spine with deterministic ordering.

    This function delegates to registered spine builders for extensibility.

    Args:
        symbol_id: Single symbol_id or list of symbol_ids
        start_ts_us: Start timestamp (microseconds, UTC, or TimestampInput)
        end_ts_us: End timestamp (microseconds, UTC, or TimestampInput)
        config: EventSpineConfig with builder_config
        cache: Optional SpineCache for caching expensive spine computations.
            When provided, spines are persisted as Parquet and reused on
            subsequent calls with identical inputs.

    Returns:
        LazyFrame with (ts_local_us, exchange_id, symbol_id)
        sorted by (exchange_id, symbol_id, ts_local_us)

    Example:
        from pointline.research.features import EventSpineConfig, build_event_spine
        from pointline.research.spines import VolumeBarConfig

        config = EventSpineConfig(
            builder_config=VolumeBarConfig(volume_threshold=1000.0)
        )
        spine = build_event_spine(
            symbol_id=12345,
            start_ts_us="2024-05-01",
            end_ts_us="2024-05-02",
            config=config,
        )
    """
    # Normalize timestamps
    start_us, end_us = _normalize_ts_inputs(start_ts_us, end_ts_us)

    # Get builder from config type
    builder = get_builder_by_config(config.builder_config)

    # Delegate through cache if provided
    if cache is not None:
        return cache.get_or_build(builder, symbol_id, start_us, end_us, config.builder_config)

    return builder.build_spine(
        symbol_id=symbol_id,
        start_ts_us=start_us,
        end_ts_us=end_us,
        config=config.builder_config,
    )


def pit_align(
    spine: pl.LazyFrame,
    tables: dict[str, pl.LazyFrame],
    *,
    on: str = "ts_local_us",
    by: tuple[str, str] = ("exchange_id", "symbol_id"),
) -> pl.LazyFrame:
    """As-of join multiple tables onto a spine with deterministic ordering."""
    base = spine.sort(_event_sort_columns(spine, on=on, by=by))

    for name, table in tables.items():
        if table is None:
            continue
        right = table.sort(_event_sort_columns(table, on=on, by=by))
        base = base.join_asof(
            right,
            on=on,
            by=list(by),
            strategy="backward",
            suffix=f"_{name}",
        )

    return base


def _event_sort_columns(
    frame: pl.LazyFrame,
    *,
    on: str,
    by: tuple[str, str],
) -> list[str]:
    """Build deterministic sort columns for event-time operations."""
    schema_names = set(frame.collect_schema().names())
    sort_cols = [*by, on]
    if "file_id" in schema_names:
        sort_cols.append("file_id")
    if "file_line_number" in schema_names:
        sort_cols.append("file_line_number")
    return sort_cols
