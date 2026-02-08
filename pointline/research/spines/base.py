"""Base protocol and types for spine builders.

Spine builders define resampling strategies for feature engineering. Each builder
implements a specific sampling method (clock, trades, volume bars, dollar bars, etc.).

Spine Contract:
- Must return LazyFrame with columns: (ts_local_us, exchange_id, symbol_id)
- Must be sorted by (exchange_id, symbol_id, ts_local_us) for deterministic ordering
- Must preserve PIT correctness (no lookahead bias)
"""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import polars as pl


@dataclass(frozen=True)
class SpineBuilderConfig:
    """Base configuration for spine builders.

    All builder-specific configs should inherit from this class.
    """

    max_rows: int = 5_000_000  # Safety limit to prevent accidental full scans


@runtime_checkable
class SpineBuilder(Protocol):
    """Protocol for spine builder plugins.

    Each builder implements a specific resampling strategy (e.g., volume bars, dollar bars).
    Builders are registered in the global registry and can be looked up by name.

    Bar Timestamp Semantics (CRITICAL):
    -----------------------------------
    - Spine timestamps are BAR ENDS (interval ends, not starts)
    - Bar at timestamp T contains all data with ts_local_us < T
    - Bar window = [T_prev, T) (half-open interval, right-exclusive)

    Example:
        Spine timestamps: [60ms, 120ms, 180ms]

        Bar at 60ms contains: data in [0ms, 60ms)
        Bar at 120ms contains: data in [60ms, 120ms)
        Bar at 180ms contains: data in [120ms, 180ms)

        Data at 50ms → assigned to bar at 60ms
        Data at 60ms → assigned to bar at 120ms (boundary goes to next)
        Data at 110ms → assigned to bar at 120ms

    This ensures Point-In-Time (PIT) correctness: all data in bar has ts < bar timestamp.

    Contract:
    - build_spine() must return LazyFrame with (ts_local_us, exchange_id, symbol_id)
    - Output must be sorted by (exchange_id, symbol_id, ts_local_us)
    - Timestamps must be bar ENDS (interval ends)
    - Must preserve PIT correctness (no lookahead bias)
    """

    @property
    def name(self) -> str:
        """Unique identifier for this builder (e.g., 'volume', 'dollar')."""
        ...

    @property
    def display_name(self) -> str:
        """Human-readable name (e.g., 'Volume Bars', 'Dollar Bars')."""
        ...

    @property
    def supports_single_symbol(self) -> bool:
        """Whether this builder supports single-symbol mode."""
        ...

    @property
    def supports_multi_symbol(self) -> bool:
        """Whether this builder supports multi-symbol mode."""
        ...

    def can_handle(self, mode: str) -> bool:
        """Check if this builder can handle the given mode string.

        Used for auto-detection and backward compatibility.

        Args:
            mode: Mode string (e.g., "clock", "trades", "volume")

        Returns:
            True if this builder recognizes the mode
        """
        ...

    def build_spine(
        self,
        symbol_id: int | list[int],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build resampled spine with specified config.

        Args:
            symbol_id: Single symbol_id or list of symbol_ids
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: Builder-specific configuration

        Returns:
            LazyFrame with columns:
            - ts_local_us (Int64): Sample timestamp
            - exchange_id (Int16): Exchange ID
            - symbol_id (Int64): Symbol ID

            Sorted by (exchange_id, symbol_id, ts_local_us)

        Raises:
            ValueError: If config is invalid or symbol_id not found
            RuntimeError: If spine exceeds max_rows safety limit
        """
        ...
