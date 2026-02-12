"""Base protocol and types for spine builders.

Spine builders define resampling strategies for feature engineering. Each builder
implements a specific sampling method (clock, trades, volume bars, dollar bars, etc.).

Spine Contract:
- Must return LazyFrame with columns: (ts_local_us, exchange, symbol)
- ts_local_us is the BAR END timestamp -- the right boundary of the half-open
  window [prev_bar_end, ts_local_us).  All data assigned to this bar satisfies
  data.ts_local_us < bar.ts_local_us (strict less-than).
- Must be sorted by (exchange, symbol, ts_local_us) for deterministic ordering
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

        Data at 50ms -> assigned to bar at 60ms
        Data at 60ms -> assigned to bar at 120ms (boundary goes to next)
        Data at 110ms -> assigned to bar at 120ms

    This ensures Point-In-Time (PIT) correctness: all data in bar has ts < bar timestamp.

    Contract:
    - build_spine() must return LazyFrame with (ts_local_us, exchange, symbol)
    - Output must be sorted by (exchange, symbol, ts_local_us)
    - ts_local_us values are BAR ENDS (interval ends)
    - Must preserve PIT correctness (no lookahead bias)
    """

    @property
    def config_type(self) -> type[SpineBuilderConfig]:
        """Return the config class this builder accepts."""
        ...

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
        exchange: str,
        symbol: str | list[str],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build resampled spine with specified config.

        Args:
            exchange: Exchange name (e.g., "binance-futures")
            symbol: Single symbol or list of symbols (e.g., "BTCUSDT")
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: Builder-specific configuration

        Returns:
            LazyFrame with columns:
            - ts_local_us (Int64): BAR END timestamp -- right boundary of
              the half-open window [prev_bar_end, ts_local_us).
              NOT the bar start. assign_to_buckets() relies on
              data.ts_local_us < spine.ts_local_us.
            - exchange (Utf8): Exchange name
            - symbol (Utf8): Symbol name

            Sorted by (exchange, symbol, ts_local_us)

        Raises:
            ValueError: If config is invalid or symbol not found
            RuntimeError: If spine exceeds max_rows safety limit
        """
        ...
