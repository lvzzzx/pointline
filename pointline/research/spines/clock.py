"""Clock spine builder: Fixed time interval resampling.

Generates spine points at regular time intervals (e.g., every 1 second).
This is the most common resampling method for time-series analysis.

UPDATED: Now includes explicit bar-end semantics per resample-aggregate design.
"""

from dataclasses import dataclass

import polars as pl

from .base import SpineBuilderConfig
from .registry import register_builder


def generate_bar_end_timestamps(start_us: int, end_us: int, step_us: int) -> list[int]:
    """Generate bar-end timestamps aligned to grid.

    First bar end = ceil(start_us / step_us) * step_us, then every step_us
    until end_us (inclusive).

    Args:
        start_us: Start timestamp in microseconds.
        end_us: End timestamp in microseconds.
        step_us: Step size in microseconds.

    Returns:
        List of bar-end timestamps.
    """
    # First bar end is one interval after the last complete interval boundary
    first_end = ((start_us // step_us) + 1) * step_us
    timestamps: list[int] = []
    current = first_end
    while current <= end_us:
        timestamps.append(current)
        current += step_us
    return timestamps


@dataclass(frozen=True)
class ClockSpineConfig(SpineBuilderConfig):
    """Configuration for clock spine resampling.

    Args:
        step_ms: Time step in milliseconds (default: 1000 = 1 second)
        max_rows: Safety limit for maximum rows (default: 5M)
    """

    step_ms: int = 1000


class ClockSpineBuilder:
    """Clock spine builder: Fixed time interval resampling.

    Bar Semantics (CRITICAL):
    -------------------------
    - Spine timestamps are BAR ENDS (interval ends, not starts)
    - Bar at timestamp T contains data with ts_local_us < T
    - Bar window = [T_prev, T) (half-open interval)

    Example:
        config = ClockSpineConfig(step_ms=60000)  # 1 minute
        spine = builder.build_spine(..., start_ts_us=0, end_ts_us=180_000_000, config=config)

        Generates: [60_000_000, 120_000_000, 180_000_000]
        These are bar ENDS: Bar at 60s contains data in [0s, 60s)
    """

    @property
    def config_type(self) -> type[SpineBuilderConfig]:
        return ClockSpineConfig

    @property
    def name(self) -> str:
        """Unique identifier for this builder."""
        return "clock"

    @property
    def display_name(self) -> str:
        """Human-readable name."""
        return "Clock (Fixed Time Intervals)"

    @property
    def supports_single_symbol(self) -> bool:
        """Whether this builder supports single-symbol mode."""
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        """Whether this builder supports multi-symbol mode."""
        return True

    def can_handle(self, mode: str) -> bool:
        """Check if this builder can handle the given mode string."""
        return mode.lower() in {"clock", "time", "fixed_time"}

    def build_spine(
        self,
        exchange: str,
        symbol: str | list[str],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build clock spine with fixed time intervals.

        CRITICAL: Spine timestamps are BAR ENDS (interval ends).

        Args:
            exchange: Exchange name (e.g., "binance-futures")
            symbol: Single symbol or list of symbols (e.g., "BTCUSDT")
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: ClockSpineConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange, symbol)
            sorted by (exchange, symbol, ts_local_us)

            IMPORTANT: ts_local_us values are bar ENDS (interval ends).

        Example:
            start_ts_us=0, end_ts_us=180_000_000, step_ms=60000
            -> Generates: [60_000_000, 120_000_000, 180_000_000]
            -> Bar at 60ms contains data in [0ms, 60ms)
            -> Bar at 120ms contains data in [60ms, 120ms)
        """
        if not isinstance(config, ClockSpineConfig):
            raise TypeError(f"Expected ClockSpineConfig, got {type(config).__name__}")

        if config.step_ms <= 0:
            raise ValueError("step_ms must be positive")

        # Ensure symbol is list
        symbols = [symbol] if isinstance(symbol, str) else list(symbol)

        # Compute step in microseconds
        step_us = config.step_ms * 1_000

        # Generate bar boundary timestamps (interval ENDS)
        timestamps = generate_bar_end_timestamps(start_ts_us, end_ts_us, step_us)

        if not timestamps:
            # Edge case: no bars in range
            return pl.LazyFrame(
                schema={
                    "ts_local_us": pl.Int64,
                    "exchange": pl.Utf8,
                    "symbol": pl.Utf8,
                }
            )

        # Safety check
        total_rows = len(timestamps) * len(symbols)
        if total_rows > config.max_rows:
            raise RuntimeError(
                f"Clock spine would generate too many rows: {total_rows:,} > {config.max_rows:,}. "
                f"Increase step_ms or max_rows."
            )

        # Create time DataFrame
        time_df = pl.DataFrame({"ts_local_us": timestamps})

        # Create symbols table with exchange
        symbols_df = pl.DataFrame(
            {
                "symbol": pl.Series(symbols, dtype=pl.Utf8),
                "exchange": pl.Series([exchange] * len(symbols), dtype=pl.Utf8),
            }
        )

        # Cross join: every symbol x every timestamp
        spine = symbols_df.lazy().join(time_df.lazy(), how="cross")

        # Sort by (exchange, symbol, ts_local_us) for deterministic ordering
        return spine.sort(["exchange", "symbol", "ts_local_us"])


# Auto-register on module import
register_builder(ClockSpineBuilder())
