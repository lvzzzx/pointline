"""Clock spine builder: Fixed time interval resampling.

Generates spine points at regular time intervals (e.g., every 1 second).
This is the most common resampling method for time-series analysis.
"""

from dataclasses import dataclass

import polars as pl

from pointline.dim_symbol import read_dim_symbol_table

from .base import SpineBuilderConfig
from .registry import register_builder


@dataclass(frozen=True)
class ClockSpineConfig(SpineBuilderConfig):
    """Configuration for clock spine resampling.

    Args:
        step_ms: Time step in milliseconds (default: 1000 = 1 second)
        max_rows: Safety limit for maximum rows (default: 5M)
    """

    step_ms: int = 1000


class ClockSpineBuilder:
    """Clock spine builder: Fixed time interval resampling."""

    @property
    def name(self) -> str:
        return "clock"

    @property
    def display_name(self) -> str:
        return "Clock (Fixed Time Intervals)"

    @property
    def supports_single_symbol(self) -> bool:
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        return True

    def can_handle(self, mode: str) -> bool:
        """Recognize: clock, time, fixed_time."""
        return mode.lower() in {"clock", "time", "fixed_time"}

    def build_spine(
        self,
        symbol_id: int | list[int],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build clock spine with fixed time intervals.

        Args:
            symbol_id: Single symbol_id or list of symbol_ids
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: ClockSpineConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange_id, symbol_id)
            sorted by (exchange_id, symbol_id, ts_local_us)
        """
        if not isinstance(config, ClockSpineConfig):
            raise TypeError(f"Expected ClockSpineConfig, got {type(config).__name__}")

        if config.step_ms <= 0:
            raise ValueError("step_ms must be positive")

        # Ensure symbol_id is list
        symbol_ids = [symbol_id] if isinstance(symbol_id, int) else list(symbol_id)

        # Resolve exchange_ids
        exchange_ids = self._resolve_exchange_ids(symbol_ids)

        # Compute step in microseconds
        step_us = config.step_ms * 1_000

        # Compute number of steps
        steps = (end_ts_us - start_ts_us) // step_us + 1
        total_rows = steps * len(symbol_ids)

        if total_rows > config.max_rows:
            raise RuntimeError(
                f"Clock spine would generate too many rows: {total_rows:,} > {config.max_rows:,}. "
                f"Increase step_ms or max_rows."
            )

        # Generate timestamps
        stop_us = start_ts_us + step_us * steps
        timestamps = pl.int_range(start_ts_us, stop_us, step_us, eager=True)
        time_df = pl.DataFrame({"ts_local_us": timestamps})

        # Create symbols table with correct types
        symbols_df = pl.DataFrame(
            {
                "symbol_id": pl.Series(symbol_ids, dtype=pl.Int64),
                "exchange_id": pl.Series(exchange_ids, dtype=pl.Int16),
            }
        )

        # Cross join: every symbol × every timestamp
        spine = symbols_df.lazy().join(time_df.lazy(), how="cross")

        # Sort by (exchange_id, symbol_id, ts_local_us) for deterministic ordering
        return spine.sort(["exchange_id", "symbol_id", "ts_local_us"])

    def _resolve_exchange_ids(self, symbol_ids: list[int]) -> list[int]:
        """Resolve exchange_ids from symbol_ids via dim_symbol."""
        dim = read_dim_symbol_table(columns=["symbol_id", "exchange_id"]).unique()
        lookup = dim.filter(pl.col("symbol_id").is_in(symbol_ids))

        if lookup.is_empty():
            raise ValueError("No matching symbol_ids found in dim_symbol.")

        exchange_ids: list[int] = []
        missing: list[int] = []

        for symbol in symbol_ids:
            rows = lookup.filter(pl.col("symbol_id") == symbol)
            if rows.is_empty():
                missing.append(symbol)
                continue
            exchange_ids.append(int(rows["exchange_id"][0]))

        if missing:
            raise ValueError(f"Missing exchange_id for symbol_id(s): {missing}")

        return exchange_ids


# Auto-register on module import
register_builder(ClockSpineBuilder())
