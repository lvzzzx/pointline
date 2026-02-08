"""Trades spine builder: Event-driven resampling on every trade.

Generates spine points at every trade event. This is useful for tick-by-tick
analysis where you want to compute features after each trade.
"""

from dataclasses import dataclass

import polars as pl

from pointline.research import core as research_core

from .base import SpineBuilderConfig
from .registry import register_builder


@dataclass(frozen=True)
class TradesSpineConfig(SpineBuilderConfig):
    """Configuration for trades spine resampling.

    Args:
        max_rows: Safety limit for maximum rows (default: 5M)
    """

    pass  # Only inherits max_rows from base


class TradesSpineBuilder:
    """Trades spine builder: Event-driven resampling on every trade."""

    @property
    def name(self) -> str:
        return "trades"

    @property
    def display_name(self) -> str:
        return "Trades (Event-Driven)"

    @property
    def supports_single_symbol(self) -> bool:
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        return True

    def can_handle(self, mode: str) -> bool:
        """Recognize: trades, trade, trade_event."""
        return mode.lower() in {"trades", "trade", "trade_event"}

    def build_spine(
        self,
        symbol_id: int | list[int],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build trades spine from trades stream.

        Args:
            symbol_id: Single symbol_id or list of symbol_ids
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: TradesSpineConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange_id, symbol_id)
            sorted by (exchange_id, symbol_id, ts_local_us, file_id, file_line_number)
        """
        if not isinstance(config, TradesSpineConfig):
            raise TypeError(f"Expected TradesSpineConfig, got {type(config).__name__}")

        # Load trades stream
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

        # Sort by deterministic ordering
        # Include file_id and file_line_number for full determinism
        return lf.sort(
            [
                "exchange_id",
                "symbol_id",
                "ts_local_us",
                "file_id",
                "file_line_number",
            ]
        )


# Auto-register on module import
register_builder(TradesSpineBuilder())
