"""Trades spine builder: Event-driven resampling at unique trade timestamps.

Generates one spine point per unique (exchange_id, symbol_id, ts_local_us).
In crypto, a single aggressive order sweeping the book produces multiple
trade records with the same timestamp — these are collapsed into one spine
point so that downstream as-of joins see one event per market decision,
not one per fill.
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
    """Trades spine builder: one spine point per unique trade timestamp.

    Sweep Deduplication:
        A large aggressive order sweeping the book generates multiple
        trade records with the same ts_local_us.  These are collapsed
        into a single spine point so that:
        - as-of joins see one event per market decision, not per fill
        - the book snapshot used for features reflects pre-sweep state once
        - event counts are not inflated by exchange fill fragmentation
    """

    @property
    def config_type(self) -> type:
        return TradesSpineConfig

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

        Loads trades and deduplicates by (exchange_id, symbol_id, ts_local_us)
        so that order-book sweeps (multiple fills at the same timestamp) produce
        a single spine point.

        Args:
            symbol_id: Single symbol_id or list of symbol_ids
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: TradesSpineConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange_id, symbol_id)
            sorted by (exchange_id, symbol_id, ts_local_us).

            ts_local_us is the BAR END — each unique trade timestamp
            is both the event time and the bar boundary.
        """
        if not isinstance(config, TradesSpineConfig):
            raise TypeError(f"Expected TradesSpineConfig, got {type(config).__name__}")

        # Load trades stream (only need spine columns)
        lf = research_core.scan_table(
            "trades",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            columns=["ts_local_us", "exchange_id", "symbol_id"],
        )

        # Deduplicate: collapse order-book sweeps (same timestamp) into
        # one spine point per (exchange_id, symbol_id, ts_local_us)
        lf = lf.unique(subset=["exchange_id", "symbol_id", "ts_local_us"])

        # Deterministic ordering
        return lf.sort(["exchange_id", "symbol_id", "ts_local_us"])


# Auto-register on module import
register_builder(TradesSpineBuilder())
