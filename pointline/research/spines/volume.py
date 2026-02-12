"""Volume bar spine builder: Sample every N contracts/shares.

Volume bars provide more stationary price dynamics than time bars by sampling
at equal volume intervals. This is particularly useful for HFT research where
trading activity varies significantly throughout the day.

References:
- Easley, D., Lopez de Prado, M., O'Hara, M. (2012). Flow Toxicity and Liquidity in a High-frequency World
- Lopez de Prado, M. (2018). Advances in Financial Machine Learning, Ch. 2
"""

from dataclasses import dataclass

import polars as pl

from pointline.encoding import get_profile
from pointline.research import core as research_core

from .base import SpineBuilderConfig
from .registry import register_builder


@dataclass(frozen=True)
class VolumeBarConfig(SpineBuilderConfig):
    """Configuration for volume bar resampling.

    Args:
        volume_threshold: Sample every N contracts/shares (default: 1000)
        use_absolute_volume: If True, ignore side (buy+sell), if False, use signed volume (default: True)
        max_rows: Safety limit for maximum rows (default: 5M)
    """

    volume_threshold: float = 1000.0
    use_absolute_volume: bool = True


class VolumeSpineBuilder:
    """Volume bar builder: Sample every N contracts/shares."""

    @property
    def config_type(self) -> type:
        return VolumeBarConfig

    @property
    def name(self) -> str:
        return "volume"

    @property
    def display_name(self) -> str:
        return "Volume Bars"

    @property
    def supports_single_symbol(self) -> bool:
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        return True

    def can_handle(self, mode: str) -> bool:
        """Recognize: volume, volume_bar, volume_bars."""
        return mode.lower() in {"volume", "volume_bar", "volume_bars"}

    def build_spine(
        self,
        exchange: str,
        symbol: str | list[str],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build volume bar spine.

        Algorithm:
        1. Load trades with qty_int
        2. Resolve encoding profile from exchange
        3. Decode volume = qty_int * profile.amount
        4. Optionally take absolute value (ignore side)
        5. Compute cumulative volume per symbol
        6. Compute bar_id = floor(cum_volume / volume_threshold)
        7. Group by (exchange, symbol, bar_id), take first timestamp
        8. Enforce max_rows safety limit

        Args:
            exchange: Exchange name (e.g., "binance-futures")
            symbol: Single symbol or list of symbols (e.g., "BTCUSDT")
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: VolumeBarConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange, symbol)
            sorted by (exchange, symbol, ts_local_us).

            ts_local_us is the BAR END -- the right boundary of each
            volume bar window, NOT the first trade timestamp.
            assign_to_buckets() relies on data.ts < bar.ts_local_us.

        Raises:
            TypeError: If config is not VolumeBarConfig
            ValueError: If volume_threshold <= 0
            RuntimeError: If spine exceeds max_rows
        """
        if not isinstance(config, VolumeBarConfig):
            raise TypeError(f"Expected VolumeBarConfig, got {type(config).__name__}")

        if config.volume_threshold <= 0:
            raise ValueError("volume_threshold must be positive")

        # Load trades with qty_int
        trades = research_core.scan_table(
            "trades",
            exchange=exchange,
            symbol=symbol,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            columns=[
                "ts_local_us",
                "exchange",
                "symbol",
                "qty_int",
                "file_id",
                "file_line_number",
            ],
        )

        # Resolve profile from exchange
        profile = get_profile(exchange)

        # Decode volume = qty_int * profile.amount
        if config.use_absolute_volume:
            # Absolute volume: ignore side (buy + sell)
            trades = trades.with_columns((pl.col("qty_int") * profile.amount).abs().alias("volume"))
        else:
            # Signed volume: preserve side (buy - sell)
            trades = trades.with_columns((pl.col("qty_int") * profile.amount).alias("volume"))

        # Compute cumulative volume per symbol
        # Must sort first to ensure correct cumulative sum
        trades = trades.sort(
            [
                "exchange",
                "symbol",
                "ts_local_us",
                "file_id",
                "file_line_number",
            ]
        )

        trades = trades.with_columns(
            pl.col("volume").cum_sum().over(["exchange", "symbol"]).alias("cum_volume")
        )

        # Compute bar_id = floor(cum_volume / volume_threshold)
        trades = trades.with_columns(
            (pl.col("cum_volume") / config.volume_threshold).floor().cast(pl.Int64).alias("bar_id")
        )

        # Group by (exchange, symbol, bar_id), take first timestamp
        # These are bar BOUNDARIES (starts), which we shift forward to get bar ENDS
        spine = trades.group_by(["exchange", "symbol", "bar_id"]).agg(
            [
                pl.col("ts_local_us").min().alias("bar_start_ts"),
            ]
        )

        # Sort by (exchange, symbol, bar_start_ts) for correct shifting
        spine = spine.sort(["exchange", "symbol", "bar_start_ts"])

        # Shift forward: bar_end[k] = bar_start[k+1], last bar gets end_ts_us
        # This converts bar STARTS to bar ENDS as required by the protocol
        spine = spine.with_columns(
            pl.col("bar_start_ts")
            .shift(-1)
            .over(["exchange", "symbol"])
            .fill_null(end_ts_us)
            .alias("ts_local_us")
        )

        # Sort by bar-end timestamps
        spine = spine.sort(["exchange", "symbol", "ts_local_us"])

        # Enforce max_rows safety limit (eager check)
        row_count = spine.select(pl.len()).collect().item()
        if row_count > config.max_rows:
            raise RuntimeError(
                f"Volume spine would generate too many rows: {row_count:,} > {config.max_rows:,}. "
                f"Increase volume_threshold or max_rows."
            )

        # Select final columns
        spine = spine.select(["ts_local_us", "exchange", "symbol"])

        return spine


# Auto-register on module import
register_builder(VolumeSpineBuilder())
