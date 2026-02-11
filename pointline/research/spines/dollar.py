"""Dollar bar spine builder: Sample every $N notional value.

Dollar bars normalize sampling by economic significance rather than physical
volume. This accounts for price variations and provides more consistent bars
across different price levels.

References:
- Easley, D., López de Prado, M., O'Hara, M. (2012). Flow Toxicity and Liquidity in a High-frequency World
- López de Prado, M. (2018). Advances in Financial Machine Learning, Ch. 2
"""

from dataclasses import dataclass

import polars as pl

from pointline.dim_symbol import read_dim_symbol_table
from pointline.research import core as research_core

from .base import SpineBuilderConfig
from .registry import register_builder


@dataclass(frozen=True)
class DollarBarConfig(SpineBuilderConfig):
    """Configuration for dollar bar resampling.

    Args:
        dollar_threshold: Sample every $N notional value (default: 100,000)
        max_rows: Safety limit for maximum rows (default: 5M)
    """

    dollar_threshold: float = 100_000.0


class DollarSpineBuilder:
    """Dollar bar builder: Sample every $N notional value."""

    @property
    def config_type(self) -> type:
        return DollarBarConfig

    @property
    def name(self) -> str:
        return "dollar"

    @property
    def display_name(self) -> str:
        return "Dollar Bars"

    @property
    def supports_single_symbol(self) -> bool:
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        return True

    def can_handle(self, mode: str) -> bool:
        """Recognize: dollar, dollar_bar, dollar_bars."""
        return mode.lower() in {"dollar", "dollar_bar", "dollar_bars"}

    def build_spine(
        self,
        symbol_id: int | list[int],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Build dollar bar spine.

        Algorithm:
        1. Load trades with px_int and qty_int
        2. Join with dim_symbol to get price_increment and amount_increment
        3. Decode: px = px_int × price_increment, qty = qty_int × amount_increment
        4. Compute notional = abs(px × qty) (dollar volume)
        5. Compute cumulative notional per symbol
        6. Compute bar_id = floor(cum_notional / dollar_threshold)
        7. Group by (exchange_id, symbol_id, bar_id), take first timestamp
        8. Enforce max_rows safety limit

        Args:
            symbol_id: Single symbol_id or list of symbol_ids
            start_ts_us: Start timestamp (microseconds, UTC)
            end_ts_us: End timestamp (microseconds, UTC)
            config: DollarBarConfig instance

        Returns:
            LazyFrame with (ts_local_us, exchange_id, symbol_id)
            sorted by (exchange_id, symbol_id, ts_local_us).

            ts_local_us is the BAR END — the right boundary of each
            dollar bar window, NOT the first trade timestamp.
            assign_to_buckets() relies on data.ts < bar.ts_local_us.

        Raises:
            TypeError: If config is not DollarBarConfig
            ValueError: If dollar_threshold <= 0
            RuntimeError: If spine exceeds max_rows
        """
        if not isinstance(config, DollarBarConfig):
            raise TypeError(f"Expected DollarBarConfig, got {type(config).__name__}")

        if config.dollar_threshold <= 0:
            raise ValueError("dollar_threshold must be positive")

        # Load trades with px_int and qty_int
        trades = research_core.scan_table(
            "trades",
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            columns=[
                "ts_local_us",
                "exchange_id",
                "symbol_id",
                "px_int",
                "qty_int",
                "file_id",
                "file_line_number",
            ],
        )

        # Join with dim_symbol to get price_increment and amount_increment
        dim_symbol = read_dim_symbol_table(
            columns=["symbol_id", "price_increment", "amount_increment"]
        ).unique(subset=["symbol_id"])

        trades = trades.join(dim_symbol.lazy(), on="symbol_id", how="left")

        # Decode fixed-point integers
        trades = trades.with_columns(
            [
                (pl.col("px_int") * pl.col("price_increment")).alias("px"),
                (pl.col("qty_int") * pl.col("amount_increment")).alias("qty"),
            ]
        )

        # Compute notional = abs(px × qty)
        # Always use absolute value to ensure positive dollar volume
        trades = trades.with_columns((pl.col("px") * pl.col("qty")).abs().alias("notional"))

        # Sort for deterministic cumulative sum
        trades = trades.sort(
            [
                "exchange_id",
                "symbol_id",
                "ts_local_us",
                "file_id",
                "file_line_number",
            ]
        )

        # Compute cumulative notional per symbol
        trades = trades.with_columns(
            pl.col("notional").cum_sum().over(["exchange_id", "symbol_id"]).alias("cum_notional")
        )

        # Compute bar_id = floor(cum_notional / dollar_threshold)
        trades = trades.with_columns(
            (pl.col("cum_notional") / config.dollar_threshold)
            .floor()
            .cast(pl.Int64)
            .alias("bar_id")
        )

        # Group by (exchange_id, symbol_id, bar_id), take first timestamp
        # These are bar BOUNDARIES (starts), which we shift forward to get bar ENDS
        spine = trades.group_by(["exchange_id", "symbol_id", "bar_id"]).agg(
            [
                pl.col("ts_local_us").min().alias("bar_start_ts"),
            ]
        )

        # Sort by (exchange_id, symbol_id, bar_start_ts) for correct shifting
        spine = spine.sort(["exchange_id", "symbol_id", "bar_start_ts"])

        # Shift forward: bar_end[k] = bar_start[k+1], last bar gets end_ts_us
        # This converts bar STARTS to bar ENDS as required by the protocol
        spine = spine.with_columns(
            pl.col("bar_start_ts")
            .shift(-1)
            .over(["exchange_id", "symbol_id"])
            .fill_null(end_ts_us)
            .alias("ts_local_us")
        )

        # Sort by bar-end timestamps
        spine = spine.sort(["exchange_id", "symbol_id", "ts_local_us"])

        # Enforce max_rows safety limit (eager check)
        row_count = spine.select(pl.len()).collect().item()
        if row_count > config.max_rows:
            raise RuntimeError(
                f"Dollar spine would generate too many rows: {row_count:,} > {config.max_rows:,}. "
                f"Increase dollar_threshold or max_rows."
            )

        # Select final columns
        spine = spine.select(["ts_local_us", "exchange_id", "symbol_id"])

        return spine


# Auto-register on module import
register_builder(DollarSpineBuilder())
