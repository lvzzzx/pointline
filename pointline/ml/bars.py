"""
Bar generation logic for the Reflexivity Engine.

This module implements "Dollar-Volume Bars" (Notional Bars) with multi-asset alignment.
It consumes Silver Layer tables directly (trades, derivative_ticker, dim_asset_stats, dim_symbol)
to produce statistically aligned features for Regime Detection (HMM).
"""

import polars as pl
from typing import Optional


def build_reflexivity_bars(
    perp_trades: pl.LazyFrame,
    dim_symbol: pl.LazyFrame,
    spot_trades: Optional[pl.LazyFrame] = None,
    derivative_ticker: Optional[pl.LazyFrame] = None,
    asset_stats: Optional[pl.LazyFrame] = None,
    threshold: float = 10_000_000.0,
) -> pl.DataFrame:
    """
    Generate Dollar-Volume Bars aligned with Spot Volume and Market State.

    The bar generation is driven by the Perpetual Contract's volume. A new bar is
    formed every time `threshold` USD is traded on the Perp.

    Args:
        perp_trades: silver.trades LazyFrame [ts_local_us, price_int, qty_int]
        dim_symbol: silver.dim_symbol LazyFrame [symbol_id, valid_from_ts, price_increment, amount_increment]
        spot_trades: silver.trades LazyFrame [ts_local_us, price_int, qty_int]
        derivative_ticker: silver.derivative_ticker LazyFrame [ts_local_us, open_interest, funding_rate]
        asset_stats: silver.dim_asset_stats LazyFrame [date, circulating_supply]
        threshold: Notional value (USD) to trigger a new bar.

    Returns:
        DataFrame with OHLC, Volumes, State Snapshots, and Reflexivity Features.
    """
    
    # Helper to decode integers using dim_symbol history
    def _decode_trades(trades: pl.LazyFrame, symbol_type: str) -> pl.LazyFrame:
        # Filter dim_symbol for relevant updates (assumes trades might span versions)
        # We perform an AS-OF JOIN: match trade.ts >= dim.valid_from_ts
        # Both sides must be sorted by the join key.
        
        # 1. Prepare Trades (Left Side) - assumed sorted by ts_local_us
        left = trades.select([
            pl.col("ts_local_us").alias("ts"),
            pl.col("symbol_id"),
            pl.col("price_int"),
            pl.col("qty_int"),
        ]).sort("ts")

        # 2. Prepare Dim Symbol (Right Side)
        # We need to join on symbol_id AND time. 
        # Polars asof_join allows 'by' argument for exact matches (symbol_id).
        right = dim_symbol.select([
            pl.col("symbol_id"),
            pl.col("valid_from_ts").alias("ts"),
            pl.col("price_increment"),
            pl.col("amount_increment"),
        ]).sort("ts")

        # 3. Join and Decode
        decoded = left.join_asof(
            right,
            on="ts",
            by="symbol_id",
            strategy="backward" # Match the most recent metadata valid before/at trade time
        )
        
        # 4. Standardize Columns for the Stream Merger
        return decoded.select([
            pl.col("ts"),
            pl.lit(f"trade_{symbol_type}").alias("event_type"),
            (pl.col("price_int") * pl.col("price_increment")).alias("price"),
            (pl.col("price_int") * pl.col("price_increment") * 
             pl.col("qty_int") * pl.col("amount_increment")).alias("notional"),
            pl.lit(None).cast(pl.Float64).alias("oi_value"),
            pl.lit(None).cast(pl.Float64).alias("funding_rate"),
            pl.lit(1 if symbol_type == "perp" else 0).alias("is_driver"),
        ])

    # 1. Standardize Streams from Silver Schema
    streams = []

    # Stream A: Perpetual Trades (The Driver)
    s_perp = _decode_trades(perp_trades, "perp")
    streams.append(s_perp)

    # Stream B: Spot Trades (The Sidecar)
    if spot_trades is not None:
        s_spot = _decode_trades(spot_trades, "spot")
        streams.append(s_spot)

    # Stream C/D: Derivative Ticker (OI + Funding)
    if derivative_ticker is not None:
        s_deriv = derivative_ticker.select([
            pl.col("ts_local_us").alias("ts"),
            pl.lit("update_ticker").alias("event_type"),
            pl.lit(None).cast(pl.Float64).alias("price"),
            pl.lit(0.0).alias("notional"),
            pl.col("open_interest").alias("oi_value"),
            pl.col("funding_rate"),
            pl.lit(0).alias("is_driver"),
        ])
        streams.append(s_deriv)

    # 2. Merge and Sort
    combined = pl.concat(streams).sort("ts")

    # 3. Forward Fill State (As-Of Semantics)
    enriched = combined.with_columns([
        pl.col("oi_value").forward_fill(),
        pl.col("funding_rate").forward_fill(),
    ])

    # 4. Filter for Trade Events & Calculate Groups
    trade_stream = enriched.filter(
        pl.col("event_type").is_in(["trade_perp", "trade_spot"])
    )

    # Handle nulls in notional
    trade_stream = trade_stream.with_columns(pl.col("notional").fill_null(0.0))

    # Calculate cumulative volume of the DRIVER (Perp)
    trade_stream = trade_stream.with_columns([
        (pl.col("notional") * pl.col("is_driver")).cum_sum().alias("cum_driver_vol")
    ])

    # Assign Group ID
    trade_stream = trade_stream.with_columns([
        (pl.col("cum_driver_vol") / threshold).floor().cast(pl.Int64).alias("bar_id")
    ])

    # 5. Aggregation
    bars = (
        trade_stream
        .group_by("bar_id")
        .agg([
            # Time Boundaries
            pl.col("ts").first().alias("open_ts"),
            pl.col("ts").last().alias("close_ts"),
            
            # OHLC (Perp)
            pl.col("price").filter(pl.col("is_driver") == 1).first().alias("open"),
            pl.col("price").filter(pl.col("is_driver") == 1).max().alias("high"),
            pl.col("price").filter(pl.col("is_driver") == 1).min().alias("low"),
            pl.col("price").filter(pl.col("is_driver") == 1).last().alias("close"),
            
            # Volume Totals
            pl.col("notional").filter(pl.col("is_driver") == 1).sum().alias("perp_volume"),
            pl.col("notional").filter(pl.col("is_driver") == 0).sum().alias("spot_volume"),
            
            # Tick Counts
            pl.col("ts").filter(pl.col("is_driver") == 1).count().alias("perp_ticks"),
            pl.col("ts").filter(pl.col("is_driver") == 0).count().alias("spot_ticks"),

            # State Snapshots (At Close)
            pl.col("oi_value").last().alias("oi_close"),
            pl.col("funding_rate").last().alias("funding_rate"),
        ])
    )

    # 6. Asset Stats Integration (Circulating Supply)
    if asset_stats is not None:
        # Join on Date. Since asset_stats is Daily, we derive date from ts.
        # ts is microseconds.
        bars = bars.with_columns([
            (pl.col("open_ts") / 1_000_000).cast(pl.Datetime).cast(pl.Date).alias("date_key")
        ])
        
        # We assume asset_stats has ['date', 'circulating_supply']
        # Left join to attach supply
        bars = bars.join(
            asset_stats.select(["date", "circulating_supply"]),
            left_on="date_key",
            right_on="date",
            how="left"
        ).drop("date_key")

    # 7. Derived Features
    bars = bars.with_columns([
        # Duration
        ((pl.col("close_ts") - pl.col("open_ts")) / 1_000_000.0).alias("duration"),
        
        # Speculation Ratio
        (pl.col("perp_volume") / (pl.col("spot_volume").fill_null(0.0) + 1.0)).alias("speculation_ratio"),
    ])

    # Leverage Strain: OI / (Supply * Price)
    # Requires supply to be present
    if "circulating_supply" in bars.columns:
        bars = bars.with_columns([
            (
                pl.col("oi_close") / 
                (pl.col("circulating_supply") * pl.col("close"))
            ).alias("leverage_strain")
        ])

    return bars.collect()
