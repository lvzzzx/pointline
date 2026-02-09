#!/usr/bin/env python
"""Example: Perp-Spot Comparison Features for Crypto MFT

This script demonstrates building perp-spot comparison features by monitoring
the same symbol across perpetual futures and spot markets to detect basis
arbitrage opportunities and lead-lag relationships.

Key Innovation: Dual-instrument aggregation with primary spine
- Perp (perpetual futures): Primary venue (10x volume), drives the spine
- Spot (spot market): Contextual information, assigned to same spine
- Basis features: Perp premium/discount vs spot, funding-basis divergence

Run: python examples/crypto_perp_spot_comparison_example.py

Author: Quant Research
Date: 2026-02-09
"""

import os
from datetime import datetime, timezone

import polars as pl
from scipy.stats import spearmanr

from pointline import research
from pointline.research import query
from pointline.research.resample import assign_to_buckets
from pointline.research.spines import VolumeBarConfig, get_builder

# =============================================================================
# CONFIGURATION
# =============================================================================

# Markets to compare
EXCHANGE_PERP = "binance-futures"  # Perpetual futures (primary)
EXCHANGE_SPOT = "binance-spot"  # Spot market (contextual)

SYMBOL = "BTCUSDT"
START_DATE = "2024-05-01"
END_DATE = "2024-05-07"  # 1 week for demonstration

# Volume bar threshold (based on PERP volume, since perp is primary)
VOLUME_THRESHOLD = 100.0  # 100 BTC per bar (perp-driven)

# Transaction costs
# Perp: Lower costs (no withdrawal, instant settlement)
PERP_TAKER_FEE_BPS = 5.0  # 5 bps per side
PERP_SLIPPAGE_BPS = 2.0  # 2 bps per side
PERP_TOTAL_COST_BPS = (PERP_TAKER_FEE_BPS + PERP_SLIPPAGE_BPS) * 2  # 14 bps

# Spot: Higher costs (includes withdrawal for arbitrage)
SPOT_TAKER_FEE_BPS = 10.0  # 10 bps per side
SPOT_SLIPPAGE_BPS = 3.0  # 3 bps per side
SPOT_WITHDRAWAL_BPS = 8.0  # ~$5 withdrawal fee / $60k = 8 bps
SPOT_TOTAL_COST_BPS = (SPOT_TAKER_FEE_BPS + SPOT_SLIPPAGE_BPS) * 2 + SPOT_WITHDRAWAL_BPS

# Total round-trip cost for cash-and-carry arbitrage
ARBITRAGE_COST_BPS = PERP_TOTAL_COST_BPS + SPOT_TOTAL_COST_BPS  # 40 bps

print("=" * 80)
print("CRYPTO PERP-SPOT COMPARISON FEATURE ENGINEERING")
print("=" * 80)
print(f"Perp Market: {EXCHANGE_PERP} {SYMBOL}")
print(f"Spot Market: {EXCHANGE_SPOT} {SYMBOL}")
print(f"Date Range: {START_DATE} to {END_DATE}")
print(f"Volume Threshold: {VOLUME_THRESHOLD} BTC per bar (perp-driven)")
print(f"Perp Round-Trip Cost: {PERP_TOTAL_COST_BPS} bps")
print(f"Spot Round-Trip Cost: {SPOT_TOTAL_COST_BPS} bps")
print(f"Cash-and-Carry Cost: {ARBITRAGE_COST_BPS} bps")
print("=" * 80)
print()

# =============================================================================
# STEP 1: DISCOVERY - CHECK DATA AVAILABILITY
# =============================================================================

print("Step 1: Discovery - Checking data availability...")
print("-" * 80)

# Find BTCUSDT perpetual
symbols_perp = research.list_symbols(
    exchange=EXCHANGE_PERP, base_asset="BTC", quote_asset="USDT", asset_type="perpetual"
)

# Find BTCUSDT spot
symbols_spot = research.list_symbols(
    exchange=EXCHANGE_SPOT, base_asset="BTC", quote_asset="USDT", asset_type="spot"
)

if symbols_perp.is_empty():
    print(f"❌ BTCUSDT perpetual not found on {EXCHANGE_PERP}")
    exit(1)

if symbols_spot.is_empty():
    print(f"❌ BTCUSDT spot not found on {EXCHANGE_SPOT}")
    exit(1)

symbol_id_perp = symbols_perp["symbol_id"][0]
symbol_id_spot = symbols_spot["symbol_id"][0]

print(f"✓ Found {SYMBOL} perp on {EXCHANGE_PERP} (symbol_id={symbol_id_perp})")
print(f"✓ Found {SYMBOL} spot on {EXCHANGE_SPOT} (symbol_id={symbol_id_spot})")
print()

# Check data coverage
coverage_perp = research.data_coverage(EXCHANGE_PERP, SYMBOL)
coverage_spot = research.data_coverage(EXCHANGE_SPOT, SYMBOL)

if not coverage_perp["trades"]["available"]:
    print(f"❌ Perp trades data not available on {EXCHANGE_PERP}")
    exit(1)

if not coverage_spot["trades"]["available"]:
    print(f"❌ Spot trades data not available on {EXCHANGE_SPOT}")
    exit(1)

if not coverage_perp["derivative_ticker"]["available"]:
    print(f"❌ Funding rate data not available on {EXCHANGE_PERP}")
    exit(1)

print("✓ Perp trades data available")
print("✓ Spot trades data available")
print("✓ Funding rate data available")
print()

# =============================================================================
# STEP 2: LOAD RAW DATA FROM BOTH MARKETS
# =============================================================================

print("Step 2: Loading raw data from both markets...")
print("-" * 80)

try:
    # Load perp trades
    print(f"Loading perp trades from {EXCHANGE_PERP}...")
    trades_perp = query.trades(
        exchange=EXCHANGE_PERP,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {trades_perp.height:,} perp trades")
    print(f"  Total volume: {trades_perp['qty'].sum():.2f} BTC")
    print(f"  Price range: ${trades_perp['price'].min():.2f} - ${trades_perp['price'].max():.2f}")
    print()

    # Load spot trades
    print(f"Loading spot trades from {EXCHANGE_SPOT}...")
    trades_spot = query.trades(
        exchange=EXCHANGE_SPOT,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {trades_spot.height:,} spot trades")
    print(f"  Total volume: {trades_spot['qty'].sum():.2f} BTC")
    print(f"  Price range: ${trades_spot['price'].min():.2f} - ${trades_spot['price'].max():.2f}")
    print()

    # Load funding rate data
    print(f"Loading funding rate data from {EXCHANGE_PERP}...")
    funding = query.derivative_ticker(
        exchange=EXCHANGE_PERP,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {funding.height:,} funding rate snapshots")
    print()

    # Volume comparison
    perp_total_volume = trades_perp["qty"].sum()
    spot_total_volume = trades_spot["qty"].sum()
    volume_ratio = perp_total_volume / spot_total_volume

    print("Volume Comparison:")
    print(f"  Perp volume: {perp_total_volume:,.2f} BTC")
    print(f"  Spot volume: {spot_total_volume:,.2f} BTC")
    print(f"  Perp/Spot ratio: {volume_ratio:.2f}x")
    print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    exit(1)

# =============================================================================
# STEP 3: BUILD PRIMARY SPINE (PERP-DRIVEN)
# =============================================================================

print("Step 3: Building primary spine from PERP volume...")
print("-" * 80)
print("⚡ Using Option 1: Primary Spine (Perp-Driven)")
print("   Rationale: Perp is dominant venue (10x volume), spot is contextual")
print()

spine_builder = get_builder("volume")

start_ts_us = int(
    datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)
end_ts_us = int(
    datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)

volume_config = VolumeBarConfig(volume_threshold=VOLUME_THRESHOLD, use_absolute_volume=True)

# Build spine from PERP trades ONLY
print(f"Building volume bars from {EXCHANGE_PERP} (threshold={VOLUME_THRESHOLD} BTC)...")
spine = spine_builder.build_spine(
    symbol_id=symbol_id_perp,  # Perp drives the spine
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=volume_config,
)

spine_df = spine.collect()
print(f"✓ Generated {spine_df.height:,} volume bars (perp-driven)")
print(
    f"  Average bar duration: {(end_ts_us - start_ts_us) / spine_df.height / 1_000_000:.1f} seconds"
)
print()

# =============================================================================
# STEP 4: ASSIGN BOTH PERP AND SPOT TRADES TO SAME SPINE
# =============================================================================

print("Step 4: Assigning BOTH perp and spot trades to same spine...")
print("-" * 80)

# Prepare spine with bucket_start
spine_with_bucket = spine.with_columns([pl.col("ts_local_us").alias("bucket_start")])

# Assign perp trades to spine
print("Assigning perp trades to spine...")
bucketed_trades_perp = assign_to_buckets(
    events=trades_perp.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)

# Assign spot trades to SAME spine
print("Assigning spot trades to SAME spine...")
bucketed_trades_spot = assign_to_buckets(
    events=trades_spot.lazy(),
    spine=spine_with_bucket,  # Same spine!
    ts_col="ts_local_us",
)

# Assign funding data to same spine
print("Assigning funding data to spine...")
bucketed_funding = assign_to_buckets(
    events=funding.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)

print(f"✓ Assigned perp trades to {spine_df.height:,} bars")
print(f"✓ Assigned spot trades to {spine_df.height:,} bars (same spine)")
print(f"✓ Assigned funding data to {spine_df.height:,} bars")
print()

# =============================================================================
# STEP 5: COMPUTE FEATURES FROM PERP MARKET
# =============================================================================

print("Step 5: Computing features from perp market...")
print("-" * 80)

features_perp = (
    bucketed_trades_perp.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("perp_open"),
            pl.col("price").last().alias("perp_close"),
            pl.col("price").max().alias("perp_high"),
            pl.col("price").min().alias("perp_low"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("perp_vwap"),
            # Volume
            pl.col("qty").sum().alias("perp_volume"),
            pl.col("qty").count().alias("perp_trade_count"),
            # Order flow
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("perp_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("perp_price_std"),
            # Spread (high - low)
            ((pl.col("price").max() - pl.col("price").min()) / pl.col("price").last()).alias(
                "perp_hl_spread"
            ),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add perp momentum features
features_perp = features_perp.with_columns(
    [
        (pl.col("perp_close") / pl.col("perp_close").shift(1) - 1).alias("perp_ret_1bar"),
        (pl.col("perp_close") / pl.col("perp_close").shift(5) - 1).alias("perp_ret_5bar"),
        ((pl.col("perp_close") - pl.col("perp_vwap")) / pl.col("perp_vwap")).alias(
            "perp_vwap_reversion"
        ),
    ]
)

print(f"✓ Computed {len(features_perp.columns)} perp features")

# =============================================================================
# STEP 6: COMPUTE FEATURES FROM SPOT MARKET
# =============================================================================

print("Step 6: Computing features from spot market...")
print("-" * 80)

features_spot = (
    bucketed_trades_spot.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("spot_open"),
            pl.col("price").last().alias("spot_close"),
            pl.col("price").max().alias("spot_high"),
            pl.col("price").min().alias("spot_low"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("spot_vwap"),
            # Volume
            pl.col("qty").sum().alias("spot_volume"),
            pl.col("qty").count().alias("spot_trade_count"),
            # Order flow
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("spot_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("spot_price_std"),
            # Spread (high - low)
            ((pl.col("price").max() - pl.col("price").min()) / pl.col("price").last()).alias(
                "spot_hl_spread"
            ),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add spot momentum features
features_spot = features_spot.with_columns(
    [
        (pl.col("spot_close") / pl.col("spot_close").shift(1) - 1).alias("spot_ret_1bar"),
        (pl.col("spot_close") / pl.col("spot_close").shift(5) - 1).alias("spot_ret_5bar"),
        ((pl.col("spot_close") - pl.col("spot_vwap")) / pl.col("spot_vwap")).alias(
            "spot_vwap_reversion"
        ),
    ]
)

print(f"✓ Computed {len(features_spot.columns)} spot features")

# =============================================================================
# STEP 7: COMPUTE FUNDING FEATURES
# =============================================================================

print("Step 7: Computing funding features...")
print("-" * 80)

features_funding = (
    bucketed_funding.group_by("bucket_start")
    .agg(
        [
            # Funding rate
            pl.col("funding_rate").last().alias("funding_close"),
            (pl.col("funding_rate").last() - pl.col("funding_rate").first()).alias("funding_step"),
            # Predicted funding
            pl.col("predicted_funding_rate").last().alias("predicted_funding_close"),
            # Funding surprise (actual - predicted)
            (pl.col("funding_rate").last() - pl.col("predicted_funding_rate").last()).alias(
                "funding_surprise"
            ),
            # Open interest
            pl.col("open_interest").last().alias("oi_close"),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add derived funding features
features_funding = features_funding.with_columns(
    [
        # Annualized funding carry (3 fundings per day)
        (pl.col("funding_close") * 365 * 3).alias("funding_carry_annual"),
        # Funding-OI pressure
        (pl.col("funding_step") * pl.col("oi_close")).alias("funding_oi_pressure"),
    ]
)

print(f"✓ Computed {len(features_funding.columns)} funding features")

# =============================================================================
# STEP 8: JOIN ALL FEATURES (INNER JOIN - NO NULLS)
# =============================================================================

print("Step 8: Joining all features...")
print("-" * 80)
print("⚡ Using inner join (same spine = perfect alignment, no nulls)")
print()

# Join perp + spot (inner join, no nulls)
features = features_perp.join(features_spot, on="bucket_start", how="inner")

# Join funding (left join, some nulls possible)
features = features.join(features_funding, on="bucket_start", how="left")

print(f"✓ Joined features: {features.height:,} rows × {len(features.columns)} columns")

# Check spot volume distribution
spot_vol_stats = features.select(
    [
        pl.col("spot_volume").min().alias("min"),
        pl.col("spot_volume").quantile(0.25).alias("q25"),
        pl.col("spot_volume").median().alias("median"),
        pl.col("spot_volume").mean().alias("mean"),
        pl.col("spot_volume").quantile(0.75).alias("q75"),
        pl.col("spot_volume").max().alias("max"),
    ]
).to_dicts()[0]

print()
print("Spot volume distribution per bar (BTC):")
print(
    f"  Min: {spot_vol_stats['min']:.2f}, "
    f"Q25: {spot_vol_stats['q25']:.2f}, "
    f"Median: {spot_vol_stats['median']:.2f}"
)
print(
    f"  Mean: {spot_vol_stats['mean']:.2f}, "
    f"Q75: {spot_vol_stats['q75']:.2f}, "
    f"Max: {spot_vol_stats['max']:.2f}"
)
print()

# Check for low spot volume bars
low_volume_bars = features.filter(pl.col("spot_volume") < 5.0).height
low_volume_ratio = low_volume_bars / features.height

print(f"Bars with low spot volume (<5 BTC): {low_volume_bars:,} ({low_volume_ratio:.2%})")
if low_volume_ratio > 0.1:
    print("  ⚠ Warning: >10% of bars have low spot volume")
    print("  This indicates perp is strongly leading spot (expected)")
print()

# =============================================================================
# STEP 9: ADD PERP-SPOT COMPARISON FEATURES
# =============================================================================

print("Step 9: Adding perp-spot comparison features...")
print("-" * 80)

features = features.with_columns(
    [
        # 1. BASIS (perp premium/discount vs spot)
        # Positive: Perp more expensive (bullish leverage demand)
        # Negative: Perp cheaper (bearish sentiment)
        (((pl.col("perp_close") - pl.col("spot_close")) / pl.col("spot_close")) * 10000).alias(
            "basis_bps"
        ),
        # 2. VWAP basis (more robust)
        (((pl.col("perp_vwap") - pl.col("spot_vwap")) / pl.col("spot_vwap")) * 10000).alias(
            "vwap_basis_bps"
        ),
        # 3. Basis momentum (widening/narrowing)
        (pl.col("basis_bps") - pl.col("basis_bps").shift(1)).alias("basis_momentum"),
        # 4. FUNDING-BASIS DIVERGENCE (key arbitrage signal)
        # When funding > basis: Perp overpriced, short perp / long spot
        # When funding < basis: Perp underpriced, long perp / short spot
        (
            pl.col("funding_close")
            - (pl.col("basis_bps") / 10000 / 3)  # Convert 8h funding to same units
        ).alias("funding_basis_divergence"),
        # 5. CASH-AND-CARRY OPPORTUNITY
        # Profitable if basis > total costs
        (pl.col("basis_bps").abs() > ARBITRAGE_COST_BPS).alias("cash_carry_opportunity"),
        # Expected profit from cash-and-carry
        (pl.col("basis_bps").abs() - ARBITRAGE_COST_BPS).alias("cash_carry_profit_bps"),
        # 6. LEAD-LAG (momentum divergence)
        # Positive: Perp leading (reacts faster)
        (pl.col("perp_ret_1bar") - pl.col("spot_ret_1bar")).alias("momentum_divergence"),
        # 7. VOLUME RATIO (leverage appetite)
        # High ratio: Strong leverage demand (bullish sentiment)
        (pl.col("perp_volume") / pl.col("spot_volume")).alias("volume_ratio"),
        # 8. FLOW DIVERGENCE (sentiment difference)
        (pl.col("perp_flow_imbalance") - pl.col("spot_flow_imbalance")).alias("flow_divergence"),
        # 9. VOLATILITY RATIO (leverage amplifies volatility)
        (pl.col("perp_price_std") / pl.col("spot_price_std")).alias("volatility_ratio"),
        # 10. SPREAD RATIO (relative liquidity)
        (pl.col("perp_hl_spread") / pl.col("spot_hl_spread")).alias("spread_ratio"),
        # 11. FLOW-BASIS INTERACTION (informed flow)
        # Positive flow + positive basis = strong bullish signal
        (pl.col("perp_flow_imbalance") * pl.col("basis_bps")).alias("flow_basis_interaction"),
    ]
)

print("✓ Added perp-spot comparison features")
print(f"  Total features: {len(features.columns)}")
print()

# Feature categories
print("Perp-Spot Feature Categories:")
print("  1. Basis - Perp premium/discount vs spot")
print("  2. Funding-Basis Divergence - Arbitrage signal")
print("  3. Cash-and-Carry - Opportunity detection")
print("  4. Lead-Lag - Price discovery mechanism")
print("  5. Volume Ratio - Leverage appetite")
print("  6. Flow Divergence - Sentiment differences")
print("  7. Volatility Ratio - Risk comparison")
print("  8. Spread Ratio - Liquidity comparison")
print()

# =============================================================================
# STEP 10: CASH-AND-CARRY OPPORTUNITY ANALYSIS
# =============================================================================

print("Step 10: Cash-and-carry opportunity analysis...")
print("-" * 80)

# Count opportunities
total_bars = features.height
carry_bars = features.filter(pl.col("cash_carry_opportunity")).height
carry_ratio = carry_bars / total_bars

print("Cash-and-Carry Opportunities:")
print(f"  Total bars: {total_bars:,}")
print(f"  Bars with opportunity: {carry_bars:,} ({carry_ratio:.2%})")
print()

if carry_bars > 0:
    carry_features = features.filter(pl.col("cash_carry_opportunity"))

    avg_profit = carry_features["cash_carry_profit_bps"].mean()
    max_profit = carry_features["cash_carry_profit_bps"].max()
    median_profit = carry_features["cash_carry_profit_bps"].median()

    print("Profit Statistics (when opportunity available):")
    print(f"  Average profit: {avg_profit:.2f} bps")
    print(f"  Median profit: {median_profit:.2f} bps")
    print(f"  Max profit: {max_profit:.2f} bps")
    print()

    # Direction analysis
    positive_basis = carry_features.filter(pl.col("basis_bps") > 0).height
    negative_basis = carry_features.filter(pl.col("basis_bps") < 0).height

    print("Opportunity Direction:")
    print(
        f"  Positive basis (short perp, long spot): {positive_basis:,} ({positive_basis / carry_bars:.1%})"
    )
    print(
        f"  Negative basis (long perp, short spot): {negative_basis:,} ({negative_basis / carry_bars:.1%})"
    )
    print()
else:
    print("⚠ No cash-and-carry opportunities detected")
    print("  This is normal for efficient markets")
    print("  Focus on:")
    print("    - Funding-basis divergence (mean reversion)")
    print("    - Lead-lag relationships (momentum trading)")
    print("    - Flow divergence (sentiment signals)")
    print()

# =============================================================================
# STEP 11: ADD LABELS (FORWARD RETURNS)
# =============================================================================

print("Step 11: Adding labels (forward returns on perp)...")
print("-" * 80)

features = features.with_columns(
    [
        # Forward returns on perp
        (pl.col("perp_close").shift(-5) / pl.col("perp_close") - 1).alias("forward_return_5bar"),
        (pl.col("perp_close").shift(-10) / pl.col("perp_close") - 1).alias("forward_return_10bar"),
    ]
)

# Standardize labels
for label_col in ["forward_return_5bar", "forward_return_10bar"]:
    mean = features[label_col].mean()
    std = features[label_col].std()
    features = features.with_columns(
        [((pl.col(label_col) - mean) / std).alias(f"{label_col}_zscore")]
    )

print("✓ Added forward return labels")
print()

# =============================================================================
# STEP 12: INFORMATION COEFFICIENT (IC) ANALYSIS
# =============================================================================

print("Step 12: IC analysis - Perp-spot features...")
print("-" * 80)


def compute_ic(df: pl.DataFrame, signal_col: str, label_col: str):
    """Compute rank IC (Spearman correlation) between signal and label."""
    valid_df = df.select([signal_col, label_col]).drop_nulls()

    if valid_df.height < 100:
        return None

    signal = valid_df[signal_col].to_numpy()
    label = valid_df[label_col].to_numpy()

    ic, p_value = spearmanr(signal, label)

    return ic, p_value


label = "forward_return_5bar_zscore"

print(f"Information Coefficient vs {label}:")
print()

perp_spot_features = [
    "basis_bps",
    "funding_basis_divergence",
    "momentum_divergence",
    "volume_ratio",
    "flow_divergence",
    "volatility_ratio",
    "flow_basis_interaction",
]

for feature_col in perp_spot_features:
    if feature_col in features.columns:
        result = compute_ic(features, feature_col, label)
        if result:
            ic, p_value = result
            significance = (
                "***"
                if p_value < 0.001
                else "**"
                if p_value < 0.01
                else "*"
                if p_value < 0.05
                else ""
            )
            print(f"  {feature_col:30s}: IC = {ic:7.4f} (p={p_value:.4f}) {significance}")

print()
print("Significance: *** p<0.001, ** p<0.01, * p<0.05")
print()

# =============================================================================
# STEP 13: SAVE FEATURES
# =============================================================================

print("Step 13: Saving features...")
print("-" * 80)

output_path = f"output/btc_perp_spot_{EXCHANGE_PERP}_{EXCHANGE_SPOT}.parquet"

os.makedirs("output", exist_ok=True)

features.write_parquet(output_path)
print(f"✓ Saved features to {output_path}")
print(f"  Rows: {features.height:,}")
print(f"  Columns: {len(features.columns)}")
print()

# =============================================================================
# STEP 14: SUMMARY
# =============================================================================

print("=" * 80)
print("PERP-SPOT COMPARISON FEATURE ENGINEERING COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  Perp market: {EXCHANGE_PERP} ({trades_perp.height:,} trades)")
print(f"  Spot market: {EXCHANGE_SPOT} ({trades_spot.height:,} trades)")
print(f"  Volume ratio: {volume_ratio:.2f}x (perp/spot)")
print(f"  Volume bars: {features.height:,} (perp-driven)")
print(f"  Features per bar: {len(features.columns)}")
print(f"  Cash-and-carry opportunities: {carry_ratio:.2%}")
print(f"  Output file: {output_path}")
print()
print("Feature Categories:")
print("  📊 Perp: Price, volume, flow, momentum, funding")
print("  📈 Spot: Price, volume, flow, momentum")
print("  🔗 Basis: Perp premium/discount, funding-basis divergence")
print("  💰 Arbitrage: Cash-and-carry opportunity, expected profit")
print("  🎯 Labels: Forward returns on perp")
print()
print("Key Insights:")
print("  - Basis mean reverts (but slowly, ~hours)")
print("  - Funding-basis divergence = strong mean reversion signal")
print("  - Perp usually leads spot (leverage amplifies price discovery)")
print("  - Volume ratio reveals leverage sentiment")
print("  - Cash-and-carry profitable only during extreme events")
print()
print("Next Steps:")
print("  1. Backtest funding-basis divergence strategy")
print("  2. Analyze basis persistence (holding period optimization)")
print("  3. Model funding rate prediction")
print("  4. Multi-exchange perp-spot comparison")
print()
print("See docs/guides/perp-spot-features-mft.md for detailed guide")
print("=" * 80)
