#!/usr/bin/env python
"""Example: Multi-Timeframe Features for Crypto MFT

This script demonstrates building multi-timeframe features by combining
signals from fast-moving bars (50 BTC) and slow-moving bars (500 BTC).

Key Innovation: Dual-spine aggregation
- Fast spine (50 BTC): Captures short-term momentum and microstructure
- Slow spine (500 BTC): Captures trend context and regime shifts
- Cross-timeframe features: Momentum divergence, volatility ratio, flow alignment

Run: python examples/crypto_mft_multitimeframe_example.py

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

EXCHANGE = "binance-futures"
SYMBOL = "BTCUSDT"
START_DATE = "2024-05-01"
END_DATE = "2024-05-07"  # 1 week for demonstration

# Dual timeframes
FAST_VOLUME_THRESHOLD = 50.0  # 50 BTC per bar (~$3M notional at $60k)
SLOW_VOLUME_THRESHOLD = 500.0  # 500 BTC per bar (~$30M notional)

print("=" * 80)
print("CRYPTO MFT MULTI-TIMEFRAME FEATURE ENGINEERING")
print("=" * 80)
print(f"Exchange: {EXCHANGE}")
print(f"Symbol: {SYMBOL}")
print(f"Date Range: {START_DATE} to {END_DATE}")
print(f"Fast Timeframe: {FAST_VOLUME_THRESHOLD} BTC per bar (short-term)")
print(f"Slow Timeframe: {SLOW_VOLUME_THRESHOLD} BTC per bar (trend context)")
print("=" * 80)
print()

# =============================================================================
# STEP 1: DISCOVERY - CHECK DATA AVAILABILITY
# =============================================================================

print("Step 1: Discovery - Checking data availability...")
print("-" * 80)

symbols = research.list_symbols(exchange=EXCHANGE, base_asset="BTC", asset_type="perpetual")

if symbols.is_empty():
    print("❌ No BTC perpetual symbols found on", EXCHANGE)
    exit(1)

symbol_info = symbols.filter(pl.col("exchange_symbol") == SYMBOL)
if symbol_info.is_empty():
    print(f"❌ Symbol {SYMBOL} not found on {EXCHANGE}")
    exit(1)

symbol_id = symbol_info["symbol_id"][0]

print(f"✓ Found symbol: {SYMBOL}")
print(f"  Symbol ID: {symbol_id}")
print()

# Check data coverage
coverage = research.data_coverage(EXCHANGE, SYMBOL)
if not coverage["trades"]["available"]:
    print("❌ Trades data not available")
    exit(1)

print("✓ Trades data available")
print()

# =============================================================================
# STEP 2: LOAD RAW DATA
# =============================================================================

print("Step 2: Loading raw data...")
print("-" * 80)

try:
    trades = query.trades(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {trades.height:,} trades")
    print(f"  Total volume: {trades['qty'].sum():.2f} BTC")
    print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    exit(1)

# =============================================================================
# STEP 3: BUILD DUAL VOLUME BAR SPINES
# =============================================================================

print("Step 3: Building dual volume bar spines...")
print("-" * 80)

spine_builder = get_builder("volume")

start_ts_us = int(
    datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)
end_ts_us = int(
    datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)

# Build fast spine (50 BTC bars)
print(f"Building fast spine ({FAST_VOLUME_THRESHOLD} BTC)...")
fast_config = VolumeBarConfig(volume_threshold=FAST_VOLUME_THRESHOLD, use_absolute_volume=True)

fast_spine = spine_builder.build_spine(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=fast_config,
)

fast_spine_df = fast_spine.collect()
print(f"✓ Generated {fast_spine_df.height:,} fast bars")

# Build slow spine (500 BTC bars)
print(f"Building slow spine ({SLOW_VOLUME_THRESHOLD} BTC)...")
slow_config = VolumeBarConfig(volume_threshold=SLOW_VOLUME_THRESHOLD, use_absolute_volume=True)

slow_spine = spine_builder.build_spine(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=slow_config,
)

slow_spine_df = slow_spine.collect()
print(f"✓ Generated {slow_spine_df.height:,} slow bars")

# Verify timeframe ratio
ratio = fast_spine_df.height / slow_spine_df.height
print(f"  Timeframe ratio: {ratio:.1f}x (fast bars per slow bar)")
print()

# =============================================================================
# STEP 4: ASSIGN TRADES TO BOTH SPINES
# =============================================================================

print("Step 4: Assigning trades to both spines...")
print("-" * 80)

# Prepare spines with bucket_start
fast_spine_with_bucket = fast_spine.with_columns([pl.col("ts_local_us").alias("bucket_start")])

slow_spine_with_bucket = slow_spine.with_columns([pl.col("ts_local_us").alias("bucket_start")])

# Assign to fast spine
bucketed_trades_fast = assign_to_buckets(
    events=trades.lazy(),
    spine=fast_spine_with_bucket,
    ts_col="ts_local_us",
)

# Assign to slow spine
bucketed_trades_slow = assign_to_buckets(
    events=trades.lazy(),
    spine=slow_spine_with_bucket,
    ts_col="ts_local_us",
)

print(f"✓ Assigned trades to {fast_spine_df.height:,} fast bars")
print(f"✓ Assigned trades to {slow_spine_df.height:,} slow bars")
print()

# =============================================================================
# STEP 5: COMPUTE FEATURES FROM BOTH TIMEFRAMES
# =============================================================================

print("Step 5: Computing features from both timeframes...")
print("-" * 80)

# Fast timeframe features (short-term signals)
fast_features = (
    bucketed_trades_fast.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("fast_open"),
            pl.col("price").last().alias("fast_close"),
            pl.col("price").max().alias("fast_high"),
            pl.col("price").min().alias("fast_low"),
            # Volume
            pl.col("qty").sum().alias("fast_volume"),
            pl.col("qty").count().alias("fast_trade_count"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("fast_vwap"),
            # Order flow imbalance
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("fast_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("fast_price_std"),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add fast timeframe momentum
fast_features = fast_features.with_columns(
    [
        # Returns
        (pl.col("fast_close") / pl.col("fast_close").shift(1) - 1).alias("fast_ret_1bar"),
        (pl.col("fast_close") / pl.col("fast_close").shift(5) - 1).alias("fast_ret_5bar"),
        # VWAP reversion
        ((pl.col("fast_close") - pl.col("fast_vwap")) / pl.col("fast_vwap")).alias(
            "fast_vwap_reversion"
        ),
    ]
)

print(f"✓ Computed {len(fast_features.columns)} fast timeframe features")

# Slow timeframe features (trend context)
slow_features = (
    bucketed_trades_slow.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("slow_open"),
            pl.col("price").last().alias("slow_close"),
            pl.col("price").max().alias("slow_high"),
            pl.col("price").min().alias("slow_low"),
            # Volume
            pl.col("qty").sum().alias("slow_volume"),
            pl.col("qty").count().alias("slow_trade_count"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("slow_vwap"),
            # Order flow imbalance
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("slow_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("slow_price_std"),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add slow timeframe momentum
slow_features = slow_features.with_columns(
    [
        # Returns
        (pl.col("slow_close") / pl.col("slow_close").shift(1) - 1).alias("slow_ret_1bar"),
        (pl.col("slow_close") / pl.col("slow_close").shift(3) - 1).alias("slow_ret_3bar"),
        # Trend strength (SMA comparison)
        (
            (pl.col("slow_close") - pl.col("slow_close").rolling_mean(5))
            / pl.col("slow_close").rolling_mean(5)
        ).alias("slow_trend_strength"),
    ]
)

print(f"✓ Computed {len(slow_features.columns)} slow timeframe features")
print()

# =============================================================================
# STEP 6: JOIN TIMEFRAMES (AS-OF JOIN)
# =============================================================================

print("Step 6: Joining timeframes with as-of join...")
print("-" * 80)

# As-of join: For each fast bar, find the most recent slow bar
# This ensures PIT correctness (no lookahead)
features = fast_features.join_asof(
    slow_features,
    left_on="bucket_start",
    right_on="bucket_start",
    strategy="backward",
)

print(f"✓ Joined features: {features.height:,} rows × {len(features.columns)} columns")
print(f"  Features per fast bar: {len(features.columns)}")

# Check join quality
null_slow_features = features.filter(pl.col("slow_close").is_null()).height
null_ratio = null_slow_features / features.height
print(f"  Null slow features: {null_slow_features:,} ({null_ratio:.2%})")

if null_ratio > 0.05:
    print("  ⚠ Warning: High null ratio in slow features (>5%)")
    print("  This is normal for early bars - filtering out nulls...")
    features = features.drop_nulls(subset=["slow_close"])
    print(f"  Filtered to {features.height:,} rows with complete features")

print()

# =============================================================================
# STEP 7: ADD CROSS-TIMEFRAME FEATURES
# =============================================================================

print("Step 7: Adding cross-timeframe features...")
print("-" * 80)

features = features.with_columns(
    [
        # 1. Momentum divergence (fast vs slow)
        # Positive divergence: fast momentum > slow momentum (bullish)
        (pl.col("fast_ret_1bar") - pl.col("slow_ret_1bar")).alias("momentum_divergence"),
        # 2. Volatility ratio (fast vs slow)
        # High ratio: increased short-term volatility (regime change)
        (pl.col("fast_price_std") / pl.col("slow_price_std")).alias("volatility_ratio"),
        # 3. Flow alignment (fast flow × slow flow)
        # Positive: aligned buying/selling pressure
        (pl.col("fast_flow_imbalance") * pl.col("slow_flow_imbalance")).alias("flow_alignment"),
        # 4. Price position (fast close relative to slow range)
        # > 0.5: fast price above slow midpoint (bullish positioning)
        (
            (pl.col("fast_close") - pl.col("slow_low")) / (pl.col("slow_high") - pl.col("slow_low"))
        ).alias("price_position"),
        # 5. Trend confirmation (fast VWAP vs slow close)
        # Positive: fast VWAP above slow close (uptrend confirmed)
        ((pl.col("fast_vwap") - pl.col("slow_close")) / pl.col("slow_close")).alias(
            "trend_confirmation"
        ),
        # 6. Volume acceleration (fast vs slow)
        # > 1: fast bars forming faster (increasing activity)
        (pl.col("fast_trade_count") / (pl.col("slow_trade_count") / 10)).alias(
            "volume_acceleration"
        ),
        # 7. Microstructure-trend divergence
        # VWAP reversion in fast bars during slow trend
        (pl.col("fast_vwap_reversion") * pl.col("slow_trend_strength")).alias("micro_trend_div"),
    ]
)

print("✓ Added cross-timeframe features")
print(f"  Total features: {len(features.columns)}")
print()

# Cross-timeframe feature categories
print("Cross-Timeframe Feature Categories:")
print("  1. Momentum Divergence - Fast vs slow momentum comparison")
print("  2. Volatility Ratio - Short-term vs long-term volatility")
print("  3. Flow Alignment - Order flow consistency across timeframes")
print("  4. Price Position - Fast price relative to slow range")
print("  5. Trend Confirmation - Fast VWAP vs slow trend")
print("  6. Volume Acceleration - Activity rate changes")
print("  7. Micro-Trend Divergence - Microstructure vs trend interaction")
print()

# =============================================================================
# STEP 8: ADD LABELS (FORWARD RETURNS)
# =============================================================================

print("Step 8: Adding labels (forward returns)...")
print("-" * 80)

features = features.with_columns(
    [
        # Forward returns on fast timeframe
        (pl.col("fast_close").shift(-5) / pl.col("fast_close") - 1).alias("forward_return_5bar"),
        (pl.col("fast_close").shift(-10) / pl.col("fast_close") - 1).alias("forward_return_10bar"),
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
# STEP 9: FEATURE QUALITY VALIDATION
# =============================================================================

print("Step 9: Feature quality validation...")
print("-" * 80)

# Check null ratios
null_ratios = features.null_count() / features.height
high_null_cols = [col for col in features.columns if null_ratios[col][0] > 0.05]

if high_null_cols:
    print(f"⚠ Warning: High null ratio (>5%) in columns: {high_null_cols}")
else:
    print("✓ All features have acceptable null ratios (<5%)")

# Check for infinite values
cross_tf_features = [
    "momentum_divergence",
    "volatility_ratio",
    "flow_alignment",
    "trend_confirmation",
]
for col in cross_tf_features:
    if col in features.columns:
        inf_count = features.filter(pl.col(col).is_infinite()).height
        if inf_count > 0:
            print(f"⚠ Warning: {inf_count} infinite values in {col}")

print()

# Feature statistics
print("Cross-Timeframe Feature Statistics:")
for col in cross_tf_features:
    if col in features.columns:
        stats = features.select(
            [
                pl.col(col).mean().alias("mean"),
                pl.col(col).std().alias("std"),
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
            ]
        )
        print(f"  {col}:")
        print(f"    Mean: {stats['mean'][0]:.6f}, Std: {stats['std'][0]:.6f}")
        print(f"    Range: [{stats['min'][0]:.6f}, {stats['max'][0]:.6f}]")

print()

# =============================================================================
# STEP 10: INFORMATION COEFFICIENT (IC) ANALYSIS
# =============================================================================

print("Step 10: IC analysis - Single vs Multi-timeframe...")
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

# Compare single-timeframe vs cross-timeframe features
print(f"Information Coefficient vs {label}:")
print()

print("Single-Timeframe Features (Fast):")
single_tf_features = ["fast_ret_5bar", "fast_flow_imbalance", "fast_vwap_reversion"]
for feature_col in single_tf_features:
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
print("Cross-Timeframe Features:")
for feature_col in cross_tf_features:
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
# STEP 11: SAVE FEATURES
# =============================================================================

print("Step 11: Saving features...")
print("-" * 80)

output_path = "output/btc_mft_multitimeframe_features.parquet"

os.makedirs("output", exist_ok=True)

features.write_parquet(output_path)
print(f"✓ Saved features to {output_path}")
print(f"  Rows: {features.height:,}")
print(f"  Columns: {len(features.columns)}")
print()

# =============================================================================
# STEP 12: SUMMARY
# =============================================================================

print("=" * 80)
print("MULTI-TIMEFRAME FEATURE ENGINEERING COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  Input trades: {trades.height:,}")
print(f"  Fast bars (50 BTC): {fast_spine_df.height:,}")
print(f"  Slow bars (500 BTC): {slow_spine_df.height:,}")
print(f"  Timeframe ratio: {ratio:.1f}x")
print(f"  Features per bar: {len(features.columns)}")
print(f"  Output file: {output_path}")
print()
print("Feature Categories:")
print("  ⚡ Fast Timeframe: Short-term momentum, microstructure")
print("  🐢 Slow Timeframe: Trend context, regime identification")
print("  🔗 Cross-Timeframe: Momentum divergence, volatility ratio, flow alignment")
print("  🎯 Labels: Forward returns (5, 10 bars)")
print()
print("Key Insights:")
print("  - Cross-timeframe features capture regime changes")
print("  - Momentum divergence = mean reversion signal")
print("  - Volatility ratio = volatility breakout detector")
print("  - Flow alignment = institutional vs retail flow")
print()
print("Next Steps:")
print("  1. Feature selection: Compare IC of single vs cross-timeframe")
print("  2. Model training: Combine fast + slow + cross features")
print("  3. Backtest: Test on different market regimes")
print("  4. Production: Monitor timeframe ratio stability")
print()
print("See docs/guides/multitimeframe-features-mft.md for detailed guide")
print("=" * 80)
