#!/usr/bin/env python
"""Example: Cross-Exchange Arbitrage Features for Crypto

This script demonstrates building cross-exchange arbitrage features by
monitoring the same symbol (BTCUSDT) across multiple exchanges and detecting
price discrepancies that can be exploited for profit.

Key Innovation: Dual-exchange aggregation
- Exchange A (e.g., Binance): Primary exchange with high liquidity
- Exchange B (e.g., Coinbase): Secondary exchange for arbitrage
- Cross-exchange features: Price spread, lead-lag, volume imbalance

Run: python examples/crypto_cross_exchange_arbitrage_example.py

Author: Quant Research
Date: 2026-02-09
"""

import os
from datetime import datetime, timezone

import polars as pl
from pointline.research.resample import assign_to_buckets
from pointline.research.spines import VolumeBarConfig, get_builder
from scipy.stats import spearmanr

from pointline import research
from pointline.research import query

# =============================================================================
# CONFIGURATION
# =============================================================================

# Exchanges to compare (both must have BTCUSDT perpetual)
EXCHANGE_A = "binance-futures"  # Primary (typically highest liquidity)
EXCHANGE_B = "bybit"  # Secondary (for arbitrage opportunities)

SYMBOL = "BTCUSDT"
START_DATE = "2024-05-01"
END_DATE = "2024-05-07"  # 1 week for demonstration

# Volume bar threshold (same for both exchanges for fair comparison)
VOLUME_THRESHOLD = 100.0  # 100 BTC per bar

# Transaction costs (typical for perpetual futures)
TAKER_FEE_BPS = 5.0  # 5 bps (0.05%) per side
SLIPPAGE_BPS = 2.0  # 2 bps estimated slippage per side
TOTAL_COST_BPS = (TAKER_FEE_BPS + SLIPPAGE_BPS) * 2  # Round-trip cost

print("=" * 80)
print("CRYPTO CROSS-EXCHANGE ARBITRAGE FEATURE ENGINEERING")
print("=" * 80)
print(f"Exchange A (Primary): {EXCHANGE_A}")
print(f"Exchange B (Secondary): {EXCHANGE_B}")
print(f"Symbol: {SYMBOL}")
print(f"Date Range: {START_DATE} to {END_DATE}")
print(f"Volume Threshold: {VOLUME_THRESHOLD} BTC per bar")
print(f"Round-Trip Cost: {TOTAL_COST_BPS} bps")
print("=" * 80)
print()

# =============================================================================
# STEP 1: DISCOVERY - CHECK DATA AVAILABILITY
# =============================================================================

print("Step 1: Discovery - Checking data availability...")
print("-" * 80)

# Find BTCUSDT perpetual on both exchanges
symbols_a = research.list_symbols(
    exchange=EXCHANGE_A, base_asset="BTC", quote_asset="USDT", asset_type="perpetual"
)

symbols_b = research.list_symbols(
    exchange=EXCHANGE_B, base_asset="BTC", quote_asset="USDT", asset_type="perpetual"
)

if symbols_a.is_empty():
    print(f"❌ BTCUSDT not found on {EXCHANGE_A}")
    exit(1)

if symbols_b.is_empty():
    print(f"❌ BTCUSDT not found on {EXCHANGE_B}")
    print("⚠ Note: This example requires data from two exchanges")
    print("Available exchanges:", research.list_exchanges()["exchange"].to_list())
    exit(1)

symbol_id_a = symbols_a["symbol_id"][0]
symbol_id_b = symbols_b["symbol_id"][0]

print(f"✓ Found {SYMBOL} on {EXCHANGE_A} (symbol_id={symbol_id_a})")
print(f"✓ Found {SYMBOL} on {EXCHANGE_B} (symbol_id={symbol_id_b})")
print()

# Check data coverage for both exchanges
coverage_a = research.data_coverage(EXCHANGE_A, SYMBOL)
coverage_b = research.data_coverage(EXCHANGE_B, SYMBOL)

if not coverage_a["trades"]["available"]:
    print(f"❌ Trades data not available on {EXCHANGE_A}")
    exit(1)

if not coverage_b["trades"]["available"]:
    print(f"❌ Trades data not available on {EXCHANGE_B}")
    exit(1)

print("✓ Trades data available on both exchanges")
print()

# =============================================================================
# STEP 2: LOAD RAW DATA FROM BOTH EXCHANGES
# =============================================================================

print("Step 2: Loading raw data from both exchanges...")
print("-" * 80)

try:
    # Load trades from Exchange A
    print(f"Loading trades from {EXCHANGE_A}...")
    trades_a = query.trades(
        exchange=EXCHANGE_A,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {trades_a.height:,} trades from {EXCHANGE_A}")
    print(f"  Total volume: {trades_a['qty'].sum():.2f} BTC")
    print(f"  Price range: ${trades_a['price'].min():.2f} - ${trades_a['price'].max():.2f}")
    print()

    # Load trades from Exchange B
    print(f"Loading trades from {EXCHANGE_B}...")
    trades_b = query.trades(
        exchange=EXCHANGE_B,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {trades_b.height:,} trades from {EXCHANGE_B}")
    print(f"  Total volume: {trades_b['qty'].sum():.2f} BTC")
    print(f"  Price range: ${trades_b['price'].min():.2f} - ${trades_b['price'].max():.2f}")
    print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    exit(1)

# =============================================================================
# STEP 3: BUILD VOLUME BAR SPINES FOR BOTH EXCHANGES
# =============================================================================

print("Step 3: Building volume bar spines for both exchanges...")
print("-" * 80)

spine_builder = get_builder("volume")

start_ts_us = int(
    datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)
end_ts_us = int(
    datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)

volume_config = VolumeBarConfig(volume_threshold=VOLUME_THRESHOLD, use_absolute_volume=True)

# Build spine for Exchange A
print(f"Building volume bars for {EXCHANGE_A}...")
spine_a = spine_builder.build_spine(
    symbol_id=symbol_id_a,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=volume_config,
)

spine_a_df = spine_a.collect()
print(f"✓ Generated {spine_a_df.height:,} volume bars on {EXCHANGE_A}")

# Build spine for Exchange B
print(f"Building volume bars for {EXCHANGE_B}...")
spine_b = spine_builder.build_spine(
    symbol_id=symbol_id_b,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=volume_config,
)

spine_b_df = spine_b.collect()
print(f"✓ Generated {spine_b_df.height:,} volume bars on {EXCHANGE_B}")
print()

# =============================================================================
# STEP 4: ASSIGN TRADES TO VOLUME BARS
# =============================================================================

print("Step 4: Assigning trades to volume bars...")
print("-" * 80)

# Prepare spines with bucket_start
spine_a_with_bucket = spine_a.with_columns([pl.col("ts_local_us").alias("bucket_start")])

spine_b_with_bucket = spine_b.with_columns([pl.col("ts_local_us").alias("bucket_start")])

# Assign trades to Exchange A bars
bucketed_trades_a = assign_to_buckets(
    events=trades_a.lazy(),
    spine=spine_a_with_bucket,
    ts_col="ts_local_us",
)

# Assign trades to Exchange B bars
bucketed_trades_b = assign_to_buckets(
    events=trades_b.lazy(),
    spine=spine_b_with_bucket,
    ts_col="ts_local_us",
)

print(f"✓ Assigned trades to {spine_a_df.height:,} bars on {EXCHANGE_A}")
print(f"✓ Assigned trades to {spine_b_df.height:,} bars on {EXCHANGE_B}")
print()

# =============================================================================
# STEP 5: COMPUTE FEATURES FROM BOTH EXCHANGES
# =============================================================================

print("Step 5: Computing features from both exchanges...")
print("-" * 80)

# Features from Exchange A
features_a = (
    bucketed_trades_a.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("a_open"),
            pl.col("price").last().alias("a_close"),
            pl.col("price").max().alias("a_high"),
            pl.col("price").min().alias("a_low"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("a_vwap"),
            # Volume
            pl.col("qty").sum().alias("a_volume"),
            pl.col("qty").count().alias("a_trade_count"),
            # Order flow
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("a_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("a_price_std"),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add Exchange A momentum
features_a = features_a.with_columns(
    [
        (pl.col("a_close") / pl.col("a_close").shift(1) - 1).alias("a_ret_1bar"),
        ((pl.col("a_close") - pl.col("a_vwap")) / pl.col("a_vwap")).alias("a_vwap_reversion"),
    ]
)

print(f"✓ Computed {len(features_a.columns)} features from {EXCHANGE_A}")

# Features from Exchange B
features_b = (
    bucketed_trades_b.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("b_open"),
            pl.col("price").last().alias("b_close"),
            pl.col("price").max().alias("b_high"),
            pl.col("price").min().alias("b_low"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("b_vwap"),
            # Volume
            pl.col("qty").sum().alias("b_volume"),
            pl.col("qty").count().alias("b_trade_count"),
            # Order flow
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("b_flow_imbalance"),
            # Volatility
            pl.col("price").std().alias("b_price_std"),
        ]
    )
    .sort("bucket_start")
    .collect()
)

# Add Exchange B momentum
features_b = features_b.with_columns(
    [
        (pl.col("b_close") / pl.col("b_close").shift(1) - 1).alias("b_ret_1bar"),
        ((pl.col("b_close") - pl.col("b_vwap")) / pl.col("b_vwap")).alias("b_vwap_reversion"),
    ]
)

print(f"✓ Computed {len(features_b.columns)} features from {EXCHANGE_B}")
print()

# =============================================================================
# STEP 6: JOIN EXCHANGES (AS-OF JOIN)
# =============================================================================

print("Step 6: Joining exchanges with as-of join...")
print("-" * 80)

# As-of join: For each Exchange A bar, find most recent Exchange B bar
# This ensures PIT correctness (no lookahead)
features = features_a.join_asof(
    features_b,
    left_on="bucket_start",
    right_on="bucket_start",
    strategy="backward",
)

print(f"✓ Joined features: {features.height:,} rows × {len(features.columns)} columns")

# Check join quality
null_b_features = features.filter(pl.col("b_close").is_null()).height
null_ratio = null_b_features / features.height
print(f"  Null Exchange B features: {null_b_features:,} ({null_ratio:.2%})")

if null_ratio > 0.05:
    print("  ⚠ Warning: High null ratio (>5%) - filtering nulls...")
    features = features.drop_nulls(subset=["b_close"])
    print(f"  Filtered to {features.height:,} rows with complete features")

print()

# =============================================================================
# STEP 7: ADD CROSS-EXCHANGE ARBITRAGE FEATURES
# =============================================================================

print("Step 7: Adding cross-exchange arbitrage features...")
print("-" * 80)

features = features.with_columns(
    [
        # 1. Price spread (absolute)
        # Positive: Exchange A more expensive (sell A, buy B)
        (pl.col("a_close") - pl.col("b_close")).alias("price_spread"),
        # 2. Price spread (normalized in bps)
        # Spread relative to average price
        (
            (
                (pl.col("a_close") - pl.col("b_close"))
                / ((pl.col("a_close") + pl.col("b_close")) / 2)
            )
            * 10000
        ).alias("spread_bps"),
        # 3. VWAP spread (more robust than close spread)
        (pl.col("a_vwap") - pl.col("b_vwap")).alias("vwap_spread"),
        (
            ((pl.col("a_vwap") - pl.col("b_vwap")) / ((pl.col("a_vwap") + pl.col("b_vwap")) / 2))
            * 10000
        ).alias("vwap_spread_bps"),
        # 4. Arbitrage opportunity (spread exceeds transaction costs)
        # Profitable if |spread_bps| > total_cost_bps
        (pl.col("spread_bps").abs() > TOTAL_COST_BPS).alias("arb_opportunity"),
        # 5. Expected profit (spread - costs)
        (pl.col("spread_bps").abs() - TOTAL_COST_BPS).alias("expected_profit_bps"),
        # 6. Lead-lag relationship (momentum divergence)
        # Positive: Exchange A leading (reacts faster to news)
        (pl.col("a_ret_1bar") - pl.col("b_ret_1bar")).alias("momentum_divergence"),
        # 7. Volume imbalance across exchanges
        # Positive: More volume on A (liquidity advantage)
        (
            (pl.col("a_volume") - pl.col("b_volume")) / (pl.col("a_volume") + pl.col("b_volume"))
        ).alias("volume_imbalance"),
        # 8. Flow divergence (different sentiment across exchanges)
        (pl.col("a_flow_imbalance") - pl.col("b_flow_imbalance")).alias("flow_divergence"),
        # 9. Volatility ratio (risk comparison)
        (pl.col("a_price_std") / pl.col("b_price_std")).alias("volatility_ratio"),
        # 10. Price convergence speed (spread mean reversion)
        # Spread change relative to previous spread
        (
            (pl.col("spread_bps") - pl.col("spread_bps").shift(1))
            / pl.col("spread_bps").shift(1).abs()
        ).alias("spread_convergence"),
    ]
)

print("✓ Added cross-exchange arbitrage features")
print(f"  Total features: {len(features.columns)}")
print()

# Cross-exchange feature categories
print("Cross-Exchange Feature Categories:")
print("  1. Price Spread - Absolute and normalized price differences")
print("  2. VWAP Spread - More robust spread using volume-weighted prices")
print("  3. Arbitrage Opportunity - Binary flag when spread > costs")
print("  4. Expected Profit - Net profit after transaction costs")
print("  5. Momentum Divergence - Lead-lag relationship detection")
print("  6. Volume Imbalance - Liquidity comparison across exchanges")
print("  7. Flow Divergence - Sentiment differences")
print("  8. Volatility Ratio - Risk comparison")
print("  9. Spread Convergence - Mean reversion speed")
print()

# =============================================================================
# STEP 8: ARBITRAGE OPPORTUNITY ANALYSIS
# =============================================================================

print("Step 8: Arbitrage opportunity analysis...")
print("-" * 80)

# Count arbitrage opportunities
total_bars = features.height
arb_bars = features.filter(pl.col("arb_opportunity")).height
arb_ratio = arb_bars / total_bars

print("Arbitrage Opportunities:")
print(f"  Total bars: {total_bars:,}")
print(f"  Bars with arbitrage: {arb_bars:,} ({arb_ratio:.2%})")
print()

if arb_bars > 0:
    # Analyze profitable opportunities
    arb_features = features.filter(pl.col("arb_opportunity"))

    avg_profit = arb_features["expected_profit_bps"].mean()
    max_profit = arb_features["expected_profit_bps"].max()
    median_profit = arb_features["expected_profit_bps"].median()

    print("Profit Statistics (when arbitrage available):")
    print(f"  Average profit: {avg_profit:.2f} bps")
    print(f"  Median profit: {median_profit:.2f} bps")
    print(f"  Max profit: {max_profit:.2f} bps")
    print()

    # Direction analysis
    positive_spread = arb_features.filter(pl.col("spread_bps") > 0).height
    negative_spread = arb_features.filter(pl.col("spread_bps") < 0).height

    print("Arbitrage Direction:")
    print(f"  A > B (sell A, buy B): {positive_spread:,} ({positive_spread / arb_bars:.1%})")
    print(f"  B > A (sell B, buy A): {negative_spread:,} ({negative_spread / arb_bars:.1%})")
    print()
else:
    print("⚠ No arbitrage opportunities detected in this sample")
    print("  This is normal for highly liquid pairs on major exchanges")
    print("  Consider:")
    print("    - Using exchanges with less liquidity (higher spreads)")
    print("    - Reducing transaction cost estimate")
    print("    - Analyzing during high volatility periods")
    print()

# =============================================================================
# STEP 9: ADD LABELS (FORWARD RETURNS)
# =============================================================================

print("Step 9: Adding labels (forward returns on Exchange A)...")
print("-" * 80)

features = features.with_columns(
    [
        # Forward returns on Exchange A
        (pl.col("a_close").shift(-5) / pl.col("a_close") - 1).alias("forward_return_5bar"),
        (pl.col("a_close").shift(-10) / pl.col("a_close") - 1).alias("forward_return_10bar"),
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
# STEP 10: INFORMATION COEFFICIENT (IC) ANALYSIS
# =============================================================================

print("Step 10: IC analysis - Cross-exchange features...")
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

# Test cross-exchange features
print(f"Information Coefficient vs {label}:")
print()

cross_ex_features = [
    "spread_bps",
    "vwap_spread_bps",
    "momentum_divergence",
    "volume_imbalance",
    "flow_divergence",
    "spread_convergence",
]

for feature_col in cross_ex_features:
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

output_path = f"output/btc_cross_exchange_{EXCHANGE_A}_{EXCHANGE_B}.parquet"

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
print("CROSS-EXCHANGE ARBITRAGE FEATURE ENGINEERING COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  Exchange A: {EXCHANGE_A} ({trades_a.height:,} trades)")
print(f"  Exchange B: {EXCHANGE_B} ({trades_b.height:,} trades)")
print(f"  Volume bars (Exchange A): {spine_a_df.height:,}")
print(f"  Volume bars (Exchange B): {spine_b_df.height:,}")
print(f"  Joined bars: {features.height:,}")
print(f"  Features per bar: {len(features.columns)}")
print(f"  Arbitrage opportunities: {arb_ratio:.2%}")
print(f"  Output file: {output_path}")
print()
print("Feature Categories:")
print("  📊 Exchange A: Price, volume, flow, momentum")
print("  📈 Exchange B: Price, volume, flow, momentum")
print("  🔗 Cross-Exchange: Spread, lead-lag, volume imbalance, flow divergence")
print("  💰 Arbitrage: Opportunity detection, expected profit")
print("  🎯 Labels: Forward returns on Exchange A")
print()
print("Key Insights:")
print("  - Cross-exchange spread mean reverts (trading opportunity)")
print("  - Lead-lag relationships reveal price discovery mechanism")
print("  - Volume imbalance predicts short-term spreads")
print("  - Transaction costs critical for profitability")
print()
print("Next Steps:")
print("  1. Latency analysis: Measure execution delay between exchanges")
print("  2. Slippage modeling: Estimate market impact on both sides")
print("  3. Funding rate arbitrage: Compare funding across exchanges")
print("  4. Triangular arbitrage: Add third exchange/currency")
print()
print("See docs/guides/cross-exchange-arbitrage-mft.md for detailed guide")
print("=" * 80)
