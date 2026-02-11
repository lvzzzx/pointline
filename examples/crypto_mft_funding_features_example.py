#!/usr/bin/env python
"""Example: Funding Rate Features for Crypto MFT

This script demonstrates building funding rate features for crypto
middle-frequency trading using volume bar resampling with multi-source
aggregation (trades + funding data).

Key Innovation: Multi-source feature aggregation
- Trade features: flow imbalance, VWAP, volatility
- Funding features: funding rate, OI, funding carry
- Cross-features: flow × funding interaction

Run: python examples/crypto_mft_funding_features_example.py

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
VOLUME_THRESHOLD = 100.0  # 100 BTC per bar (~$6M notional at $60k)

print("=" * 80)
print("CRYPTO MFT FUNDING RATE FEATURE ENGINEERING")
print("=" * 80)
print(f"Exchange: {EXCHANGE}")
print(f"Symbol: {SYMBOL}")
print(f"Date Range: {START_DATE} to {END_DATE}")
print(f"Volume Threshold: {VOLUME_THRESHOLD} BTC per bar")
print("=" * 80)
print()

# =============================================================================
# STEP 1: DISCOVERY - CHECK DATA AVAILABILITY
# =============================================================================

print("Step 1: Discovery - Checking data availability...")
print("-" * 80)

# Check symbol exists
symbols = research.list_symbols(exchange=EXCHANGE, base_asset="BTC", asset_type="perpetual")

if symbols.is_empty():
    print("❌ No BTC perpetual symbols found on", EXCHANGE)
    print("This example requires data to be ingested first.")
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
print("Data Coverage:")
for table, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table}")
print()

if not coverage["trades"]["available"]:
    print("❌ Trades data not available - required for volume bars")
    exit(1)

if not coverage["derivative_ticker"]["available"]:
    print("❌ Derivative ticker data not available - required for funding features")
    print("This example demonstrates funding rate features.")
    exit(1)

print("✓ All required data tables available")
print()

# =============================================================================
# STEP 2: LOAD RAW DATA (MULTI-SOURCE)
# =============================================================================

print("Step 2: Loading raw data from multiple sources...")
print("-" * 80)

try:
    # Load trades
    print(f"Loading trades for {SYMBOL} ({START_DATE} to {END_DATE})...")
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

    # Load funding data
    print("Loading derivative ticker (funding rate + OI)...")
    funding = query.derivative_ticker(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=False,
    )
    print(f"✓ Loaded {funding.height:,} funding snapshots")
    print(
        f"  Funding rate range: {funding['funding_rate'].min():.6f} to {funding['funding_rate'].max():.6f}"
    )
    print(
        f"  OI range: {funding['open_interest'].min():.0f} to {funding['open_interest'].max():.0f}"
    )
    print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    exit(1)

# =============================================================================
# STEP 3: BUILD VOLUME BAR SPINE
# =============================================================================

print("Step 3: Building volume bar spine...")
print("-" * 80)

spine_builder = get_builder("volume")

start_ts_us = int(
    datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)
end_ts_us = int(
    datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp() * 1_000_000
)

spine_config = VolumeBarConfig(volume_threshold=VOLUME_THRESHOLD, use_absolute_volume=True)

print(f"Building volume bars (threshold: {VOLUME_THRESHOLD} BTC)...")
spine = spine_builder.build_spine(
    symbol_id=symbol_id,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
    config=spine_config,
    source_data=trades.lazy(),
)

spine_df = spine.collect()
print(f"✓ Generated {spine_df.height:,} volume bars")
print()

# =============================================================================
# STEP 4: ASSIGN EVENTS TO VOLUME BARS (MULTI-SOURCE)
# =============================================================================

print("Step 4: Assigning events to volume bars (multi-source)...")
print("-" * 80)

# Prepare spine with bucket_start column
spine_with_bucket = spine.with_columns([pl.col("ts_local_us").alias("bucket_start")])

# Assign trades to buckets
bucketed_trades = assign_to_buckets(
    events=trades.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)

# Assign funding snapshots to buckets
bucketed_funding = assign_to_buckets(
    events=funding.lazy(),
    spine=spine_with_bucket,
    ts_col="ts_local_us",
)

print(f"✓ Assigned {trades.height:,} trades to {spine_df.height:,} volume bars")
print(f"✓ Assigned {funding.height:,} funding snapshots to {spine_df.height:,} volume bars")
print()

# =============================================================================
# STEP 5: COMPUTE FEATURES (MULTI-SOURCE AGGREGATION)
# =============================================================================

print("Step 5: Computing features from multiple sources...")
print("-" * 80)

# Aggregate trade features
trade_features = (
    bucketed_trades.group_by("bucket_start")
    .agg(
        [
            # Price features
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            # Volume features
            pl.col("qty").sum().alias("volume"),
            pl.col("qty").count().alias("trade_count"),
            # VWAP
            ((pl.col("price") * pl.col("qty")).sum() / pl.col("qty").sum()).alias("vwap"),
            # Order flow imbalance
            (
                (
                    pl.col("qty").filter(pl.col("side") == 0).sum()
                    - pl.col("qty").filter(pl.col("side") == 1).sum()
                )
                / pl.col("qty").sum()
            ).alias("flow_imbalance"),
        ]
    )
    .sort("bucket_start")
)

# Aggregate funding features
funding_features = (
    bucketed_funding.group_by("bucket_start")
    .agg(
        [
            # Funding rate features
            pl.col("funding_rate").first().alias("funding_open"),
            pl.col("funding_rate").last().alias("funding_close"),
            pl.col("funding_rate").mean().alias("funding_mean"),
            (pl.col("funding_rate").last() - pl.col("funding_rate").first()).alias("funding_step"),
            # Predicted funding (for surprise calculation)
            pl.col("predicted_funding_rate").last().alias("predicted_funding_close"),
            # Open interest features
            pl.col("open_interest").first().alias("oi_open"),
            pl.col("open_interest").last().alias("oi_close"),
            (pl.col("open_interest").last() - pl.col("open_interest").first()).alias("oi_change"),
        ]
    )
    .sort("bucket_start")
)

# Join features from both sources
features = trade_features.join(funding_features, on="bucket_start", how="left").collect()

print(f"✓ Computed {len(features.columns)} base features")
print(f"  Trade features: {trade_features.collect().columns}")
print(f"  Funding features: {funding_features.collect().columns}")
print()

# =============================================================================
# STEP 6: ADD DERIVED FUNDING FEATURES
# =============================================================================

print("Step 6: Adding derived funding features...")
print("-" * 80)

features = features.with_columns(
    [
        # 1. Funding carry (annualized)
        # Binance funding: 3 times per day (every 8 hours)
        (pl.col("funding_close") * 365 * 3).alias("funding_carry_annual"),
        # 2. Funding surprise (actual vs predicted)
        (pl.col("funding_close") - pl.col("predicted_funding_close")).alias("funding_surprise"),
        # 3. OI percentage change
        (pl.col("oi_change") / pl.col("oi_open")).alias("oi_pct_change"),
        # 4. Funding-OI pressure (interaction)
        (pl.col("funding_step") * pl.col("oi_pct_change")).alias("funding_oi_pressure"),
        # 5. Cross-feature: flow imbalance × funding
        # High flow imbalance + high funding = strong directional signal
        (pl.col("flow_imbalance") * pl.col("funding_close")).alias("flow_funding_interaction"),
        # 6. Funding momentum (rate of change)
        (pl.col("funding_close") / pl.col("funding_close").shift(1) - 1).alias("funding_momentum"),
        # 7. OI momentum
        (pl.col("oi_close") / pl.col("oi_close").shift(5) - 1).alias("oi_momentum_5bar"),
    ]
)

print("✓ Added derived funding features")
print(f"  Total features: {len(features.columns)}")
print()

# =============================================================================
# STEP 7: ADD LABELS (FORWARD RETURNS)
# =============================================================================

print("Step 7: Adding labels (forward returns)...")
print("-" * 80)

features = features.with_columns(
    [
        # Forward returns (labels for prediction)
        (pl.col("close").shift(-5) / pl.col("close") - 1).alias("forward_return_5bar"),
        (pl.col("close").shift(-10) / pl.col("close") - 1).alias("forward_return_10bar"),
        (pl.col("close").shift(-20) / pl.col("close") - 1).alias("forward_return_20bar"),
    ]
)

# Standardize labels (z-score)
for label_col in ["forward_return_5bar", "forward_return_10bar", "forward_return_20bar"]:
    mean = features[label_col].mean()
    std = features[label_col].std()
    features = features.with_columns(
        [((pl.col(label_col) - mean) / std).alias(f"{label_col}_zscore")]
    )

print("✓ Added forward return labels")
print()

# =============================================================================
# STEP 8: FEATURE QUALITY VALIDATION
# =============================================================================

print("Step 8: Feature quality validation...")
print("-" * 80)

# Check null ratios
null_ratios = features.null_count() / features.height
high_null_cols = [col for col in features.columns if null_ratios[col][0] > 0.05]

if high_null_cols:
    print(f"⚠ Warning: High null ratio (>5%) in columns: {high_null_cols}")
else:
    print("✓ All features have acceptable null ratios (<5%)")

# Check for infinite values
for col in ["funding_carry_annual", "funding_surprise", "flow_funding_interaction"]:
    if col in features.columns:
        inf_count = features.filter(pl.col(col).is_infinite()).height
        if inf_count > 0:
            print(f"⚠ Warning: {inf_count} infinite values in {col}")

# Check value ranges
print()
print("Funding Feature Statistics:")
funding_feature_cols = [
    "funding_close",
    "funding_surprise",
    "funding_carry_annual",
    "oi_pct_change",
    "flow_funding_interaction",
]
for col in funding_feature_cols:
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
# STEP 9: INFORMATION COEFFICIENT (IC) ANALYSIS
# =============================================================================

print("Step 9: Information Coefficient (IC) analysis...")
print("-" * 80)


def compute_ic(df: pl.DataFrame, signal_col: str, label_col: str):
    """Compute rank IC (Spearman correlation) between signal and label."""
    valid_df = df.select([signal_col, label_col]).drop_nulls()

    if valid_df.height < 100:
        print(f"⚠ Too few samples ({valid_df.height}) for IC calculation")
        return None

    signal = valid_df[signal_col].to_numpy()
    label = valid_df[label_col].to_numpy()

    ic, p_value = spearmanr(signal, label)

    return ic, p_value


# Test funding features
test_features = [
    "funding_surprise",
    "funding_carry_annual",
    "oi_pct_change",
    "funding_oi_pressure",
    "flow_funding_interaction",
    "flow_imbalance",  # Compare with pure trade feature
]
label = "forward_return_5bar_zscore"

print(f"Information Coefficient vs {label}:")
print()

for feature_col in test_features:
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
print("Interpretation:")
print("  - Funding surprise: Deviation from predicted funding (shock signal)")
print("  - Funding carry: Annualized funding rate (mean reversion)")
print("  - Flow-funding interaction: Combined order flow + funding signal")
print("  - IC > 0.03 with p<0.05 = statistically significant predictive power")
print()

# =============================================================================
# STEP 10: SAVE FEATURES
# =============================================================================

print("Step 10: Saving features...")
print("-" * 80)

output_path = "output/btc_mft_funding_features.parquet"

os.makedirs("output", exist_ok=True)

features.write_parquet(output_path)
print(f"✓ Saved features to {output_path}")
print(f"  Rows: {features.height:,}")
print(f"  Columns: {len(features.columns)}")
print()

# =============================================================================
# STEP 11: FEATURE SUMMARY
# =============================================================================

print("=" * 80)
print("FUNDING RATE FEATURE ENGINEERING COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  Input trades: {trades.height:,}")
print(f"  Input funding snapshots: {funding.height:,}")
print(f"  Volume bars generated: {spine_df.height:,}")
print(f"  Features per bar: {len(features.columns)}")
print(f"  Output file: {output_path}")
print()
print("Feature Categories:")
print("  📊 Trade features: price (OHLC), volume, flow imbalance, VWAP")
print("  💰 Funding features: funding rate, funding surprise, funding carry")
print("  📈 OI features: open interest change, OI momentum")
print("  🔗 Cross-features: flow × funding interaction, funding-OI pressure")
print("  🎯 Labels: forward returns (5, 10, 20 bars)")
print()
print("Key Innovation:")
print("  - Multi-source aggregation: Combined trades + funding data")
print("  - Cross-feature engineering: flow_imbalance × funding_rate")
print("  - Funding mechanics: Carry, surprise, momentum signals")
print()
print("Next Steps:")
print("  1. Feature selection: Select features with IC > 0.03 (p<0.05)")
print("  2. Model training: Train ML model combining trade + funding features")
print("  3. Backtest: Evaluate strategy with funding-aware position sizing")
print("  4. Production: Monitor funding rate changes for signal decay")
print()
print("See docs/guides/funding-rate-features-mft.md for detailed guide")
print("=" * 80)
