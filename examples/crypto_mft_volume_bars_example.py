#!/usr/bin/env python
"""Example: Volume Bar Feature Engineering for Crypto MFT

This script demonstrates building a production-ready feature set for
crypto middle-frequency trading using volume bar resampling.

Run: python examples/crypto_mft_volume_bars_example.py

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
print("CRYPTO MFT VOLUME BAR FEATURE ENGINEERING")
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
    print("See: docs/quickstart.md for data ingestion instructions")
    exit(1)

symbol_info = symbols.filter(pl.col("exchange_symbol") == SYMBOL)
if symbol_info.is_empty():
    print(f"❌ Symbol {SYMBOL} not found on {EXCHANGE}")
    exit(1)

symbol_id = symbol_info["symbol_id"][0]
price_increment = symbol_info["price_increment"][0]
amount_increment = symbol_info["amount_increment"][0]

print(f"✓ Found symbol: {SYMBOL}")
print(f"  Symbol ID: {symbol_id}")
print(f"  Tick Size: {price_increment}")
print(f"  Lot Size: {amount_increment}")
print()

# Check data coverage
coverage = research.data_coverage(EXCHANGE, SYMBOL)
print("Data Coverage:")
for table, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table}")
print()

if not all(
    [
        coverage["trades"]["available"],
        coverage["quotes"]["available"],
        coverage["book_snapshot_25"]["available"],
    ]
):
    print("⚠ Warning: Not all required tables available")
    print("This example works best with trades + quotes + book_snapshot_25")
    print("Continuing with available data...")
    print()

# =============================================================================
# STEP 2: LOAD RAW DATA (QUERY API)
# =============================================================================

print("Step 2: Loading raw data with query API...")
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
        lazy=False,  # Collect for demonstration
    )
    print(f"✓ Loaded {trades.height:,} trades")
    print(f"  Date range: {trades['ts_local_us'].min()} to {trades['ts_local_us'].max()}")
    print(f"  Total volume: {trades['qty'].sum():.2f} BTC")
    print()

    # Load quotes (if available)
    if coverage["quotes"]["available"]:
        print("Loading quotes...")
        quotes = query.quotes(
            exchange=EXCHANGE,
            symbol=SYMBOL,
            start=START_DATE,
            end=END_DATE,
            decoded=True,
            lazy=False,
        )
        print(f"✓ Loaded {quotes.height:,} quotes")
        print()
    else:
        quotes = None
        print("⚠ Quotes not available, skipping spread features")
        print()

    # Load book snapshots (if available)
    if coverage["book_snapshot_25"]["available"]:
        print("Loading book snapshots...")
        book = query.book_snapshot_25(
            exchange=EXCHANGE,
            symbol=SYMBOL,
            start=START_DATE,
            end=END_DATE,
            decoded=True,
            lazy=False,
        )
        print(f"✓ Loaded {book.height:,} book snapshots")
        print()
    else:
        book = None
        print("⚠ Book snapshots not available, skipping book imbalance features")
        print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    print()
    print("This example requires historical data to be present in the data lake.")
    print("If you're setting up Pointline for the first time:")
    print("  1. Run: pointline bronze discover --vendor tardis --pending-only")
    print(
        "  2. Run: pointline ingest run --table trades --exchange binance-futures --date 2024-05-01"
    )
    print("  3. Repeat for quotes and book_snapshot_25")
    print()
    print("See docs/quickstart.md for detailed instructions.")
    exit(1)

# =============================================================================
# STEP 3: BUILD VOLUME BAR SPINE
# =============================================================================

print("Step 3: Building volume bar spine...")
print("-" * 80)

# Get volume bar builder
spine_builder = get_builder("volume")

# Build spine
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
    source_data=trades.lazy(),  # Pass trades for volume computation
)

spine_df = spine.collect()
print(f"✓ Generated {spine_df.height:,} volume bars")
print()

# Compute bar statistics
spine_df = spine_df.with_columns(
    [
        pl.from_epoch("ts_local_us", time_unit="us").alias("timestamp"),
        pl.col("ts_local_us").diff().alias("bar_duration_us"),
    ]
)

bar_durations_seconds = spine_df["bar_duration_us"].drop_nulls() / 1_000_000
print("Volume Bar Statistics:")
print(f"  Mean duration: {bar_durations_seconds.mean():.1f} seconds")
print(f"  Median duration: {bar_durations_seconds.median():.1f} seconds")
print(f"  Min duration: {bar_durations_seconds.min():.1f} seconds")
print(f"  Max duration: {bar_durations_seconds.max():.1f} seconds")
print()

# =============================================================================
# STEP 4: ASSIGN TRADES TO VOLUME BARS
# =============================================================================

print("Step 4: Assigning trades to volume bars...")
print("-" * 80)

# Assign trades to buckets
bucketed_trades = assign_to_buckets(
    events=trades.lazy(),
    spine=spine.with_columns(
        [
            pl.col("ts_local_us").alias("bucket_start"),
        ]
    ),
    ts_col="ts_local_us",
)

bucketed_trades_df = bucketed_trades.collect()
print(f"✓ Assigned {bucketed_trades_df.height:,} trades to {spine_df.height:,} volume bars")

# Check assignment coverage
unassigned = bucketed_trades_df.filter(pl.col("bucket_start").is_null()).height
unassigned_ratio = unassigned / bucketed_trades_df.height
print(f"  Unassigned trades: {unassigned:,} ({unassigned_ratio:.2%})")

if unassigned_ratio > 0.01:
    print("  ⚠ Warning: High unassigned ratio (should be < 1%)")
print()

# =============================================================================
# STEP 5: COMPUTE FEATURES (AGGREGATION)
# =============================================================================

print("Step 5: Computing features via aggregation...")
print("-" * 80)

# Aggregate features per volume bar
features = (
    bucketed_trades_df.group_by("bucket_start")
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
            # Trade size
            pl.col("qty").mean().alias("avg_trade_size"),
            pl.col("qty").median().alias("median_trade_size"),
            # Volatility (std of prices within bar)
            pl.col("price").std().alias("price_std"),
        ]
    )
    .sort("bucket_start")
)

print(f"✓ Computed {len(features.columns)} base features")
print(f"  Features: {features.columns}")
print()

# =============================================================================
# STEP 6: ADD DERIVED FEATURES
# =============================================================================

print("Step 6: Adding derived features...")
print("-" * 80)

features = features.with_columns(
    [
        # 1. VWAP reversion (mean reversion signal)
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap")).alias("vwap_reversion"),
        # 2. Momentum (returns over different horizons)
        (pl.col("close") / pl.col("close").shift(1) - 1).alias("ret_1bar"),
        (pl.col("close") / pl.col("close").shift(5) - 1).alias("ret_5bar"),
        (pl.col("close") / pl.col("close").shift(10) - 1).alias("ret_10bar"),
        # 3. Volume acceleration
        (pl.col("trade_count") / pl.col("trade_count").shift(1) - 1).alias("trade_count_accel"),
        # 4. Flow persistence (autocorrelation)
        (pl.col("flow_imbalance") * pl.col("flow_imbalance").shift(1)).alias("flow_persistence"),
        # 5. High-low range (volatility proxy)
        ((pl.col("high") - pl.col("low")) / pl.col("vwap")).alias("hl_range_pct"),
        # 6. Size ratio (institutional vs retail)
        (pl.col("avg_trade_size") / pl.col("avg_trade_size").rolling_median(50)).alias(
            "size_ratio"
        ),
        # 7. Timestamp features (for seasonality)
        pl.from_epoch("bucket_start", time_unit="us").dt.hour().alias("hour"),
        pl.from_epoch("bucket_start", time_unit="us").dt.weekday().alias("weekday"),
    ]
)

print("✓ Added derived features")
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
print("  Labels: forward_return_5bar, forward_return_10bar, forward_return_20bar")
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
for col in ["vwap_reversion", "ret_1bar", "flow_imbalance"]:
    if col in features.columns:
        inf_count = features.filter(pl.col(col).is_infinite()).height
        if inf_count > 0:
            print(f"⚠ Warning: {inf_count} infinite values in {col}")

# Check value ranges
print()
print("Feature Statistics:")
key_features = ["flow_imbalance", "vwap_reversion", "ret_5bar", "hl_range_pct"]
for col in key_features:
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
# STEP 9: QUICK BACKTEST (IC ANALYSIS)
# =============================================================================

print("Step 9: Quick backtest - Information Coefficient (IC) analysis...")
print("-" * 80)


def compute_ic(df: pl.DataFrame, signal_col: str, label_col: str):
    """Compute rank IC (Spearman correlation) between signal and label."""
    # Drop nulls
    valid_df = df.select([signal_col, label_col]).drop_nulls()

    if valid_df.height < 100:
        print(f"⚠ Too few samples ({valid_df.height}) for IC calculation")
        return None

    signal = valid_df[signal_col].to_numpy()
    label = valid_df[label_col].to_numpy()

    ic, p_value = spearmanr(signal, label)

    return ic, p_value


# Test key features
test_features = ["flow_imbalance", "vwap_reversion", "ret_5bar", "flow_persistence"]
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
            print(f"  {feature_col:20s}: IC = {ic:7.4f} (p={p_value:.4f}) {significance}")

print()
print("Significance: *** p<0.001, ** p<0.01, * p<0.05")
print()

# =============================================================================
# STEP 10: SAVE FEATURES
# =============================================================================

print("Step 10: Saving features...")
print("-" * 80)

output_path = "output/btc_mft_volume_bar_features.parquet"

# Create output directory
os.makedirs("output", exist_ok=True)

# Save
features.write_parquet(output_path)
print(f"✓ Saved features to {output_path}")
print(f"  Rows: {features.height:,}")
print(f"  Columns: {len(features.columns)}")
print()

# =============================================================================
# STEP 11: FEATURE SUMMARY
# =============================================================================

print("=" * 80)
print("FEATURE ENGINEERING COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  Input trades: {trades.height:,}")
print(f"  Volume bars generated: {spine_df.height:,}")
print(f"  Features per bar: {len(features.columns)}")
print(f"  Output file: {output_path}")
print()
print("Feature Categories:")
print("  📊 Price: open, high, low, close, vwap")
print("  📈 Volume: volume, trade_count, avg_trade_size")
print("  💹 Order Flow: flow_imbalance, flow_persistence")
print("  📉 Momentum: ret_1bar, ret_5bar, ret_10bar, vwap_reversion")
print("  🔀 Volatility: price_std, hl_range_pct")
print("  🎯 Labels: forward_return_5bar, forward_return_10bar, forward_return_20bar")
print()
print("Next Steps:")
print("  1. Feature selection: Use IC analysis to select top features")
print("  2. Model training: Train ML model (LightGBM, XGBoost) on features")
print("  3. Backtest: Evaluate strategy performance on historical data")
print("  4. Production: Deploy model for live trading")
print()
print("See docs/guides/volume-bar-features-crypto-mft.md for detailed guide")
print("=" * 80)
