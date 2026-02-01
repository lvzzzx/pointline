#!/usr/bin/env python
"""Experiment Template - Query API Workflow

This template provides starter code for a new experiment using the query API.

Instructions:
1. Copy this template to research/03_experiments/exp_YYYY-MM-DD_your-name/
2. Update the configuration below
3. Update README.md with hypothesis, method, and results
4. Run the experiment and log results to logs/runs.jsonl
"""

import polars as pl

from pointline import research
from pointline.research import query

# =============================================================================
# CONFIGURATION
# =============================================================================

# Experiment metadata
EXPERIMENT_NAME = "your-experiment-name"
EXCHANGE = "binance-futures"
SYMBOL = "BTCUSDT"
START_DATE = "2024-05-01"
END_DATE = "2024-05-31"

# Data sources (uncomment what you need)
LOAD_TRADES = True
LOAD_QUOTES = False
LOAD_BOOK_SNAPSHOTS = False
LOAD_DERIVATIVE_TICKER = False

# =============================================================================
# STEP 1: VERIFY DATA AVAILABILITY
# =============================================================================

print(f"Experiment: {EXPERIMENT_NAME}")
print(f"Checking data coverage for {SYMBOL} on {EXCHANGE}...")
print()

coverage = research.data_coverage(EXCHANGE, SYMBOL)
for table_name, info in coverage.items():
    status = "✓" if info["available"] else "✗"
    print(f"  {status} {table_name}")
print()

# =============================================================================
# STEP 2: LOAD DATA
# =============================================================================

print(f"Loading data from {START_DATE} to {END_DATE}...")
print()

# Load trades
if LOAD_TRADES:
    trades = query.trades(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=True,  # Use LazyFrame for memory efficiency
    )
    print("✓ Loaded trades (LazyFrame)")

# Load quotes
if LOAD_QUOTES:
    quotes = query.quotes(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=True,
    )
    print("✓ Loaded quotes (LazyFrame)")

# Load book snapshots
if LOAD_BOOK_SNAPSHOTS:
    book = query.book_snapshot_25(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        decoded=True,
        lazy=True,
    )
    print("✓ Loaded book snapshots (LazyFrame)")

# Load derivative ticker data
if LOAD_DERIVATIVE_TICKER:
    ticker = query.derivative_ticker(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        start=START_DATE,
        end=END_DATE,
        lazy=True,
    )
    print("✓ Loaded derivative ticker (LazyFrame)")

print()

# =============================================================================
# STEP 3: FEATURE ENGINEERING
# =============================================================================

print("Creating features...")

if LOAD_TRADES:
    # Example: Filter and aggregate trades
    # Only materialize data AFTER filtering to save memory
    trades_filtered = trades.filter(
        pl.col("qty") > 0.1  # Example filter
    )

    # Aggregate to bars
    bars = (
        trades_filtered.sort("ts_local_us")
        .group_by_dynamic(
            "ts_local_us",
            every="1m",
        )
        .agg(
            [
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.col("qty").sum().alias("volume"),
                pl.count().alias("trade_count"),
            ]
        )
    )

    # NOW collect (only aggregated data, not raw trades)
    bars_df = bars.collect()
    print(f"✓ Created {bars_df.height:,} 1-minute bars")

# =============================================================================
# STEP 4: YOUR ANALYSIS HERE
# =============================================================================

print("\nRunning analysis...")
print()

# TODO: Add your analysis code here
# Examples:
# - Calculate signals/indicators
# - Backtest strategy
# - Compute metrics
# - Generate features for ML

# Example placeholder:
if LOAD_TRADES:
    vwap = bars_df.select(
        [(pl.col("close") * pl.col("volume")).sum() / pl.col("volume").sum()]
    ).item()
    print(f"VWAP: ${vwap:,.2f}")

# =============================================================================
# STEP 5: LOG RESULTS
# =============================================================================

print("\nLogging results to logs/runs.jsonl...")

# TODO: Log your results
# Example:
# import json
# from pathlib import Path
#
# results = {
#     "run_id": f"{EXPERIMENT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
#     "timestamp": datetime.now(timezone.utc).isoformat(),
#     "exchange": EXCHANGE,
#     "symbol": SYMBOL,
#     "start_date": START_DATE,
#     "end_date": END_DATE,
#     "metrics": {
#         "vwap": vwap,
#         # Add your metrics here
#     },
# }
#
# log_file = Path(__file__).parent / "logs" / "runs.jsonl"
# with open(log_file, "a") as f:
#     f.write(json.dumps(results) + "\n")

print("✓ Complete!")
print()

# =============================================================================
# NEXT STEPS
# =============================================================================

print("Next steps:")
print("1. Review results in logs/runs.jsonl")
print("2. Update README.md with findings")
print("3. Save plots to plots/")
print("4. Iterate on hypothesis")
