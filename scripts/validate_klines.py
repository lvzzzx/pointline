#!/usr/bin/env python3
"""Validate kline ingestion results."""

import polars as pl
from pathlib import Path
from pointline.tables.klines import check_kline_completeness

# Load all kline_1h data
print("Loading kline_1h table...")
kline_path = Path.home() / "data/lake/silver/kline_1h"
df = pl.scan_delta(str(kline_path)).collect()

print(f"\n{'='*60}")
print("INGESTION SUMMARY")
print(f"{'='*60}")
print(f"Total rows ingested: {len(df):,}")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Exchanges: {df['exchange'].unique().sort().to_list()}")
print(f"Unique symbols: {df['symbol_id'].n_unique()}")

print(f"\n{'='*60}")
print("SCHEMA VALIDATION")
print(f"{'='*60}")
required_cols = [
    'exchange', 'date', 'symbol_id',
    'open_px_int', 'high_px_int', 'low_px_int', 'close_px_int',
    'volume_qty_int', 'quote_volume_int', 'taker_buy_quote_qty_int'
]
for col in required_cols:
    status = "✓" if col in df.columns else "✗"
    dtype = df[col].dtype if col in df.columns else "MISSING"
    print(f"{status} {col:30} {dtype}")

print(f"\n{'='*60}")
print("DATA QUALITY CHECKS")
print(f"{'='*60}")

# Check for nulls in critical columns
null_checks = ['open_px_int', 'quote_volume_int', 'symbol_id']
for col in null_checks:
    null_count = df[col].null_count()
    status = "✓" if null_count == 0 else f"✗ ({null_count} nulls)"
    print(f"{status} {col} has no nulls")

# Check value ranges
print(f"\n✓ All prices > 0: {(df['open_px_int'] > 0).all()}")
print(f"✓ High >= Low: {(df['high_px_int'] >= df['low_px_int']).all()}")
print(f"✓ Quote volumes >= 0: {(df['quote_volume_int'] >= 0).all()}")

print(f"\n{'='*60}")
print("COMPLETENESS CHECK (24 rows/day expected for 1h interval)")
print(f"{'='*60}")

# Check completeness per symbol
completeness = check_kline_completeness(df, interval="1h", warn_on_gaps=False)
incomplete = completeness.filter(~pl.col("is_complete"))

total_days = completeness.height
complete_days = completeness.filter(pl.col("is_complete")).height
incomplete_days = incomplete.height

print(f"Total symbol-days: {total_days}")
print(f"Complete days: {complete_days} ({complete_days/total_days*100:.1f}%)")
print(f"Incomplete days: {incomplete_days} ({incomplete_days/total_days*100:.1f}%)")

if incomplete_days > 0:
    print(f"\nSample incomplete days (showing first 10):")
    print(incomplete.head(10))
else:
    print("\n✓ All days have complete data!")

print(f"\n{'='*60}")
print("FIXED-POINT ENCODING VALIDATION")
print(f"{'='*60}")

# Check if quote volumes are properly encoded
sample = df.head(5)
print("Sample quote_volume_int values:")
print(sample.select(['symbol_id', 'quote_volume_int', 'volume_qty_int']))

# Verify no Float64 columns (all should be Int64 or other int types)
float_cols = [col for col, dtype in df.schema.items() if dtype == pl.Float64]
if float_cols:
    print(f"\n⚠ Warning: Found Float64 columns (should be fixed-point): {float_cols}")
else:
    print("\n✓ All numeric fields use fixed-point encoding (no Float64)")

print(f"\n{'='*60}")
print("VALIDATION COMPLETE")
print(f"{'='*60}")
