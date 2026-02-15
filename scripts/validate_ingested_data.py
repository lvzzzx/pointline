"""Validate ingested data in silver tables.

Usage:
    python scripts/validate_ingested_data.py
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

pl.Config.set_fmt_str_lengths(50)
pl.Config.set_tbl_width_chars(120)

SILVER_ROOT = Path("/Users/zjx/data/lake/silver")


def validate_options_chain() -> dict:
    """Validate options_chain table."""
    print("\n" + "=" * 60)
    print("VALIDATING: options_chain (Deribit)")
    print("=" * 60)

    dt = DeltaTable(SILVER_ROOT / "options_chain")
    df = pl.from_arrow(dt.to_pyarrow_table())

    issues = []
    checks = {}

    # 1. Basic counts
    checks["total_rows"] = len(df)
    print(f"\n📊 Total rows: {checks['total_rows']:,}")

    # 2. Schema check
    required_cols = [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "option_type",
        "strike",
        "expiration_ts_us",
        "mark_price",
        "mark_iv",
        "delta",
        "gamma",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
    else:
        print("✅ All required columns present")

    # 3. Null checks on critical fields
    null_counts = df.select(
        [
            pl.col("ts_event_us").null_count().alias("ts_event_us_nulls"),
            pl.col("symbol").null_count().alias("symbol_nulls"),
            pl.col("exchange").null_count().alias("exchange_nulls"),
            pl.col("strike").null_count().alias("strike_nulls"),
        ]
    )
    print("\n📝 Null counts:")
    for col, count in null_counts.to_dicts()[0].items():
        if count > 0:
            issues.append(f"{col}: {count} nulls")
        print(f"   {col}: {count:,}")

    # 4. Timestamp range validation
    ts_min = df["ts_event_us"].min()
    ts_max = df["ts_event_us"].max()
    dt_min = datetime.fromtimestamp(ts_min / 1e6)
    dt_max = datetime.fromtimestamp(ts_max / 1e6)
    checks["timestamp_range"] = (dt_min, dt_max)
    print("\n⏰ Timestamp range:")
    print(f"   Min: {dt_min} ({ts_min:,})")
    print(f"   Max: {dt_max} ({ts_max:,})")

    # Check if timestamps align with trading_date
    df_dt = df.with_columns(
        [pl.from_epoch(pl.col("ts_event_us") // 1e6, time_unit="s").dt.date().alias("derived_date")]
    )
    mismatched = df_dt.filter(pl.col("derived_date") != pl.col("trading_date"))
    if len(mismatched) > 0:
        issues.append(f"{len(mismatched)} rows with trading_date != derived_date from ts_event_us")
        print(f"⚠️  {len(mismatched)} rows have mismatched trading_date")
    else:
        print("✅ All trading_date values match ts_event_us")

    # 5. Value range checks
    print("\n💰 Value ranges:")
    # Prices should be positive
    negative_prices = df.filter(pl.col("mark_price") < 0).height
    if negative_prices > 0:
        issues.append(f"{negative_prices} rows with negative mark_price")
    print(f"   Negative mark_price: {negative_prices}")

    # IV should be reasonable (0-500%)
    iv_outliers = df.filter((pl.col("mark_iv") < 0) | (pl.col("mark_iv") > 500)).height
    if iv_outliers > 0:
        issues.append(f"{iv_outliers} rows with mark_iv outside [0, 500]")
    print(f"   IV outside [0, 500]: {iv_outliers}")

    # Delta should be in [-1, 1]
    delta_outliers = df.filter(pl.col("delta").abs() > 1).height
    if delta_outliers > 0:
        issues.append(f"{delta_outliers} rows with |delta| > 1")
    print(f"   Delta outside [-1, 1]: {delta_outliers}")

    # 6. Option type validation
    valid_types = df["option_type"].is_in(["call", "put"]).all()
    if not valid_types:
        invalid = df.filter(~pl.col("option_type").is_in(["call", "put"]))["option_type"].unique()
        issues.append(f"Invalid option_type values: {invalid}")
    else:
        print("✅ All option_type values are valid (call/put)")

    # 7. Symbol check
    print(f"\n🏷️  Symbols: {df['symbol'].n_unique()} unique")
    print(f"   Sample: {df['symbol'].unique()[:5].to_list()}")

    # 8. Exchange consistency
    exchanges = df["exchange"].unique().to_list()
    print(f"\n🏛️  Exchanges: {exchanges}")

    # Summary
    print(f"\n{'=' * 60}")
    if issues:
        print(f"❌ VALIDATION FAILED - {len(issues)} issues:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ VALIDATION PASSED")
    print("=" * 60)

    return {
        "table": "options_chain",
        "checks": checks,
        "issues": issues,
        "passed": len(issues) == 0,
    }


def validate_liquidations() -> dict:
    """Validate liquidations table."""
    print("\n" + "=" * 60)
    print("VALIDATING: liquidations (Deribit)")
    print("=" * 60)

    dt = DeltaTable(SILVER_ROOT / "liquidations")
    df = pl.from_arrow(dt.to_pyarrow_table())

    issues = []
    checks = {}

    # 1. Basic counts
    checks["total_rows"] = len(df)
    print(f"\n📊 Total rows: {checks['total_rows']:,}")

    # 2. Schema check
    required_cols = ["symbol", "exchange", "ts_event_us", "side", "price", "qty", "liquidation_id"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
    else:
        print("✅ All required columns present")

    # 3. Null checks
    null_counts = df.select(
        [
            pl.col("ts_event_us").null_count().alias("ts_event_us_nulls"),
            pl.col("side").null_count().alias("side_nulls"),
            pl.col("price").null_count().alias("price_nulls"),
        ]
    )
    print("\n📝 Null counts:")
    for col, count in null_counts.to_dicts()[0].items():
        if count > 0:
            issues.append(f"{col}: {count} nulls")
        print(f"   {col}: {count:,}")

    # 4. Timestamp range
    ts_min = df["ts_event_us"].min()
    ts_max = df["ts_event_us"].max()
    dt_min = datetime.fromtimestamp(ts_min / 1e6)
    dt_max = datetime.fromtimestamp(ts_max / 1e6)
    checks["timestamp_range"] = (dt_min, dt_max)
    print("\n⏰ Timestamp range:")
    print(f"   Min: {dt_min}")
    print(f"   Max: {dt_max}")

    # 5. Side validation (buy=short liq, sell=long liq)
    sides = df["side"].unique().to_list()
    print(f"\n⚖️  Sides: {sides}")
    if not all(s in ["buy", "sell"] for s in sides):
        issues.append(f"Invalid side values: {sides}")

    # 6. Price and quantity validation
    zero_prices = df.filter(pl.col("price") <= 0).height
    if zero_prices > 0:
        issues.append(f"{zero_prices} rows with zero or negative price")
    print(f"\n💰 Zero/negative prices: {zero_prices}")

    zero_qty = df.filter(pl.col("qty") <= 0).height
    if zero_qty > 0:
        issues.append(f"{zero_qty} rows with zero or negative qty")
    print(f"   Zero/negative qty: {zero_qty}")

    # 7. Symbol breakdown
    print("\n🏷️  Symbols breakdown:")
    sym_breakdown = df.group_by("symbol").agg(
        [
            pl.len().alias("count"),
            pl.col("qty").sum().alias("total_qty"),
        ]
    )
    print(sym_breakdown)

    # 8. Check for duplicates
    dups = (
        df.group_by(["symbol", "ts_event_us", "liquidation_id"])
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
    )
    if len(dups) > 0:
        issues.append(f"{len(dups)} duplicate (symbol, ts_event_us, liquidation_id) combinations")
        print(f"\n⚠️  Found {len(dups)} duplicate keys")
    else:
        print("\n✅ No duplicate keys found")

    # Summary
    print(f"\n{'=' * 60}")
    if issues:
        print(f"❌ VALIDATION FAILED - {len(issues)} issues:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ VALIDATION PASSED")
    print("=" * 60)

    return {"table": "liquidations", "checks": checks, "issues": issues, "passed": len(issues) == 0}


def validate_derivative_ticker() -> dict:
    """Validate derivative_ticker table."""
    print("\n" + "=" * 60)
    print("VALIDATING: derivative_ticker (BitMEX)")
    print("=" * 60)

    dt = DeltaTable(SILVER_ROOT / "derivative_ticker")
    df = pl.from_arrow(dt.to_pyarrow_table())

    issues = []
    checks = {}

    # 1. Basic counts
    checks["total_rows"] = len(df)
    print(f"\n📊 Total rows: {checks['total_rows']:,}")

    # 2. Schema check
    required_cols = [
        "symbol",
        "exchange",
        "ts_event_us",
        "mark_price",
        "index_price",
        "funding_rate",
        "open_interest",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
    else:
        print("✅ All required columns present")

    # 3. Null checks
    null_counts = df.select(
        [
            pl.col("ts_event_us").null_count().alias("ts_event_us_nulls"),
            pl.col("mark_price").null_count().alias("mark_price_nulls"),
            pl.col("funding_rate").null_count().alias("funding_rate_nulls"),
        ]
    )
    print("\n📝 Null counts:")
    for col, count in null_counts.to_dicts()[0].items():
        if count > 0:
            issues.append(f"{col}: {count} nulls")
        print(f"   {col}: {count:,}")

    # 4. Timestamp range
    ts_min = df["ts_event_us"].min()
    ts_max = df["ts_event_us"].max()
    dt_min = datetime.fromtimestamp(ts_min / 1e6)
    dt_max = datetime.fromtimestamp(ts_max / 1e6)
    checks["timestamp_range"] = (dt_min, dt_max)
    print("\n⏰ Timestamp range:")
    print(f"   Min: {dt_min}")
    print(f"   Max: {dt_max}")

    # 5. Price validation
    zero_mark = df.filter(pl.col("mark_price") <= 0).height
    if zero_mark > 0:
        issues.append(f"{zero_mark} rows with zero or negative mark_price")
    print(f"\n💰 Zero/negative mark_price: {zero_mark}")

    # 6. Funding rate validation (BitMEX: typically ±0.5% per 8 hours)
    funding_outliers = df.filter(pl.col("funding_rate").abs() > 0.01).height  # > 1%
    if funding_outliers > 0:
        extreme = df.filter(pl.col("funding_rate").abs() > 0.01)["funding_rate"].max()
        issues.append(f"{funding_outliers} rows with |funding_rate| > 1% (extreme: {extreme})")
    print(f"   Extreme funding (|rate| > 1%): {funding_outliers}")

    # 7. Mark vs Index spread check
    df_spread = df.with_columns(
        [
            (
                (pl.col("mark_price") - pl.col("index_price")).abs() / pl.col("index_price") * 100
            ).alias("spread_pct")
        ]
    )
    large_spread = df_spread.filter(pl.col("spread_pct") > 1).height  # > 1% deviation
    if large_spread > 0:
        max_spread = df_spread["spread_pct"].max()
        issues.append(f"{large_spread} rows with mark-index spread > 1% (max: {max_spread:.2f}%)")
    print(f"   Large mark-index spread (>1%): {large_spread}")

    # 8. Symbol breakdown
    print("\n🏷️  Symbols breakdown:")
    sym_stats = (
        df.group_by("symbol")
        .agg(
            [
                pl.len().alias("updates"),
                pl.col("funding_rate").mean().alias("avg_funding"),
                pl.col("open_interest").max().alias("max_oi"),
            ]
        )
        .sort("updates", descending=True)
    )
    print(sym_stats)

    # 9. Chronological ordering check
    out_of_order = 0
    for symbol in df["symbol"].unique():
        sym_df = df.filter(pl.col("symbol") == symbol).sort("ts_event_us")
        if len(sym_df) > 1:
            ts_diff = sym_df["ts_event_us"].diff().drop_nulls()
            if (ts_diff < 0).any():
                out_of_order += 1
    if out_of_order > 0:
        issues.append(f"{out_of_order} symbols have out-of-order timestamps")
    else:
        print("\n✅ All timestamps are chronologically ordered per symbol")

    # Summary
    print(f"\n{'=' * 60}")
    if issues:
        print(f"❌ VALIDATION FAILED - {len(issues)} issues:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ VALIDATION PASSED")
    print("=" * 60)

    return {
        "table": "derivative_ticker",
        "checks": checks,
        "issues": issues,
        "passed": len(issues) == 0,
    }


def main():
    print("\n" + "=" * 60)
    print("SILVER TABLE VALIDATION REPORT")
    print("=" * 60)

    results = []
    results.append(validate_options_chain())
    results.append(validate_liquidations())
    results.append(validate_derivative_ticker())

    # Final summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)

    all_passed = True
    for r in results:
        status = "✅ PASS" if r["passed"] else "❌ FAIL"
        issue_count = len(r["issues"])
        print(f"{status} - {r['table']} ({issue_count} issues)")
        if not r["passed"]:
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("🎉 ALL VALIDATIONS PASSED")
    else:
        print("⚠️  SOME VALIDATIONS FAILED - Review issues above")
    print("=" * 60)


if __name__ == "__main__":
    main()
