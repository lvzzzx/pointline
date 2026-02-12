"""Bucket assignment for resample operations.

This module implements the CRITICAL bucket assignment semantics:
- Bar at timestamp T contains all data with ts_local_us < T
- Bar window = [T_prev, T) (half-open interval, right-exclusive)
- Uses explicit window map approach for correctness

The window map approach ensures PIT correctness by:
1. Creating explicit [bucket_start, bucket_ts) windows from spine
2. Using backward join on bucket_start (not bucket_ts)
3. Guaranteeing data timestamp < bar timestamp
"""

import polars as pl


def assign_to_buckets(
    data: pl.LazyFrame,
    spine: pl.LazyFrame,
    *,
    deterministic: bool = True,
) -> pl.LazyFrame:
    """Assign each data point to its bucket via window-map as-of join.

    CORRECTED SEMANTICS (v3 strict windows):
    - Uses explicit [bucket_start, bucket_ts) windows derived from spine
    - Data at ts is assigned where bucket_start <= ts < bucket_ts
    - Data at boundary ts==T goes to next bar (half-open interval)
    - Bar at T contains data with ts_local_us < T

    Example:
        Spine: [60ms, 120ms, 180ms]

        Window map:
        - [0ms, 60ms) → bucket_ts = 60ms
        - [60ms, 120ms) → bucket_ts = 120ms
        - [120ms, 180ms) → bucket_ts = 180ms

        Data at 50ms → bucket_ts = 60ms ✅
        Data at 110ms → bucket_ts = 120ms ✅
        Data at 60ms → bucket_ts = 120ms ✅ (boundary to next bar)

        Bar at 60ms contains: [0ms, 60ms) ✅
        Bar at 120ms contains: [60ms, 120ms) ✅

    Args:
        data: Raw data (trades, quotes, book, etc.)
            Required columns: ts_local_us, exchange_id, symbol
        spine: Spine with bucket boundaries
            Required columns: ts_local_us, exchange_id, symbol
            Timestamps are bar ENDS (interval ends)
        deterministic: Enforce deterministic sort order (default: True)

    Returns:
        Data with bucket_ts column (bar end timestamp)

    Raises:
        ValueError: If required columns are missing

    Example:
        >>> spine = build_clock_spine(...)  # [60ms, 120ms, 180ms]
        >>> trades = load_trades(...)  # ts_local_us: [50ms, 110ms, 170ms]
        >>> bucketed = assign_to_buckets(trades, spine)
        >>> bucketed.select(["ts_local_us", "bucket_ts"]).collect()
        shape: (3, 2)
        ┌─────────────┬───────────┐
        │ ts_local_us │ bucket_ts │
        │ ---         │ ---       │
        │ i64         │ i64       │
        ╞═════════════╪═══════════╡
        │ 50000000    │ 60000000  │
        │ 110000000   │ 120000000 │
        │ 170000000   │ 180000000 │
        └─────────────┴───────────┘
    """
    # Step 1: Validate columns
    _validate_bucket_assignment(data, spine)

    # Step 2: Enforce deterministic ordering
    if deterministic:
        data = _enforce_deterministic_sort(data)
        spine = _enforce_deterministic_sort(spine)

    # Step 3: Build explicit window map [bucket_start, bucket_ts)
    window_map = (
        spine.sort(["exchange_id", "symbol", "ts_local_us"])
        .with_columns(
            [
                # bucket_start = previous spine timestamp (start of window)
                # For first bar, use 0 as start (all data before first boundary)
                pl.col("ts_local_us")
                .shift(1)
                .over(["exchange_id", "symbol"])
                .fill_null(0)  # First bar starts at timestamp 0
                .alias("bucket_start"),
                # bucket_ts = current spine timestamp (end of window)
                pl.col("ts_local_us").alias("bucket_ts"),
            ]
        )
        .select(["exchange_id", "symbol", "bucket_start", "bucket_ts"])
    )

    # Step 4: Assign using backward as-of on bucket_start
    # This ensures: bucket_start <= data.ts_local_us < bucket_ts
    bucketed = data.join_asof(
        window_map,
        left_on="ts_local_us",
        right_on="bucket_start",
        by=["exchange_id", "symbol"],
        strategy="backward",
    )

    # Enforce strict half-open interval [bucket_start, bucket_ts):
    # rows after the final boundary (or malformed joins) are left unassigned.
    bucketed = bucketed.with_columns(
        [
            # Keep candidate assignment for diagnostics and PIT gate checks.
            pl.col("bucket_ts").alias("bucket_ts_candidate"),
            pl.when(
                pl.col("bucket_ts").is_not_null() & (pl.col("ts_local_us") < pl.col("bucket_ts"))
            )
            .then(pl.col("bucket_ts"))
            .otherwise(None)
            .alias("bucket_ts"),
        ]
    )

    return bucketed


def _validate_bucket_assignment(data: pl.LazyFrame, spine: pl.LazyFrame) -> None:
    """Validate required columns for bucket assignment.

    Args:
        data: Data LazyFrame
        spine: Spine LazyFrame

    Raises:
        ValueError: If required columns are missing
    """
    required = {"ts_local_us", "exchange_id", "symbol"}

    data_cols = set(data.columns)
    spine_cols = set(spine.columns)

    if not required.issubset(data_cols):
        missing = required - data_cols
        raise ValueError(f"Data missing required columns: {missing}")

    if not required.issubset(spine_cols):
        missing = required - spine_cols
        raise ValueError(f"Spine missing required columns: {missing}")


def _enforce_deterministic_sort(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Enforce deterministic sort order for PIT correctness.

    Args:
        lf: Input LazyFrame

    Returns:
        Sorted LazyFrame with deterministic ordering

    Note:
        Sort order: (exchange_id, symbol, ts_local_us, file_id, file_line_number)
        File-level tie-breakers ensure reproducibility across reruns.
    """
    sort_cols = ["exchange_id", "symbol", "ts_local_us"]

    # Add tie-breakers if available
    if "file_id" in lf.columns:
        sort_cols.append("file_id")
    if "file_line_number" in lf.columns:
        sort_cols.append("file_line_number")

    return lf.sort(sort_cols)
