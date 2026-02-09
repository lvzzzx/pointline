"""Test bucket assignment with window-map semantics.

This module tests the CRITICAL bucket assignment semantics:
- Bar at timestamp T contains data with ts_local_us < T
- Bar window = [T_prev, T) (half-open interval)
- Window map approach ensures PIT correctness
"""

import polars as pl
import pytest

from pointline.research.resample import assign_to_buckets


class TestBucketAssignmentWindowMap:
    """Test bucket assignment uses strict [start, end) windows."""

    def test_bucket_assignment_strict_windows(self):
        """CRITICAL: Test bucket assignment uses strict [start, end) windows.

        COMPLETE: Full test specification.
        """
        # Create spine (1-minute boundaries = bar ends)
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000, 180_000_000],
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
            }
        )

        # Create data
        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000, 110_000_000, 170_000_000],
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
                "value": [100, 200, 300],
            }
        )

        # Assign to buckets
        bucketed = assign_to_buckets(data, spine).collect()

        # CORRECTED ASSERTIONS:
        # Data at 50ms → bucket_ts = 60ms (next boundary)
        assert bucketed["bucket_ts"][0] == 60_000_000, "50ms data should be in bar at 60ms"

        # Data at 110ms → bucket_ts = 120ms (next boundary)
        assert bucketed["bucket_ts"][1] == 120_000_000, "110ms data should be in bar at 120ms"

        # Data at 170ms → bucket_ts = 180ms (next boundary)
        assert bucketed["bucket_ts"][2] == 180_000_000, "170ms data should be in bar at 180ms"

        # Verify PIT correctness: data timestamp < bucket timestamp
        for i in range(len(bucketed)):
            data_ts = bucketed["ts_local_us"][i]
            bucket_ts = bucketed["bucket_ts"][i]
            assert data_ts < bucket_ts, f"PIT violation: data at {data_ts} in bar at {bucket_ts}"

    def test_bucket_assignment_pit_correctness(self):
        """Test PIT correctness: all data in bar has ts < bar_timestamp.

        COMPLETE: Full test specification.
        """
        # Spine boundaries
        spine = pl.LazyFrame(
            {
                "ts_local_us": [100_000_000, 200_000_000, 300_000_000],
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
            }
        )

        # Data with various timestamps
        data = pl.LazyFrame(
            {
                "ts_local_us": [
                    50_000_000,  # → bar at 100ms
                    99_999_999,  # → bar at 100ms (edge case)
                    100_000_000,  # → bar at 200ms (boundary data goes to NEXT bar)
                    150_000_000,  # → bar at 200ms
                    199_999_999,  # → bar at 200ms
                    250_000_000,  # → bar at 300ms
                ],
                "exchange_id": [1] * 6,
                "symbol_id": [12345] * 6,
                "value": [100, 200, 300, 400, 500, 600],
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()

        # Verify PIT invariant for all rows
        for i in range(len(bucketed)):
            data_ts = bucketed["ts_local_us"][i]
            bucket_ts = bucketed["bucket_ts"][i]

            # CRITICAL: data timestamp must be < bar timestamp
            assert data_ts < bucket_ts, (
                f"PIT violation at row {i}: data {data_ts} >= bar {bucket_ts}"
            )

        # Verify specific assignments
        assert bucketed["bucket_ts"][0] == 100_000_000  # 50ms → 100ms
        assert bucketed["bucket_ts"][1] == 100_000_000  # 99.999ms → 100ms
        assert bucketed["bucket_ts"][2] == 200_000_000  # 100ms → 200ms (boundary)
        assert bucketed["bucket_ts"][3] == 200_000_000  # 150ms → 200ms
        assert bucketed["bucket_ts"][4] == 200_000_000  # 199.999ms → 200ms
        assert bucketed["bucket_ts"][5] == 300_000_000  # 250ms → 300ms

    def test_bucket_assignment_boundary_edge_case(self):
        """Test data exactly at spine boundary goes to NEXT bar.

        COMPLETE: Critical edge case test.
        """
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
            }
        )

        # Data exactly at 60ms boundary
        data = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
                "value": [100],
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()

        # Data at 60ms should go to bar at 120ms (next boundary)
        # Bar at 60ms contains [0, 60), not including 60
        assert bucketed["bucket_ts"][0] == 120_000_000, (
            "Boundary data should go to next bar (half-open interval)"
        )

    def test_bucket_assignment_deterministic_sort(self):
        """Test deterministic sort enforcement with tie-breakers.

        COMPLETE: Full test specification.
        """
        spine = pl.LazyFrame(
            {
                "ts_local_us": [100_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
            }
        )

        # Data with same timestamp but different tie-breakers
        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000, 50_000_000, 50_000_000],
                "exchange_id": [1, 1, 1],
                "symbol_id": [12345, 12345, 12345],
                "file_id": [1, 1, 2],
                "file_line_number": [100, 200, 100],
                "value": [10, 20, 30],
            }
        )

        bucketed = assign_to_buckets(data, spine, deterministic=True).collect()

        # Verify order preserved by tie-breakers
        # Order: (file_id=1, line=100), (file_id=1, line=200), (file_id=2, line=100)
        assert bucketed["value"].to_list() == [
            10,
            20,
            30,
        ], "Deterministic sort should preserve tie-breaker order"


class TestBucketAssignmentMultiSymbol:
    """Test bucket assignment with multiple symbols."""

    def test_multi_symbol_bucket_assignment(self):
        """Test bucket assignment with multiple symbols.

        COMPLETE: Full test specification.
        """
        # Spine with 2 symbols
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000, 60_000_000, 120_000_000],
                "exchange_id": [1, 1, 1, 1],
                "symbol_id": [12345, 12345, 12346, 12346],
            }
        )

        # Data for both symbols
        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000, 110_000_000, 55_000_000, 115_000_000],
                "exchange_id": [1, 1, 1, 1],
                "symbol_id": [12345, 12345, 12346, 12346],
                "value": [100, 200, 300, 400],
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()

        # Check symbol 12345
        symbol_12345 = bucketed.filter(pl.col("symbol_id") == 12345)
        assert symbol_12345["bucket_ts"][0] == 60_000_000  # 50ms → 60ms
        assert symbol_12345["bucket_ts"][1] == 120_000_000  # 110ms → 120ms

        # Check symbol 12346
        symbol_12346 = bucketed.filter(pl.col("symbol_id") == 12346)
        assert symbol_12346["bucket_ts"][0] == 60_000_000  # 55ms → 60ms
        assert symbol_12346["bucket_ts"][1] == 120_000_000  # 115ms → 120ms


class TestBucketAssignmentValidation:
    """Test validation and error handling."""

    def test_missing_columns_in_data(self):
        """Test validation fails when data missing required columns.

        COMPLETE: Full test specification.
        """
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
            }
        )

        # Data missing exchange_id
        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000],
                "symbol_id": [12345],
                "value": [100],
            }
        )

        with pytest.raises(ValueError, match="Data missing required columns"):
            assign_to_buckets(data, spine)

    def test_missing_columns_in_spine(self):
        """Test validation fails when spine missing required columns.

        COMPLETE: Full test specification.
        """
        # Spine missing symbol_id
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000],
                "exchange_id": [1],
            }
        )

        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
                "value": [100],
            }
        )

        with pytest.raises(ValueError, match="Spine missing required columns"):
            assign_to_buckets(data, spine)


class TestBucketAssignmentEmptyCases:
    """Test edge cases with empty data or spine."""

    def test_empty_data(self):
        """Test bucket assignment with empty data.

        COMPLETE: Full test specification.
        """
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
            }
        )

        # Empty data with matching dtypes
        data = pl.LazyFrame(
            schema={
                "ts_local_us": pl.Int64,
                "exchange_id": pl.Int64,  # Match spine dtype
                "symbol_id": pl.Int64,
                "value": pl.Int64,
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()

        # Should return empty with correct schema
        assert len(bucketed) == 0
        assert "bucket_ts" in bucketed.columns

    def test_data_before_first_spine_point(self):
        """Test data before first spine point has no bucket.

        COMPLETE: Full test specification.
        """
        # Spine starts at 60ms
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
            }
        )

        # Data at 10ms (before first spine point at 60ms)
        # Window map starts at [0ms, 60ms), so this should still map to 60ms
        data = pl.LazyFrame(
            {
                "ts_local_us": [10_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
                "value": [100],
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()

        # Data at 10ms should map to bar at 60ms
        assert bucketed["bucket_ts"][0] == 60_000_000

    def test_data_after_last_spine_point_unassigned(self):
        """Data after last spine boundary must remain unassigned."""
        spine = pl.LazyFrame(
            {
                "ts_local_us": [60_000_000, 120_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
            }
        )
        data = pl.LazyFrame(
            {
                "ts_local_us": [130_000_000],
                "exchange_id": [1],
                "symbol_id": [12345],
                "value": [100],
            }
        )

        bucketed = assign_to_buckets(data, spine).collect()
        assert bucketed["bucket_ts"][0] is None
