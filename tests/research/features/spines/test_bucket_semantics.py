"""Test critical bucket assignment semantics for spine builders.

This module tests the CRITICAL bar timestamp semantics:
- Spine timestamps are BAR ENDS (interval ends)
- Bar at timestamp T contains data with ts_local_us < T
- Bar window = [T_prev, T) (half-open interval)

These tests ensure Point-In-Time (PIT) correctness.
"""

from unittest.mock import patch

import polars as pl
import pytest

from pointline.research.spines import ClockSpineConfig
from pointline.research.spines.clock import ClockSpineBuilder


@pytest.fixture
def mock_dim_symbol():
    """Mock dim_symbol table for testing."""
    with patch("pointline.research.spines.clock.read_dim_symbol_table") as mock:
        # Return a mock DataFrame with test symbol_ids
        mock_df = pl.DataFrame(
            {
                "symbol_id": [12345, 12346],
                "exchange_id": [1, 1],
            }
        )
        mock.return_value = mock_df
        yield mock


class TestClockSpineBarEndSemantics:
    """Test clock spine generates bar ENDS (interval ends)."""

    def test_clock_spine_bar_end_semantics(self, mock_dim_symbol):
        """CRITICAL: Verify spine timestamps are bar ENDS."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=180_000_000,  # 3 minutes
            config=config,
        ).collect()

        # Timestamps should be at interval ENDS
        assert spine["ts_local_us"][0] == 60_000_000, "First bar end at 1m"
        assert spine["ts_local_us"][1] == 120_000_000, "Second bar end at 2m"
        assert spine["ts_local_us"][2] == 180_000_000, "Third bar end at 3m"

        # Bar at 60ms SHOULD contain data in [0ms, 60ms)
        # Bar at 120ms SHOULD contain data in [60ms, 120ms)

    def test_clock_spine_protocol_compliance(self):
        """Verify ClockSpineBuilder implements protocol."""
        builder = ClockSpineBuilder()

        assert hasattr(builder, "build_spine")
        assert hasattr(builder, "name")
        assert hasattr(builder, "display_name")
        assert hasattr(builder, "supports_single_symbol")
        assert hasattr(builder, "supports_multi_symbol")
        assert hasattr(builder, "can_handle")
        assert builder.name == "clock"

    def test_clock_spine_grid_alignment(self, mock_dim_symbol):
        """Verify clock spine aligns to grid correctly."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=300_000_000,  # 5 minutes
            config=config,
        ).collect()

        # Should have 5 boundaries (1m, 2m, 3m, 4m, 5m)
        assert len(spine) == 5

        # Timestamps should be at 1-minute intervals (bar ends)
        timestamps = spine["ts_local_us"].to_list()
        assert timestamps == [
            60_000_000,
            120_000_000,
            180_000_000,
            240_000_000,
            300_000_000,
        ]

    def test_clock_spine_non_zero_start(self, mock_dim_symbol):
        """Verify clock spine handles non-zero start correctly."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        # Start at 50ms, should align to next bar end at 60ms
        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=50_000_000,  # 50 seconds
            end_ts_us=180_000_000,  # 3 minutes
            config=config,
        ).collect()

        # First bar should end at 60s (aligned to grid)
        assert spine["ts_local_us"][0] == 60_000_000
        assert spine["ts_local_us"][1] == 120_000_000
        assert spine["ts_local_us"][2] == 180_000_000

    def test_clock_spine_mid_interval_start(self, mock_dim_symbol):
        """Verify clock spine aligns mid-interval starts correctly."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        # Start at 70ms (after first bar), should start at 120ms
        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=70_000_000,  # 70 seconds (1m 10s)
            end_ts_us=300_000_000,  # 5 minutes
            config=config,
        ).collect()

        # First bar should be at 120ms (next bar end after 70ms)
        timestamps = spine["ts_local_us"].to_list()
        assert timestamps == [120_000_000, 180_000_000, 240_000_000, 300_000_000]

    def test_clock_spine_multi_symbol(self, mock_dim_symbol):
        """Verify clock spine supports multiple symbols correctly."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        spine = builder.build_spine(
            symbol_id=[12345, 12346],  # Two symbols
            start_ts_us=0,
            end_ts_us=120_000_000,  # 2 minutes
            config=config,
        ).collect()

        # Should have 2 timestamps × 2 symbols = 4 rows
        assert len(spine) == 4

        # Check both symbols have same timestamps
        symbol_12345 = spine.filter(pl.col("symbol_id") == 12345)
        symbol_12346 = spine.filter(pl.col("symbol_id") == 12346)

        assert symbol_12345["ts_local_us"].to_list() == [60_000_000, 120_000_000]
        assert symbol_12346["ts_local_us"].to_list() == [60_000_000, 120_000_000]

    def test_clock_spine_empty_range(self, mock_dim_symbol):
        """Verify clock spine handles empty range (no bars)."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        # End before first bar
        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=30_000_000,  # 30 seconds (before first bar at 60s)
            config=config,
        ).collect()

        # Should have no rows
        assert len(spine) == 0


class TestClockSpineBarWindowSemantics:
    """Test that bar windows are half-open [T_prev, T)."""

    def test_bar_window_interpretation(self, mock_dim_symbol):
        """Test conceptual bar window boundaries.

        This test documents the expected behavior for bucket assignment:
        - Bar at 60ms should contain data in [0ms, 60ms)
        - Data at 60ms should go to bar at 120ms (boundary goes to next)
        """
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=180_000_000,
            config=config,
        ).collect()

        # Spine timestamps are bar ENDS
        assert spine["ts_local_us"][0] == 60_000_000
        assert spine["ts_local_us"][1] == 120_000_000
        assert spine["ts_local_us"][2] == 180_000_000

        # Interpretation for bucket assignment (tested in Phase 2):
        # - Data at 50ms → bar at 60ms (50ms < 60ms, in window [0, 60))
        # - Data at 60ms → bar at 120ms (60ms >= 60ms, in window [60, 120))
        # - Data at 110ms → bar at 120ms (110ms < 120ms, in window [60, 120))

    def test_pit_correctness_invariant(self, mock_dim_symbol):
        """Document the PIT correctness invariant.

        PIT invariant: For any data point assigned to bar at time T,
        the data timestamp must be < T (strictly less than).

        This ensures no lookahead bias: the bar only contains data
        that was available BEFORE the bar timestamp.
        """
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=120_000_000,
            config=config,
        ).collect()

        # PIT invariant will be enforced by bucket assignment (Phase 2)
        # For bar at 60ms: all data must have ts < 60ms
        # For bar at 120ms: all data must have ts < 120ms

        # Spine just provides the bar boundaries
        assert spine["ts_local_us"].to_list() == [60_000_000, 120_000_000]


class TestClockSpineEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_step_ms(self):
        """Test validation of step_ms parameter."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=0)

        with pytest.raises(ValueError, match="step_ms must be positive"):
            builder.build_spine(
                symbol_id=12345,
                start_ts_us=0,
                end_ts_us=100_000_000,
                config=config,
            )

    def test_negative_step_ms(self):
        """Test validation of negative step_ms."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=-1000)

        with pytest.raises(ValueError, match="step_ms must be positive"):
            builder.build_spine(
                symbol_id=12345,
                start_ts_us=0,
                end_ts_us=100_000_000,
                config=config,
            )

    def test_max_rows_safety_limit(self, mock_dim_symbol):
        """Test max_rows safety limit prevents runaway queries."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(
            step_ms=1,  # 1 millisecond (very fine granularity)
            max_rows=1000,  # Low limit
        )

        with pytest.raises(RuntimeError, match="too many rows"):
            builder.build_spine(
                symbol_id=12345,
                start_ts_us=0,
                end_ts_us=10_000_000,  # 10 seconds = 10,000 bars
                config=config,
            )

    def test_wrong_config_type(self):
        """Test validation of config type."""
        builder = ClockSpineBuilder()
        from pointline.research.spines.base import SpineBuilderConfig

        # Wrong config type
        config = SpineBuilderConfig()

        with pytest.raises(TypeError, match="Expected ClockSpineConfig"):
            builder.build_spine(
                symbol_id=12345,
                start_ts_us=0,
                end_ts_us=100_000_000,
                config=config,
            )

    def test_deterministic_ordering(self, mock_dim_symbol):
        """Test spine is sorted deterministically."""
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=[12346, 12345],  # Intentionally out of order
            start_ts_us=0,
            end_ts_us=120_000_000,
            config=config,
        ).collect()

        # Should be sorted by (exchange_id, symbol_id, ts_local_us)
        # Since both symbols likely have same exchange_id, check symbol_id ordering
        prev_symbol_id = None
        prev_ts = None

        for row in spine.iter_rows(named=True):
            symbol_id = row["symbol_id"]
            ts = row["ts_local_us"]

            if prev_symbol_id is not None:
                # Either symbol_id increases, or same symbol with increasing ts
                if symbol_id == prev_symbol_id:
                    assert ts > prev_ts, "Timestamps should be increasing for same symbol"
                # Symbol IDs should be in sorted order for same timestamp
                # (but with cross join, we'll see all symbol_id values for each timestamp)

            prev_symbol_id = symbol_id
            prev_ts = ts


class TestClockSpineAlignment:
    """Test grid alignment logic."""

    def test_align_to_grid_zero_start(self):
        """Test alignment from timestamp 0."""
        builder = ClockSpineBuilder()

        # Access private method for unit test
        bar_end = builder._align_to_grid(ts=0, step_us=60_000_000)

        # First bar ends at 1 minute
        assert bar_end == 60_000_000

    def test_align_to_grid_mid_interval(self):
        """Test alignment from mid-interval."""
        builder = ClockSpineBuilder()

        # 50 seconds, should align to next bar at 60s
        bar_end = builder._align_to_grid(ts=50_000_000, step_us=60_000_000)
        assert bar_end == 60_000_000

        # 70 seconds, should align to next bar at 120s
        bar_end = builder._align_to_grid(ts=70_000_000, step_us=60_000_000)
        assert bar_end == 120_000_000

    def test_align_to_grid_exact_boundary(self):
        """Test alignment when timestamp is exactly on boundary."""
        builder = ClockSpineBuilder()

        # Exactly at 60s boundary, should align to NEXT bar at 120s
        bar_end = builder._align_to_grid(ts=60_000_000, step_us=60_000_000)
        assert bar_end == 120_000_000

        # Exactly at 120s boundary, should align to NEXT bar at 180s
        bar_end = builder._align_to_grid(ts=120_000_000, step_us=60_000_000)
        assert bar_end == 180_000_000


# NOTE: Volume spine tests would go here but volume.py needs similar updates
# This will be part of Phase 0 completion after clock spine is validated
