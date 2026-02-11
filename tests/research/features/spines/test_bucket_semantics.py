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

from pointline.research.spines import (
    ClockSpineConfig,
    DollarBarConfig,
    VolumeBarConfig,
    get_builder_by_config,
)
from pointline.research.spines.clock import ClockSpineBuilder


@pytest.fixture
def mock_dim_symbol():
    """Mock resolve_exchange_ids for testing."""
    with patch("pointline.research.spines.clock.resolve_exchange_ids") as mock:
        # Map each symbol_id to exchange_id=1
        mock.side_effect = lambda ids: [1] * len(ids)
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
    """Test grid alignment logic via generate_bar_end_timestamps."""

    def test_align_to_grid_zero_start(self):
        """Test alignment from timestamp 0."""
        from pointline.research.spines.clock import generate_bar_end_timestamps

        timestamps = generate_bar_end_timestamps(0, 60_000_000, 60_000_000)
        # First bar ends at 1 minute
        assert timestamps[0] == 60_000_000

    def test_align_to_grid_mid_interval(self):
        """Test alignment from mid-interval."""
        from pointline.research.spines.clock import generate_bar_end_timestamps

        # 50 seconds, should align to next bar at 60s
        timestamps = generate_bar_end_timestamps(50_000_000, 120_000_000, 60_000_000)
        assert timestamps[0] == 60_000_000

        # 70 seconds, should align to next bar at 120s
        timestamps = generate_bar_end_timestamps(70_000_000, 120_000_000, 60_000_000)
        assert timestamps[0] == 120_000_000

    def test_align_to_grid_exact_boundary(self):
        """Test alignment when timestamp is exactly on boundary."""
        from pointline.research.spines.clock import generate_bar_end_timestamps

        # Exactly at 60s boundary, should align to NEXT bar at 120s
        timestamps = generate_bar_end_timestamps(60_000_000, 180_000_000, 60_000_000)
        assert timestamps[0] == 120_000_000

        # Exactly at 120s boundary, should align to NEXT bar at 180s
        timestamps = generate_bar_end_timestamps(120_000_000, 180_000_000, 60_000_000)
        assert timestamps[0] == 180_000_000


class TestVolumeSpineBarEndSemantics:
    """Test volume spine generates bar ENDS (not starts)."""

    def _make_trades_lf(self) -> pl.LazyFrame:
        """Create synthetic trades for volume bar testing."""
        # 6 trades, each with volume=100 (qty_int=1, amount_increment=100)
        # Sorted by timestamp. volume_threshold=200 → 3 bars.
        return pl.LazyFrame(
            {
                "ts_local_us": [100, 200, 300, 400, 500, 600],
                "exchange_id": pl.Series([1] * 6, dtype=pl.Int16),
                "symbol_id": pl.Series([42] * 6, dtype=pl.Int64),
                "qty_int": pl.Series([1] * 6, dtype=pl.Int64),
                "file_id": pl.Series([1] * 6, dtype=pl.Int32),
                "file_line_number": pl.Series(list(range(1, 7)), dtype=pl.Int32),
            }
        )

    def _mock_dim_symbol(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol_id": pl.Series([42], dtype=pl.Int64),
                "amount_increment": [100.0],
            }
        )

    @patch("pointline.research.spines.volume.research_core")
    @patch("pointline.research.spines.volume.read_dim_symbol_table")
    def test_volume_bar_end_timestamps(self, mock_dim, mock_core):
        """Volume spine timestamps must be bar ENDS, not starts."""
        mock_core.scan_table.return_value = self._make_trades_lf()
        mock_dim.return_value = self._mock_dim_symbol()

        from pointline.research.spines.volume import VolumeSpineBuilder

        builder = VolumeSpineBuilder()
        config = VolumeBarConfig(volume_threshold=200.0)

        spine = builder.build_spine(
            symbol_id=42,
            start_ts_us=0,
            end_ts_us=1000,
            config=config,
        ).collect()

        # With volume_threshold=200 and volume=100 per trade:
        # cum_vol:  [100,  200,  300,  400,  500,  600]
        # bar_id:   [  0,    1,    1,    2,    2,    3]
        # bar_start (min ts per bar_id): [100, 200, 400, 600]
        # After shift: bar_end = [200, 400, 600, 1000(=end_ts_us)]
        assert spine.height == 4
        ts_list = spine["ts_local_us"].to_list()
        assert ts_list == [200, 400, 600, 1000], (
            f"Expected bar ENDS [200, 400, 600, 1000], got {ts_list}"
        )

    @patch("pointline.research.spines.volume.research_core")
    @patch("pointline.research.spines.volume.read_dim_symbol_table")
    def test_volume_max_rows_enforcement(self, mock_dim, mock_core):
        """Volume spine must enforce max_rows with an eager check."""
        mock_core.scan_table.return_value = self._make_trades_lf()
        mock_dim.return_value = self._mock_dim_symbol()

        from pointline.research.spines.volume import VolumeSpineBuilder

        builder = VolumeSpineBuilder()
        # threshold=100 → 6 bars, but max_rows=2
        config = VolumeBarConfig(volume_threshold=100.0, max_rows=2)

        with pytest.raises(RuntimeError, match="too many rows"):
            builder.build_spine(
                symbol_id=42,
                start_ts_us=0,
                end_ts_us=1000,
                config=config,
            ).collect()


class TestDollarSpineBarEndSemantics:
    """Test dollar spine generates bar ENDS (not starts)."""

    def _make_trades_lf(self) -> pl.LazyFrame:
        # 4 trades, notional = px_int * price_inc * qty_int * amount_inc
        # = 10 * 1.0 * 1 * 100.0 = 1000 per trade
        # dollar_threshold=2000 → 2 bars
        return pl.LazyFrame(
            {
                "ts_local_us": [100, 200, 300, 400],
                "exchange_id": pl.Series([1] * 4, dtype=pl.Int16),
                "symbol_id": pl.Series([42] * 4, dtype=pl.Int64),
                "px_int": pl.Series([10] * 4, dtype=pl.Int64),
                "qty_int": pl.Series([1] * 4, dtype=pl.Int64),
                "file_id": pl.Series([1] * 4, dtype=pl.Int32),
                "file_line_number": pl.Series(list(range(1, 5)), dtype=pl.Int32),
            }
        )

    def _mock_dim_symbol(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol_id": pl.Series([42], dtype=pl.Int64),
                "price_increment": [1.0],
                "amount_increment": [100.0],
            }
        )

    @patch("pointline.research.spines.dollar.research_core")
    @patch("pointline.research.spines.dollar.read_dim_symbol_table")
    def test_dollar_bar_end_timestamps(self, mock_dim, mock_core):
        """Dollar spine timestamps must be bar ENDS, not starts."""
        mock_core.scan_table.return_value = self._make_trades_lf()
        mock_dim.return_value = self._mock_dim_symbol()

        from pointline.research.spines.dollar import DollarSpineBuilder

        builder = DollarSpineBuilder()
        config = DollarBarConfig(dollar_threshold=2000.0)

        spine = builder.build_spine(
            symbol_id=42,
            start_ts_us=0,
            end_ts_us=1000,
            config=config,
        ).collect()

        # notional per trade = |10*1.0 * 1*100.0| = 1000
        # cum_notional: [1000, 2000, 3000, 4000]
        # bar_id:       [   0,    1,    1,    2]
        # bar_start (min ts per bar_id): [100, 200, 400]
        # After shift: bar_end = [200, 400, 1000(=end_ts_us)]
        assert spine.height == 3
        ts_list = spine["ts_local_us"].to_list()
        assert ts_list == [200, 400, 1000], f"Expected bar ENDS [200, 400, 1000], got {ts_list}"

    @patch("pointline.research.spines.dollar.research_core")
    @patch("pointline.research.spines.dollar.read_dim_symbol_table")
    def test_dollar_max_rows_enforcement(self, mock_dim, mock_core):
        """Dollar spine must enforce max_rows with an eager check."""
        mock_core.scan_table.return_value = self._make_trades_lf()
        mock_dim.return_value = self._mock_dim_symbol()

        from pointline.research.spines.dollar import DollarSpineBuilder

        builder = DollarSpineBuilder()
        # threshold=1000 → 4 bars, but max_rows=2
        config = DollarBarConfig(dollar_threshold=1000.0, max_rows=2)

        with pytest.raises(RuntimeError, match="too many rows"):
            builder.build_spine(
                symbol_id=42,
                start_ts_us=0,
                end_ts_us=1000,
                config=config,
            ).collect()


class TestConfigTypeDispatch:
    """Test dynamic config_type dispatch in registry."""

    def test_dispatch_clock(self):
        builder = get_builder_by_config(ClockSpineConfig())
        assert builder.name == "clock"

    def test_dispatch_volume(self):
        builder = get_builder_by_config(VolumeBarConfig())
        assert builder.name == "volume"

    def test_dispatch_dollar(self):
        builder = get_builder_by_config(DollarBarConfig())
        assert builder.name == "dollar"

    def test_dispatch_trades(self):
        from pointline.research.spines import TradesSpineConfig

        builder = get_builder_by_config(TradesSpineConfig())
        assert builder.name == "trades"

    def test_config_type_property(self):
        """Each builder's config_type should return its config class."""
        from pointline.research.spines import get_builder

        assert get_builder("clock").config_type is ClockSpineConfig
        assert get_builder("volume").config_type is VolumeBarConfig
        assert get_builder("dollar").config_type is DollarBarConfig
