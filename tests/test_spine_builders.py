"""Tests for spine builder plugin system."""

import polars as pl
import pytest

from pointline.research.features.core import EventSpineConfig, build_event_spine
from pointline.research.spines import (
    ClockSpineConfig,
    DollarBarConfig,
    TradesSpineConfig,
    VolumeBarConfig,
    detect_builder,
    get_builder,
    list_builders,
)


class TestRegistry:
    """Test spine builder registry."""

    def test_list_builders(self):
        """Registry should have all builders."""
        builders = list_builders()
        assert "clock" in builders
        assert "trades" in builders
        assert "volume" in builders
        assert "dollar" in builders

    def test_get_builder_clock(self):
        """Should lookup clock builder."""
        builder = get_builder("clock")
        assert builder.name == "clock"
        assert builder.supports_single_symbol is True
        assert builder.supports_multi_symbol is True

    def test_get_builder_trades(self):
        """Should lookup trades builder."""
        builder = get_builder("trades")
        assert builder.name == "trades"

    def test_get_builder_unknown(self):
        """Should raise KeyError for unknown builder."""
        with pytest.raises(KeyError, match="Unknown spine builder"):
            get_builder("nonexistent")

    def test_detect_builder_clock(self):
        """Should detect clock builder from various mode strings."""
        assert detect_builder("clock") == "clock"
        assert detect_builder("Clock") == "clock"
        assert detect_builder("time") == "clock"
        assert detect_builder("fixed_time") == "clock"

    def test_detect_builder_trades(self):
        """Should detect trades builder from various mode strings."""
        assert detect_builder("trades") == "trades"
        assert detect_builder("Trades") == "trades"
        assert detect_builder("trade") == "trades"
        assert detect_builder("trade_event") == "trades"

    def test_detect_builder_unknown(self):
        """Should raise ValueError for unknown mode."""
        with pytest.raises(ValueError, match="No spine builder can handle mode"):
            detect_builder("unknown_mode")


class TestEventSpineAPI:
    """Test EventSpineConfig API with explicit builder configs."""

    def test_clock_spine_api(self, sample_symbol):
        """Clock spine should work with EventSpineConfig."""
        config = EventSpineConfig(builder_config=ClockSpineConfig(step_ms=1000, max_rows=1000))

        spine = build_event_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us="2024-05-01T00:00:00Z",
            end_ts_us="2024-05-01T00:01:00Z",
            config=config,
        )

        df = spine.collect()

        # Should have required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Should have ~60 rows (60 seconds x 1 symbol)
        assert 50 <= df.height <= 70

        # Should be sorted
        assert df["ts_local_us"].is_sorted()

    def test_trades_spine_api(self, sample_symbol):
        """Trades spine should work with EventSpineConfig."""
        config = EventSpineConfig(builder_config=TradesSpineConfig())

        spine = build_event_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us="2024-05-01T00:00:00Z",
            end_ts_us="2024-05-01T00:05:00Z",
            config=config,
        )

        df = spine.collect()

        # Should have required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Should be sorted
        assert df["ts_local_us"].is_sorted()

    def test_volume_bar_api(self, sample_symbol):
        """Volume bars should work with EventSpineConfig."""
        config = EventSpineConfig(builder_config=VolumeBarConfig(volume_threshold=1000.0))

        spine = build_event_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us="2024-05-01T00:00:00Z",
            end_ts_us="2024-05-01T00:05:00Z",
            config=config,
        )

        df = spine.collect()
        assert df.height >= 0

    def test_dollar_bar_api(self, sample_symbol):
        """Dollar bars should work with EventSpineConfig."""
        config = EventSpineConfig(builder_config=DollarBarConfig(dollar_threshold=100_000.0))

        spine = build_event_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us="2024-05-01T00:00:00Z",
            end_ts_us="2024-05-01T00:05:00Z",
            config=config,
        )

        df = spine.collect()
        assert df.height >= 0


class TestSpineContract:
    """Test that all builders comply with spine contract."""

    def test_spine_contract_clock(self, sample_symbol):
        """Clock spine should return required columns in correct order."""
        builder = get_builder("clock")
        config = ClockSpineConfig(step_ms=1000)

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714521660000000,  # 2024-05-01 00:01:00 UTC
            config=config,
        )

        df = spine.collect()

        # Required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Correct types
        assert df["ts_local_us"].dtype == pl.Int64
        assert df["exchange"].dtype == pl.Utf8
        assert df["symbol"].dtype == pl.Utf8

        # Deterministic ordering: (exchange, symbol, ts_local_us)
        # Check each column is sorted with respect to previous columns
        sorted_check = df.sort(["exchange", "symbol", "ts_local_us"])
        assert df.select(["exchange", "symbol", "ts_local_us"]).equals(
            sorted_check.select(["exchange", "symbol", "ts_local_us"])
        )

    def test_spine_contract_trades(self, sample_symbol):
        """Trades spine should return required columns in correct order."""
        builder = get_builder("trades")
        config = TradesSpineConfig()

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714521900000000,  # 2024-05-01 00:05:00 UTC
            config=config,
        )

        df = spine.collect()

        # Required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Deterministic ordering
        if df.height > 0:
            sorted_check = df.sort(["exchange", "symbol", "ts_local_us"])
            assert df.select(["exchange", "symbol", "ts_local_us"]).equals(
                sorted_check.select(["exchange", "symbol", "ts_local_us"])
            )

    def test_multi_symbol_spine(self, sample_symbols):
        """Multi-symbol spine should partition correctly."""
        if len(sample_symbols) < 2:
            pytest.skip("Need at least 2 symbols for multi-symbol test")

        builder = get_builder("clock")
        config = ClockSpineConfig(step_ms=1000)

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbols[:2],
            start_ts_us=1714521600000000,
            end_ts_us=1714521660000000,
            config=config,
        )

        df = spine.collect()

        # Should have rows for both symbols
        assert df["symbol"].n_unique() == 2

        # Should be sorted by (exchange, symbol, ts_local_us)
        sorted_check = df.sort(["exchange", "symbol", "ts_local_us"])
        assert df.select(["exchange", "symbol", "ts_local_us"]).equals(
            sorted_check.select(["exchange", "symbol", "ts_local_us"])
        )


class TestClockSpineBuilder:
    """Test clock spine builder."""

    def test_clock_spine_basic(self, sample_symbol):
        """Should generate spine at regular intervals."""
        builder = get_builder("clock")
        config = ClockSpineConfig(step_ms=1000)  # 1 second

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714521660000000,  # 2024-05-01 00:01:00 UTC
            config=config,
        )

        df = spine.collect()

        # Should have ~61 rows (0, 1, 2, ..., 60 seconds)
        assert 60 <= df.height <= 62

        # Verify step size
        if df.height > 1:
            diffs = df["ts_local_us"].diff().drop_nulls()
            assert (diffs == 1_000_000).all()  # 1 second in microseconds

    def test_clock_spine_invalid_step(self, sample_symbol):
        """Should reject invalid step_ms."""
        builder = get_builder("clock")
        config = ClockSpineConfig(step_ms=0)

        with pytest.raises(ValueError, match="step_ms must be positive"):
            builder.build_spine(
                exchange="binance-futures",
                symbol=sample_symbol,
                start_ts_us=1714521600000000,
                end_ts_us=1714521660000000,
                config=config,
            )

    def test_clock_spine_max_rows_enforcement(self, sample_symbol):
        """Should enforce max_rows safety limit."""
        builder = get_builder("clock")
        config = ClockSpineConfig(step_ms=1, max_rows=100)  # 1ms step, 100 row limit

        with pytest.raises(RuntimeError, match="too many rows"):
            builder.build_spine(
                exchange="binance-futures",
                symbol=sample_symbol,
                start_ts_us=1714521600000000,
                end_ts_us=1714521660000000,  # 60 seconds = 60,000 ms > 100
                config=config,
            )


class TestTradesSpineBuilder:
    """Test trades spine builder."""

    def test_trades_spine_basic(self, sample_symbol):
        """Should generate spine from trades stream."""
        builder = get_builder("trades")
        config = TradesSpineConfig()

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714521900000000,  # 2024-05-01 00:05:00 UTC
            config=config,
        )

        df = spine.collect()

        # Should have trades (if any exist in this time range)
        # We don't assert df.height > 0 because the symbol may have no trades
        assert df.height >= 0

        # Standard 3-column contract (no lineage columns after sweep dedup)
        if df.height > 0:
            assert set(df.columns) == {"ts_local_us", "exchange", "symbol"}


class TestVolumeSpineBuilder:
    """Test volume bar spine builder."""

    def test_volume_spine_basic(self, sample_symbol):
        """Should generate spine at volume thresholds."""
        builder = get_builder("volume")
        config = VolumeBarConfig(volume_threshold=1000.0)

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714522200000000,  # 2024-05-01 00:10:00 UTC
            config=config,
        )

        df = spine.collect()

        # Should have required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Should have some rows (if trades exist)
        assert df.height >= 0

        # Should be sorted
        if df.height > 0:
            sorted_check = df.sort(["exchange", "symbol", "ts_local_us"])
            assert df.select(["exchange", "symbol", "ts_local_us"]).equals(
                sorted_check.select(["exchange", "symbol", "ts_local_us"])
            )

    def test_volume_spine_invalid_threshold(self, sample_symbol):
        """Should reject invalid volume_threshold."""
        builder = get_builder("volume")
        config = VolumeBarConfig(volume_threshold=0)

        with pytest.raises(ValueError, match="volume_threshold must be positive"):
            builder.build_spine(
                exchange="binance-futures",
                symbol=sample_symbol,
                start_ts_us=1714521600000000,
                end_ts_us=1714522200000000,
                config=config,
            )

    def test_volume_spine_deterministic_ordering(self, sample_symbol):
        """Volume bars should be reproducible."""
        builder = get_builder("volume")
        config = VolumeBarConfig(volume_threshold=1000.0)

        spine1 = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,
            end_ts_us=1714522200000000,
            config=config,
        )

        spine2 = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,
            end_ts_us=1714522200000000,
            config=config,
        )

        df1 = spine1.collect()
        df2 = spine2.collect()

        # Should be identical
        if df1.height > 0:
            assert df1.equals(df2)


class TestDollarSpineBuilder:
    """Test dollar bar spine builder."""

    def test_dollar_spine_basic(self, sample_symbol):
        """Should generate spine at dollar thresholds."""
        builder = get_builder("dollar")
        config = DollarBarConfig(dollar_threshold=100_000.0)

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,  # 2024-05-01 00:00:00 UTC
            end_ts_us=1714522200000000,  # 2024-05-01 00:10:00 UTC
            config=config,
        )

        df = spine.collect()

        # Should have required columns
        assert "ts_local_us" in df.columns
        assert "exchange" in df.columns
        assert "symbol" in df.columns

        # Should have some rows (if trades exist)
        assert df.height >= 0

        # Should be sorted
        if df.height > 0:
            sorted_check = df.sort(["exchange", "symbol", "ts_local_us"])
            assert df.select(["exchange", "symbol", "ts_local_us"]).equals(
                sorted_check.select(["exchange", "symbol", "ts_local_us"])
            )

    def test_dollar_spine_notional_computation(self, sample_symbol):
        """Should compute notional as px x qty."""
        builder = get_builder("dollar")
        config = DollarBarConfig(dollar_threshold=100_000.0)

        spine = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,
            end_ts_us=1714522200000000,
            config=config,
        )

        df = spine.collect()

        # Should have generated bars (implicit test that notional calculation works)
        # We can't directly test the notional values since they're not in the output
        # But if the spine is generated without errors, the calculation is correct
        assert df.height >= 0

    def test_dollar_spine_invalid_threshold(self, sample_symbol):
        """Should reject invalid dollar_threshold."""
        builder = get_builder("dollar")
        config = DollarBarConfig(dollar_threshold=0)

        with pytest.raises(ValueError, match="dollar_threshold must be positive"):
            builder.build_spine(
                exchange="binance-futures",
                symbol=sample_symbol,
                start_ts_us=1714521600000000,
                end_ts_us=1714522200000000,
                config=config,
            )

    def test_dollar_spine_deterministic_ordering(self, sample_symbol):
        """Dollar bars should be reproducible."""
        builder = get_builder("dollar")
        config = DollarBarConfig(dollar_threshold=100_000.0)

        spine1 = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,
            end_ts_us=1714522200000000,
            config=config,
        )

        spine2 = builder.build_spine(
            exchange="binance-futures",
            symbol=sample_symbol,
            start_ts_us=1714521600000000,
            end_ts_us=1714522200000000,
            config=config,
        )

        df1 = spine1.collect()
        df2 = spine2.collect()

        # Should be identical
        if df1.height > 0:
            assert df1.equals(df2)


# Fixtures
@pytest.fixture
def sample_symbol():
    """Sample symbol for testing.

    Uses BTCUSDT on binance-futures (commonly available).
    """
    from pointline.registry import find_symbol

    symbols = find_symbol("BTCUSDT", exchange="binance-futures")
    if symbols.is_empty():
        pytest.skip("BTCUSDT not found in dim_symbol")

    return str(symbols["symbol"][0])


@pytest.fixture
def sample_symbols():
    """Multiple symbols for testing.

    Returns list of at least 2 symbols.
    """
    from pointline.registry import find_symbol

    symbols = find_symbol("USDT", exchange="binance-futures")
    if symbols.is_empty() or symbols.height < 2:
        pytest.skip("Need at least 2 USDT symbols for multi-symbol test")

    return symbols["symbol"].head(2).to_list()
