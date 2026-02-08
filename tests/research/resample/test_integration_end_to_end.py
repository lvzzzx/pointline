"""Integration test for complete resample-aggregate pipeline.

This test validates the full workflow:
1. Build spine with ClockSpineBuilder
2. Assign data to buckets with assign_to_buckets
3. Apply aggregations (built-in and custom) with aggregate
4. Verify PIT correctness and results
"""

from unittest.mock import patch

import polars as pl
import pytest

from pointline.research.features.spines import ClockSpineBuilder, ClockSpineConfig
from pointline.research.resample import (
    AggregateConfig,
    AggregationSpec,
    aggregate,
    assign_to_buckets,
)


@pytest.fixture
def mock_dim_symbol():
    """Mock dim_symbol table for testing."""
    with patch("pointline.research.features.spines.clock.read_dim_symbol_table") as mock:
        # Return a mock DataFrame with test symbol_ids
        mock_df = pl.DataFrame(
            {
                "symbol_id": [12345, 12346],
                "exchange_id": [1, 1],
            }
        )
        mock.return_value = mock_df
        yield mock


class TestEndToEndPipeline:
    """Test complete pipeline from spine to aggregated bars."""

    def test_complete_pipeline_with_trades(self, mock_dim_symbol):
        """Test complete pipeline with synthetic trade data.

        This test validates:
        - Spine generation (clock bars)
        - Bucket assignment (window-map semantics)
        - Aggregation (Pattern A built-ins)
        - PIT correctness
        - Result accuracy
        """
        # Step 1: Build spine (1-minute bars)
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)  # 1 minute

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=300_000_000,  # 5 minutes
            config=config,
        )

        spine_collected = spine.collect()
        print("\n=== Spine ===")
        print(spine_collected)
        assert len(spine_collected) == 5  # 5 bars
        assert spine_collected["ts_local_us"].to_list() == [
            60_000_000,
            120_000_000,
            180_000_000,
            240_000_000,
            300_000_000,
        ]

        # Step 2: Create synthetic trade data
        trades = pl.LazyFrame(
            {
                "ts_local_us": [
                    10_000_000,  # 10s → bar at 60s
                    50_000_000,  # 50s → bar at 60s
                    70_000_000,  # 70s → bar at 120s
                    110_000_000,  # 110s → bar at 120s
                    130_000_000,  # 130s → bar at 180s
                    170_000_000,  # 170s → bar at 180s
                    250_000_000,  # 250s → bar at 300s
                ],
                "exchange_id": pl.Series([1] * 7, dtype=pl.Int16),
                "symbol_id": [12345] * 7,
                "qty_int": [100, 200, 150, 250, 120, 180, 300],
                "price_int": [50000, 50010, 50020, 50015, 50025, 50030, 50040],
                "side": [0, 1, 0, 1, 0, 1, 0],  # 0=buy, 1=sell
            }
        )

        # Step 3: Assign to buckets
        bucketed = assign_to_buckets(trades, spine)
        bucketed_collected = bucketed.collect()
        print("\n=== Bucketed Data ===")
        print(bucketed_collected.select(["ts_local_us", "bucket_ts", "qty_int"]))

        # Verify bucket assignments
        assert bucketed_collected["bucket_ts"][0] == 60_000_000  # 10s → 60s
        assert bucketed_collected["bucket_ts"][1] == 60_000_000  # 50s → 60s
        assert bucketed_collected["bucket_ts"][2] == 120_000_000  # 70s → 120s
        assert bucketed_collected["bucket_ts"][3] == 120_000_000  # 110s → 120s

        # Verify PIT correctness: all data ts < bucket_ts
        for i in range(len(bucketed_collected)):
            assert bucketed_collected["ts_local_us"][i] < bucketed_collected["bucket_ts"][i]

        # Step 4: Apply aggregations
        agg_config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty_int", agg="sum"),
                AggregationSpec(name="trade_count", source_column="qty_int", agg="count"),
                AggregationSpec(name="avg_price", source_column="price_int", agg="mean"),
                AggregationSpec(name="min_price", source_column="price_int", agg="min"),
                AggregationSpec(name="max_price", source_column="price_int", agg="max"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(bucketed, agg_config, spine=spine)
        result_collected = result.collect()
        print("\n=== Aggregated Bars ===")
        print(result_collected)

        # Verify results
        # Bar at 60s: trades at 10s (qty=100) and 50s (qty=200) → volume=300
        bar_60 = result_collected.filter(pl.col("ts_local_us") == 60_000_000)
        assert bar_60["volume"][0] == 300
        assert bar_60["trade_count"][0] == 2
        assert bar_60["avg_price"][0] == (50000 + 50010) / 2

        # Bar at 120s: trades at 70s (qty=150) and 110s (qty=250) → volume=400
        bar_120 = result_collected.filter(pl.col("ts_local_us") == 120_000_000)
        assert bar_120["volume"][0] == 400
        assert bar_120["trade_count"][0] == 2

        # Bar at 180s: trades at 130s (qty=120) and 170s (qty=180) → volume=300
        bar_180 = result_collected.filter(pl.col("ts_local_us") == 180_000_000)
        assert bar_180["volume"][0] == 300

        # Bar at 240s: no trades → volume=null (spine preserved)
        bar_240 = result_collected.filter(pl.col("ts_local_us") == 240_000_000)
        assert bar_240["volume"][0] is None

        # Bar at 300s: trade at 250s (qty=300) → volume=300
        bar_300 = result_collected.filter(pl.col("ts_local_us") == 300_000_000)
        assert bar_300["volume"][0] == 300

    def test_complete_pipeline_with_custom_aggregations(self, mock_dim_symbol):
        """Test pipeline with custom aggregations (Pattern A and B).

        This test validates:
        - Trade flow aggregations (Pattern A)
        - Multiple aggregation types
        - Mode validation
        """
        # Step 1: Build spine
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=120_000_000,  # 2 minutes
            config=config,
        )

        # Step 2: Create trade data with buy/sell sides
        trades = pl.LazyFrame(
            {
                "ts_local_us": [
                    10_000_000,  # Buy
                    20_000_000,  # Buy
                    30_000_000,  # Sell
                    70_000_000,  # Sell
                    80_000_000,  # Buy
                    90_000_000,  # Sell
                ],
                "exchange_id": pl.Series([1] * 6, dtype=pl.Int16),
                "symbol_id": [12345] * 6,
                "qty_int": [100, 200, 150, 200, 300, 100],
                "side": [0, 0, 1, 1, 0, 1],  # 0=buy, 1=sell
            }
        )

        # Step 3: Assign to buckets
        bucketed = assign_to_buckets(trades, spine)

        # Step 4: Apply aggregations including custom
        agg_config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty_int", agg="sum"),
                AggregationSpec(
                    name="imbalance",
                    source_column="qty_int",
                    agg="signed_trade_imbalance",
                ),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(bucketed, agg_config, spine=spine)
        result_collected = result.collect()
        print("\n=== Custom Aggregations Result ===")
        print(result_collected)

        # Bar at 60s:
        # Buy: 100 + 200 = 300
        # Sell: 150
        # Imbalance: (300 - 150) / (300 + 150) = 150/450 = 0.333
        bar_60 = result_collected.filter(pl.col("ts_local_us") == 60_000_000)
        assert bar_60["volume"][0] == 450
        expected_imbalance = (300 - 150) / (300 + 150)
        assert abs(bar_60["imbalance"][0] - expected_imbalance) < 0.001

        # Bar at 120s:
        # Buy: 300
        # Sell: 200 + 100 = 300
        # Imbalance: 0 (balanced)
        bar_120 = result_collected.filter(pl.col("ts_local_us") == 120_000_000)
        assert bar_120["volume"][0] == 600
        assert abs(bar_120["imbalance"][0] - 0.0) < 0.001

    def test_multi_symbol_pipeline(self, mock_dim_symbol):
        """Test pipeline with multiple symbols.

        This test validates:
        - Multi-symbol spine generation
        - Correct bucket assignment per symbol
        - Aggregation grouped by symbol
        """
        # Step 1: Build multi-symbol spine
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=[12345, 12346],  # Two symbols
            start_ts_us=0,
            end_ts_us=120_000_000,
            config=config,
        )

        spine_collected = spine.collect()
        print("\n=== Multi-Symbol Spine ===")
        print(spine_collected)
        # Should have 2 symbols × 2 bars = 4 rows
        assert len(spine_collected) == 4

        # Step 2: Create data for both symbols
        data = pl.LazyFrame(
            {
                "ts_local_us": [10_000_000, 20_000_000, 70_000_000, 80_000_000],
                "exchange_id": pl.Series([1, 1, 1, 1], dtype=pl.Int16),
                "symbol_id": [12345, 12346, 12345, 12346],
                "qty_int": [100, 200, 150, 250],
            }
        )

        # Step 3: Assign to buckets
        bucketed = assign_to_buckets(data, spine)
        bucketed_collected = bucketed.collect()
        print("\n=== Multi-Symbol Bucketed ===")
        print(bucketed_collected)

        # Step 4: Aggregate
        agg_config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty_int", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        result = aggregate(bucketed, agg_config, spine=spine)
        result_collected = result.collect()
        print("\n=== Multi-Symbol Result ===")
        print(result_collected)

        # Verify symbol 12345
        symbol_12345 = result_collected.filter(pl.col("symbol_id") == 12345)
        bar_60 = symbol_12345.filter(pl.col("ts_local_us") == 60_000_000)
        assert bar_60["volume"][0] == 100  # Only 10s trade
        bar_120 = symbol_12345.filter(pl.col("ts_local_us") == 120_000_000)
        assert bar_120["volume"][0] == 150  # Only 70s trade

        # Verify symbol 12346
        symbol_12346 = result_collected.filter(pl.col("symbol_id") == 12346)
        bar_60 = symbol_12346.filter(pl.col("ts_local_us") == 60_000_000)
        assert bar_60["volume"][0] == 200  # Only 20s trade
        bar_120 = symbol_12346.filter(pl.col("ts_local_us") == 120_000_000)
        assert bar_120["volume"][0] == 250  # Only 80s trade


class TestPITCorrectnessValidation:
    """Test PIT correctness across the full pipeline."""

    def test_pit_invariant_enforced(self, mock_dim_symbol):
        """Test that PIT invariant is enforced throughout pipeline.

        PIT Invariant: For any bar at timestamp T, all data must have ts < T.
        """
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=180_000_000,
            config=config,
        )

        # Data including boundary timestamp
        data = pl.LazyFrame(
            {
                "ts_local_us": [
                    59_999_999,  # Just before 60s
                    60_000_000,  # Exactly at 60s (should go to 120s bar)
                    119_999_999,  # Just before 120s
                    120_000_000,  # Exactly at 120s (should go to 180s bar)
                ],
                "exchange_id": pl.Series([1] * 4, dtype=pl.Int16),
                "symbol_id": [12345] * 4,
                "qty_int": [100, 200, 300, 400],
            }
        )

        bucketed = assign_to_buckets(data, spine)
        bucketed_collected = bucketed.collect()

        # Verify PIT correctness for every row
        for i in range(len(bucketed_collected)):
            data_ts = bucketed_collected["ts_local_us"][i]
            bucket_ts = bucketed_collected["bucket_ts"][i]
            assert data_ts < bucket_ts, f"PIT violation: {data_ts} >= {bucket_ts}"

        # Verify specific assignments
        assert bucketed_collected["bucket_ts"][0] == 60_000_000  # 59.999s → 60s
        assert bucketed_collected["bucket_ts"][1] == 120_000_000  # 60s → 120s (boundary)
        assert bucketed_collected["bucket_ts"][2] == 120_000_000  # 119.999s → 120s
        assert bucketed_collected["bucket_ts"][3] == 180_000_000  # 120s → 180s (boundary)


class TestDeterministicReproducibility:
    """Test deterministic reproducibility."""

    def test_deterministic_output(self, mock_dim_symbol):
        """Test that same inputs produce identical outputs.

        This validates:
        - Deterministic sorting
        - Stable aggregation results
        - Reproducibility across runs
        """
        builder = ClockSpineBuilder()
        config = ClockSpineConfig(step_ms=60_000)

        spine = builder.build_spine(
            symbol_id=12345,
            start_ts_us=0,
            end_ts_us=120_000_000,
            config=config,
        )

        # Same data, run twice
        data = pl.LazyFrame(
            {
                "ts_local_us": [50_000_000, 110_000_000, 30_000_000],
                "exchange_id": pl.Series([1, 1, 1], dtype=pl.Int16),
                "symbol_id": [12345, 12345, 12345],
                "qty_int": [100, 200, 150],
            }
        )

        agg_config = AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[
                AggregationSpec(name="volume", source_column="qty_int", agg="sum"),
            ],
            mode="bar_then_feature",
            research_mode="MFT",
        )

        # Run 1
        bucketed_1 = assign_to_buckets(data, spine, deterministic=True)
        result_1 = aggregate(bucketed_1, agg_config, spine=spine).collect()

        # Run 2
        bucketed_2 = assign_to_buckets(data, spine, deterministic=True)
        result_2 = aggregate(bucketed_2, agg_config, spine=spine).collect()

        # Results should be identical
        assert result_1.equals(result_2)
