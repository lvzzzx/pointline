"""Test crypto_mft aggregations for middle-frequency trading.

This module tests the 9 custom aggregations in crypto_mft.py:
- flow_imbalance: Order flow imbalance
- spread_bps: Bid-ask spread in basis points
- book_imbalance_top5: Order book imbalance over top 5 levels
- realized_volatility: Realized volatility from tick returns
- avg_trade_size, median_trade_size, max_trade_size: Trade size statistics
- aggressive_ratio: Aggressive order ratio (simplified)
- vw_return: Volume-weighted return
"""

import polars as pl

from pointline.research.resample import AggregationRegistry


class TestFlowImbalance:
    """Test flow_imbalance aggregation."""

    def test_flow_imbalance_registration(self):
        """Test flow_imbalance is registered correctly."""
        assert "flow_imbalance" in AggregationRegistry._registry
        meta = AggregationRegistry.get("flow_imbalance")
        assert meta.name == "flow_imbalance"
        assert meta.stage == "aggregate_then_feature"
        assert "MFT" in meta.mode_allowlist
        assert "LFT" in meta.mode_allowlist
        assert meta.aggregate_raw is not None

    def test_flow_imbalance_balanced(self):
        """Test flow imbalance with balanced buy/sell volume."""
        from pointline.research.resample.aggregations.crypto_mft import flow_imbalance

        data = pl.DataFrame(
            {
                "qty_int": [100, 200, 150, 50],  # Total: 500
                "side": [0, 0, 1, 1],  # 300 buy, 200 sell
            }
        )

        result = data.select(flow_imbalance("qty_int").alias("flow_imbalance"))

        # Flow imbalance: (300 - 200) / 500 = 0.2
        assert abs(result["flow_imbalance"][0] - 0.2) < 0.001

    def test_flow_imbalance_all_buys(self):
        """Test flow imbalance with all buy trades."""
        from pointline.research.resample.aggregations.crypto_mft import flow_imbalance

        data = pl.DataFrame(
            {
                "qty_int": [100, 200, 150],
                "side": [0, 0, 0],  # All buys
            }
        )

        result = data.select(flow_imbalance("qty_int").alias("flow_imbalance"))

        # All buys: imbalance = 1.0
        assert abs(result["flow_imbalance"][0] - 1.0) < 0.001

    def test_flow_imbalance_all_sells(self):
        """Test flow imbalance with all sell trades."""
        from pointline.research.resample.aggregations.crypto_mft import flow_imbalance

        data = pl.DataFrame(
            {
                "qty_int": [100, 200, 150],
                "side": [1, 1, 1],  # All sells
            }
        )

        result = data.select(flow_imbalance("qty_int").alias("flow_imbalance"))

        # All sells: imbalance = -1.0
        assert abs(result["flow_imbalance"][0] - (-1.0)) < 0.001


class TestSpreadBPS:
    """Test spread_bps aggregation."""

    def test_spread_bps_registration(self):
        """Test spread_bps is registered correctly."""
        assert "spread_bps" in AggregationRegistry._registry
        meta = AggregationRegistry.get("spread_bps")
        assert meta.name == "spread_bps"
        assert meta.stage == "aggregate_then_feature"
        assert "MFT" in meta.mode_allowlist

    def test_spread_bps_computation(self):
        """Test spread in basis points computation."""
        from pointline.research.resample.aggregations.crypto_mft import spread_bps

        # Last quote: bid=50000, ask=50005
        # Mid = 50002.5, spread = 5
        # BPS = 5 / 50002.5 * 10000 ≈ 0.9999 bps
        # (Works directly on integer columns — encoding scalar cancels in ratio)
        data = pl.DataFrame(
            {
                "bid_px_int": [49990, 49995, 50000],
                "ask_px_int": [49995, 50000, 50005],
            }
        )

        result = data.select(spread_bps("bid_px_int").alias("spread_bps"))

        # Expected: (50005 - 50000) / ((50000 + 50005) / 2) * 10000
        # = 5 / 50002.5 * 10000 ≈ 0.9999 bps
        expected_bps = 5 / 50002.5 * 10000
        assert abs(result["spread_bps"][0] - expected_bps) < 0.01


class TestBookImbalanceTop5:
    """Test book_imbalance_top5 aggregation."""

    def test_book_imbalance_registration(self):
        """Test book_imbalance_top5 is registered correctly."""
        assert "book_imbalance_top5" in AggregationRegistry._registry
        meta = AggregationRegistry.get("book_imbalance_top5")
        assert meta.name == "book_imbalance_top5"
        assert meta.stage == "aggregate_then_feature"

    def test_book_imbalance_computation(self):
        """Test book imbalance over top 5 levels."""
        from pointline.research.resample.aggregations.crypto_mft import (
            book_imbalance_top5,
        )

        # Create book snapshot with 25 levels (top 5: indices 0-4)
        # Bids top 5: [100, 90, 80, 70, 60] = 400
        # Asks top 5: [80, 70, 60, 50, 40] = 300
        data = pl.DataFrame(
            {
                "bids_qty_int": [[100, 90, 80, 70, 60] + [10] * 20],  # Top 5 + remaining 20 levels
                "asks_qty_int": [[80, 70, 60, 50, 40] + [10] * 20],
            }
        )

        result = data.select(book_imbalance_top5("bids_qty_int").alias("book_imbalance_top5"))

        # Imbalance: (400 - 300) / (400 + 300) = 100 / 700 ≈ 0.143
        expected_imbalance = (400 - 300) / (400 + 300)
        assert abs(result["book_imbalance_top5"][0] - expected_imbalance) < 0.001

    def test_book_imbalance_bid_heavy(self):
        """Test book imbalance with bid-heavy book."""
        from pointline.research.resample.aggregations.crypto_mft import (
            book_imbalance_top5,
        )

        data = pl.DataFrame(
            {
                "bids_qty_int": [[1000, 900, 800, 700, 600] + [10] * 20],
                "asks_qty_int": [[100, 90, 80, 70, 60] + [10] * 20],
            }
        )

        result = data.select(book_imbalance_top5("bids_qty_int").alias("book_imbalance_top5"))

        # Bid depth = 4000, Ask depth = 400
        # Imbalance: (4000 - 400) / 4400 ≈ 0.818
        expected_imbalance = (4000 - 400) / (4000 + 400)
        assert abs(result["book_imbalance_top5"][0] - expected_imbalance) < 0.001


class TestRealizedVolatility:
    """Test realized_volatility aggregation."""

    def test_realized_volatility_registration(self):
        """Test realized_volatility is registered correctly."""
        assert "realized_volatility" in AggregationRegistry._registry
        meta = AggregationRegistry.get("realized_volatility")
        assert meta.name == "realized_volatility"
        assert meta.stage == "aggregate_then_feature"

    def test_realized_volatility_computation(self):
        """Test realized volatility from tick returns."""
        from pointline.research.resample.aggregations.crypto_mft import (
            realized_volatility,
        )

        # Prices (integer encoded): 10000, 10010, 10005, 10020
        # Log returns: log(10010/10000), log(10005/10010), log(10020/10005)
        # (scale-invariant — encoding scalar cancels in log ratio)
        data = pl.DataFrame(
            {
                "px_int": [10000, 10010, 10005, 10020],
            }
        )

        result = data.select(realized_volatility("px_int").alias("realized_vol"))

        # Verify result is a positive number (actual value depends on std calculation)
        assert result["realized_vol"][0] > 0
        assert result["realized_vol"][0] < 0.01  # Reasonable range for this data

    def test_realized_volatility_zero_movement(self):
        """Test realized volatility with no price movement."""
        from pointline.research.resample.aggregations.crypto_mft import (
            realized_volatility,
        )

        data = pl.DataFrame(
            {
                "px_int": [10000, 10000, 10000, 10000],  # No movement
            }
        )

        result = data.select(realized_volatility("px_int").alias("realized_vol"))

        # No movement = zero volatility
        assert result["realized_vol"][0] == 0.0


class TestTradeSize:
    """Test trade size aggregations."""

    def test_avg_trade_size_registration(self):
        """Test avg_trade_size is registered correctly."""
        assert "avg_trade_size" in AggregationRegistry._registry
        meta = AggregationRegistry.get("avg_trade_size")
        assert meta.name == "avg_trade_size"
        assert meta.stage == "aggregate_then_feature"

    def test_trade_size_statistics(self):
        """Test trade size statistics computation."""
        from pointline.research.resample.aggregations.crypto_mft import (
            avg_trade_size,
            max_trade_size,
            median_trade_size,
        )

        # Trade sizes in encoded integer domain: 100, 200, 150, 50
        # Results are in integer domain (multiply by profile.amount to get floats)
        data = pl.DataFrame(
            {
                "qty_int": [100, 200, 150, 50],
            }
        )

        result = data.select(
            [
                avg_trade_size("qty_int").alias("avg_trade_size"),
                median_trade_size("qty_int").alias("median_trade_size"),
                max_trade_size("qty_int").alias("max_trade_size"),
            ]
        )

        # Average: (100 + 200 + 150 + 50) / 4 = 125.0
        assert abs(result["avg_trade_size"][0] - 125.0) < 0.001

        # Median: sorted [50, 100, 150, 200] -> (100 + 150) / 2 = 125.0
        assert abs(result["median_trade_size"][0] - 125.0) < 0.001

        # Max: 200.0
        assert abs(result["max_trade_size"][0] - 200.0) < 0.001


class TestAggressiveRatio:
    """Test aggressive_ratio aggregation."""

    def test_aggressive_ratio_registration(self):
        """Test aggressive_ratio is registered correctly."""
        assert "aggressive_ratio" in AggregationRegistry._registry
        meta = AggregationRegistry.get("aggressive_ratio")
        assert meta.name == "aggressive_ratio"
        assert meta.stage == "aggregate_then_feature"

    def test_aggressive_ratio_placeholder(self):
        """Test aggressive ratio placeholder (currently returns 1.0)."""
        from pointline.research.resample.aggregations.crypto_mft import aggressive_ratio

        data = pl.DataFrame(
            {
                "qty_int": [100, 200, 150, 50],
                "side": [0, 0, 1, 1],
            }
        )

        result = data.select(aggressive_ratio("qty_int").alias("aggressive_ratio"))

        # Placeholder: all trades assumed aggressive
        assert result["aggressive_ratio"][0] == 1.0


class TestVolumeWeightedReturn:
    """Test vw_return aggregation."""

    def test_vw_return_registration(self):
        """Test vw_return is registered correctly."""
        assert "vw_return" in AggregationRegistry._registry
        meta = AggregationRegistry.get("vw_return")
        assert meta.name == "vw_return"
        assert meta.stage == "aggregate_then_feature"

    def test_vw_return_computation(self):
        """Test volume-weighted return computation."""
        from pointline.research.resample.aggregations.crypto_mft import vw_return

        # Prices (integer encoded): 10000, 10010, 10020
        # Works directly on integer columns — log returns are scale-invariant
        data = pl.DataFrame(
            {
                "px_int": [10000, 10010, 10020],
                "qty_int": [100, 200, 100],
            }
        )

        result = data.select(vw_return("px_int").alias("vw_return"))

        # VW return = [100 * log(1.0) + 200 * log(1.001) + 100 * log(1.002)] / 400
        # Should be positive and small
        assert result["vw_return"][0] > 0
        assert result["vw_return"][0] < 0.01

    def test_vw_return_zero_movement(self):
        """Test volume-weighted return with no price change."""
        from pointline.research.resample.aggregations.crypto_mft import vw_return

        data = pl.DataFrame(
            {
                "px_int": [10000, 10000, 10000],
                "qty_int": [100, 200, 100],
            }
        )

        result = data.select(vw_return("px_int").alias("vw_return"))

        # No price movement = zero return
        assert result["vw_return"][0] == 0.0
