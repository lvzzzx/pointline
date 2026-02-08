"""Test custom aggregations with correct schema alignment.

This module tests:
- Microstructure aggregations (microprice, spread, OFI)
- Trade flow aggregations (imbalance)
- Derivative aggregations (funding rate, OI)
- Registration correctness
- Schema alignment with actual tables
"""

import polars as pl
import pytest

from pointline.research.resample import AggregationRegistry, AggregationSpec


class TestMicrostructureAggregations:
    """Test microstructure aggregations."""

    def test_microprice_close_registration(self):
        """Test microprice_close is registered correctly.

        COMPLETE: Full test specification.
        """
        assert "microprice_close" in AggregationRegistry._registry
        meta = AggregationRegistry.get("microprice_close")
        assert meta.stage == "feature_then_aggregate"
        assert meta.compute_features is not None
        assert meta.aggregate_raw is None
        assert "HFT" in meta.mode_allowlist
        assert "MFT" in meta.mode_allowlist

    def test_microprice_close_computation(self):
        """Test microprice computation with actual column names.

        COMPLETE: Full test with actual book schema.
        """
        from pointline.research.resample.aggregations.microstructure import (
            microprice_close,
        )

        # Create synthetic book snapshot data with arrays
        data = pl.LazyFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                "bids_px_int": [[50000], [50010], [49990]],  # Array with best bid
                "asks_px_int": [[50005], [50015], [49995]],  # Array with best ask
                "bids_sz_int": [[100], [150], [120]],  # Array with best bid size
                "asks_sz_int": [[80], [90], [100]],  # Array with best ask size
            }
        )

        spec = AggregationSpec(
            name="microprice",
            source_column="bids_px_int",
            agg="microprice_close",
        )
        result = microprice_close(data, spec).collect()

        # Verify feature column created
        assert "_microprice_close_feature" in result.columns

        # Manual calculation for first row:
        # (50000 * 80 + 50005 * 100) / (100 + 80) = (4000000 + 5000500) / 180 = 50002.78
        expected = (50000 * 80 + 50005 * 100) / (100 + 80)
        assert abs(result["_microprice_close_feature"][0] - expected) < 0.01

    def test_spread_distribution_computation(self):
        """Test spread distribution computes correctly.

        COMPLETE: Full test with actual column names.
        """
        from pointline.research.resample.aggregations.microstructure import (
            spread_distribution,
        )

        # Create synthetic data with actual quotes schema
        data = pl.LazyFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                "bid_px_int": [50000, 50010, 49990],
                "ask_px_int": [50005, 50015, 49995],
            }
        )

        spec = AggregationSpec(
            name="spread",
            source_column="bid_px_int",
            agg="spread_distribution",
        )
        result = spread_distribution(data, spec).collect()

        # Verify feature column created
        assert "_spread_distribution_feature" in result.columns

        # Manual calculation: (50005-50000)/50000 * 10000 = 1 bps
        expected_spread_0 = (50005 - 50000) / 50000 * 10000
        assert abs(result["_spread_distribution_feature"][0] - expected_spread_0) < 0.01

    def test_ofi_sum_diff_calculation(self):
        """Test OFI computes differences correctly.

        COMPLETE: Full test with book array columns.
        """
        from pointline.research.resample.aggregations.microstructure import ofi_sum

        # Book snapshot data with arrays
        data = pl.LazyFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                # Best bid/ask sizes as first element of arrays
                "bids_sz_int": [[100], [150], [120]],
                "asks_sz_int": [[80], [90], [100]],
            }
        )

        spec = AggregationSpec(name="ofi", source_column="bids_sz_int", agg="ofi_sum")
        result = ofi_sum(data, spec).collect()

        # OFI = ΔBid - ΔAsk
        # Row 0: 0 (no prior, filled with 0)
        # Row 1: (150-100) - (90-80) = 50 - 10 = 40
        # Row 2: (120-150) - (100-90) = -30 - 10 = -40
        assert result["_ofi_sum_feature"][0] == 0  # First row, no prior
        assert result["_ofi_sum_feature"][1] == 40
        assert result["_ofi_sum_feature"][2] == -40

    def test_spread_distribution_registration(self):
        """Test spread_distribution is registered correctly.

        COMPLETE: Full test specification.
        """
        assert "spread_distribution" in AggregationRegistry._registry
        meta = AggregationRegistry.get("spread_distribution")
        assert meta.stage == "feature_then_aggregate"
        assert meta.semantic_type == "quote_top"

    def test_ofi_sum_registration(self):
        """Test ofi_sum is registered correctly.

        COMPLETE: Full test specification.
        """
        assert "ofi_sum" in AggregationRegistry._registry
        meta = AggregationRegistry.get("ofi_sum")
        assert meta.stage == "feature_then_aggregate"
        assert "HFT" in meta.mode_allowlist
        # OFI is HFT-only (not in MFT/LFT)
        assert "MFT" not in meta.mode_allowlist


class TestTradeFlowAggregations:
    """Test trade flow aggregations."""

    def test_signed_trade_imbalance_registration(self):
        """Test signed_trade_imbalance is registered.

        COMPLETE: Full test specification.
        """
        assert "signed_trade_imbalance" in AggregationRegistry._registry
        meta = AggregationRegistry.get("signed_trade_imbalance")
        assert meta.stage == "aggregate_then_feature"
        assert meta.aggregate_raw is not None
        assert "HFT" in meta.mode_allowlist
        assert "MFT" in meta.mode_allowlist

    def test_signed_trade_imbalance_computation(self):
        """Test signed trade imbalance with correct side values.

        COMPLETE: Full test with actual trades schema.
        """
        from pointline.research.resample.aggregations.trade_flow import (
            signed_trade_imbalance,
        )

        # Trades with side: 0=buy, 1=sell
        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 4,
                "qty_int": [100, 200, 150, 50],
                "side": [0, 0, 1, 1],  # 2 buys, 2 sells
            }
        )

        # Aggregate
        result = data.group_by("bucket_ts").agg(
            [signed_trade_imbalance("qty_int").alias("imbalance")]
        )

        # Buy vol: 100 + 200 = 300
        # Sell vol: 150 + 50 = 200
        # Imbalance: (300 - 200) / (300 + 200) = 100/500 = 0.2
        assert abs(result["imbalance"][0] - 0.2) < 0.001

    def test_signed_trade_imbalance_all_buys(self):
        """Test imbalance with all buy trades.

        COMPLETE: Full test specification.
        """
        from pointline.research.resample.aggregations.trade_flow import (
            signed_trade_imbalance,
        )

        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 2,
                "qty_int": [100, 200],
                "side": [0, 0],  # All buys
            }
        )

        result = data.group_by("bucket_ts").agg(
            [signed_trade_imbalance("qty_int").alias("imbalance")]
        )

        # Imbalance should be 1.0 (all buy volume)
        assert abs(result["imbalance"][0] - 1.0) < 0.001

    def test_signed_trade_imbalance_all_sells(self):
        """Test imbalance with all sell trades.

        COMPLETE: Full test specification.
        """
        from pointline.research.resample.aggregations.trade_flow import (
            signed_trade_imbalance,
        )

        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 2,
                "qty_int": [100, 200],
                "side": [1, 1],  # All sells
            }
        )

        result = data.group_by("bucket_ts").agg(
            [signed_trade_imbalance("qty_int").alias("imbalance")]
        )

        # Imbalance should be -1.0 (all sell volume)
        assert abs(result["imbalance"][0] - (-1.0)) < 0.001


class TestDerivativeAggregations:
    """Test derivative aggregations."""

    def test_funding_rate_mean_registration(self):
        """Test funding_rate_mean is registered.

        COMPLETE: Full test specification.
        """
        assert "funding_rate_mean" in AggregationRegistry._registry
        meta = AggregationRegistry.get("funding_rate_mean")
        assert meta.stage == "aggregate_then_feature"
        assert "MFT" in meta.mode_allowlist
        assert "LFT" in meta.mode_allowlist

    def test_funding_rate_mean_computation(self):
        """Test funding rate aggregation with float column.

        COMPLETE: Full test with actual derivative_ticker schema.
        """
        from pointline.research.resample.aggregations.derivatives import (
            funding_rate_mean,
        )

        # Derivative ticker data (funding_rate is float)
        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                "funding_rate": [0.0001, 0.0002, 0.00015],
            }
        )

        result = data.group_by("bucket_ts").agg(
            [funding_rate_mean("funding_rate").alias("funding_mean")]
        )

        # Mean: (0.0001 + 0.0002 + 0.00015) / 3 = 0.00015
        assert abs(result["funding_mean"][0] - 0.00015) < 0.0000001

    def test_oi_change_computation(self):
        """Test OI change aggregation.

        COMPLETE: Full test with actual derivative_ticker schema.
        """
        from pointline.research.resample.aggregations.derivatives import oi_change

        # OI snapshots (float values)
        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                "open_interest": [1000000.0, 1005000.0, 1008000.0],
            }
        )

        result = data.group_by("bucket_ts").agg([oi_change("open_interest").alias("oi_delta")])

        # Change: last - first = 1008000 - 1000000 = 8000
        assert result["oi_delta"][0] == 8000.0

    def test_oi_last_computation(self):
        """Test OI last value aggregation.

        COMPLETE: Full test specification.
        """
        from pointline.research.resample.aggregations.derivatives import oi_last

        data = pl.DataFrame(
            {
                "bucket_ts": [60_000_000] * 3,
                "open_interest": [1000000.0, 1005000.0, 1008000.0],
            }
        )

        result = data.group_by("bucket_ts").agg([oi_last("open_interest").alias("oi_close")])

        # Last value: 1008000
        assert result["oi_close"][0] == 1008000.0

    def test_oi_change_registration(self):
        """Test oi_change is registered correctly.

        COMPLETE: Full test specification.
        """
        assert "oi_change" in AggregationRegistry._registry
        meta = AggregationRegistry.get("oi_change")
        assert meta.stage == "aggregate_then_feature"
        assert meta.semantic_type == "state_variable"


class TestCustomAggregationsModeValidation:
    """Test mode validation for custom aggregations."""

    def test_ofi_restricted_to_hft(self):
        """Test OFI is HFT-only.

        COMPLETE: Full test specification.
        """
        meta = AggregationRegistry.get("ofi_sum")
        assert "HFT" in meta.mode_allowlist
        assert "MFT" not in meta.mode_allowlist
        assert "LFT" not in meta.mode_allowlist

        # Should pass for HFT
        AggregationRegistry.validate_for_mode("ofi_sum", "HFT")

        # Should fail for MFT
        with pytest.raises(ValueError, match="not allowed in MFT"):
            AggregationRegistry.validate_for_mode("ofi_sum", "MFT")

    def test_microprice_allowed_hft_mft(self):
        """Test microprice allowed in HFT and MFT.

        COMPLETE: Full test specification.
        """
        meta = AggregationRegistry.get("microprice_close")
        assert "HFT" in meta.mode_allowlist
        assert "MFT" in meta.mode_allowlist

        # Should pass for both
        AggregationRegistry.validate_for_mode("microprice_close", "HFT")
        AggregationRegistry.validate_for_mode("microprice_close", "MFT")

    def test_funding_rate_allowed_mft_lft(self):
        """Test funding rate allowed in MFT and LFT.

        COMPLETE: Full test specification.
        """
        meta = AggregationRegistry.get("funding_rate_mean")
        assert "MFT" in meta.mode_allowlist
        assert "LFT" in meta.mode_allowlist

        # Should pass for both
        AggregationRegistry.validate_for_mode("funding_rate_mean", "MFT")
        AggregationRegistry.validate_for_mode("funding_rate_mean", "LFT")
