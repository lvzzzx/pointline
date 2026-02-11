"""Tests for volume/dollar spine builder integration in pipeline v2."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pointline.research.pipeline import PipelineError, pipeline
from pointline.research.resample import AggregationRegistry
from pointline.research.spines import SpineCache
from pointline.research.spines.base import SpineBuilderConfig

pipeline_module = importlib.import_module("pointline.research.pipeline")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _operator_contract(agg: str, *, source_column: str, name: str | None = None) -> dict:
    meta = AggregationRegistry.get(agg)
    op_name = name or agg
    return {
        "name": op_name,
        "output_name": op_name,
        "stage": meta.stage,
        "agg": agg,
        "source_column": source_column,
        "required_columns": meta.required_columns,
        "mode_allowlist": meta.mode_allowlist,
        "pit_policy": meta.pit_policy,
        "determinism_policy": {**meta.determinism, "stateful": False},
        "impl_ref": meta.impl_ref,
        "version": meta.version,
    }


def _base_request(spine_type: str = "clock", **spine_kwargs: object) -> dict:
    """Minimal bar_then_feature request with inline source data."""
    return {
        "schema_version": "2.0",
        "request_id": f"req-spine-builder-{spine_type}",
        "mode": "bar_then_feature",
        "timeline": {"time_col": "ts_local_us", "timezone": "UTC"},
        "sources": [
            {
                "name": "trades",
                "symbol_id": 12345,
                "start_ts_us": 0,
                "end_ts_us": 120_000_000,
                "inline_rows": [
                    {
                        "ts_local_us": 10_000_000,
                        "exchange_id": 1,
                        "symbol_id": 12345,
                        "qty_int": 100,
                        "px_int": 50000,
                        "side": 0,
                        "file_id": 1,
                        "file_line_number": 10,
                    },
                    {
                        "ts_local_us": 70_000_000,
                        "exchange_id": 1,
                        "symbol_id": 12345,
                        "qty_int": 200,
                        "px_int": 50010,
                        "side": 1,
                        "file_id": 1,
                        "file_line_number": 11,
                    },
                ],
            }
        ],
        "spine": {"type": spine_type, **spine_kwargs},
        "operators": [_operator_contract("sum", source_column="qty_int", name="volume")],
        "labels": [
            {
                "name": "future_volume_1",
                "source_column": "volume",
                "direction": "forward",
                "horizon_bars": 1,
            }
        ],
        "evaluation": {"metrics": ["row_count"], "split_method": "fixed"},
        "constraints": {
            "pit_timeline": "ts_local_us",
            "forbid_lookahead": True,
            "cost_model": {"fees_bps": 1.0, "slippage_bps": 2.0},
        },
        "artifacts": {"include_artifacts": False},
    }


def _synthetic_spine() -> pl.LazyFrame:
    """Return a tiny spine LazyFrame suitable for bucket assignment."""
    return pl.LazyFrame(
        {
            "ts_local_us": [60_000_000, 120_000_000],
            "exchange_id": [1, 1],
            "symbol_id": [12345, 12345],
        },
        schema={
            "ts_local_us": pl.Int64,
            "exchange_id": pl.Int64,
            "symbol_id": pl.Int64,
        },
    )


@dataclass(frozen=True)
class _FakeConfig(SpineBuilderConfig):
    volume_threshold: float = 1000.0


class _FakeBuilder:
    """Minimal spine builder mock that satisfies the protocol."""

    @property
    def config_type(self) -> type:
        return _FakeConfig

    @property
    def name(self) -> str:
        return "volume"

    @property
    def display_name(self) -> str:
        return "Fake Volume Bars"

    @property
    def supports_single_symbol(self) -> bool:
        return True

    @property
    def supports_multi_symbol(self) -> bool:
        return True

    def can_handle(self, mode: str) -> bool:
        return mode == "volume"

    def build_spine(
        self,
        symbol_id: int | list[int],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        return _synthetic_spine()


# ---------------------------------------------------------------------------
# Tests: _extract_spine_params
# ---------------------------------------------------------------------------


class TestExtractSpineParams:
    def test_extracts_from_primary_source(self):
        compiled = {
            "sources": [{"symbol_id": 99, "start_ts_us": 100, "end_ts_us": 200}],
            "spine": {"type": "volume"},
        }
        sym, start, end = pipeline_module._extract_spine_params(compiled)
        assert sym == 99
        assert start == 100
        assert end == 200

    def test_spine_overrides_timestamps(self):
        compiled = {
            "sources": [{"symbol_id": 99, "start_ts_us": 100, "end_ts_us": 200}],
            "spine": {"type": "volume", "start_ts_us": 50, "end_ts_us": 300},
        }
        sym, start, end = pipeline_module._extract_spine_params(compiled)
        assert sym == 99
        assert start == 50
        assert end == 300


# ---------------------------------------------------------------------------
# Tests: _resolve_builder_config
# ---------------------------------------------------------------------------


class TestResolveBuilderConfig:
    def test_maps_kwargs_to_config(self):
        builder = _FakeBuilder()
        spine_cfg = {"type": "volume", "volume_threshold": 500.0, "max_rows": 100}
        config = pipeline_module._resolve_builder_config(builder, spine_cfg)
        assert isinstance(config, _FakeConfig)
        assert config.volume_threshold == 500.0
        assert config.max_rows == 100

    def test_strips_pipeline_keys(self):
        builder = _FakeBuilder()
        spine_cfg = {"type": "volume", "source": "trades", "volume_threshold": 42.0}
        config = pipeline_module._resolve_builder_config(builder, spine_cfg)
        assert isinstance(config, _FakeConfig)
        assert config.volume_threshold == 42.0


# ---------------------------------------------------------------------------
# Tests: _build_spine with builder dispatch
# ---------------------------------------------------------------------------


class TestBuildSpineBuilderDispatch:
    def test_volume_spine_dispatches_to_builder(self):
        builder = _FakeBuilder()
        builder.build_spine = MagicMock(return_value=_synthetic_spine())

        compiled = {
            "sources": [{"symbol_id": 12345, "start_ts_us": 0, "end_ts_us": 120_000_000}],
            "spine": {"type": "volume", "volume_threshold": 1000.0},
        }
        source = pl.LazyFrame({"ts_local_us": [1], "exchange_id": [1], "symbol_id": [12345]})

        with patch.object(pipeline_module, "get_builder", return_value=builder):
            result = pipeline_module._build_spine(compiled, source)

        builder.build_spine.assert_called_once()
        call_args = builder.build_spine.call_args
        assert call_args[0][0] == 12345  # symbol_id
        assert call_args[0][1] == 0  # start_ts_us
        assert call_args[0][2] == 120_000_000  # end_ts_us
        assert isinstance(call_args[0][3], _FakeConfig)
        assert result.collect().height == 2

    def test_dollar_spine_dispatches_to_builder(self):
        @dataclass(frozen=True)
        class FakeDollarConfig(SpineBuilderConfig):
            dollar_threshold: float = 100_000.0

        class FakeDollarBuilder:
            @property
            def config_type(self) -> type:
                return FakeDollarConfig

            @property
            def name(self) -> str:
                return "dollar"

            def build_spine(self, symbol_id, start_ts_us, end_ts_us, config):
                return _synthetic_spine()

        builder = FakeDollarBuilder()
        builder_mock = MagicMock(wraps=builder)
        builder_mock.config_type = FakeDollarConfig
        builder_mock.build_spine = MagicMock(return_value=_synthetic_spine())

        compiled = {
            "sources": [{"symbol_id": 99, "start_ts_us": 0, "end_ts_us": 200_000_000}],
            "spine": {"type": "dollar", "dollar_threshold": 50_000.0},
        }
        source = pl.LazyFrame({"ts_local_us": [1], "exchange_id": [1], "symbol_id": [99]})

        with patch.object(pipeline_module, "get_builder", return_value=builder_mock):
            result = pipeline_module._build_spine(compiled, source)

        builder_mock.build_spine.assert_called_once()
        config_arg = builder_mock.build_spine.call_args[0][3]
        assert isinstance(config_arg, FakeDollarConfig)
        assert config_arg.dollar_threshold == 50_000.0
        assert result.collect().height == 2

    def test_builder_spine_with_cache(self):
        builder = _FakeBuilder()
        cache = MagicMock(spec=SpineCache)
        cache.get_or_build = MagicMock(return_value=_synthetic_spine())

        compiled = {
            "sources": [{"symbol_id": 12345, "start_ts_us": 0, "end_ts_us": 120_000_000}],
            "spine": {"type": "volume", "volume_threshold": 1000.0},
        }
        source = pl.LazyFrame({"ts_local_us": [1], "exchange_id": [1], "symbol_id": [12345]})

        with patch.object(pipeline_module, "get_builder", return_value=builder):
            result = pipeline_module._build_spine(compiled, source, cache=cache)

        cache.get_or_build.assert_called_once()
        call_args = cache.get_or_build.call_args
        assert call_args[0][0] is builder
        assert call_args[0][1] == 12345
        assert result.collect().height == 2

    def test_builder_spine_without_cache(self):
        builder = _FakeBuilder()
        builder.build_spine = MagicMock(return_value=_synthetic_spine())

        compiled = {
            "sources": [{"symbol_id": 12345, "start_ts_us": 0, "end_ts_us": 120_000_000}],
            "spine": {"type": "volume", "volume_threshold": 1000.0},
        }
        source = pl.LazyFrame({"ts_local_us": [1], "exchange_id": [1], "symbol_id": [12345]})

        with patch.object(pipeline_module, "get_builder", return_value=builder):
            result = pipeline_module._build_spine(compiled, source, cache=None)

        builder.build_spine.assert_called_once()
        assert result.collect().height == 2

    def test_unknown_spine_type_raises(self):
        compiled = {
            "sources": [{"symbol_id": 1, "start_ts_us": 0, "end_ts_us": 100}],
            "spine": {"type": "nonexistent_bar_type"},
        }
        source = pl.LazyFrame({"ts_local_us": [1], "exchange_id": [1], "symbol_id": [1]})

        with pytest.raises(PipelineError, match="Unsupported spine.type"):
            pipeline_module._build_spine(compiled, source)

    def test_clock_spine_still_works(self):
        """Ensure the existing clock spine path is unchanged."""
        compiled = {
            "spine": {
                "type": "clock",
                "step_ms": 60_000,
                "start_ts_us": 0,
                "end_ts_us": 120_000_000,
            },
        }
        source = pl.LazyFrame(
            {
                "ts_local_us": [10_000_000, 70_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
            }
        )
        result = pipeline_module._build_spine(compiled, source)
        df = result.collect()
        assert "ts_local_us" in df.columns
        assert df.height == 2  # 60M and 120M

    def test_trades_spine_still_works(self):
        """Ensure the existing trades spine path is unchanged."""
        compiled = {"spine": {"type": "trades"}}
        source = pl.LazyFrame(
            {
                "ts_local_us": [10_000_000, 70_000_000],
                "exchange_id": [1, 1],
                "symbol_id": [12345, 12345],
                "file_id": [1, 1],
                "file_line_number": [10, 11],
            }
        )
        result = pipeline_module._build_spine(compiled, source)
        df = result.collect()
        assert "ts_local_us" in df.columns
        assert df.height == 2


# ---------------------------------------------------------------------------
# Tests: full pipeline integration with mocked builder
# ---------------------------------------------------------------------------


class TestPipelineWithBuilder:
    def test_volume_spine_end_to_end(self):
        """Full pipeline with volume spine using a mocked builder."""
        builder = _FakeBuilder()
        builder.build_spine = MagicMock(return_value=_synthetic_spine())

        request = _base_request("volume", volume_threshold=1000.0)

        with patch.object(pipeline_module, "get_builder", return_value=builder):
            output = pipeline(request)

        assert output["run"]["status"] == "success"
        assert output["results"]["row_count"] >= 1
        builder.build_spine.assert_called()

    def test_pipeline_with_cache(self):
        """Pipeline passes cache through to _build_spine."""
        builder = _FakeBuilder()
        cache = MagicMock(spec=SpineCache)
        cache.get_or_build = MagicMock(return_value=_synthetic_spine())

        request = _base_request("volume", volume_threshold=1000.0)

        with patch.object(pipeline_module, "get_builder", return_value=builder):
            output = pipeline(request, cache=cache)

        assert output["run"]["status"] == "success"
        cache.get_or_build.assert_called()
