"""Tests for spine persistence cache (content-addressed Parquet)."""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest

from pointline.research.spines.cache import CacheEntry, SpineCache, _spine_cache_key
from pointline.research.spines.clock import ClockSpineBuilder, ClockSpineConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_dim_symbol():
    """Mock resolve_exchange_ids so tests don't need the data lake."""
    with patch("pointline.research.spines.clock.resolve_exchange_ids") as mock:
        mock.side_effect = lambda ids: [1] * len(ids)
        yield mock


@pytest.fixture()
def cache(tmp_path):
    return SpineCache(cache_dir=tmp_path / "spines")


@pytest.fixture()
def builder():
    return ClockSpineBuilder()


@pytest.fixture()
def config():
    return ClockSpineConfig(step_ms=60_000)


# Shared constants
START = 0
END = 180_000_000  # 3 minutes → 3 bars at 60s step


# ---------------------------------------------------------------------------
# TestSpineCacheKey
# ---------------------------------------------------------------------------


class TestSpineCacheKey:
    def test_deterministic(self, config):
        k1 = _spine_cache_key("clock", config, [1, 2], START, END)
        k2 = _spine_cache_key("clock", config, [1, 2], START, END)
        assert k1 == k2

    def test_order_invariant(self, config):
        k1 = _spine_cache_key("clock", config, [2, 1], START, END)
        k2 = _spine_cache_key("clock", config, [1, 2], START, END)
        assert k1 == k2

    def test_different_builder_name(self, config):
        k1 = _spine_cache_key("clock", config, [1], START, END)
        k2 = _spine_cache_key("volume", config, [1], START, END)
        assert k1 != k2

    def test_different_config(self):
        c1 = ClockSpineConfig(step_ms=1000)
        c2 = ClockSpineConfig(step_ms=5000)
        k1 = _spine_cache_key("clock", c1, [1], START, END)
        k2 = _spine_cache_key("clock", c2, [1], START, END)
        assert k1 != k2

    def test_different_symbols(self, config):
        k1 = _spine_cache_key("clock", config, [1], START, END)
        k2 = _spine_cache_key("clock", config, [2], START, END)
        assert k1 != k2

    def test_different_time_range(self, config):
        k1 = _spine_cache_key("clock", config, [1], START, END)
        k2 = _spine_cache_key("clock", config, [1], START, END + 1)
        assert k1 != k2

    def test_key_is_sha256_hex(self, config):
        key = _spine_cache_key("clock", config, [1], START, END)
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


# ---------------------------------------------------------------------------
# TestSpineCache
# ---------------------------------------------------------------------------


class TestSpineCache:
    def test_miss_then_hit(self, cache, builder, config, tmp_path):
        """First call builds + caches; second call returns from cache."""
        lf1 = cache.get_or_build(builder, [1], START, END, config)
        df1 = lf1.collect()
        assert len(df1) > 0

        # Second call should hit cache
        lf2 = cache.get_or_build(builder, [1], START, END, config)
        df2 = lf2.collect()
        assert df1.equals(df2)

        # Parquet file should exist
        cached = list(cache.cache_dir.glob("*.parquet"))
        assert len(cached) == 1

    def test_returns_lazy_frame(self, cache, builder, config):
        result = cache.get_or_build(builder, [1], START, END, config)
        assert isinstance(result, pl.LazyFrame)

    def test_schema_preserved(self, cache, builder, config):
        lf = cache.get_or_build(builder, [1], START, END, config)
        schema = lf.collect_schema()
        assert "ts_local_us" in schema.names()
        assert "exchange_id" in schema.names()
        assert "symbol_id" in schema.names()
        assert schema["ts_local_us"] == pl.Int64
        assert schema["exchange_id"] == pl.Int16
        assert schema["symbol_id"] == pl.Int64

    def test_data_preserved(self, cache, builder, config):
        """Cached data must match direct builder output."""
        direct = builder.build_spine([1], START, END, config).collect()
        cached = cache.get_or_build(builder, [1], START, END, config).collect()
        assert direct.equals(cached)

    def test_sort_preserved(self, cache, builder, config):
        lf = cache.get_or_build(builder, [1, 2], START, END, config)
        df = lf.collect()
        sorted_df = df.sort(["exchange_id", "symbol_id", "ts_local_us"])
        assert df.equals(sorted_df)

    def test_skip_builders(self, tmp_path, builder, config):
        cache = SpineCache(cache_dir=tmp_path / "spines", skip_builders={"clock"})
        lf = cache.get_or_build(builder, [1], START, END, config)
        df = lf.collect()
        assert len(df) > 0
        # No files should be cached
        if cache.cache_dir.exists():
            assert len(list(cache.cache_dir.glob("*.parquet"))) == 0

    def test_sidecar_metadata(self, cache, builder, config):
        cache.get_or_build(builder, [1], START, END, config)
        meta_files = list(cache.cache_dir.glob("*.meta.json"))
        assert len(meta_files) == 1
        data = json.loads(meta_files[0].read_text())
        assert data["builder_name"] == "clock"
        assert data["symbol_ids"] == [1]
        assert data["start_ts_us"] == START
        assert data["end_ts_us"] == END
        assert data["row_count"] > 0
        assert data["file_size_bytes"] > 0
        assert "created_at_utc" in data

    def test_single_symbol_id_int(self, cache, builder, config):
        """Passing symbol_id as int should work the same as [int]."""
        lf1 = cache.get_or_build(builder, 1, START, END, config)
        lf2 = cache.get_or_build(builder, [1], START, END, config)
        # Both should hit the same cache entry
        assert lf1.collect().equals(lf2.collect())
        cached = list(cache.cache_dir.glob("*.parquet"))
        assert len(cached) == 1

    def test_lookup_hit(self, cache, builder, config):
        cache.get_or_build(builder, [1], START, END, config)
        result = cache.lookup("clock", [1], START, END, config)
        assert result is not None
        assert isinstance(result, pl.LazyFrame)

    def test_lookup_miss(self, cache, config):
        result = cache.lookup("clock", [1], START, END, config)
        assert result is None


# ---------------------------------------------------------------------------
# TestSpineCacheManagement
# ---------------------------------------------------------------------------


class TestSpineCacheManagement:
    def test_list_cached_empty(self, cache):
        assert cache.list_cached() == []

    def test_list_cached(self, cache, builder, config):
        cache.get_or_build(builder, [1], START, END, config)
        entries = cache.list_cached()
        assert len(entries) == 1
        assert isinstance(entries[0], CacheEntry)
        assert entries[0].builder_name == "clock"

    def test_evict(self, cache, builder, config):
        cache.get_or_build(builder, [1], START, END, config)
        entries = cache.list_cached()
        assert len(entries) == 1

        removed = cache.evict(entries[0].cache_key)
        assert removed is True
        assert cache.list_cached() == []

    def test_evict_nonexistent(self, cache):
        assert cache.evict("nonexistent") is False

    def test_clear(self, cache, builder):
        c1 = ClockSpineConfig(step_ms=60_000)
        c2 = ClockSpineConfig(step_ms=30_000)
        cache.get_or_build(builder, [1], START, END, c1)
        cache.get_or_build(builder, [1], START, END, c2)
        assert len(cache.list_cached()) == 2

        removed = cache.clear()
        assert removed == 2
        assert cache.list_cached() == []

    def test_clear_empty(self, cache):
        assert cache.clear() == 0

    def test_cache_stats(self, cache, builder, config):
        stats = cache.cache_stats()
        assert stats["entry_count"] == 0
        assert stats["total_size_bytes"] == 0

        cache.get_or_build(builder, [1], START, END, config)
        stats = cache.cache_stats()
        assert stats["entry_count"] == 1
        assert stats["total_size_bytes"] > 0

    def test_max_entries_warning(self, tmp_path, builder, caplog):
        cache = SpineCache(cache_dir=tmp_path / "spines", max_entries=1)
        c1 = ClockSpineConfig(step_ms=60_000)
        c2 = ClockSpineConfig(step_ms=30_000)
        cache.get_or_build(builder, [1], START, END, c1)
        cache.get_or_build(builder, [1], START, END, c2)
        assert any("soft limit" in r.message for r in caplog.records)
