"""Content-addressed Parquet cache for spine builders.

Caches expensive spine computations (volume bars, dollar bars, etc.) as
compressed Parquet files keyed by SHA-256 of canonicalized inputs.

Storage layout::

    LAKE_ROOT/cache/spines/
      {sha256}.parquet        # Spine data (ZSTD compressed)
      {sha256}.meta.json      # Sidecar metadata (human-inspectable)

Usage::

    from pointline.research.spines import SpineCache, get_builder, VolumeBarConfig

    cache = SpineCache(skip_builders={"clock"})
    builder = get_builder("volume")
    spine = cache.get_or_build(
        builder, "binance-futures", "BTCUSDT", start, end,
        VolumeBarConfig(volume_threshold=1000),
    )
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from pointline.config import LAKE_ROOT

from .base import SpineBuilder, SpineBuilderConfig

logger = logging.getLogger(__name__)


def _spine_cache_key(
    builder_name: str,
    config: SpineBuilderConfig,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
) -> str:
    """Compute deterministic SHA-256 cache key from spine inputs.

    The key is the hex digest of the canonicalized JSON representation
    of all inputs.  Symbols are sorted to ensure order invariance.
    """
    payload = {
        "builder_name": builder_name,
        "config": asdict(config),
        "exchange": exchange,
        "symbols": sorted(symbols),
        "start_ts_us": start_ts_us,
        "end_ts_us": end_ts_us,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class CacheEntry:
    """Metadata for a cached spine file."""

    cache_key: str
    builder_name: str
    config: dict[str, Any]
    exchange: str
    symbols: list[str]
    start_ts_us: int
    end_ts_us: int
    created_at_utc: str
    row_count: int
    file_size_bytes: int


class SpineCache:
    """Content-addressed Parquet cache for spine computations.

    Args:
        cache_dir: Directory for cache files. Defaults to LAKE_ROOT/cache/spines.
        skip_builders: Set of builder names to bypass (e.g. ``{"clock"}``).
        max_entries: Soft limit on cached entries; logs a warning when exceeded.
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        skip_builders: set[str] | None = None,
        max_entries: int = 1000,
    ) -> None:
        if cache_dir is None:
            self._cache_dir = LAKE_ROOT / "cache" / "spines"
        else:
            self._cache_dir = Path(cache_dir)
        self._skip_builders: set[str] = skip_builders or set()
        self._max_entries = max_entries

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_or_build(
        self,
        builder: SpineBuilder,
        exchange: str,
        symbol: str | list[str],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame:
        """Return cached spine or build, cache, and return it.

        Args:
            builder: SpineBuilder instance.
            exchange: Exchange name (e.g., "binance-futures").
            symbol: Single or list of symbol names.
            start_ts_us: Start timestamp (microseconds UTC).
            end_ts_us: End timestamp (microseconds UTC).
            config: Builder-specific configuration.

        Returns:
            LazyFrame with the spine data.
        """
        symbols = _normalize_symbols(symbol)

        # Bypass cache for skipped builders
        if builder.name in self._skip_builders:
            return builder.build_spine(exchange, symbols, start_ts_us, end_ts_us, config)

        cache_key = _spine_cache_key(
            builder.name, config, exchange, symbols, start_ts_us, end_ts_us
        )

        # Try cache hit
        hit = self._read_cached(cache_key)
        if hit is not None:
            logger.debug("Spine cache HIT: %s (builder=%s)", cache_key[:12], builder.name)
            return hit

        # Cache miss -- build
        logger.debug("Spine cache MISS: %s (builder=%s)", cache_key[:12], builder.name)
        lf = builder.build_spine(exchange, symbols, start_ts_us, end_ts_us, config)
        df = lf.collect()

        # Write atomically
        self._write_cached(
            cache_key, df, builder.name, config, exchange, symbols, start_ts_us, end_ts_us
        )

        # Return as lazy scan of the written file
        return pl.scan_parquet(self._parquet_path(cache_key))

    def lookup(
        self,
        builder_name: str,
        exchange: str,
        symbol: str | list[str],
        start_ts_us: int,
        end_ts_us: int,
        config: SpineBuilderConfig,
    ) -> pl.LazyFrame | None:
        """Check cache without building on miss.

        Returns:
            LazyFrame if cached, None otherwise.
        """
        symbols = _normalize_symbols(symbol)
        cache_key = _spine_cache_key(
            builder_name, config, exchange, symbols, start_ts_us, end_ts_us
        )
        return self._read_cached(cache_key)

    def list_cached(self) -> list[CacheEntry]:
        """List all cached spine entries."""
        entries: list[CacheEntry] = []
        if not self._cache_dir.exists():
            return entries
        for meta_path in sorted(self._cache_dir.glob("*.meta.json")):
            try:
                data = json.loads(meta_path.read_text())
                entries.append(CacheEntry(**data))
            except (json.JSONDecodeError, TypeError, KeyError):
                logger.warning("Skipping corrupt metadata: %s", meta_path.name)
        return entries

    def evict(self, cache_key: str) -> bool:
        """Remove a specific cache entry.

        Returns:
            True if the entry existed and was removed.
        """
        parquet = self._parquet_path(cache_key)
        meta = self._meta_path(cache_key)
        removed = False
        if parquet.exists():
            parquet.unlink()
            removed = True
        if meta.exists():
            meta.unlink()
            removed = True
        return removed

    def clear(self) -> int:
        """Remove all cached entries.

        Returns:
            Number of entries removed.
        """
        if not self._cache_dir.exists():
            return 0
        count = 0
        for meta_path in list(self._cache_dir.glob("*.meta.json")):
            cache_key = meta_path.stem.removesuffix(".meta")
            if self.evict(cache_key):
                count += 1
        # Also remove orphan parquet files
        for pq_path in list(self._cache_dir.glob("*.parquet")):
            pq_path.unlink()
        return count

    def cache_stats(self) -> dict[str, Any]:
        """Summary statistics for the cache."""
        entries = self.list_cached()
        total_size = sum(e.file_size_bytes for e in entries)
        return {
            "entry_count": len(entries),
            "total_size_bytes": total_size,
            "cache_dir": str(self._cache_dir),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parquet_path(self, cache_key: str) -> Path:
        return self._cache_dir / f"{cache_key}.parquet"

    def _meta_path(self, cache_key: str) -> Path:
        return self._cache_dir / f"{cache_key}.meta.json"

    def _read_cached(self, cache_key: str) -> pl.LazyFrame | None:
        parquet = self._parquet_path(cache_key)
        if parquet.exists():
            return pl.scan_parquet(parquet)
        return None

    def _write_cached(
        self,
        cache_key: str,
        df: pl.DataFrame,
        builder_name: str,
        config: SpineBuilderConfig,
        exchange: str,
        symbols: list[str],
        start_ts_us: int,
        end_ts_us: int,
    ) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = self._parquet_path(cache_key)
        meta_path = self._meta_path(cache_key)

        # Atomic write: temp file + rename
        fd, tmp_path = tempfile.mkstemp(dir=self._cache_dir, suffix=".parquet.tmp")
        os.close(fd)
        try:
            df.write_parquet(tmp_path, compression="zstd")
            os.rename(tmp_path, parquet_path)
        except BaseException:
            # Clean up temp file on failure
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        # Write sidecar metadata (not atomic, but not critical)
        entry = CacheEntry(
            cache_key=cache_key,
            builder_name=builder_name,
            config=asdict(config),
            exchange=exchange,
            symbols=sorted(symbols),
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            created_at_utc=datetime.now(timezone.utc).isoformat(),
            row_count=len(df),
            file_size_bytes=parquet_path.stat().st_size,
        )
        meta_path.write_text(json.dumps(asdict(entry), indent=2) + "\n")

        # Warn if over soft limit
        entry_count = len(list(self._cache_dir.glob("*.meta.json")))
        if entry_count > self._max_entries:
            logger.warning(
                "Spine cache has %d entries (soft limit: %d). "
                "Consider calling cache.clear() or cache.evict().",
                entry_count,
                self._max_entries,
            )


def _normalize_symbols(symbol: str | list[str]) -> list[str]:
    if isinstance(symbol, str):
        return [symbol]
    return list(symbol)
