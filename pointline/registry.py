"""Registry for resolving symbol metadata.

Acts as a read-only interface to the dim_symbol table.
Uses a TTL-based in-process cache to avoid repeated disk reads.
"""

from __future__ import annotations

import logging
import threading
import time

import polars as pl

from pointline.dim_symbol import read_dim_symbol_table

logger = logging.getLogger(__name__)

# Cache configuration
_CACHE_TTL_SECONDS: float = 300.0  # 5 minutes

# Cache state (module-level, thread-safe)
_cache_lock = threading.Lock()
_cached_df: pl.DataFrame | None = None
_cached_at: float = 0.0

_REGISTRY_COLUMNS = [
    "symbol_id",
    "exchange_id",
    "exchange",
    "exchange_symbol",
    "base_asset",
    "quote_asset",
    "asset_type",
    "tick_size",
    "lot_size",
    "contract_size",
    "valid_from_ts",
    "valid_until_ts",
]

_EMPTY_SCHEMA = {
    "symbol_id": pl.Int64,
    "exchange_id": pl.Int16,
    "exchange": pl.Utf8,
    "exchange_symbol": pl.Utf8,
    "base_asset": pl.Utf8,
    "quote_asset": pl.Utf8,
    "asset_type": pl.UInt8,
    "tick_size": pl.Float64,
    "lot_size": pl.Float64,
    "contract_size": pl.Float64,
    "valid_from_ts": pl.Int64,
    "valid_until_ts": pl.Int64,
}


def _read_dim_symbol() -> pl.DataFrame:
    """Read dim_symbol with TTL cache.

    Returns cached DataFrame if within TTL window.
    Thread-safe via module-level lock.
    """
    global _cached_df, _cached_at

    now = time.monotonic()

    # Fast path: check cache without lock
    if _cached_df is not None and (now - _cached_at) < _CACHE_TTL_SECONDS:
        return _cached_df

    with _cache_lock:
        # Re-check after acquiring lock (another thread may have refreshed)
        now = time.monotonic()
        if _cached_df is not None and (now - _cached_at) < _CACHE_TTL_SECONDS:
            return _cached_df

        try:
            df = read_dim_symbol_table(
                columns=_REGISTRY_COLUMNS,
                unique_by=["symbol_id"],
            )
        except Exception as exc:
            logger.warning(f"Failed to load dim_symbol registry: {exc}")
            df = pl.DataFrame(schema=_EMPTY_SCHEMA)

        _cached_df = df
        _cached_at = time.monotonic()
        return df


def invalidate_cache() -> None:
    """Force the next _read_dim_symbol() call to re-read from disk."""
    global _cached_df, _cached_at
    with _cache_lock:
        _cached_df = None
        _cached_at = 0.0


def find_symbol(
    query: str | None = None,
    *,
    exchange: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None,
) -> pl.DataFrame:
    """
    Find symbols matching search criteria.

    Args:
        query: Fuzzy search string (matches against exchange_symbol, base_asset, quote_asset)
        exchange: Exact filter for exchange name (case-insensitive)
        base_asset: Exact filter for base asset (case-insensitive)
        quote_asset: Exact filter for quote asset (case-insensitive)

    Returns:
        pl.DataFrame: DataFrame containing matching symbols with their IDs and metadata.
    """
    df = _read_dim_symbol()

    if exchange:
        df = df.filter(pl.col("exchange").str.to_lowercase() == exchange.lower())

    if base_asset:
        df = df.filter(pl.col("base_asset").str.to_lowercase() == base_asset.lower())

    if quote_asset:
        df = df.filter(pl.col("quote_asset").str.to_lowercase() == quote_asset.lower())

    if query:
        q = query.lower()
        # Case-insensitive contains search across descriptive fields
        df = df.filter(
            pl.col("exchange_symbol").str.to_lowercase().str.contains(q)
            | pl.col("base_asset").str.to_lowercase().str.contains(q)
            | pl.col("quote_asset").str.to_lowercase().str.contains(q)
        )

    return df
