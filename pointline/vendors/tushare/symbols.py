"""Map Tushare stock_basic output to v2 dim_symbol snapshots.

Pure functions — no I/O, no API calls.
"""

from __future__ import annotations

import polars as pl

# 0.01 CNY × PRICE_SCALE (1e9)
CN_TICK_SIZE: int = 10_000_000
# 100 shares × QTY_SCALE (1e9)
CN_LOT_SIZE: int = 100_000_000_000
# 200 shares × QTY_SCALE (1e9) - STAR Market (科创板)
STAR_MARKET_LOT_SIZE: int = 200_000_000_000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_exchange(expr: pl.Expr) -> pl.Expr:
    """Map Tushare exchange names to pointline convention (lowercase)."""
    upper = expr.cast(pl.Utf8, strict=False).str.to_uppercase()
    return (
        pl.when(upper == "SZSE")
        .then(pl.lit("szse"))
        .when(upper == "SSE")
        .then(pl.lit("sse"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def _parse_yyyymmdd_us(expr: pl.Expr) -> pl.Expr:
    """Parse YYYYMMDD string to UTC midnight microseconds (Int64)."""
    return (
        expr.cast(pl.Utf8, strict=False)
        .str.strptime(pl.Date, "%Y%m%d", strict=False)
        .cast(pl.Datetime("us"), strict=False)
        .cast(pl.Int64, strict=False)
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def stock_basic_to_snapshot(
    raw: pl.DataFrame,
    *,
    effective_ts_us: int | None = None,
) -> pl.DataFrame:
    """Convert Tushare stock_basic to a full historical SCD2 load.

    Unlike a traditional SCD2 "snapshot" (which contains only current active
    symbols), this function creates a **complete historical view** including:
    - Listed (L) and Paused (P) stocks: is_current=True, valid_until=MAX
    - Delisted (D) stocks: is_current=False, valid_until=delist_date

    This design supports "full refresh" workflows where the entire symbol
    history is fetched and replaced on each run, rather than incremental
    updates. The output is ready for direct storage without additional
    bootstrap/upsert logic.

    Key properties:
    - Symbol IDs are stable (derived from exchange|symbol|list_date)
    - Re-running with same data produces identical SCD2 state
    - PIT queries work correctly for any historical date
    - Only updated_at_ts_us changes between runs

    Args:
        raw: Raw DataFrame from Tushare stock_basic API (should include
            list_status=L/P/D for complete history)
        effective_ts_us: Timestamp for updated_at_ts_us (defaults to now)

    Returns:
        DataFrame with all dim_symbol columns including SCD2 metadata
        (valid_from_ts_us, valid_until_ts_us, is_current, symbol_id).
        Ready for direct save to DeltaDimensionStore.

    Example:
        >>> # Full historical refresh workflow
        >>> listed = fetch_stock_basic(list_status="L")  # current symbols
        >>> delisted = fetch_stock_basic(list_status="D")  # historical symbols
        >>> all_raw = pl.concat([listed, delisted])
        >>> snapshot = stock_basic_to_snapshot(all_raw)
        >>> result = assign_symbol_ids(snapshot)  # adds symbol_id
        >>> store.save_dim_symbol(result)  # ready to save
    """
    from datetime import datetime

    if effective_ts_us is None:
        effective_ts_us = int(datetime.now().timestamp() * 1_000_000)

    max_valid_until = 2**63 - 1

    df = raw.filter(pl.col("list_status").is_in(["L", "P", "D"]))

    df = df.with_columns(_normalize_exchange(pl.col("exchange")).alias("exchange"))
    df = df.filter(pl.col("exchange").is_not_null())

    # STAR Market (科创板) uses 200-share lots; all others use 100
    lot_size_expr = (
        pl.when(pl.col("market") == "科创板")
        .then(pl.lit(STAR_MARKET_LOT_SIZE))
        .otherwise(pl.lit(CN_LOT_SIZE))
        .cast(pl.Int64)
    )

    # Parse dates for SCD2 validity windows
    list_date_us = _parse_yyyymmdd_us(pl.col("list_date"))
    delist_date_us = _parse_yyyymmdd_us(pl.col("delist_date"))

    # For PIT correctness:
    # - Listed (L,P): valid_from = list_date, valid_until = MAX, is_current = True
    # - Delisted (D): valid_from = list_date, valid_until = delist_date, is_current = False
    return df.select(
        pl.col("exchange"),
        pl.col("symbol").alias("exchange_symbol"),
        pl.col("ts_code").alias("canonical_symbol"),
        pl.col("market").alias("market_type"),
        pl.coalesce([pl.col("name"), pl.col("symbol")]).alias("base_asset"),
        pl.lit("CNY").alias("quote_asset"),
        pl.lit(CN_TICK_SIZE).cast(pl.Int64).alias("tick_size"),
        lot_size_expr.alias("lot_size"),
        pl.lit(None, dtype=pl.Int64).alias("contract_size"),
        # SCD2 metadata
        list_date_us.alias("valid_from_ts_us"),
        pl.when(pl.col("list_status") == "D")
        .then(delist_date_us)
        .otherwise(pl.lit(max_valid_until))
        .alias("valid_until_ts_us"),
        pl.when(pl.col("list_status") == "D")
        .then(pl.lit(False))
        .otherwise(pl.lit(True))
        .alias("is_current"),
        pl.lit(effective_ts_us).alias("updated_at_ts_us"),
    )


def stock_basic_to_delistings(raw: pl.DataFrame) -> pl.DataFrame:
    """Extract delistings for incremental SCD2 workflows.

    This function is used for **incremental updates** (not full refreshes).
    When doing daily syncs with only current data, use this to identify
    symbols that should be closed in the existing dim_symbol table.

    For full historical loads, use stock_basic_to_snapshot() instead,
    which handles delisted stocks directly with proper validity windows.

    Args:
        raw: Raw DataFrame from Tushare stock_basic API

    Returns:
        DataFrame with (exchange, exchange_symbol, delisted_at_ts_us)
        for use with dim_symbol.upsert(delistings=...).

    Example:
        >>> # Incremental workflow (daily sync)
        >>> current = fetch_stock_basic(list_status="L")  # today's snapshot
        >>> snap = stock_basic_to_snapshot(current)  # L/P only, no SCD2 cols
        >>> delisted_today = fetch_stock_basic(list_status="D")  # newly delisted
        >>> dl = stock_basic_to_delistings(delisted_today)
        >>> dim = upsert(existing_dim, snap, delistings=dl)  # close delisted
    """
    df = raw.filter((pl.col("list_status") == "D") & pl.col("delist_date").is_not_null())

    df = df.with_columns(_normalize_exchange(pl.col("exchange")).alias("exchange"))
    df = df.filter(pl.col("exchange").is_not_null())

    return df.select(
        pl.col("exchange"),
        pl.col("symbol").alias("exchange_symbol"),
        _parse_yyyymmdd_us(pl.col("delist_date")).alias("delisted_at_ts_us"),
    )
