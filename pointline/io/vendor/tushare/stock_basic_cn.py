"""Helpers for Tushare stock_basic snapshots (China-only)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from pointline.config import EXCHANGE_MAP
from pointline.tables.stock_basic_cn import normalize_stock_basic_cn_schema

_STOCK_BASIC_FIELDS = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "act_name",
    "act_ent_type",
)


def _parse_yyyymmdd_to_date(expr: pl.Expr) -> pl.Expr:
    as_text = expr.cast(pl.Utf8, strict=False)
    return (
        as_text.str.strptime(pl.Date, "%Y%m%d", strict=False)
        .fill_null(as_text.str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    )


def _exchange_mappings() -> tuple[pl.Expr, pl.Expr]:
    szse_id = EXCHANGE_MAP.get("szse", 30)
    sse_id = EXCHANGE_MAP.get("sse", 31)
    exchange_raw = pl.col("exchange").cast(pl.Utf8, strict=False).str.to_uppercase()
    exchange_expr = (
        pl.when(exchange_raw == "SZSE")
        .then(pl.lit("szse"))
        .when(exchange_raw == "SSE")
        .then(pl.lit("sse"))
        .otherwise(pl.lit("unknown"))
    )
    exchange_id_expr = (
        pl.when(exchange_raw == "SZSE")
        .then(pl.lit(szse_id))
        .when(exchange_raw == "SSE")
        .then(pl.lit(sse_id))
        .otherwise(pl.lit(0))
        .cast(pl.Int16)
    )
    return exchange_expr, exchange_id_expr


def build_stock_basic_cn_snapshot(
    df: pl.DataFrame,
    *,
    as_of_date: date | None = None,
    ingest_ts_us: int | None = None,
) -> pl.DataFrame:
    """Normalize raw Tushare stock_basic data into stock_basic_cn snapshot.

    Args:
        df: DataFrame from Tushare stock_basic endpoint
        as_of_date: Snapshot date (defaults to UTC today)
        ingest_ts_us: Ingest timestamp in microseconds (defaults to now UTC)

    Returns:
        Normalized stock_basic_cn DataFrame
    """
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).date()
    if ingest_ts_us is None:
        ingest_ts_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    # Ensure all expected fields exist
    result = df
    for col in _STOCK_BASIC_FIELDS:
        if col not in result.columns:
            result = result.with_columns(pl.lit(None).alias(col))

    exchange_expr, exchange_id_expr = _exchange_mappings()

    result = result.with_columns(
        [
            exchange_expr.alias("exchange"),
            exchange_id_expr.alias("exchange_id"),
            pl.col("symbol").alias("exchange_symbol"),
            _parse_yyyymmdd_to_date(pl.col("list_date")).alias("list_date"),
            _parse_yyyymmdd_to_date(pl.col("delist_date")).alias("delist_date"),
            pl.lit(as_of_date).cast(pl.Date).alias("as_of_date"),
            pl.lit(ingest_ts_us).cast(pl.Int64).alias("ingest_ts_us"),
        ]
    )

    # Filter unknown exchange rows
    result = result.filter(pl.col("exchange_id") != 0)

    result = result.select([
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
        "exchange_id",
        "exchange_symbol",
        "as_of_date",
        "ingest_ts_us",
    ])

    return normalize_stock_basic_cn_schema(result)


def build_dim_symbol_updates_from_stock_basic_cn(df: pl.DataFrame) -> pl.DataFrame:
    """Build dim_symbol updates from stock_basic snapshot data."""
    exchange_expr, exchange_id_expr = _exchange_mappings()

    if "exchange_id" in df.columns:
        exchange_id_col = pl.col("exchange_id").cast(pl.Int16)
    else:
        exchange_id_col = exchange_id_expr

    exchange_col = pl.col("exchange") if "exchange" in df.columns else exchange_expr

    if "exchange_symbol" in df.columns:
        exchange_symbol_col = pl.col("exchange_symbol")
    else:
        exchange_symbol_col = pl.col("symbol")

    if "list_date" in df.columns:
        list_date_expr = _parse_yyyymmdd_to_date(pl.col("list_date"))
    else:
        list_date_expr = pl.lit(None, dtype=pl.Date)

    valid_from_expr = (
        list_date_expr.cast(pl.Datetime("us"), strict=False)
        .cast(pl.Int64, strict=False)
        .fill_null(0)
    )

    updates = df.with_columns(
        [
            exchange_id_col.alias("exchange_id"),
            exchange_col.alias("exchange"),
            exchange_symbol_col.alias("exchange_symbol"),
            pl.coalesce([pl.col("name"), pl.col("symbol")]).alias("base_asset"),
            pl.lit("CNY").alias("quote_asset"),
            pl.lit(0).cast(pl.UInt8).alias("asset_type"),
            pl.lit(0.01).alias("tick_size"),
            pl.lit(100.0).alias("lot_size"),
            pl.lit(0.01).alias("price_increment"),
            pl.lit(100.0).alias("amount_increment"),
            pl.lit(1.0).alias("contract_size"),
            valid_from_expr.alias("valid_from_ts"),
        ]
    ).select(
        [
            "exchange_id",
            "exchange",
            "exchange_symbol",
            "base_asset",
            "quote_asset",
            "asset_type",
            "tick_size",
            "lot_size",
            "price_increment",
            "amount_increment",
            "contract_size",
            "valid_from_ts",
        ]
    )

    return updates.filter(pl.col("exchange_id") != 0)
