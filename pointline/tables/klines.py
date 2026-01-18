"""Binance kline domain logic for parsing, validation, and transformation."""

from __future__ import annotations

from typing import Sequence

import polars as pl

from pointline.validation_utils import with_expected_exchange_id

KLINE_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "ts_bucket_start_us": pl.Int64,
    "ts_bucket_end_us": pl.Int64,
    "open_px_int": pl.Int64,
    "high_px_int": pl.Int64,
    "low_px_int": pl.Int64,
    "close_px_int": pl.Int64,
    "volume_qty_int": pl.Int64,
    "quote_volume": pl.Float64,
    "trade_count": pl.Int64,
    "taker_buy_base_qty_int": pl.Int64,
    "taker_buy_quote_qty": pl.Float64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
    "ingest_seq": pl.Int32,
}

RAW_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def parse_binance_klines_csv(df: pl.DataFrame) -> pl.DataFrame:
    """Parse raw Binance kline CSV rows into typed columns.

    Binance kline CSV columns (no header in public data):
    open_time, open, high, low, close, volume, close_time, quote_volume,
    trade_count, taker_buy_base_volume, taker_buy_quote_volume, ignore
    """
    if df.is_empty():
        return df

    result = _ensure_raw_columns(df)

    def to_us_expr(col_name: str) -> pl.Expr:
        col = pl.col(col_name).cast(pl.Int64, strict=False)
        return pl.when(col >= 1_000_000_000_000_000).then(col).otherwise(col * 1_000)

    parsed = result.select(
        [
            to_us_expr("open_time").alias("ts_bucket_start_us"),
            to_us_expr("close_time").alias("ts_bucket_end_us"),
            pl.col("open").cast(pl.Float64, strict=False).alias("open"),
            pl.col("high").cast(pl.Float64, strict=False).alias("high"),
            pl.col("low").cast(pl.Float64, strict=False).alias("low"),
            pl.col("close").cast(pl.Float64, strict=False).alias("close"),
            pl.col("volume").cast(pl.Float64, strict=False).alias("volume"),
            pl.col("quote_volume").cast(pl.Float64, strict=False).alias("quote_volume"),
            pl.col("trade_count").cast(pl.Int64, strict=False).alias("trade_count"),
            pl.col("taker_buy_base_volume")
            .cast(pl.Float64, strict=False)
            .alias("taker_buy_base_volume"),
            pl.col("taker_buy_quote_volume")
            .cast(pl.Float64, strict=False)
            .alias("taker_buy_quote_volume"),
        ]
    )
    return parsed.filter(
        pl.col("ts_bucket_start_us").is_not_null()
        & pl.col("ts_bucket_end_us").is_not_null()
    )


def encode_fixed_point(df: pl.DataFrame, dim_symbol: pl.DataFrame) -> pl.DataFrame:
    """Encode OHLC and volume fields using dim_symbol increments."""
    if "symbol_id" not in df.columns:
        raise ValueError("encode_fixed_point: df must have 'symbol_id' column")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing = [c for c in required_dims if c not in dim_symbol.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: dim_symbol missing columns: {missing}")

    joined = df.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )

    missing_ids = joined.filter(pl.col("price_increment").is_null())
    if not missing_ids.is_empty():
        missing_symbols = missing_ids.select("symbol_id").unique()
        raise ValueError(
            f"encode_fixed_point: {missing_symbols.height} symbol_ids not found in dim_symbol"
        )

    result = joined.with_columns(
        [
            (pl.col("open") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("open_px_int"),
            (pl.col("high") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("high_px_int"),
            (pl.col("low") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("low_px_int"),
            (pl.col("close") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("close_px_int"),
            (pl.col("volume") / pl.col("amount_increment"))
            .round()
            .cast(pl.Int64)
            .alias("volume_qty_int"),
            (pl.col("taker_buy_base_volume") / pl.col("amount_increment"))
            .round()
            .cast(pl.Int64)
            .alias("taker_buy_base_qty_int"),
        ]
    )

    drop_cols = [
        "price_increment",
        "amount_increment",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "taker_buy_base_volume",
    ]
    return result.drop(drop_cols)


def normalize_klines_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure kline DataFrame matches the canonical schema and column order."""
    for col, dtype in KLINE_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(dtype))
    return df.select(list(KLINE_SCHEMA.keys()))


def validate_klines(df: pl.DataFrame) -> pl.DataFrame:
    """Validate kline rows; returns filtered DataFrame."""
    if df.is_empty():
        return df

    required = [
        "ts_bucket_start_us",
        "ts_bucket_end_us",
        "open_px_int",
        "high_px_int",
        "low_px_int",
        "close_px_int",
        "volume_qty_int",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_klines: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)
    valid = df_with_expected.filter(
        (pl.col("ts_bucket_start_us") > 0)
        & (pl.col("ts_bucket_end_us") > 0)
        & (pl.col("ts_bucket_end_us") > pl.col("ts_bucket_start_us"))
        & (pl.col("open_px_int") > 0)
        & (pl.col("high_px_int") > 0)
        & (pl.col("low_px_int") > 0)
        & (pl.col("close_px_int") > 0)
        & (pl.col("high_px_int") >= pl.col("low_px_int"))
        & (pl.col("volume_qty_int") >= 0)
        & (pl.col("exchange").is_not_null())
        & (pl.col("exchange_id").is_not_null())
        & (pl.col("symbol_id").is_not_null())
        & (pl.col("exchange_id") == pl.col("expected_exchange_id"))
    ).select(df.columns)

    if valid.height < df.height:
        import warnings

        line_col = "file_line_number" if "file_line_number" in df.columns else "__row_nr"
        df_with_line = df_with_expected
        if line_col == "__row_nr":
            df_with_line = (
                df_with_expected.with_row_index("__row_nr")
                if hasattr(df_with_expected, "with_row_index")
                else df_with_expected.with_row_count("__row_nr")
            )

        rules = [
            (
                "ts_bucket_start_us",
                pl.col("ts_bucket_start_us").is_null()
                | (pl.col("ts_bucket_start_us") <= 0)
                | (pl.col("ts_bucket_start_us") >= 2**63),
            ),
            (
                "ts_bucket_end_us",
                pl.col("ts_bucket_end_us").is_null()
                | (pl.col("ts_bucket_end_us") <= 0)
                | (pl.col("ts_bucket_end_us") >= 2**63),
            ),
            (
                "invalid_bucket_order",
                pl.col("ts_bucket_end_us") <= pl.col("ts_bucket_start_us"),
            ),
            ("open_px_int", pl.col("open_px_int").is_null() | (pl.col("open_px_int") <= 0)),
            ("high_px_int", pl.col("high_px_int").is_null() | (pl.col("high_px_int") <= 0)),
            ("low_px_int", pl.col("low_px_int").is_null() | (pl.col("low_px_int") <= 0)),
            (
                "close_px_int",
                pl.col("close_px_int").is_null() | (pl.col("close_px_int") <= 0),
            ),
            ("high_lt_low", pl.col("high_px_int") < pl.col("low_px_int")),
            ("volume_qty_int", pl.col("volume_qty_int").is_null() | (pl.col("volume_qty_int") < 0)),
            ("exchange", pl.col("exchange").is_null()),
            ("exchange_id", pl.col("exchange_id").is_null()),
            ("symbol_id", pl.col("symbol_id").is_null()),
            (
                "exchange_id_mismatch",
                pl.col("expected_exchange_id").is_null()
                | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
            ),
        ]

        counts = df_with_line.select([rule.sum().alias(name) for name, rule in rules]).row(0)
        breakdown = []
        for (name, rule), count in zip(rules, counts):
            if count:
                sample = (
                    df_with_line.filter(rule)
                    .select(line_col)
                    .head(5)
                    .to_series()
                    .to_list()
                )
                breakdown.append(f"{name}={count} lines={sample}")

        detail = "; ".join(breakdown) if breakdown else "no rule breakdown available"
        warnings.warn(
            f"validate_klines: filtered {df.height - valid.height} invalid rows; {detail}",
            UserWarning,
        )

    return valid


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_bucket_start_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for kline data using as-of join with dim_symbol."""
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids

    result = data.clone()
    if "exchange_id" not in result.columns:
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))
    if "exchange_symbol" not in result.columns:
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))

    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col)


def required_kline_columns() -> Sequence[str]:
    """Columns required for kline DataFrame after normalization."""
    return tuple(KLINE_SCHEMA.keys())


def _ensure_raw_columns(df: pl.DataFrame) -> pl.DataFrame:
    if len(df.columns) < len(RAW_KLINE_COLUMNS):
        raise ValueError(
            f"parse_binance_klines_csv: expected at least {len(RAW_KLINE_COLUMNS)} columns, "
            f"got {len(df.columns)}"
        )

    if df.columns[: len(RAW_KLINE_COLUMNS)] == RAW_KLINE_COLUMNS:
        return df

    rename_map = {
        df.columns[i]: RAW_KLINE_COLUMNS[i] for i in range(len(RAW_KLINE_COLUMNS))
    }
    return df.rename(rename_map)
