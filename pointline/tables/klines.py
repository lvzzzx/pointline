"""Binance kline domain logic for parsing, validation, and transformation."""

from __future__ import annotations

import logging
from collections.abc import Sequence

import polars as pl

from pointline.tables._base import (
    exchange_id_validation_expr,
    generic_resolve_symbol_ids,
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.validation_utils import with_expected_exchange_id

logger = logging.getLogger(__name__)

# Expected row counts per day for each kline interval
KLINE_INTERVAL_ROWS_PER_DAY = {
    "1m": 1440,  # 60 minutes × 24 hours
    "3m": 480,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "2h": 12,
    "4h": 6,
    "6h": 4,
    "8h": 3,
    "12h": 2,
    "1d": 1,
}

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
    "quote_volume_int": pl.Int64,
    "trade_count": pl.Int64,
    "taker_buy_base_qty_int": pl.Int64,
    "taker_buy_quote_qty_int": pl.Int64,
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
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

    Binance kline CSV columns (may or may not have header):
    open_time, open, high, low, close, volume, close_time, quote_volume,
    trade_count, taker_buy_base_volume, taker_buy_quote_volume, ignore

    Handles both cases:
    - CSVs without headers (raw data rows only)
    - CSVs with headers (filters out the header row automatically)
    """
    if df.is_empty():
        return df

    result = _ensure_raw_columns(df)
    result = _filter_header_row(result)

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
        pl.col("ts_bucket_start_us").is_not_null() & pl.col("ts_bucket_end_us").is_not_null()
    )


def encode_fixed_point(df: pl.DataFrame, dim_symbol: pl.DataFrame) -> pl.DataFrame:
    """Encode OHLC and volume fields using dim_symbol increments.

    Uses computed quote_increment = price_increment × amount_increment for quote volumes.
    """
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

    # Validate increments are positive
    invalid_increments = joined.filter(
        (pl.col("price_increment") <= 0) | (pl.col("amount_increment") <= 0)
    )
    if not invalid_increments.is_empty():
        raise ValueError("encode_fixed_point: Invalid increments (<=0) detected in dim_symbol")

    # Compute quote_increment = price_increment × amount_increment
    result = joined.with_columns(
        (pl.col("price_increment") * pl.col("amount_increment")).alias("quote_increment")
    )

    # Encode all fields
    result = result.with_columns(
        [
            (pl.col("open") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("open_px_int"),
            (pl.col("high") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("high_px_int"),
            (pl.col("low") / pl.col("price_increment")).round().cast(pl.Int64).alias("low_px_int"),
            (pl.col("close") / pl.col("price_increment"))
            .round()
            .cast(pl.Int64)
            .alias("close_px_int"),
            (pl.col("volume") / pl.col("amount_increment"))
            .round()
            .cast(pl.Int64)
            .alias("volume_qty_int"),
            (pl.col("quote_volume") / pl.col("quote_increment"))
            .round()
            .cast(pl.Int64)
            .alias("quote_volume_int"),
            (pl.col("taker_buy_base_volume") / pl.col("amount_increment"))
            .round()
            .cast(pl.Int64)
            .alias("taker_buy_base_qty_int"),
            (pl.col("taker_buy_quote_volume") / pl.col("quote_increment"))
            .round()
            .cast(pl.Int64)
            .alias("taker_buy_quote_qty_int"),
        ]
    )

    # Validate no Int64 overflow (check for nulls after cast, which indicate overflow)
    overflow_check = result.filter(
        pl.col("quote_volume_int").is_null() & pl.col("quote_volume").is_not_null()
    )
    if not overflow_check.is_empty():
        raise ValueError(
            f"encode_fixed_point: Int64 overflow detected when encoding quote_volume "
            f"(quote_increment too small or values too large)"
        )

    drop_cols = [
        "price_increment",
        "amount_increment",
        "quote_increment",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    return result.drop(drop_cols)


def decode_fixed_point(
    df: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float OHLC and volume columns using dim_symbol metadata.

    Uses computed quote_increment = price_increment × amount_increment for quote volumes.

    Requires:
    - df must have 'symbol_id' column
    - df must have '*_px_int' and '*_qty_int' columns
    - dim_symbol must have 'symbol_id', 'price_increment', 'amount_increment' columns

    Returns DataFrame with open, high, low, close, volume, quote_volume,
    taker_buy_base_volume, taker_buy_quote_volume added (Float64).
    By default, drops the *_int columns.
    """
    if "symbol_id" not in df.columns:
        raise ValueError("decode_fixed_point: df must have 'symbol_id' column")

    required_cols = [
        "open_px_int",
        "high_px_int",
        "low_px_int",
        "close_px_int",
        "volume_qty_int",
        "quote_volume_int",
        "taker_buy_base_qty_int",
        "taker_buy_quote_qty_int",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    required_dims = ["symbol_id", "price_increment", "amount_increment"]
    missing_dims = [c for c in required_dims if c not in dim_symbol.columns]
    if missing_dims:
        raise ValueError(f"decode_fixed_point: dim_symbol missing columns: {missing_dims}")

    joined = df.join(
        dim_symbol.select(["symbol_id", "price_increment", "amount_increment"]),
        on="symbol_id",
        how="left",
    )

    missing_ids = joined.filter(pl.col("price_increment").is_null())
    if not missing_ids.is_empty():
        missing_symbols = missing_ids.select("symbol_id").unique()
        raise ValueError(
            f"decode_fixed_point: {missing_symbols.height} symbol_ids not found in dim_symbol"
        )

    # Compute quote_increment
    result = joined.with_columns(
        (pl.col("price_increment") * pl.col("amount_increment")).alias("quote_increment")
    )

    # Decode all fields
    result = result.with_columns(
        [
            pl.when(pl.col("open_px_int").is_not_null())
            .then((pl.col("open_px_int") * pl.col("price_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("open"),
            pl.when(pl.col("high_px_int").is_not_null())
            .then((pl.col("high_px_int") * pl.col("price_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("high"),
            pl.when(pl.col("low_px_int").is_not_null())
            .then((pl.col("low_px_int") * pl.col("price_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("low"),
            pl.when(pl.col("close_px_int").is_not_null())
            .then((pl.col("close_px_int") * pl.col("price_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("close"),
            pl.when(pl.col("volume_qty_int").is_not_null())
            .then((pl.col("volume_qty_int") * pl.col("amount_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("volume"),
            pl.when(pl.col("quote_volume_int").is_not_null())
            .then((pl.col("quote_volume_int") * pl.col("quote_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("quote_volume"),
            pl.when(pl.col("taker_buy_base_qty_int").is_not_null())
            .then((pl.col("taker_buy_base_qty_int") * pl.col("amount_increment")).cast(pl.Float64))
            .otherwise(None)
            .alias("taker_buy_base_volume"),
            pl.when(pl.col("taker_buy_quote_qty_int").is_not_null())
            .then(
                (pl.col("taker_buy_quote_qty_int") * pl.col("quote_increment")).cast(pl.Float64)
            )
            .otherwise(None)
            .alias("taker_buy_quote_volume"),
        ]
    )

    drop_cols = ["price_increment", "amount_increment", "quote_increment"]
    if not keep_ints:
        drop_cols += required_cols
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
        "quote_volume_int",
        "taker_buy_quote_qty_int",
        "exchange",
        "exchange_id",
        "symbol_id",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_klines: missing required columns: {missing}")

    df_with_expected = with_expected_exchange_id(df)

    combined_filter = (
        timestamp_validation_expr("ts_bucket_start_us")
        & timestamp_validation_expr("ts_bucket_end_us")
        & (pl.col("ts_bucket_end_us") > pl.col("ts_bucket_start_us"))
        & (pl.col("open_px_int") > 0)
        & (pl.col("high_px_int") > 0)
        & (pl.col("low_px_int") > 0)
        & (pl.col("close_px_int") > 0)
        & (pl.col("high_px_int") >= pl.col("low_px_int"))
        & (pl.col("volume_qty_int") >= 0)
        & (pl.col("quote_volume_int") >= 0)
        & (pl.col("taker_buy_quote_qty_int") >= 0)
        & required_columns_validation_expr([
            "exchange",
            "exchange_id",
            "symbol_id",
            "quote_volume_int",
            "taker_buy_quote_qty_int",
        ])
        & exchange_id_validation_expr()
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
        (
            "quote_volume_int",
            pl.col("quote_volume_int").is_null() | (pl.col("quote_volume_int") < 0),
        ),
        (
            "taker_buy_quote_qty_int",
            pl.col("taker_buy_quote_qty_int").is_null() | (pl.col("taker_buy_quote_qty_int") < 0),
        ),
        ("exchange", pl.col("exchange").is_null()),
        ("exchange_id", pl.col("exchange_id").is_null()),
        ("symbol_id", pl.col("symbol_id").is_null()),
        (
            "exchange_id_mismatch",
            pl.col("expected_exchange_id").is_null()
            | (pl.col("exchange_id") != pl.col("expected_exchange_id")),
        ),
    ]

    valid = generic_validate(df_with_expected, combined_filter, rules, "klines")
    return valid.select(df.columns)


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_bucket_start_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for kline data using as-of join with dim_symbol.

    This is a wrapper around the generic symbol resolution function.

    Args:
        data: DataFrame with timestamp column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_bucket_start_us)

    Returns:
        DataFrame with symbol_id column added
    """
    return generic_resolve_symbol_ids(data, dim_symbol, exchange_id, exchange_symbol, ts_col=ts_col)


def check_kline_completeness(
    df: pl.DataFrame,
    interval: str = "1h",
    *,
    warn_on_gaps: bool = True,
    by_exchange_symbol: bool = False,
) -> pl.DataFrame:
    """Check kline completeness per (date, symbol_id) or (date, exchange_symbol).

    Validates that each symbol has the expected number of klines per day.
    For example, 1h klines should have exactly 24 rows per day per symbol.

    Args:
        df: Kline DataFrame with 'date' and 'symbol_id' columns
        interval: Kline interval (e.g., '1h', '4h', '1d')
        warn_on_gaps: If True, log warnings for incomplete days
        by_exchange_symbol: If True, group by exchange_symbol instead of symbol_id
                           (recommended to handle symbol metadata transitions)

    Returns:
        DataFrame with completeness statistics per (date, symbol_id) or (date, exchange_symbol):
        - date, symbol_id/exchange_symbol, row_count, expected_count, is_complete

    Note:
        Gaps can be legitimate (new listings, delistings, exchange downtime).
        Use this for data quality monitoring, not hard validation.

        For symbols with metadata changes (SCD Type 2), use by_exchange_symbol=True
        to combine hours across different symbol_id versions on transition days.
    """
    if df.is_empty():
        group_col = "exchange_symbol" if by_exchange_symbol else "symbol_id"
        col_type = pl.Utf8 if by_exchange_symbol else pl.Int64
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                group_col: col_type,
                "row_count": pl.Int64,
                "expected_count": pl.Int64,
                "is_complete": pl.Boolean,
            }
        )

    # Determine grouping column
    if by_exchange_symbol:
        if "exchange_symbol" not in df.columns:
            raise ValueError(
                "check_kline_completeness: by_exchange_symbol=True requires "
                "'exchange_symbol' column in df"
            )
        group_cols = ["date", "exchange_symbol"]
    else:
        if "date" not in df.columns or "symbol_id" not in df.columns:
            raise ValueError("check_kline_completeness: df must have 'date' and 'symbol_id' columns")
        group_cols = ["date", "symbol_id"]

    expected_count = KLINE_INTERVAL_ROWS_PER_DAY.get(interval)
    if expected_count is None:
        raise ValueError(
            f"check_kline_completeness: unknown interval '{interval}'. "
            f"Valid intervals: {list(KLINE_INTERVAL_ROWS_PER_DAY.keys())}"
        )

    # Count rows per group
    completeness = df.group_by(group_cols).agg(
        pl.len().alias("row_count")
    ).with_columns([
        pl.lit(expected_count, dtype=pl.Int64).alias("expected_count"),
        (pl.col("row_count") == expected_count).alias("is_complete"),
    ]).sort(group_cols)

    if warn_on_gaps:
        incomplete = completeness.filter(~pl.col("is_complete"))
        if not incomplete.is_empty():
            total_incomplete = incomplete.height
            group_desc = "exchange_symbol" if by_exchange_symbol else "symbol_id"
            logger.warning(
                f"Found {total_incomplete} incomplete day(s) for interval '{interval}' "
                f"(expected {expected_count} rows/day, grouped by {group_desc}). "
                f"This may indicate data gaps, new listings, or exchange downtime."
            )
            # Log sample of incomplete days
            sample = incomplete.head(10)
            logger.warning(f"Sample incomplete days:\n{sample}")

    return completeness


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

    rename_map = {df.columns[i]: RAW_KLINE_COLUMNS[i] for i in range(len(RAW_KLINE_COLUMNS))}
    return df.rename(rename_map)


def _filter_header_row(df: pl.DataFrame) -> pl.DataFrame:
    """Filter out header row if present.

    Some Binance CSV files have headers, some don't. Detect and remove header rows.
    A row is considered a header if 'open_time' column contains the string 'open_time'.
    """
    if df.is_empty():
        return df

    # Check if first row is a header by looking for string 'open_time' in the open_time column
    # Cast to string for comparison since the column might be read as various types
    first_row_open_time = df.select(pl.col("open_time").cast(pl.Utf8).first()).item()

    # If the first value is literally the column name, it's a header row
    if first_row_open_time and first_row_open_time.lower() == "open_time":
        return df.slice(1, df.height - 1)

    return df

