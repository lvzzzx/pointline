"""Binance kline domain logic for parsing, validation, and transformation."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

# Import parser from new location for backward compatibility
from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

logger = logging.getLogger(__name__)

# Required metadata fields for ingestion (klines require interval)
REQUIRED_METADATA_FIELDS: set[str] = set()

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
    "symbol": pl.Utf8,
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


def _encode_storage(df: pl.DataFrame) -> pl.DataFrame:
    """Encode OHLC and volume fields using per-asset-class scalar profile.

    Uses profile.price for OHLC, profile.amount for base volumes,
    and profile.quote_vol for quote volumes.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_QUOTE_VOL_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    working = with_profile_scalars(df)

    # Encode all fields
    result = working.with_columns(
        [
            (pl.col("open_px") / pl.col(PROFILE_PRICE_COL))
            .round()
            .cast(pl.Int64)
            .alias("open_px_int"),
            (pl.col("high_px") / pl.col(PROFILE_PRICE_COL))
            .round()
            .cast(pl.Int64)
            .alias("high_px_int"),
            (pl.col("low_px") / pl.col(PROFILE_PRICE_COL))
            .round()
            .cast(pl.Int64)
            .alias("low_px_int"),
            (pl.col("close_px") / pl.col(PROFILE_PRICE_COL))
            .round()
            .cast(pl.Int64)
            .alias("close_px_int"),
            (pl.col("volume") / pl.col(PROFILE_AMOUNT_COL))
            .round()
            .cast(pl.Int64)
            .alias("volume_qty_int"),
            (pl.col("quote_volume") / pl.col(PROFILE_QUOTE_VOL_COL))
            .round()
            .cast(pl.Int64)
            .alias("quote_volume_int"),
            (pl.col("taker_buy_base_volume") / pl.col(PROFILE_AMOUNT_COL))
            .round()
            .cast(pl.Int64)
            .alias("taker_buy_base_qty_int"),
            (pl.col("taker_buy_quote_volume") / pl.col(PROFILE_QUOTE_VOL_COL))
            .round()
            .cast(pl.Int64)
            .alias("taker_buy_quote_qty_int"),
        ]
    )

    drop_cols = [
        "open_px",
        "high_px",
        "low_px",
        "close_px",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    return result.drop(drop_cols + [col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float OHLC and volume columns.

    Uses per-row profile scalars resolved from the exchange column.

    Requires:
    - df must have '*_px_int' and '*_qty_int' columns
    - df must have non-null 'exchange' values

    Returns DataFrame with open, high, low, close, volume, quote_volume,
    taker_buy_base_volume, taker_buy_quote_volume added (Float64).
    By default, drops the *_int columns.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_QUOTE_VOL_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

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

    working = with_profile_scalars(df)

    # Decode all fields
    result = working.with_columns(
        [
            (pl.col("open_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("open_px"),
            (pl.col("high_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("high_px"),
            (pl.col("low_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("low_px"),
            (pl.col("close_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("close_px"),
            (pl.col("volume_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Float64)
            .alias("volume"),
            (pl.col("quote_volume_int") * pl.col(PROFILE_QUOTE_VOL_COL))
            .cast(pl.Float64)
            .alias("quote_volume"),
            (pl.col("taker_buy_base_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Float64)
            .alias("taker_buy_base_volume"),
            (pl.col("taker_buy_quote_qty_int") * pl.col(PROFILE_QUOTE_VOL_COL))
            .cast(pl.Float64)
            .alias("taker_buy_quote_volume"),
        ]
    )

    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode fixed-point integers lazily into float OHLC and volume columns."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_QUOTE_VOL_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
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
    missing = [c for c in required_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            (pl.col("open_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("open_px"),
            (pl.col("high_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("high_px"),
            (pl.col("low_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("low_px"),
            (pl.col("close_px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("close_px"),
            (pl.col("volume_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Float64)
            .alias("volume"),
            (pl.col("quote_volume_int") * pl.col(PROFILE_QUOTE_VOL_COL))
            .cast(pl.Float64)
            .alias("quote_volume"),
            (pl.col("taker_buy_base_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Float64)
            .alias("taker_buy_base_volume"),
            (pl.col("taker_buy_quote_qty_int") * pl.col(PROFILE_QUOTE_VOL_COL))
            .cast(pl.Float64)
            .alias("taker_buy_quote_volume"),
        ]
    )
    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure kline DataFrame matches the canonical schema and column order."""
    for col, dtype in KLINE_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(dtype))
    return df.select(list(KLINE_SCHEMA.keys()))


def _validate(df: pl.DataFrame) -> pl.DataFrame:
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
        "symbol",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_klines: missing required columns: {missing}")

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
        & required_columns_validation_expr(
            ["exchange", "symbol", "quote_volume_int", "taker_buy_quote_qty_int"]
        )
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
        ("symbol", pl.col("symbol").is_null()),
    ]

    valid = generic_validate(df, combined_filter, rules, "klines")
    return valid.select(df.columns)


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
        if by_exchange_symbol:
            group_col = "exchange_symbol"
            col_type = pl.Utf8
        elif "symbol_id" in df.columns:
            group_col = "symbol_id"
            col_type = pl.Int64
        else:
            group_col = "symbol"
            col_type = pl.Utf8
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
        if "date" not in df.columns:
            raise ValueError(
                "check_kline_completeness: df must have 'date' plus one of "
                "'symbol_id' or 'symbol' columns"
            )
        if "symbol_id" in df.columns:
            group_cols = ["date", "symbol_id"]
        elif "symbol" in df.columns:
            group_cols = ["date", "symbol"]
        else:
            raise ValueError(
                "check_kline_completeness: df must have 'date' plus one of "
                "'symbol_id' or 'symbol' columns"
            )

    expected_count = KLINE_INTERVAL_ROWS_PER_DAY.get(interval)
    if expected_count is None:
        raise ValueError(
            f"check_kline_completeness: unknown interval '{interval}'. "
            f"Valid intervals: {list(KLINE_INTERVAL_ROWS_PER_DAY.keys())}"
        )

    # Count rows per group
    completeness = (
        df.group_by(group_cols)
        .agg(pl.len().alias("row_count"))
        .with_columns(
            [
                pl.lit(expected_count, dtype=pl.Int64).alias("expected_count"),
                (pl.col("row_count") == pl.lit(expected_count)).alias("is_complete"),
            ]
        )
        .sort(group_cols)
    )

    if warn_on_gaps:
        incomplete = completeness.filter(~pl.col("is_complete"))
        if not incomplete.is_empty():
            total_incomplete = incomplete.height
            group_desc = "exchange_symbol" if by_exchange_symbol else group_cols[1]
            logger.warning(
                f"Found {total_incomplete} incomplete day(s) for interval '{interval}' "
                f"(expected {expected_count} rows/day, grouped by {group_desc}). "
                f"This may indicate data gaps, new listings, or exchange downtime."
            )
            # Log sample of incomplete days
            sample = incomplete.head(10)
            logger.warning(f"Sample incomplete days:\n{sample}")

    return completeness


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Klines have no enum remapping at canonicalization stage."""
    return df


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for kline tables."""
    return (
        "exchange",
        "open_px_int",
        "high_px_int",
        "low_px_int",
        "close_px_int",
        "volume_qty_int",
        "quote_volume_int",
        "taker_buy_base_qty_int",
        "taker_buy_quote_qty_int",
    )


@dataclass(frozen=True)
class _KlineDomain(EventTableDomain):
    spec: TableSpec

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return _canonicalize_vendor_frame(df)

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        return _encode_storage(df)

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return _normalize_schema(df)

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        return _validate(df)

    def required_decode_columns(self) -> tuple[str, ...]:
        return _required_decode_columns()

    def decode_storage(self, df: pl.DataFrame, *, keep_ints: bool = False) -> pl.DataFrame:
        return _decode_storage(df, keep_ints=keep_ints)

    def decode_storage_lazy(self, lf: pl.LazyFrame, *, keep_ints: bool = False) -> pl.LazyFrame:
        return _decode_storage_lazy(lf, keep_ints=keep_ints)


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


KLINE_1H_DOMAIN = _KlineDomain(
    spec=TableSpec(
        table_name="kline_1h",
        table_kind="event",
        schema=KLINE_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_bucket_start_us",
    )
)
KLINE_1D_DOMAIN = _KlineDomain(
    spec=TableSpec(
        table_name="kline_1d",
        table_kind="event",
        schema=KLINE_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_bucket_start_us",
    )
)


register_domain(KLINE_1H_DOMAIN)
register_domain(KLINE_1D_DOMAIN)
