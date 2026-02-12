"""Book snapshots domain logic for parsing, validation, and transformation.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl
    from pointline.tables.book_snapshots import (
        BOOK_SNAPSHOTS_DOMAIN,
        parse_tardis_book_snapshots_csv,
        resolve_symbol_ids,
    )

    raw_df = pl.read_csv("book_snapshots.csv")
    parsed = parse_tardis_book_snapshots_csv(raw_df)
    resolved = resolve_symbol_ids(
        parsed,
        dim_symbol,
        exchange_id=1,
        exchange_symbol="BTCUSDT",
    )
    encoded = BOOK_SNAPSHOTS_DOMAIN.encode_storage(resolved)
    normalized = BOOK_SNAPSHOTS_DOMAIN.normalize_schema(encoded)
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

# Import parser from new location for backward compatibility
from pointline.validation_utils import DataQualityWarning

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition matching design.md Section 5.2
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - These are automatically converted to signed types (Int16 and Int32) when written
# - Use Int32 instead of UInt32 for file_id, file_line_number
# - Use Int64 for symbol_id to match dim_symbol
#
# This schema is the single source of truth - all code should use these types directly.
BOOK_SNAPSHOTS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,  # Exchange name (string) for partitioning and human readability
    "symbol": pl.Utf8,
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "bids_px_int": pl.List(pl.Int64),  # List of 25 bid prices (nulls for missing levels)
    "bids_sz_int": pl.List(pl.Int64),  # List of 25 bid sizes (nulls for missing levels)
    "asks_px_int": pl.List(pl.Int64),  # List of 25 ask prices (nulls for missing levels)
    "asks_sz_int": pl.List(pl.Int64),  # List of 25 ask sizes (nulls for missing levels)
    "file_id": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
    "file_line_number": pl.Int32,  # Delta Lake stores as Int32 (not UInt32)
}


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical book snapshots schema and select only schema columns.

    Ensures all required columns exist and have correct types.
    Drops any extra columns (e.g., original float columns, dim_symbol metadata).
    """
    # Check for missing required columns
    missing_required = [col for col in BOOK_SNAPSHOTS_SCHEMA if col not in df.columns]
    if missing_required:
        raise ValueError(f"book_snapshots missing required columns: {missing_required}")

    # Cast columns to schema types
    casts = []
    for col, dtype in BOOK_SNAPSHOTS_SCHEMA.items():
        if col in df.columns:
            if isinstance(dtype, pl.List):
                # For list columns, ensure inner type matches
                casts.append(pl.col(col).cast(dtype))
            else:
                casts.append(pl.col(col).cast(dtype))
        else:
            raise ValueError(f"Required column {col} is missing")

    # Cast and select only schema columns (drops extra columns)
    return df.with_columns(casts).select(list(BOOK_SNAPSHOTS_SCHEMA.keys()))


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to book snapshots data.

    Validates:
    - Required columns exist
    - List lengths are 25 (or pad/truncate to 25)
    - Bid prices are descending (bids_px_int[0] >= bids_px_int[1] >= ...)
    - Ask prices are ascending (asks_px_int[0] <= asks_px_int[1] <= ...)
    - Crossed book check: best bid < best ask
    - Non-negative sizes when present
    - Valid timestamp ranges (reasonable values)

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    # Check required columns
    required = [
        "bids_px_int",
        "bids_sz_int",
        "asks_px_int",
        "asks_sz_int",
        "ts_local_us",
        "exchange",
        "symbol",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_book_snapshots: missing required columns: {missing}")

    # Build validation filters
    filters = [
        (pl.col("ts_local_us") > 0)
        & (pl.col("ts_local_us") < 2**63)
        & (pl.col("exchange").is_not_null())
        & (pl.col("symbol").is_not_null())
    ]

    # Ensure list lengths are 25 (pad with nulls if needed, truncate if longer)
    # Slice to max 25 elements, then pad with nulls if needed
    # Preserve the existing dtype (Int64 after encoding, Float64 before encoding)
    def normalize_list_length(col_name: str) -> pl.Expr:
        """Ensure list has exactly 25 elements, padding with nulls or truncating."""
        col_expr = pl.col(col_name)
        # Prefer actual column dtype; fall back to schema or Int64.
        col_dtype = df.schema.get(col_name)
        if isinstance(col_dtype, pl.List):
            inner_dtype = col_dtype.inner
        else:
            expected_dtype = BOOK_SNAPSHOTS_SCHEMA.get(col_name, pl.List(pl.Int64))
            inner_dtype = expected_dtype.inner if isinstance(expected_dtype, pl.List) else pl.Int64

        # Slice to max 25, then pad with nulls (vectorized).
        sliced = col_expr.list.slice(0, 25)
        null_pad = pl.lit(None, dtype=inner_dtype).repeat_by(pl.lit(25))
        padded = pl.concat_list([sliced, null_pad]).list.slice(0, 25)
        return pl.when(col_expr.is_null()).then(null_pad).otherwise(padded)

    # Normalize all list columns to length 25
    result = df.with_columns(
        [
            normalize_list_length("bids_px_int").alias("bids_px_int"),
            normalize_list_length("bids_sz_int").alias("bids_sz_int"),
            normalize_list_length("asks_px_int").alias("asks_px_int"),
            normalize_list_length("asks_sz_int").alias("asks_sz_int"),
        ]
    )

    # Validate bid prices are descending (bids_px_int[0] >= bids_px_int[1] >= ...)
    # Vectorized monotonicity check using list.diff on non-null values.
    def validate_bid_ordering() -> pl.Expr:
        """Check that bid prices are descending (non-increasing) across all adjacent levels."""
        bids = pl.col("bids_px_int").list.drop_nulls()
        diffs = bids.list.diff()
        max_diff = diffs.list.max()
        return pl.when(max_diff.is_null()).then(True).otherwise(max_diff <= 0)

    # Validate ask prices are ascending (asks_px_int[0] <= asks_px_int[1] <= ...)
    # Vectorized monotonicity check using list.diff on non-null values.
    def validate_ask_ordering() -> pl.Expr:
        """Check that ask prices are ascending (non-decreasing) across all adjacent levels."""
        asks = pl.col("asks_px_int").list.drop_nulls()
        diffs = asks.list.diff()
        min_diff = diffs.list.min()
        return pl.when(min_diff.is_null()).then(True).otherwise(min_diff >= 0)

    # Crossed book check: bids_px_int[i] < asks_px_int[i] at each level
    # Check that best bid < best ask (bids_px_int[0] < asks_px_int[0])
    def validate_crossed_book() -> pl.Expr:
        """Check that best bid < best ask."""
        best_bid = pl.col("bids_px_int").list.first()
        best_ask = pl.col("asks_px_int").list.first()
        return (
            pl.when(best_bid.is_not_null() & best_ask.is_not_null())
            .then(best_bid < best_ask)
            .otherwise(True)
        )

    # Non-negative sizes when present
    def validate_non_negative_sizes() -> pl.Expr:
        """Check that sizes are non-negative when present."""
        bid_sz_min = pl.col("bids_sz_int").list.min()
        ask_sz_min = pl.col("asks_sz_int").list.min()
        return (bid_sz_min.is_null() | (bid_sz_min >= 0)) & (
            ask_sz_min.is_null() | (ask_sz_min >= 0)
        )

    filters.extend(
        [
            validate_bid_ordering(),
            validate_ask_ordering(),
            validate_crossed_book(),
            validate_non_negative_sizes(),
        ]
    )

    # Combine all filters
    combined_filter = filters[0]
    for f in filters[1:]:
        combined_filter = combined_filter & f

    valid = result.filter(combined_filter).select(df.columns)

    # Warn if rows were filtered
    if valid.height < result.height:
        import warnings

        warnings.warn(
            f"validate_book_snapshots: filtered {result.height - valid.height} invalid rows",
            DataQualityWarning,
            stacklevel=2,
        )

    return valid


def _encode_storage(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Encode bid/ask prices and sizes as fixed-point integers using asset-class profile.

    Requires:
    - df must have 'symbol_id' column (from resolve_symbol_ids)
    - df must have raw level columns: asks[0..24].price/amount, bids[0..24].price/amount
    - exchange: exchange name used to resolve the ScalarProfile

    Computes:
    - bids_px_int = [floor(price / profile.price) for price in bids_px]
    - bids_sz_int = [round(size / profile.amount) for size in bids_sz]
    - asks_px_int = [ceil(price / profile.price) for price in asks_px]
    - asks_sz_int = [round(size / profile.amount) for size in asks_sz]

    Returns DataFrame with bids_px_int, bids_sz_int, asks_px_int, asks_sz_int as Int64 lists.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    asks_price_cols = [f"asks[{i}].price" for i in range(25)]
    asks_amount_cols = [f"asks[{i}].amount" for i in range(25)]
    bids_price_cols = [f"bids[{i}].price" for i in range(25)]
    bids_amount_cols = [f"bids[{i}].amount" for i in range(25)]
    raw_cols = asks_price_cols + asks_amount_cols + bids_price_cols + bids_amount_cols
    has_raw_cols = any(col in df.columns for col in raw_cols)

    if not has_raw_cols:
        raise ValueError("encode_fixed_point: df must contain raw level columns")

    def encode_list(
        cols: list[str],
        existing_cols: list[str],
        scalar_col: str,
        *,
        mode: str,
    ) -> pl.Expr:
        list_exprs: list[pl.Expr] = []
        for col in cols:
            if col in existing_cols:
                scaled = pl.col(col) / pl.col(scalar_col)
                if mode == "floor":
                    scaled = scaled.floor()
                elif mode == "ceil":
                    scaled = scaled.ceil()
                else:
                    scaled = scaled.round()

                list_exprs.append(
                    pl.when(pl.col(col).is_not_null()).then(scaled.cast(pl.Int64)).otherwise(None)
                )
            else:
                list_exprs.append(pl.lit(None, dtype=pl.Int64))
        return pl.concat_list(list_exprs)

    if df.is_empty():
        drop_cols = [c for c in raw_cols if c in df.columns]
        return df.drop(drop_cols)

    existing_asks_price = [c for c in asks_price_cols if c in df.columns]
    existing_asks_amount = [c for c in asks_amount_cols if c in df.columns]
    existing_bids_price = [c for c in bids_price_cols if c in df.columns]
    existing_bids_amount = [c for c in bids_amount_cols if c in df.columns]

    working = with_profile_scalars(df)
    result = working.with_columns(
        [
            encode_list(
                asks_price_cols,
                existing_asks_price,
                PROFILE_PRICE_COL,
                mode="ceil",
            ).alias("asks_px_int"),
            encode_list(
                asks_amount_cols,
                existing_asks_amount,
                PROFILE_AMOUNT_COL,
                mode="round",
            ).alias("asks_sz_int"),
            encode_list(
                bids_price_cols,
                existing_bids_price,
                PROFILE_PRICE_COL,
                mode="floor",
            ).alias("bids_px_int"),
            encode_list(
                bids_amount_cols,
                existing_bids_amount,
                PROFILE_AMOUNT_COL,
                mode="round",
            ).alias("bids_sz_int"),
        ]
    )

    drop_cols = [c for c in raw_cols if c in result.columns]
    return result.drop(drop_cols + [col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point list columns into float lists.

    Requires:
    - df must have 'bids_px_int', 'bids_sz_int', 'asks_px_int', 'asks_sz_int' columns (lists of ints)
    - df must have non-null 'exchange' values

    Args:
        df: DataFrame with encoded list columns.
        keep_ints: If True, keep the *_int columns alongside decoded floats.

    Returns DataFrame with bids_px, bids_sz, asks_px, asks_sz added as Float64 lists.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    int_cols = ["bids_px_int", "bids_sz_int", "asks_px_int", "asks_sz_int"]
    missing = [c for c in int_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            (pl.col("bids_px_int") * pl.col(PROFILE_PRICE_COL))
            .cast(pl.List(pl.Float64))
            .alias("bids_px"),
            (pl.col("bids_sz_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.List(pl.Float64))
            .alias("bids_sz"),
            (pl.col("asks_px_int") * pl.col(PROFILE_PRICE_COL))
            .cast(pl.List(pl.Float64))
            .alias("asks_px"),
            (pl.col("asks_sz_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.List(pl.Float64))
            .alias("asks_sz"),
        ]
    )

    if not keep_ints:
        result = result.drop(int_cols)
    return result.drop([col for col in PROFILE_SCALAR_COLS if col in result.columns])


def _decode_storage_lazy(
    lf: pl.LazyFrame,
    *,
    keep_ints: bool = False,
) -> pl.LazyFrame:
    """Decode fixed-point list columns lazily into float lists."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    int_cols = ["bids_px_int", "bids_sz_int", "asks_px_int", "asks_sz_int"]
    missing = [c for c in int_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            (pl.col("bids_px_int") * pl.col(PROFILE_PRICE_COL))
            .cast(pl.List(pl.Float64))
            .alias("bids_px"),
            (pl.col("bids_sz_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.List(pl.Float64))
            .alias("bids_sz"),
            (pl.col("asks_px_int") * pl.col(PROFILE_PRICE_COL))
            .cast(pl.List(pl.Float64))
            .alias("asks_px"),
            (pl.col("asks_sz_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.List(pl.Float64))
            .alias("asks_sz"),
        ]
    )
    if not keep_ints:
        result = result.drop(int_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Book snapshots have no enum remapping at canonicalization stage."""
    return df


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for book snapshots."""
    return ("exchange", "bids_px_int", "bids_sz_int", "asks_px_int", "asks_sz_int")


@dataclass(frozen=True)
class BookSnapshotsDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="book_snapshot_25",
        table_kind="event",
        schema=BOOK_SNAPSHOTS_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=None,
        ts_column="ts_local_us",
    )

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


BOOK_SNAPSHOTS_DOMAIN = BookSnapshotsDomain()


register_domain(BOOK_SNAPSHOTS_DOMAIN)
