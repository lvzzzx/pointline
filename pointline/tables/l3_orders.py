"""CN Level 3 order placements domain logic for parsing, validation, and transformation.

This module handles SZSE/SSE order stream data, which represents new limit and market
orders entering the matching engine. This is fundamentally different from L2 aggregated
data — each row represents an individual order with a unique order ID (appl_seq_num).

Restricted to Chinese stock exchanges (SZSE, SSE) only. These tables are structurally
coupled to Chinese market microstructure and cannot be used for other exchanges.

Deterministic Ordering
----------------------
The replay key for L3 orders is ``(channel_no, appl_seq_num)``, NOT ``ts_local_us``.

``appl_seq_num`` properties:
- Scope: per-channel (SZSE stocks, convertible bonds, and funds each use separate
  channels; SSE convertible bonds use yet another channel)
- Starts at 1 each trading day
- Unique and contiguous within a channel for a given trading day — gaps indicate
  message loss
- NOT unique across channels — two channels can both have appl_seq_num=1

``channel_no`` identifies the independent exchange channel. Within a single channel,
``appl_seq_num`` provides a total order over all events (orders and ticks share the
same sequence space). Cross-channel ordering falls back to ``ts_local_us``.

For intra-channel, single day:  sort by ``(channel_no, appl_seq_num)``
For intra-channel, multi-day:   sort by ``(date, channel_no, appl_seq_num)``
For cross-channel merge:        sort by ``(ts_local_us, file_id, file_line_number)``
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

# Import parsers from new location for backward compatibility
from pointline.tables._base import (
    generic_validate,
    required_columns_validation_expr,
    timestamp_validation_expr,
)
from pointline.tables.cn_trading_phase import (
    TRADING_PHASE_CLOSING_CALL,
    TRADING_PHASE_CONTINUOUS,
    TRADING_PHASE_OPENING_CALL,
    TRADING_PHASE_UNKNOWN,
    derive_cn_trading_phase_expr,
)
from pointline.tables.domain_contract import EventTableDomain, TableSpec
from pointline.tables.domain_registry import register_domain

# L3 tables are exclusive to Chinese stock exchanges (SZSE, SSE).
# These tables use channel_no, appl_seq_num sequencing, and CN trading phases
# that are structurally coupled to Chinese market microstructure.
ALLOWED_EXCHANGES: frozenset[str] = frozenset({"szse", "sse"})

# Required metadata fields for ingestion
REQUIRED_METADATA_FIELDS: set[str] = set()

# Schema definition for l3_orders Silver table
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - Use Int32 for file_id, file_line_number, channel_no
# - Use Int64 for symbol_id, appl_seq_num, px_int, order_qty_int
# - UInt8 is supported (used for side, ord_type)
L3_ORDERS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "ts_local_us": pl.Int64,  # Arrival time in UTC (converted from Asia/Shanghai TransactTime)
    "appl_seq_num": pl.Int64,  # Order ID (unique per channel per day, starts at 1)
    "side": pl.UInt8,  # 0=buy, 1=sell
    "ord_type": pl.UInt8,  # 0=market, 1=limit
    "px_int": pl.Int64,  # Fixed-point encoded (price / profile.price)
    "order_qty_int": pl.Int64,  # Lot-based encoding (qty / 100 shares)
    "channel_no": pl.Int32,  # Exchange channel ID
    "trading_phase": pl.UInt8,  # 0=unknown, 1=open call, 2=continuous, 3=close call
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}

# Side codes
SIDE_BUY = 0
SIDE_SELL = 1

# Order type codes
ORD_TYPE_MARKET = 0
ORD_TYPE_LIMIT = 1


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical l3_orders schema and select only schema columns."""
    if "trading_phase" not in df.columns:
        df = df.with_columns(derive_cn_trading_phase_expr())

    missing_required = [col for col in L3_ORDERS_SCHEMA if col not in df.columns]
    if missing_required:
        raise ValueError(f"l3_orders missing required columns: {missing_required}")

    casts = [pl.col(col).cast(dtype) for col, dtype in L3_ORDERS_SCHEMA.items()]
    return df.with_columns(casts).select(list(L3_ORDERS_SCHEMA.keys()))


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to CN L3 order data.

    Validates:
    - Non-negative px_int and order_qty_int
    - Valid timestamp ranges
    - Valid side codes (0-1)
    - Valid order type codes (0-1)
    - Non-null required fields
    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    required = [
        "px_int",
        "order_qty_int",
        "ts_local_us",
        "appl_seq_num",
        "side",
        "ord_type",
        "exchange",
        "symbol",
        "trading_phase",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_l3_orders: missing required columns: {missing}")

    # Reject non-Chinese exchanges (l3_orders is SZSE/SSE only)
    bad_exchanges = set(df["exchange"].unique().to_list()) - ALLOWED_EXCHANGES
    if bad_exchanges:
        raise ValueError(
            f"validate_l3_orders: exchange(s) {sorted(bad_exchanges)} not allowed. "
            f"l3_orders is restricted to {sorted(ALLOWED_EXCHANGES)}"
        )

    combined_filter = (
        (pl.col("px_int") >= 0)  # Market orders can have price=0
        & (pl.col("order_qty_int") > 0)
        & timestamp_validation_expr("ts_local_us")
        & (pl.col("appl_seq_num") > 0)
        & (pl.col("side").is_in([SIDE_BUY, SIDE_SELL]))
        & (pl.col("ord_type").is_in([ORD_TYPE_MARKET, ORD_TYPE_LIMIT]))
        & pl.col("trading_phase").is_in(
            [
                TRADING_PHASE_UNKNOWN,
                TRADING_PHASE_OPENING_CALL,
                TRADING_PHASE_CONTINUOUS,
                TRADING_PHASE_CLOSING_CALL,
            ]
        )
        & required_columns_validation_expr(["exchange", "symbol"])
    )

    rules = [
        ("px_int", (pl.col("px_int").is_null()) | (pl.col("px_int") < 0)),
        ("order_qty_int", (pl.col("order_qty_int").is_null()) | (pl.col("order_qty_int") <= 0)),
        (
            "ts_local_us",
            pl.col("ts_local_us").is_null()
            | (pl.col("ts_local_us") <= 0)
            | (pl.col("ts_local_us") >= 2**63),
        ),
        ("appl_seq_num", (pl.col("appl_seq_num").is_null()) | (pl.col("appl_seq_num") <= 0)),
        ("side", ~pl.col("side").is_in([SIDE_BUY, SIDE_SELL]) | pl.col("side").is_null()),
        (
            "ord_type",
            ~pl.col("ord_type").is_in([ORD_TYPE_MARKET, ORD_TYPE_LIMIT])
            | pl.col("ord_type").is_null(),
        ),
        ("exchange", pl.col("exchange").is_null()),
        ("symbol", pl.col("symbol").is_null()),
        (
            "trading_phase",
            ~pl.col("trading_phase").is_in(
                [
                    TRADING_PHASE_UNKNOWN,
                    TRADING_PHASE_OPENING_CALL,
                    TRADING_PHASE_CONTINUOUS,
                    TRADING_PHASE_CLOSING_CALL,
                ]
            )
            | pl.col("trading_phase").is_null(),
        ),
    ]

    valid = generic_validate(df, combined_filter, rules, "l3_orders")
    return valid.select(df.columns)


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Apply canonical enum semantics for L3 order vendor-neutral frames."""
    result = df
    if "side" not in result.columns and "side_raw" in result.columns:
        side_raw = pl.col("side_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        result = result.with_columns(
            pl.when(side_raw.is_in(["buy", "b", "1", "0"]))
            .then(pl.lit(SIDE_BUY, dtype=pl.UInt8))
            .when(side_raw.is_in(["sell", "s", "2"]))
            .then(pl.lit(SIDE_SELL, dtype=pl.UInt8))
            .otherwise(pl.lit(255, dtype=pl.UInt8))
            .alias("side")
        )
    if "ord_type" not in result.columns and "ord_type_raw" in result.columns:
        ord_type_raw = pl.col("ord_type_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        result = result.with_columns(
            pl.when(ord_type_raw.is_in(["market", "m", "1", "0"]))
            .then(pl.lit(ORD_TYPE_MARKET, dtype=pl.UInt8))
            .when(ord_type_raw.is_in(["limit", "l", "2"]))
            .then(pl.lit(ORD_TYPE_LIMIT, dtype=pl.UInt8))
            .otherwise(pl.lit(255, dtype=pl.UInt8))
            .alias("ord_type")
        )
    return result


def _encode_storage(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Encode price and quantity as fixed-point integers using asset-class scalar profile.

    For cn-equity: price scalar=1e-4 (sub-fen), amount scalar=1.0 (1 share).
    order_qty is in shares, so qty_int = shares directly.

    Returns DataFrame with px_int, order_qty_int columns added.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["price_px", "order_qty"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)
    result = working.with_columns(
        [
            (pl.col("price_px") / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64).alias("px_int"),
            (pl.col("order_qty") / pl.col(PROFILE_AMOUNT_COL))
            .round()
            .cast(pl.Int64)
            .alias("order_qty_int"),
        ]
    )
    return result.drop(
        ["price_px", "order_qty"] + [c for c in PROFILE_SCALAR_COLS if c in result.columns]
    )


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float price and int quantity.

    Returns DataFrame with price_px (Float64) and order_qty (Int64) columns added.
    By default, drops the *_int columns.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["px_int", "order_qty_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("price_px"),
            (pl.col("order_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Int64)
            .alias("order_qty"),
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
    """Decode fixed-point integers lazily into float price and int quantity."""
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars_lazy,
    )

    schema = lf.collect_schema()
    required_cols = ["px_int", "order_qty_int"]
    missing = [c for c in required_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("price_px"),
            (pl.col("order_qty_int") * pl.col(PROFILE_AMOUNT_COL))
            .cast(pl.Int64)
            .alias("order_qty"),
        ]
    )
    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for l3_orders."""
    return ("exchange", "px_int", "order_qty_int")


@dataclass(frozen=True)
class L3OrdersDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="l3_orders",
        table_kind="event",
        schema=L3_ORDERS_SCHEMA,
        partition_by=("exchange", "date"),
        has_date=True,
        layer="silver",
        allowed_exchanges=ALLOWED_EXCHANGES,
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


L3_ORDERS_DOMAIN = L3OrdersDomain()


register_domain(L3_ORDERS_DOMAIN)
