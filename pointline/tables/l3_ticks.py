"""CN Level 3 tick executions and cancellations domain logic for parsing, validation, and transformation.

This module handles SZSE/SSE tick stream data, which represents trade executions and
order cancellations. Each tick links back to the original orders via bid_appl_seq_num
and offer_appl_seq_num, enabling full order book reconstruction.

Restricted to Chinese stock exchanges (SZSE, SSE) only. These tables are structurally
coupled to Chinese market microstructure and cannot be used for other exchanges.

Tick Types:
- Executions (exec_type=0): Price > 0, both bid_appl_seq_num and offer_appl_seq_num set
- Cancellations (exec_type=1): Price = 0, one of bid_appl_seq_num or offer_appl_seq_num set

Deterministic Ordering
----------------------
The replay key for L3 ticks is ``(channel_no, appl_seq_num)``, NOT ``ts_local_us``.

``appl_seq_num`` properties:
- Scope: per-channel (SZSE stocks, convertible bonds, and funds each use separate
  channels; SSE convertible bonds use yet another channel)
- Starts at 1 each trading day
- Unique and contiguous within a channel for a given trading day — gaps indicate
  message loss
- NOT unique across channels — two channels can both have appl_seq_num=1

Orders and ticks share the same ``(channel_no, appl_seq_num)`` sequence space within
a channel. A tick's ``bid_appl_seq_num`` / ``offer_appl_seq_num`` reference the
``appl_seq_num`` of the originating orders in the l3_orders table.

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

# Schema definition for l3_ticks Silver table
#
# Delta Lake Integer Type Limitations:
# - Delta Lake (via Parquet) does not support unsigned integer types UInt16 and UInt32
# - Use Int32 for file_id, file_line_number, channel_no
# - Use Int64 for symbol_id, appl_seq_num, bid_appl_seq_num, offer_appl_seq_num, px_int, qty_int
# - UInt8 is supported (used for exec_type)
L3_TICKS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "ts_local_us": pl.Int64,  # Arrival time in UTC (converted from Asia/Shanghai TransactTime)
    "appl_seq_num": pl.Int64,  # Tick ID (unique per channel per day, starts at 1)
    "bid_appl_seq_num": pl.Int64,  # Buy order ID (0 if N/A)
    "offer_appl_seq_num": pl.Int64,  # Sell order ID (0 if N/A)
    "exec_type": pl.UInt8,  # 0=fill, 1=cancel
    "px_int": pl.Int64,  # Fixed-point encoded (price / profile.price), 0 for cancellations
    "qty_int": pl.Int64,  # Lot-based encoding (qty / 100 shares)
    "channel_no": pl.Int32,  # Exchange channel ID
    "trading_phase": pl.UInt8,  # 0=unknown, 1=open call, 2=continuous, 3=close call
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}

# Execution type codes
EXEC_TYPE_FILL = 0
EXEC_TYPE_CANCEL = 1


def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical l3_ticks schema and select only schema columns."""
    if "trading_phase" not in df.columns:
        df = df.with_columns(derive_cn_trading_phase_expr())

    missing_required = [col for col in L3_TICKS_SCHEMA if col not in df.columns]
    if missing_required:
        raise ValueError(f"l3_ticks missing required columns: {missing_required}")

    casts = [pl.col(col).cast(dtype) for col, dtype in L3_TICKS_SCHEMA.items()]
    return df.with_columns(casts).select(list(L3_TICKS_SCHEMA.keys()))


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to CN L3 tick data.

    Validates:
    - Non-negative px_int and qty_int
    - Valid timestamp ranges
    - Valid exec_type codes (0-1)
    - Non-null required fields
    - Cancellations: px_int = 0, exactly one of {bid_appl_seq_num, offer_appl_seq_num} > 0
    - Executions: px_int > 0, both bid_appl_seq_num and offer_appl_seq_num > 0

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    required = [
        "px_int",
        "qty_int",
        "ts_local_us",
        "appl_seq_num",
        "bid_appl_seq_num",
        "offer_appl_seq_num",
        "exec_type",
        "exchange",
        "symbol",
        "trading_phase",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"validate_l3_ticks: missing required columns: {missing}")

    # Reject non-Chinese exchanges (l3_ticks is SZSE/SSE only)
    bad_exchanges = set(df["exchange"].unique().to_list()) - ALLOWED_EXCHANGES
    if bad_exchanges:
        raise ValueError(
            f"validate_l3_ticks: exchange(s) {sorted(bad_exchanges)} not allowed. "
            f"l3_ticks is restricted to {sorted(ALLOWED_EXCHANGES)}"
        )

    # Helper expressions for tick semantics
    is_fill = pl.col("exec_type") == EXEC_TYPE_FILL
    is_cancel = pl.col("exec_type") == EXEC_TYPE_CANCEL
    has_bid = pl.col("bid_appl_seq_num") > 0
    has_offer = pl.col("offer_appl_seq_num") > 0

    # Semantic validation:
    # - Fills: price_px > 0, both bid and offer set
    # - Cancels: price_px = 0, exactly one of bid/offer set
    valid_fill = is_fill & (pl.col("px_int") > 0) & has_bid & has_offer
    valid_cancel = is_cancel & (pl.col("px_int") == 0) & (has_bid ^ has_offer)  # XOR
    valid_tick_semantics = valid_fill | valid_cancel

    combined_filter = (
        (pl.col("px_int") >= 0)
        & (pl.col("qty_int") > 0)
        & timestamp_validation_expr("ts_local_us")
        & (pl.col("appl_seq_num") > 0)
        & (pl.col("exec_type").is_in([EXEC_TYPE_FILL, EXEC_TYPE_CANCEL]))
        & pl.col("trading_phase").is_in(
            [
                TRADING_PHASE_UNKNOWN,
                TRADING_PHASE_OPENING_CALL,
                TRADING_PHASE_CONTINUOUS,
                TRADING_PHASE_CLOSING_CALL,
            ]
        )
        & required_columns_validation_expr(["exchange", "symbol"])
        & valid_tick_semantics
    )

    rules = [
        ("px_int", (pl.col("px_int").is_null()) | (pl.col("px_int") < 0)),
        ("qty_int", (pl.col("qty_int").is_null()) | (pl.col("qty_int") <= 0)),
        (
            "ts_local_us",
            pl.col("ts_local_us").is_null()
            | (pl.col("ts_local_us") <= 0)
            | (pl.col("ts_local_us") >= 2**63),
        ),
        ("appl_seq_num", (pl.col("appl_seq_num").is_null()) | (pl.col("appl_seq_num") <= 0)),
        (
            "exec_type",
            ~pl.col("exec_type").is_in([EXEC_TYPE_FILL, EXEC_TYPE_CANCEL])
            | pl.col("exec_type").is_null(),
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
        (
            "tick_semantics",
            ~valid_tick_semantics,
        ),
    ]

    valid = generic_validate(df, combined_filter, rules, "l3_ticks")
    return valid.select(df.columns)


def _canonicalize_vendor_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Apply canonical enum semantics for L3 tick vendor-neutral frames."""
    if "exec_type" in df.columns or "exec_type_raw" not in df.columns:
        return df

    exec_type_raw = pl.col("exec_type_raw").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    return df.with_columns(
        pl.when(exec_type_raw.is_in(["fill", "f", "0"]))
        .then(pl.lit(EXEC_TYPE_FILL, dtype=pl.UInt8))
        .when(exec_type_raw.is_in(["cancel", "c", "1", "4"]))
        .then(pl.lit(EXEC_TYPE_CANCEL, dtype=pl.UInt8))
        .otherwise(pl.lit(255, dtype=pl.UInt8))
        .alias("exec_type")
    )


def _encode_storage(
    df: pl.DataFrame,
) -> pl.DataFrame:
    """Encode price and quantity as fixed-point integers using asset-class scalar profile.

    For cn-equity: price scalar=1e-4 (sub-fen), amount scalar=1.0 (1 share).

    Returns DataFrame with px_int, qty_int columns added.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["price_px", "qty"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"encode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)
    result = working.with_columns(
        [
            (pl.col("price_px") / pl.col(PROFILE_PRICE_COL)).round().cast(pl.Int64).alias("px_int"),
            (pl.col("qty") / pl.col(PROFILE_AMOUNT_COL)).round().cast(pl.Int64).alias("qty_int"),
        ]
    )
    return result.drop(
        ["price_px", "qty"] + [c for c in PROFILE_SCALAR_COLS if c in result.columns]
    )


def _decode_storage(
    df: pl.DataFrame,
    *,
    keep_ints: bool = False,
) -> pl.DataFrame:
    """Decode fixed-point integers into float price and int quantity.

    Returns DataFrame with price_px (Float64) and qty (Int64) columns added.
    By default, drops the *_int columns.
    """
    from pointline.encoding import (
        PROFILE_AMOUNT_COL,
        PROFILE_PRICE_COL,
        PROFILE_SCALAR_COLS,
        with_profile_scalars,
    )

    required_cols = ["px_int", "qty_int"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars(df)

    result = working.with_columns(
        [
            (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("price_px"),
            (pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Int64).alias("qty"),
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
    required_cols = ["px_int", "qty_int"]
    missing = [c for c in required_cols if c not in schema]
    if missing:
        raise ValueError(f"decode_fixed_point: df missing columns: {missing}")

    working = with_profile_scalars_lazy(lf)
    result = working.with_columns(
        [
            (pl.col("px_int") * pl.col(PROFILE_PRICE_COL)).cast(pl.Float64).alias("price_px"),
            (pl.col("qty_int") * pl.col(PROFILE_AMOUNT_COL)).cast(pl.Int64).alias("qty"),
        ]
    )

    if not keep_ints:
        result = result.drop(required_cols)
    return result.drop(list(PROFILE_SCALAR_COLS))


def _required_decode_columns() -> tuple[str, ...]:
    """Columns needed to decode storage fields for l3_ticks."""
    return ("exchange", "px_int", "qty_int")


@dataclass(frozen=True)
class L3TicksDomain(EventTableDomain):
    spec: TableSpec = TableSpec(
        table_name="l3_ticks",
        table_kind="event",
        schema=L3_TICKS_SCHEMA,
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


L3_TICKS_DOMAIN = L3TicksDomain()


register_domain(L3_TICKS_DOMAIN)
