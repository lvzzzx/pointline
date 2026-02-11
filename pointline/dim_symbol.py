"""SCD Type 2 utilities for dim_symbol in Polars.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl

    from pointline.dim_symbol import scd2_bootstrap, scd2_upsert

    initial = pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange": ["binance"],  # Added for consistency
            "exchange_symbol": ["BTC-PERPETUAL"],
            "base_asset": ["BTC"],
            "quote_asset": ["USD"],
            "asset_type": [1],
            "tick_size": [0.5],
            "lot_size": [1.0],
            "price_increment": [0.5],
            "amount_increment": [0.1],
            "contract_size": [1.0],
            "valid_from_ts": [100],
        }
    )

    dim = scd2_bootstrap(initial)

    update = initial.with_columns(
        pl.lit(200).alias("valid_from_ts"),
        pl.lit(1.0).alias("tick_size"),
        pl.lit(1.0).alias("price_increment"),
    )

    dim = scd2_upsert(dim, update)
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import polars as pl

DEFAULT_VALID_UNTIL_TS_US = 2**63 - 1

NATURAL_KEY_COLS: tuple[str, ...] = ("exchange_id", "exchange_symbol")
TRACKED_COLS: tuple[str, ...] = (
    "base_asset",
    "quote_asset",
    "asset_type",
    "tick_size",
    "lot_size",
    "price_increment",
    "amount_increment",
    "contract_size",
    # Options fields — nullable for asset classes that don't use them
    "expiry_ts_us",
    "underlying_symbol_id",
    "strike",
    "put_call",
)

# Tracked columns that may be omitted from updates (filled with null if missing).
OPTIONAL_TRACKED_COLS: frozenset[str] = frozenset(
    {
        "expiry_ts_us",
        "underlying_symbol_id",
        "strike",
        "put_call",
    }
)

SCHEMA: dict[str, pl.DataType] = {
    "symbol_id": pl.Int64,
    "exchange_id": pl.Int16,  # Delta Lake doesn't support UInt16, stores as Int16
    "exchange": pl.Utf8,  # Normalized exchange name (e.g., "binance-futures") for consistency with silver tables
    "exchange_symbol": pl.Utf8,
    "base_asset": pl.Utf8,
    "quote_asset": pl.Utf8,
    "asset_type": pl.UInt8,
    "tick_size": pl.Float64,
    "lot_size": pl.Float64,
    "price_increment": pl.Float64,
    "amount_increment": pl.Float64,
    "contract_size": pl.Float64,
    # Options fields — nullable for asset classes that don't use them
    "expiry_ts_us": pl.Int64,  # Contract expiry (nullable)
    "underlying_symbol_id": pl.Int64,  # Underlying instrument symbol_id (nullable)
    "strike": pl.Float64,  # Options strike price (nullable)
    "put_call": pl.Utf8,  # "put" / "call" (nullable)
    # SCD Type 2 metadata
    "valid_from_ts": pl.Int64,
    "valid_until_ts": pl.Int64,
    "is_current": pl.Boolean,
}


def read_dim_symbol_table(
    *, columns: Sequence[str] | None = None, unique_by: Sequence[str] | None = None
) -> pl.DataFrame:
    """Read dim_symbol from Delta and optionally select columns/unique rows."""
    from pointline.config import get_table_path

    lf = pl.scan_delta(str(get_table_path("dim_symbol")))
    if columns:
        lf = lf.select(list(columns))
    df = lf.collect()
    if unique_by:
        df = df.unique(subset=list(unique_by))
    return df


def assign_symbol_id_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Assign deterministic symbol_id based on natural key + valid_from_ts.

    If symbol_id already exists, it is preserved.
    """
    if "symbol_id" in df.columns:
        return df

    # Create payload string column in Polars (fast)
    payloads = pl.format(
        "{}|{}|{}",
        pl.col("exchange_id"),
        pl.col("exchange_symbol"),
        pl.col("valid_from_ts"),
    )

    # Hash the payloads (Python loop over strings in batches is faster than map_elements on structs)
    def _hash_payloads(s: pl.Series) -> pl.Series:
        return pl.Series(
            [
                int.from_bytes(
                    hashlib.blake2b(x.encode("utf-8"), digest_size=8).digest(),
                    "little",
                    signed=True,
                )
                for x in s
            ],
            dtype=pl.Int64,
        )

    return df.with_columns(payloads.map_batches(_hash_payloads).alias("symbol_id"))


def normalize_dim_symbol_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical dim_symbol schema where possible.

    If exchange column is missing, it will be derived from exchange_id
    using dim_exchange.
    """
    from pointline.config import _ensure_dim_exchange

    # Derive exchange from exchange_id if missing
    if "exchange" not in df.columns and "exchange_id" in df.columns:
        # Create reverse mapping: exchange_id -> exchange name
        dim_ex = _ensure_dim_exchange()
        id_to_exchange = {row["exchange_id"]: name for name, row in dim_ex.items()}
        exchange_map = pl.DataFrame(
            {
                "exchange_id": list(id_to_exchange.keys()),
                "exchange": list(id_to_exchange.values()),
            }
        )
        df = df.join(exchange_map, on="exchange_id", how="left")
        # If some exchange_ids don't have a mapping, fill with None
        # (shouldn't happen in practice)
        df = df.with_columns(pl.col("exchange").fill_null("unknown"))

    # Columns that are nullable (options fields).
    # These are filled with null if absent from the input DataFrame.
    nullable_cols = {
        "expiry_ts_us",
        "underlying_symbol_id",
        "strike",
        "put_call",
    }

    missing = [col for col in SCHEMA if col not in df.columns and col not in nullable_cols]
    if missing:
        raise ValueError(f"dim_symbol missing required columns: {missing}")

    # Fill missing nullable columns with null
    for col in nullable_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=SCHEMA[col]).alias(col))

    return df.with_columns([pl.col(col).cast(dtype) for col, dtype in SCHEMA.items()])


def _as_boolean_change_mask(joined: pl.DataFrame) -> pl.Expr:
    """Return an expression for whether a row differs from its current version."""
    diffs: list[pl.Expr] = []
    for col in TRACKED_COLS:
        col_new = pl.col(col)
        col_cur = pl.col(f"{col}_cur")
        diffs.append(
            (col_new.is_null() & col_cur.is_not_null())
            | (col_new.is_not_null() & col_cur.is_null())
            | (col_new.is_not_null() & col_cur.is_not_null() & (col_new != col_cur))
        )
    return pl.any_horizontal(diffs)


def scd2_upsert(
    dim_symbol: pl.DataFrame,
    updates: pl.DataFrame,
    *,
    valid_from_col: str = "valid_from_ts",
) -> pl.DataFrame:
    """Apply SCD2 updates to dim_symbol.

    Requirements:
    - dim_symbol must already follow the canonical schema.
    - updates must include the natural key columns plus tracked columns.
    - updates must include valid_from_ts (or set valid_from_col to another column name).
    """
    if dim_symbol.is_empty():
        updates_prepped = updates.rename({valid_from_col: "valid_from_ts"})
        updates_prepped = updates_prepped.with_columns(
            pl.lit(DEFAULT_VALID_UNTIL_TS_US).cast(pl.Int64).alias("valid_until_ts"),
            pl.lit(True).alias("is_current"),
        )
        updates_prepped = assign_symbol_id_hash(updates_prepped)
        return normalize_dim_symbol_schema(updates_prepped)

    dim_symbol = normalize_dim_symbol_schema(dim_symbol)

    if valid_from_col != "valid_from_ts":
        updates = updates.rename({valid_from_col: "valid_from_ts"})

    # exchange column is optional in updates - will be derived from
    # exchange_id if missing
    required_cols = (
        set(NATURAL_KEY_COLS + TRACKED_COLS + ("valid_from_ts",)) - OPTIONAL_TRACKED_COLS
    )
    missing = [col for col in required_cols if col not in updates.columns]
    if missing:
        raise ValueError(f"updates missing required columns: {missing}")

    # Fill missing optional tracked columns with null
    for col in OPTIONAL_TRACKED_COLS:
        if col not in updates.columns:
            updates = updates.with_columns(pl.lit(None, dtype=SCHEMA[col]).alias(col))

    # Derive exchange from exchange_id if missing in updates
    if "exchange" not in updates.columns and "exchange_id" in updates.columns:
        from pointline.config import _ensure_dim_exchange

        dim_ex = _ensure_dim_exchange()
        id_to_exchange = {row["exchange_id"]: name for name, row in dim_ex.items()}
        exchange_map = pl.DataFrame(
            {
                "exchange_id": list(id_to_exchange.keys()),
                "exchange": list(id_to_exchange.values()),
            }
        )
        updates = updates.join(exchange_map, on="exchange_id", how="left")
        updates = updates.with_columns(pl.col("exchange").fill_null("unknown"))

    current = dim_symbol.filter(pl.col("is_current") == True)  # noqa: E712
    current_for_join = current.rename({"symbol_id": "symbol_id_cur"})

    joined = updates.join(current_for_join, on=list(NATURAL_KEY_COLS), how="left", suffix="_cur")

    is_new = pl.col("symbol_id_cur").is_null()
    is_changed = is_new | _as_boolean_change_mask(joined)

    # Incremental upsert is append-forward only. Backdated or same-timestamp
    # changes against an existing current row can invert/zero validity windows.
    invalid_ordering = joined.filter(
        pl.col("symbol_id_cur").is_not_null()
        & is_changed
        & (pl.col("valid_from_ts") <= pl.col("valid_from_ts_cur"))
    )
    if not invalid_ordering.is_empty():
        raise ValueError(
            "scd2_upsert: updates must have valid_from_ts greater than the current version. "
            "Use rebuild_from_history for backfills/corrections."
        )

    changed_updates = joined.filter(is_changed).select(updates.columns)

    if changed_updates.is_empty():
        return dim_symbol

    changed_keys = changed_updates.select(list(NATURAL_KEY_COLS)).unique()

    closed_rows = (
        current.join(changed_updates, on=list(NATURAL_KEY_COLS), how="inner", suffix="_upd")
        .with_columns(
            pl.col("valid_from_ts_upd").cast(pl.Int64).alias("_close_at_ts"),
        )
        .select(current.columns + ["_close_at_ts"])
        .with_columns(
            pl.col("_close_at_ts").alias("valid_until_ts"),
            pl.lit(False).alias("is_current"),
        )
        .drop("_close_at_ts")
    )

    new_rows = changed_updates.with_columns(
        pl.lit(DEFAULT_VALID_UNTIL_TS_US).cast(pl.Int64).alias("valid_until_ts"),
        pl.lit(True).alias("is_current"),
    )
    new_rows = assign_symbol_id_hash(new_rows)
    new_rows = normalize_dim_symbol_schema(new_rows)

    # Remove only the current rows that are being replaced; keep all history.
    history = dim_symbol.filter(pl.col("is_current") == False)  # noqa: E712
    current_kept = current.join(changed_keys, on=list(NATURAL_KEY_COLS), how="anti")
    dim_symbol_kept = pl.concat([history, current_kept], how="vertical")
    dim_symbol_kept = normalize_dim_symbol_schema(dim_symbol_kept)

    closed_rows = normalize_dim_symbol_schema(closed_rows)
    new_rows = normalize_dim_symbol_schema(new_rows)

    # Ensure all DataFrames have columns in the same order (SCHEMA order)
    schema_cols = list(SCHEMA.keys())
    dim_symbol_kept = dim_symbol_kept.select(schema_cols)
    closed_rows = closed_rows.select(schema_cols)
    new_rows = new_rows.select(schema_cols)

    result = pl.concat([dim_symbol_kept, closed_rows, new_rows], how="vertical")
    return normalize_dim_symbol_schema(result)


def scd2_bootstrap(
    updates: pl.DataFrame,
    *,
    valid_from_col: str = "valid_from_ts",
) -> pl.DataFrame:
    """Create a dim_symbol table from an initial full snapshot of metadata."""
    if valid_from_col != "valid_from_ts":
        updates = updates.rename({valid_from_col: "valid_from_ts"})

    bootstrap = updates.with_columns(
        pl.lit(DEFAULT_VALID_UNTIL_TS_US).cast(pl.Int64).alias("valid_until_ts"),
        pl.lit(True).alias("is_current"),
    )
    bootstrap = assign_symbol_id_hash(bootstrap)
    return normalize_dim_symbol_schema(bootstrap)


def rebuild_from_history(
    history: pl.DataFrame,
    *,
    valid_from_col: str = "valid_from_ts",
) -> pl.DataFrame:
    """Rebuild a full SCD2 history from a set of historical states.

    Expects 'history' to contain multiple rows per symbol, each representing
    a state that became valid at 'valid_from_ts'.

    This function:
    1. Sorts by natural key and valid_from_ts.
    2. Infers 'valid_until_ts' from the next row's 'valid_from_ts'.
    3. Sets 'is_current' to True only for the last row of each symbol.
    4. Generates symbol_ids.
    """
    if valid_from_col != "valid_from_ts":
        history = history.rename({valid_from_col: "valid_from_ts"})

    required = set(NATURAL_KEY_COLS + TRACKED_COLS + ("valid_from_ts",)) - OPTIONAL_TRACKED_COLS
    missing = [c for c in required if c not in history.columns]
    if missing:
        raise ValueError(f"History missing required columns: {missing}")

    # Fill missing optional tracked columns with null
    for col in OPTIONAL_TRACKED_COLS:
        if col not in history.columns:
            history = history.with_columns(pl.lit(None, dtype=SCHEMA[col]).alias(col))

    # Sort to ensure correct windowing
    df = history.sort(list(NATURAL_KEY_COLS) + ["valid_from_ts"])

    # Dedup to prevent windowing errors on bad input
    if df.select(list(NATURAL_KEY_COLS) + ["valid_from_ts"]).is_duplicated().any():
        raise ValueError("Duplicate valid_from_ts detected for a symbol in history input.")

    # Calculate valid_until_ts using lead(valid_from_ts) over the natural key partition
    # We partition by natural key to avoid bleeding dates across symbols
    df = df.with_columns(
        pl.col("valid_from_ts")
        .shift(-1)
        .over(list(NATURAL_KEY_COLS))
        .fill_null(DEFAULT_VALID_UNTIL_TS_US)
        .cast(pl.Int64)
        .alias("valid_until_ts")
    )

    # Determine is_current: it's true if valid_until_ts is the default max
    df = df.with_columns(
        (pl.col("valid_until_ts") == DEFAULT_VALID_UNTIL_TS_US).alias("is_current")
    )

    df = assign_symbol_id_hash(df)
    return normalize_dim_symbol_schema(df)


def check_coverage(
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    start_ts: int,
    end_ts: int,
) -> bool:
    """Verify that dim_symbol provides contiguous coverage for a time range.

    Returns True if:
    1. The symbol exists in the table.
    2. The union of valid intervals fully covers [start_ts, end_ts).
    3. There are no gaps between intervals within the range.
    """
    # Filter for the specific symbol and relevant time range
    # We include rows that overlap with [start_ts, end_ts)
    # Overlap logic: row_start < range_end AND row_end > range_start
    rows = dim_symbol.filter(
        (pl.col("exchange_id") == exchange_id)
        & (pl.col("exchange_symbol") == exchange_symbol)
        & (pl.col("valid_from_ts") < end_ts)
        & (pl.col("valid_until_ts") > start_ts)
    ).sort("valid_from_ts")

    if rows.is_empty():
        return False

    # Check boundaries
    # The first row must start at or before start_ts
    first_start = rows["valid_from_ts"][0]
    if first_start > start_ts:
        return False

    # The last row must end at or after end_ts
    last_end = rows["valid_until_ts"][-1]
    if last_end < end_ts:
        return False

    # Check contiguity
    # valid_until_ts of row[i] must equal valid_from_ts of row[i+1]
    # We can check this by comparing shifted columns
    # Note: This assumes strict equality (no gaps, no overlaps).
    # If overlaps are allowed but cover the gap, a more complex merge is needed.
    # For SCD2, we usually enforce strict contiguity (end == next_start).

    # We only care about gaps *within* the requested window [start_ts, end_ts].
    # But checking the whole sorted set of overlapping rows is safer/simpler.

    prev_ends = rows["valid_until_ts"][:-1]
    next_starts = rows["valid_from_ts"][1:]

    # Check if any gap exists
    return not (prev_ends != next_starts).any()


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    ts_col: str = "ts_local_us",
    tie_breaker_cols: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Resolve symbol_ids for a data DataFrame using as-of join.

    The join is performed on natural keys and the provided timestamp column.
    It expects dim_symbol to be in the canonical schema.
    """
    # Ensure join columns match dim_symbol schema
    # Schema is single source of truth - cast both data and dim_symbol to SCHEMA
    for col in NATURAL_KEY_COLS:
        if col in data.columns:
            data = data.with_columns(pl.col(col).cast(SCHEMA[col]))
        if col in dim_symbol.columns:
            dim_symbol = dim_symbol.with_columns(pl.col(col).cast(SCHEMA[col]))

    sort_cols = [ts_col]
    if tie_breaker_cols:
        for col in tie_breaker_cols:
            if col in data.columns and col not in sort_cols:
                sort_cols.append(col)

    sort_cols = [*NATURAL_KEY_COLS, *sort_cols]
    join_kwargs: dict[str, object] = {}
    if "check_sortedness" in pl.DataFrame.join_asof.__code__.co_varnames:
        join_kwargs["check_sortedness"] = False

    resolved = data.sort(sort_cols).join_asof(
        dim_symbol.sort([*NATURAL_KEY_COLS, "valid_from_ts"]),
        left_on=ts_col,
        right_on="valid_from_ts",
        by=list(NATURAL_KEY_COLS),
        strategy="backward",
        **join_kwargs,
    )

    # Guard against matching rows that are outside the SCD2 validity window.
    # join_asof only enforces valid_from_ts <= ts, so we additionally require
    # ts < valid_until_ts for matched rows.
    if "valid_until_ts" in resolved.columns:
        stale_match = pl.col("symbol_id").is_not_null() & (
            pl.col(ts_col) >= pl.col("valid_until_ts")
        )
        right_side_cols = [col for col in resolved.columns if col not in data.columns]
        if "symbol_id" in resolved.columns and "symbol_id" not in right_side_cols:
            right_side_cols.append("symbol_id")
        if right_side_cols:
            resolved = resolved.with_columns(
                [
                    pl.when(stale_match)
                    .then(pl.lit(None, dtype=resolved.schema[col]))
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col in right_side_cols
                ]
            )

    return resolved


def required_update_columns() -> Sequence[str]:
    """Columns required for an updates DataFrame (excludes optional multi-asset cols)."""
    return tuple(
        c
        for c in (*NATURAL_KEY_COLS, *TRACKED_COLS, "valid_from_ts")
        if c not in OPTIONAL_TRACKED_COLS
    )


def required_dim_symbol_columns() -> Sequence[str]:
    """Columns required for a dim_symbol DataFrame."""
    return tuple(SCHEMA.keys())


# ---------------------------------------------------------------------------
# Schema registry registration
# ---------------------------------------------------------------------------
from pointline.schema_registry import register_schema as _register_schema  # noqa: E402

_register_schema("dim_symbol", SCHEMA)


def resolve_exchange_ids(symbol_ids: list[int]) -> list[int]:
    """Resolve exchange_ids from symbol_ids via dim_symbol.

    Args:
        symbol_ids: List of symbol_ids to resolve.

    Returns:
        List of exchange_ids in the same order as symbol_ids.

    Raises:
        ValueError: If no matching symbol_ids found or some are missing.
    """
    dim = read_dim_symbol_table(columns=["symbol_id", "exchange_id"]).unique()
    lookup = dim.filter(pl.col("symbol_id").is_in(symbol_ids))

    if lookup.is_empty():
        raise ValueError("No matching symbol_ids found in dim_symbol.")

    exchange_ids: list[int] = []
    missing: list[int] = []

    for symbol in symbol_ids:
        rows = lookup.filter(pl.col("symbol_id") == symbol)
        if rows.is_empty():
            missing.append(symbol)
            continue
        exchange_ids.append(int(rows["exchange_id"][0]))

    if missing:
        raise ValueError(f"Missing exchange_id for symbol_id(s): {missing}")

    return exchange_ids
