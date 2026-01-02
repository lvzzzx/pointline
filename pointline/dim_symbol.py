"""SCD Type 2 utilities for dim_symbol in Polars.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl

    from pointline.dim_symbol import scd2_bootstrap, scd2_upsert

    initial = pl.DataFrame(
        {
            "exchange_id": [1],
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
from typing import Iterable, Sequence

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
)

SCHEMA: dict[str, pl.DataType] = {
    "symbol_id": pl.Int64,
    "exchange_id": pl.Int16,  # Delta Lake doesn't support UInt16, stores as Int16
    "exchange_symbol": pl.Utf8,
    "base_asset": pl.Utf8,
    "quote_asset": pl.Utf8,
    "asset_type": pl.UInt8,
    "tick_size": pl.Float64,
    "lot_size": pl.Float64,
    "price_increment": pl.Float64,
    "amount_increment": pl.Float64,
    "contract_size": pl.Float64,
    "valid_from_ts": pl.Int64,
    "valid_until_ts": pl.Int64,
    "is_current": pl.Boolean,
}


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
                int.from_bytes(hashlib.blake2b(x.encode("utf-8"), digest_size=4).digest(), "little")
                for x in s
            ],
            dtype=pl.Int64,
        )

    return df.with_columns(payloads.map_batches(_hash_payloads).alias("symbol_id"))


def normalize_dim_symbol_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to the canonical dim_symbol schema where possible."""
    missing = [col for col in SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"dim_symbol missing required columns: {missing}")

    return df.with_columns([pl.col(col).cast(dtype) for col, dtype in SCHEMA.items()])


def _as_boolean_change_mask(joined: pl.DataFrame) -> pl.Expr:
    """Return an expression for whether a row differs from its current version."""
    diffs: list[pl.Expr] = []
    for col in TRACKED_COLS:
        diffs.append(pl.col(col) != pl.col(f"{col}_cur"))
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

    required_cols = set(NATURAL_KEY_COLS + TRACKED_COLS + ("valid_from_ts",))
    missing = [col for col in required_cols if col not in updates.columns]
    if missing:
        raise ValueError(f"updates missing required columns: {missing}")

    current = dim_symbol.filter(pl.col("is_current") == True)  # noqa: E712
    current_for_join = current.rename({"symbol_id": "symbol_id_cur"})

    joined = updates.join(
        current_for_join, on=list(NATURAL_KEY_COLS), how="left", suffix="_cur"
    )

    is_new = pl.col("symbol_id_cur").is_null()
    is_changed = is_new | _as_boolean_change_mask(joined)

    changed_updates = joined.filter(is_changed).select(updates.columns)

    if changed_updates.is_empty():
        return dim_symbol

    changed_keys = changed_updates.select(list(NATURAL_KEY_COLS)).unique()

    closed_rows = (
        current.join(
            changed_updates, on=list(NATURAL_KEY_COLS), how="inner", suffix="_upd"
        )
        .with_columns(
            pl.col("valid_from_ts_upd").cast(pl.Int64).alias("_close_at_ts"),
        )
        .select(dim_symbol.columns + ["_close_at_ts"])
        .with_columns(
            pl.col("_close_at_ts").alias("valid_until_ts"),
            pl.lit(False).alias("is_current"),
        )
        .drop("_close_at_ts")
    )

    new_rows = (
        changed_updates.with_columns(
            pl.lit(DEFAULT_VALID_UNTIL_TS_US).cast(pl.Int64).alias("valid_until_ts"),
            pl.lit(True).alias("is_current"),
        )
    )
    new_rows = assign_symbol_id_hash(new_rows)
    new_rows = normalize_dim_symbol_schema(new_rows)

    # Remove only the current rows that are being replaced; keep all history.
    history = dim_symbol.filter(pl.col("is_current") == False)  # noqa: E712
    current_kept = current.join(changed_keys, on=list(NATURAL_KEY_COLS), how="anti")
    dim_symbol_kept = pl.concat([history, current_kept], how="vertical")

    closed_rows = normalize_dim_symbol_schema(closed_rows)
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

    required = set(NATURAL_KEY_COLS + TRACKED_COLS + ("valid_from_ts",))
    missing = [c for c in required if c not in history.columns]
    if missing:
        raise ValueError(f"History missing required columns: {missing}")

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
    if (prev_ends != next_starts).any():
        return False

    return True


def resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for a data DataFrame using as-of join.

    The join is performed on natural keys and the provided timestamp column.
    It expects dim_symbol to be in the canonical schema.
    """
    # Ensure join columns match dim_symbol schema
    # Schema is single source of truth - no dynamic schema reading
    for col in NATURAL_KEY_COLS:
        if col in data.columns:
            data = data.with_columns(pl.col(col).cast(SCHEMA[col]))

    return data.sort(ts_col).join_asof(
        dim_symbol.sort("valid_from_ts"),
        left_on=ts_col,
        right_on="valid_from_ts",
        by=list(NATURAL_KEY_COLS),
        strategy="backward",
    )


def required_update_columns() -> Sequence[str]:
    """Columns required for an updates DataFrame."""
    return (*NATURAL_KEY_COLS, *TRACKED_COLS, "valid_from_ts")


def required_dim_symbol_columns() -> Sequence[str]:
    """Columns required for a dim_symbol DataFrame."""
    return tuple(SCHEMA.keys())
