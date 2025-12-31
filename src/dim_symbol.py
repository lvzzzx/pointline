"""SCD Type 2 utilities for dim_symbol in Polars.

This module keeps the implementation storage-agnostic; it operates on Polars DataFrames.

Example:
    import polars as pl

    from src.dim_symbol import scd2_bootstrap, scd2_upsert

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
    "symbol_id": pl.UInt32,
    "exchange_id": pl.UInt16,
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


def _hash_symbol_id(exchange_id: int, exchange_symbol: str, valid_from_ts: int) -> int:
    payload = f"{exchange_id}|{exchange_symbol}|{valid_from_ts}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=4).digest()
    return int.from_bytes(digest, "little", signed=False)


def assign_symbol_id_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Assign deterministic symbol_id based on natural key + valid_from_ts.

    If symbol_id already exists, it is preserved.
    """
    if "symbol_id" in df.columns:
        return df

    return df.with_columns(
        pl.struct(["exchange_id", "exchange_symbol", "valid_from_ts"])  # type: ignore[arg-type]
        .map_elements(
            lambda row: _hash_symbol_id(row["exchange_id"], row["exchange_symbol"], row["valid_from_ts"]),
            return_dtype=pl.UInt32,
        )
        .alias("symbol_id")
    )


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
