"""Pure-function SCD Type 2 logic for v2 dim_symbol.

All functions operate on Polars DataFrames — no storage, no caching, no I/O.
"""

from __future__ import annotations

import hashlib

import polars as pl

from pointline.schemas.dimensions import DIM_SYMBOL

VALID_UNTIL_MAX: int = 2**63 - 1
NATURAL_KEY: tuple[str, str] = ("exchange", "exchange_symbol")
TRACKED_COLS: tuple[str, ...] = (
    "canonical_symbol",
    "market_type",
    "base_asset",
    "quote_asset",
    "tick_size",
    "lot_size",
    "contract_size",
)

_SCHEMA = DIM_SYMBOL.to_polars()
_NULLABLE = set(DIM_SYMBOL.nullable_columns())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def bootstrap(snapshot: pl.DataFrame, effective_ts_us: int) -> pl.DataFrame:
    """Create dim_symbol from an initial full snapshot.

    Every row becomes current with valid_from_ts_us=effective_ts_us.
    """
    return _to_schema(_stamp_current(snapshot, effective_ts_us))


def upsert(
    dim: pl.DataFrame,
    snapshot: pl.DataFrame,
    effective_ts_us: int,
    *,
    delistings: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Apply a full snapshot to dim_symbol (SCD2, per-exchange scoped).

    Args:
        dim: Current dim_symbol table (may be empty).
        snapshot: Current-state snapshot of symbol metadata.
        effective_ts_us: Timestamp for new/changed/closed rows.
        delistings: If None, implicit delisting (missing from snapshot = closed).
                    If DataFrame(exchange, exchange_symbol, delisted_at_ts_us),
                    only those symbols are closed at their individual timestamps.
    """
    if dim.is_empty():
        return bootstrap(snapshot, effective_ts_us)

    nk = list(NATURAL_KEY)

    # Scope: exchanges in snapshot ∪ delistings
    scope_ex = snapshot.select("exchange").unique()
    if delistings is not None and not delistings.is_empty():
        scope_ex = pl.concat([scope_ex, delistings.select("exchange").unique()]).unique()

    out_of_scope = dim.join(scope_ex, on="exchange", how="anti")
    in_scope = dim.join(scope_ex, on="exchange", how="semi")
    current = in_scope.filter(pl.col("is_current"))
    history = in_scope.filter(~pl.col("is_current"))

    # Ensure snapshot has nullable tracked cols with correct types
    snap = _ensure_nullable(snapshot)

    # Classify current rows: in snapshot vs not
    not_in_snap = current.join(snap.select(nk).unique(), on=nk, how="anti")
    joined = snap.join(current, on=nk, how="inner", suffix="_cur")
    changed_mask = _any_tracked_changed(joined)
    changed_snap = joined.filter(changed_mask).select(snap.columns)
    changed_keys = changed_snap.select(nk)
    unchanged = current.join(snap.select(nk).unique(), on=nk, how="semi").join(
        changed_keys, on=nk, how="anti"
    )

    parts: list[pl.DataFrame] = [out_of_scope, history, unchanged]

    # Handle symbols not in snapshot
    if delistings is None:
        # Implicit: close all missing from snapshot
        if not not_in_snap.is_empty():
            parts.append(_close_rows(current, not_in_snap.select(nk), effective_ts_us))
    else:
        # Explicit: close only those in delistings; rest stay current
        if not delistings.is_empty():
            delist_keys = delistings.select(nk)
            to_delist = not_in_snap.join(delist_keys, on=nk, how="semi")
            if not to_delist.is_empty():
                parts.append(_close_rows_varied(current, delistings))
            still_current = not_in_snap.join(delist_keys, on=nk, how="anti")
            if not still_current.is_empty():
                parts.append(still_current)
        elif not not_in_snap.is_empty():
            parts.append(not_in_snap)

    # Close changed rows + open new versions
    if not changed_keys.is_empty():
        parts.append(_close_rows(current, changed_keys, effective_ts_us))
        parts.append(_stamp_current(changed_snap, effective_ts_us))

    # New listings
    new_listings = snap.join(current.select(nk).unique(), on=nk, how="anti")
    if not new_listings.is_empty():
        parts.append(_stamp_current(new_listings, effective_ts_us))

    non_empty = [_to_schema(p) for p in parts if not p.is_empty()]
    return pl.concat(non_empty) if non_empty else _to_schema(dim.clear())


def validate(dim: pl.DataFrame) -> None:
    """Validate dim_symbol invariants. Raises ValueError on violation."""
    if dim.is_empty():
        return

    nk = list(NATURAL_KEY)

    bad = dim.filter(pl.col("valid_until_ts_us") <= pl.col("valid_from_ts_us"))
    if not bad.is_empty():
        raise ValueError(f"valid_until_ts_us must be > valid_from_ts_us ({bad.height} rows)")

    dup_current = dim.filter(pl.col("is_current")).group_by(nk).len().filter(pl.col("len") > 1)
    if not dup_current.is_empty():
        raise ValueError("Multiple is_current=True rows for the same natural key")

    sorted_dim = dim.sort(nk + ["valid_from_ts_us"])
    overlaps = sorted_dim.with_columns(
        pl.col("valid_from_ts_us").shift(-1).over(nk).alias("_next_from")
    ).filter(
        pl.col("_next_from").is_not_null() & (pl.col("valid_until_ts_us") > pl.col("_next_from"))
    )
    if not overlaps.is_empty():
        raise ValueError("Overlapping validity windows")

    dup_ids = dim.group_by("symbol_id").len().filter(pl.col("len") > 1)
    if not dup_ids.is_empty():
        raise ValueError("Duplicate symbol_ids")


def assign_symbol_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Assign symbol_id = blake2b(exchange|exchange_symbol|valid_from_ts_us)."""
    payloads = pl.format(
        "{}|{}|{}",
        pl.col("exchange"),
        pl.col("exchange_symbol"),
        pl.col("valid_from_ts_us"),
    )

    def _hash(s: pl.Series) -> pl.Series:
        return pl.Series(
            [
                int.from_bytes(
                    hashlib.blake2b(x.encode(), digest_size=8).digest(),
                    "little",
                    signed=True,
                )
                for x in s
            ],
            dtype=pl.Int64,
        )

    return df.with_columns(payloads.map_batches(_hash).alias("symbol_id"))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _stamp_current(df: pl.DataFrame, effective_ts_us: int) -> pl.DataFrame:
    """Add SCD2 metadata columns and assign symbol_id."""
    return assign_symbol_ids(
        df.with_columns(
            pl.lit(effective_ts_us).cast(pl.Int64).alias("valid_from_ts_us"),
            pl.lit(VALID_UNTIL_MAX).cast(pl.Int64).alias("valid_until_ts_us"),
            pl.lit(True).alias("is_current"),
            pl.lit(effective_ts_us).cast(pl.Int64).alias("updated_at_ts_us"),
        )
    )


def _close_rows(current: pl.DataFrame, keys: pl.DataFrame, effective_ts_us: int) -> pl.DataFrame:
    """Close current rows matching keys at effective_ts_us."""
    return current.join(keys, on=list(NATURAL_KEY), how="semi").with_columns(
        pl.lit(effective_ts_us).cast(pl.Int64).alias("valid_until_ts_us"),
        pl.lit(False).alias("is_current"),
        pl.lit(effective_ts_us).cast(pl.Int64).alias("updated_at_ts_us"),
    )


def _close_rows_varied(current: pl.DataFrame, delistings: pl.DataFrame) -> pl.DataFrame:
    """Close current rows at per-row delisted_at_ts_us."""
    nk = list(NATURAL_KEY)
    closed = current.join(delistings.select(nk + ["delisted_at_ts_us"]), on=nk, how="inner")
    return closed.with_columns(
        pl.col("delisted_at_ts_us").alias("valid_until_ts_us"),
        pl.lit(False).alias("is_current"),
        pl.col("delisted_at_ts_us").alias("updated_at_ts_us"),
    ).drop("delisted_at_ts_us")


def _any_tracked_changed(joined: pl.DataFrame) -> pl.Expr:
    """Null-safe diff expression across tracked columns (col vs col_cur)."""
    diffs = []
    for col in TRACKED_COLS:
        if col not in joined.columns or f"{col}_cur" not in joined.columns:
            continue
        c_new, c_cur = pl.col(col), pl.col(f"{col}_cur")
        diffs.append(
            (c_new.is_null() != c_cur.is_null())
            | (c_new.is_not_null() & c_cur.is_not_null() & (c_new != c_cur))
        )
    if not diffs:
        return pl.lit(False)
    return pl.any_horizontal(diffs)


def _ensure_nullable(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure nullable columns exist with correct schema types."""
    for col in _NULLABLE:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=_SCHEMA[col]).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(_SCHEMA[col]))
    return df


def _to_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Select + cast to canonical DIM_SYMBOL schema."""
    df = _ensure_nullable(df)
    return df.select([pl.col(c).cast(dtype) for c, dtype in _SCHEMA.items()])
