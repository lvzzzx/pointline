"""Built-in rollup helpers for feature_then_aggregate Pattern B."""

from __future__ import annotations

import polars as pl

DEFAULT_FEATURE_ROLLUPS = ["mean", "std", "min", "max"]
BUILTIN_FEATURE_ROLLUPS = {
    "sum",
    "mean",
    "std",
    "min",
    "max",
    "last",
    "first",
    "count",
    "close",
    "open",
}


def normalize_feature_rollup_names(
    rollups: list[str] | None,
    *,
    default: list[str] | None = None,
) -> list[str]:
    """Normalize rollup names (lowercase + de-duplicate while preserving order)."""
    base = rollups if rollups else (default if default is not None else DEFAULT_FEATURE_ROLLUPS)

    normalized: list[str] = []
    seen: set[str] = set()
    for rollup in base:
        key = str(rollup).lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def is_builtin_feature_rollup(name: str) -> bool:
    return str(name).lower() in BUILTIN_FEATURE_ROLLUPS


def build_builtin_feature_rollup_expr(feature_col: str, rollup: str) -> pl.Expr:
    col = pl.col(feature_col)
    key = str(rollup).lower()

    match key:
        case "sum":
            return col.sum()
        case "mean":
            return col.mean()
        case "std":
            return col.std()
        case "min":
            return col.min()
        case "max":
            return col.max()
        case "last" | "close":
            return col.last()
        case "first" | "open":
            return col.first()
        case "count":
            return col.count()
        case _:
            raise ValueError(f"Unknown built-in feature rollup: {rollup}")
