"""Base utilities for table domain logic modules.

This module provides common patterns shared across table modules to reduce duplication.
"""

from __future__ import annotations

import warnings
from typing import Sequence

import polars as pl

from pointline.validation_utils import DataQualityWarning, with_expected_exchange_id


def generic_resolve_symbol_ids(
    data: pl.DataFrame,
    dim_symbol: pl.DataFrame,
    exchange_id: int,
    exchange_symbol: str,
    *,
    ts_col: str = "ts_local_us",
) -> pl.DataFrame:
    """Resolve symbol_ids for data using as-of join with dim_symbol.

    This is a reusable wrapper around dim_symbol.resolve_symbol_ids that handles
    adding exchange_id and exchange_symbol columns if not present.

    Args:
        data: DataFrame with timestamp column
        dim_symbol: dim_symbol table in canonical schema
        exchange_id: Exchange ID to use for all rows
        exchange_symbol: Exchange symbol to use for all rows
        ts_col: Timestamp column name (default: ts_local_us)

    Returns:
        DataFrame with symbol_id column added
    """
    from pointline.dim_symbol import resolve_symbol_ids as _resolve_symbol_ids

    result = data.clone()
    if "exchange_id" not in result.columns:
        # Cast to match dim_symbol's exchange_id type (Int16, not UInt16)
        result = result.with_columns(pl.lit(exchange_id, dtype=pl.Int16).alias("exchange_id"))
    else:
        # Ensure existing exchange_id matches dim_symbol type
        result = result.with_columns(pl.col("exchange_id").cast(pl.Int16))
    if "exchange_symbol" not in result.columns:
        result = result.with_columns(pl.lit(exchange_symbol).alias("exchange_symbol"))

    return _resolve_symbol_ids(result, dim_symbol, ts_col=ts_col)


def generic_validate(
    df: pl.DataFrame,
    combined_filter: pl.Expr,
    validation_rules: list[tuple[str, pl.Expr]],
    table_name: str,
) -> pl.DataFrame:
    """Apply validation rules with detailed diagnostics.

    This function implements the common validation pattern used across all tables:
    1. Apply combined filter to get valid rows
    2. If rows were filtered, compute per-rule breakdowns with sample line numbers
    3. Emit DataQualityWarning with detailed diagnostics

    Args:
        df: DataFrame to validate (must already have expected_exchange_id column)
        combined_filter: Combined Polars expression for all validation rules
        validation_rules: List of (rule_name, rule_expr) tuples for diagnostics
        table_name: Name of the table being validated (for warning message)

    Returns:
        Filtered DataFrame with valid rows only

    Raises:
        DataQualityWarning: If any rows were filtered (shows breakdown by rule)
    """
    if df.is_empty():
        return df

    # Apply filter
    valid = df.filter(combined_filter)

    # Warn if rows were filtered
    if valid.height < df.height:
        line_col = "file_line_number" if "file_line_number" in df.columns else "__row_nr"
        df_with_line = df
        if line_col == "__row_nr":
            df_with_line = (
                df.with_row_index("__row_nr")
                if hasattr(df, "with_row_index")
                else df.with_row_count("__row_nr")
            )

        # Compute counts for each rule
        counts = df_with_line.select(
            [rule.sum().alias(name) for name, rule in validation_rules]
        ).row(0)

        # Build breakdown with sample line numbers
        breakdown = []
        for (name, rule), count in zip(validation_rules, counts):
            if count:
                sample = (
                    df_with_line.filter(rule).select(line_col).head(5).to_series().to_list()
                )
                breakdown.append(f"{name}={count} lines={sample}")

        detail = "; ".join(breakdown) if breakdown else "no rule breakdown available"
        warnings.warn(
            f"validate_{table_name}: filtered {df.height - valid.height} invalid rows; {detail}",
            DataQualityWarning,
        )

    return valid


def timestamp_validation_expr(col_name: str) -> pl.Expr:
    """Return standard timestamp validation expression (positive and within Int64 range).

    Args:
        col_name: Name of timestamp column to validate

    Returns:
        Polars expression that evaluates to True for valid timestamps
    """
    return (pl.col(col_name) > 0) & (pl.col(col_name) < 2**63)


def required_columns_validation_expr(columns: Sequence[str]) -> pl.Expr:
    """Return expression that checks all required columns are not null.

    Args:
        columns: List of column names to check for non-null values

    Returns:
        Polars expression that evaluates to True when all columns are non-null
    """
    expr = pl.lit(True)
    for col in columns:
        expr = expr & pl.col(col).is_not_null()
    return expr


def exchange_id_validation_expr() -> pl.Expr:
    """Return expression that validates exchange_id matches expected value.

    Assumes DataFrame has been processed with with_expected_exchange_id().

    Returns:
        Polars expression that evaluates to True when exchange_id matches expected
    """
    return pl.col("exchange_id") == pl.col("expected_exchange_id")
