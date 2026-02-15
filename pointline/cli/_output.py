"""Output formatting helpers for CLI commands."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def print_table(rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    """Print a list of dicts as an aligned text table to stdout."""
    if not rows:
        return
    cols = columns or list(rows[0].keys())
    # compute column widths
    widths = {c: len(c) for c in cols}
    str_rows = []
    for row in rows:
        str_row = {c: str(row.get(c, "")) for c in cols}
        for c in cols:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)
    # header
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    sep = "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for sr in str_rows:
        print("  ".join(sr[c].ljust(widths[c]) for c in cols))


def write_output(
    df: Any,
    *,
    fmt: str = "table",
    output: str | Path | None = None,
    limit: int | None = None,
) -> None:
    """Write a Polars DataFrame to stdout or file in the requested format.

    Supported formats: table, csv, json, parquet.
    """
    import polars as pl

    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Expected polars DataFrame, got {type(df)}")

    if limit is not None:
        df = df.head(limit)

    sink = open(output, "w") if output and fmt != "parquet" else sys.stdout  # noqa: SIM115

    try:
        if fmt == "table":
            print(df, file=sink)
        elif fmt == "csv":
            sink.write(df.write_csv())
        elif fmt == "json":
            # row-oriented JSON
            sink.write(json.dumps(df.to_dicts(), default=str, indent=2))
            sink.write("\n")
        elif fmt == "parquet":
            if output is None:
                raise SystemExit("error: --output required for parquet format")
            df.write_parquet(str(output))
            print(f"Written to {output}")
        else:
            raise SystemExit(f"error: unknown format '{fmt}'")
    finally:
        if sink is not sys.stdout:
            sink.close()
