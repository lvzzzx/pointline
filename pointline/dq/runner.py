"""Data quality runner for silver tables."""

from __future__ import annotations

import json
import os
import time
from datetime import date
from pathlib import Path

import polars as pl

from pointline.config import TABLE_HAS_DATE, get_table_path
from pointline.dq.registry import TableDQConfig, get_dq_config, list_dq_tables
from pointline.tables.dq_summary import create_dq_summary_record


def _safe_int(value: int | None) -> int | None:
    return int(value) if value is not None else None


def _table_exists(path: str | Path) -> bool:
    return Path(path).exists()


def _build_profile_exprs(
    numeric_columns: tuple[str, ...],
    schema: dict[str, pl.DataType],
) -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
    for col in numeric_columns:
        if col not in schema:
            continue
        exprs.extend(
            [
                pl.col(col).min().alias(f"min__{col}"),
                pl.col(col).max().alias(f"max__{col}"),
                pl.col(col).mean().alias(f"mean__{col}"),
            ]
        )
    return exprs


def _partition_file_stats(
    table_path: str | Path,
    *,
    date_partition: date,
) -> tuple[int | None, int | None]:
    try:
        from deltalake import DeltaTable
    except Exception:
        return None, None

    try:
        dt = DeltaTable(str(table_path))
        partition_filters = [("date", "=", date_partition.isoformat())]
        files = dt.files(partition_filters=partition_filters)
    except Exception:
        return None, None

    file_count = 0
    total_bytes = 0
    for file_path in files:
        file_count += 1
        try:
            path = Path(file_path)
            if not path.is_absolute():
                path = Path(table_path) / file_path
            total_bytes += os.path.getsize(path)
        except Exception:
            continue

    return file_count, total_bytes


def _read_manifest_dates(
    data_type: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
    newest_first: bool = True,
) -> list[date]:
    manifest_path = get_table_path("ingest_manifest")
    if not _table_exists(manifest_path):
        return []

    lf = (
        pl.scan_delta(str(manifest_path))
        .filter((pl.col("data_type") == data_type) & (pl.col("status") == "success"))
        .select("date")
        .unique()
    )

    if start_date:
        lf = lf.filter(pl.col("date") >= pl.lit(start_date))
    if end_date:
        lf = lf.filter(pl.col("date") <= pl.lit(end_date))

    df = lf.collect()
    if df.is_empty():
        return []

    df = df.sort("date", descending=newest_first)
    if limit:
        df = df.head(limit)

    return df.get_column("date").to_list()


def run_dq_for_table(
    table_name: str,
    *,
    date_partition: date | None = None,
    table_path: str | Path | None = None,
    now_us: int | None = None,
) -> pl.DataFrame:
    """Run DQ checks for a single table and return a dq_summary record."""
    start_ms = int(time.time() * 1000)
    now_us = now_us or int(time.time() * 1_000_000)

    config = get_dq_config(table_name)
    path = table_path or get_table_path(table_name)

    if not _table_exists(path):
        end_ms = int(time.time() * 1000)
        return create_dq_summary_record(
            table_name=table_name,
            date_partition=date_partition,
            row_count=0,
            duplicate_rows=0,
            status="failed",
            validation_duration_ms=end_ms - start_ms,
            issue_counts={"missing_table": 1},
            run_id=now_us,
        )

    lf = pl.scan_delta(str(path))
    schema = lf.collect_schema()

    missing_columns = [col for col in config.key_columns if col not in schema]

    if date_partition and TABLE_HAS_DATE.get(table_name, False):
        lf = lf.filter(pl.col("date") == pl.lit(date_partition))

    agg_exprs: list[pl.Expr] = [pl.len().alias("row_count")]

    present_key_columns = [col for col in config.key_columns if col in schema]
    for col in present_key_columns:
        agg_exprs.append(pl.col(col).is_null().sum().alias(f"null__{col}"))

    if config.ts_column and config.ts_column in schema:
        agg_exprs.append(pl.col(config.ts_column).min().alias("min_ts_us"))
        agg_exprs.append(pl.col(config.ts_column).max().alias("max_ts_us"))

    agg_exprs.extend(_build_profile_exprs(config.numeric_columns, schema))

    if present_key_columns:
        agg_exprs.append(pl.struct(present_key_columns).n_unique().alias("unique_key_count"))

    summary_row = lf.select(agg_exprs).collect().row(0)
    summary_cols = [expr.meta.output_name() for expr in agg_exprs]
    summary = dict(zip(summary_cols, summary_row, strict=False))

    row_count = int(summary.get("row_count", 0) or 0)

    null_counts: dict[str, int] = {}
    null_key_rows = 0
    for col in present_key_columns:
        count = int(summary.get(f"null__{col}", 0) or 0)
        null_counts[col] = count
        null_key_rows += count

    duplicate_rows = 0
    if present_key_columns:
        unique_key_count = int(summary.get("unique_key_count", 0) or 0)
        duplicate_rows = max(0, row_count - unique_key_count)

    min_ts_us = _safe_int(summary.get("min_ts_us"))
    max_ts_us = _safe_int(summary.get("max_ts_us"))
    freshness_lag_sec = None
    if max_ts_us:
        freshness_lag_sec = max(0, (now_us - max_ts_us) // 1_000_000)

    profile_stats: dict[str, dict[str, float | int | None]] = {}
    for col in config.numeric_columns:
        if col not in schema:
            continue
        profile_stats[col] = {
            "min": summary.get(f"min__{col}"),
            "max": summary.get(f"max__{col}"),
            "mean": summary.get(f"mean__{col}"),
        }

    if date_partition and TABLE_HAS_DATE.get(table_name, False):
        file_count, total_bytes = _partition_file_stats(path, date_partition=date_partition)
        if file_count is not None:
            profile_stats["_partition"] = {
                "file_count": file_count,
                "total_bytes": total_bytes,
            }

    issue_counts: dict[str, int] = {}
    if missing_columns:
        issue_counts["missing_columns"] = len(missing_columns)
    if row_count == 0:
        issue_counts["empty_table"] = 1
    if null_key_rows:
        issue_counts["null_key_rows"] = null_key_rows
    if duplicate_rows:
        issue_counts["duplicate_rows"] = duplicate_rows

    status = "failed" if issue_counts else "passed"

    end_ms = int(time.time() * 1000)
    return create_dq_summary_record(
        table_name=table_name,
        date_partition=date_partition,
        row_count=row_count,
        duplicate_rows=duplicate_rows,
        status=status,
        validation_duration_ms=end_ms - start_ms,
        null_counts=null_counts if null_counts else None,
        min_ts_us=min_ts_us,
        max_ts_us=max_ts_us,
        freshness_lag_sec=freshness_lag_sec,
        issue_counts=issue_counts if issue_counts else None,
        profile_stats=profile_stats if profile_stats else None,
        run_id=now_us,
    )


def run_dq_partitioned(
    table_name: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_dates: int | None = None,
    include_rollup: bool = True,
    now_us: int | None = None,
    progress_cb: callable | None = None,
) -> pl.DataFrame:
    """Run DQ per date partition and optionally include a rollup summary."""
    config = get_dq_config(table_name)
    if not config.manifest_data_type:
        return run_dq_for_table(table_name, now_us=now_us)

    dates = _read_manifest_dates(
        config.manifest_data_type,
        start_date=start_date,
        end_date=end_date,
        limit=max_dates,
        newest_first=True,
    )
    if not dates:
        return run_dq_for_table(table_name, now_us=now_us)

    records = []
    total = len(dates)
    for idx, dt in enumerate(dates, start=1):
        if progress_cb:
            progress_cb(table_name=table_name, index=idx, total=total, date_partition=dt)
        records.append(run_dq_for_table(table_name, date_partition=dt, now_us=now_us))
    summary_df = pl.concat(records)

    if not include_rollup:
        return summary_df

    rollup = _rollup_dq_summary(summary_df, table_name=table_name, now_us=now_us)
    return pl.concat([summary_df, rollup])


def _rollup_dq_summary(
    summary_df: pl.DataFrame,
    *,
    table_name: str,
    now_us: int | None = None,
) -> pl.DataFrame:
    """Aggregate partition-level summaries into a single rollup record."""
    if summary_df.is_empty():
        return summary_df

    now_us = now_us or int(time.time() * 1_000_000)
    row_count = int(summary_df["row_count"].sum())
    duplicate_rows = int(summary_df["duplicate_rows"].sum())

    min_ts_us = (
        int(summary_df["min_ts_us"].drop_nulls().min())
        if "min_ts_us" in summary_df.columns and not summary_df["min_ts_us"].drop_nulls().is_empty()
        else None
    )
    max_ts_us = (
        int(summary_df["max_ts_us"].drop_nulls().max())
        if "max_ts_us" in summary_df.columns and not summary_df["max_ts_us"].drop_nulls().is_empty()
        else None
    )
    freshness_lag_sec = None
    if max_ts_us:
        freshness_lag_sec = max(0, (now_us - max_ts_us) // 1_000_000)

    issue_counts: dict[str, int] = {}
    null_counts: dict[str, int] = {}
    for row in summary_df.iter_rows(named=True):
        for field, target in (("issue_counts", issue_counts), ("null_counts", null_counts)):
            raw = row.get(field)
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            for key, value in parsed.items():
                if value is None:
                    continue
                target[key] = target.get(key, 0) + int(value)

    status = "failed" if issue_counts else "passed"
    return create_dq_summary_record(
        table_name=table_name,
        date_partition=None,
        row_count=row_count,
        duplicate_rows=duplicate_rows,
        status=status,
        validation_duration_ms=0,
        null_counts=null_counts if null_counts else None,
        min_ts_us=min_ts_us,
        max_ts_us=max_ts_us,
        freshness_lag_sec=freshness_lag_sec,
        issue_counts=issue_counts if issue_counts else None,
        profile_stats=None,
        run_id=now_us,
    )


def run_dq_for_all_tables(
    *,
    date_partition: date | None = None,
    now_us: int | None = None,
) -> pl.DataFrame:
    """Run DQ for all configured tables and return a concatenated DataFrame."""
    records = [
        run_dq_for_table(table_name, date_partition=date_partition, now_us=now_us)
        for table_name in list_dq_tables()
    ]
    return pl.concat(records)


def run_dq_for_all_tables_partitioned(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_dates: int | None = None,
    include_rollup: bool = True,
    now_us: int | None = None,
    progress_cb: callable | None = None,
) -> pl.DataFrame:
    """Run DQ for all tables, using per-date partitions where supported."""
    records: list[pl.DataFrame] = []
    for table_name in list_dq_tables():
        config = get_dq_config(table_name)
        if TABLE_HAS_DATE.get(table_name, False) and config.manifest_data_type:
            records.append(
                run_dq_partitioned(
                    table_name,
                    start_date=start_date,
                    end_date=end_date,
                    max_dates=max_dates,
                    include_rollup=include_rollup,
                    now_us=now_us,
                    progress_cb=progress_cb,
                )
            )
        else:
            records.append(run_dq_for_table(table_name, now_us=now_us))
    return pl.concat(records)
