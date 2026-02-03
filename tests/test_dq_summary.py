from __future__ import annotations

import json
from datetime import date

import polars as pl

from pointline.dq.runner import run_dq_for_table


def _write_trades_delta(path: str, df: pl.DataFrame) -> None:
    df.write_delta(path, mode="overwrite")


def test_run_dq_summary_passed(tmp_path):
    table_path = tmp_path / "trades"
    data = pl.DataFrame(
        {
            "date": [date(2026, 1, 30), date(2026, 1, 30)],
            "exchange": ["binance", "binance"],
            "exchange_id": [1, 1],
            "symbol_id": [101, 101],
            "ts_local_us": [10, 20],
            "ts_exch_us": [10, 20],
            "trade_id": ["t1", "t2"],
            "side": [0, 1],
            "px_int": [100, 110],
            "qty_int": [5, 6],
            "flags": [0, 0],
            "file_id": [1, 1],
            "file_line_number": [1, 2],
        }
    )
    _write_trades_delta(str(table_path), data)

    summary = run_dq_for_table(
        "trades",
        date_partition=date(2026, 1, 30),
        table_path=str(table_path),
        now_us=30,
    )

    assert summary.item(0, "row_count") == 2
    assert summary.item(0, "duplicate_rows") == 0
    assert summary.item(0, "status") == "passed"

    null_counts = json.loads(summary.item(0, "null_counts"))
    assert null_counts["file_id"] == 0
    assert null_counts["file_line_number"] == 0


def test_run_dq_summary_detects_issues(tmp_path):
    table_path = tmp_path / "trades"
    data = pl.DataFrame(
        {
            "date": [date(2026, 1, 30), date(2026, 1, 30), date(2026, 1, 30)],
            "exchange": ["binance", "binance", "binance"],
            "exchange_id": [1, 1, 1],
            "symbol_id": [101, 101, 101],
            "ts_local_us": [10, 20, 30],
            "ts_exch_us": [10, 20, 30],
            "trade_id": ["t1", "t2", "t3"],
            "side": [0, 1, 0],
            "px_int": [100, 110, 120],
            "qty_int": [5, 6, 7],
            "flags": [0, 0, 0],
            "file_id": [1, 1, 1],
            "file_line_number": [1, 1, None],
        }
    )
    _write_trades_delta(str(table_path), data)

    summary = run_dq_for_table(
        "trades",
        date_partition=date(2026, 1, 30),
        table_path=str(table_path),
        now_us=40,
    )

    assert summary.item(0, "status") == "failed"
    issue_counts = json.loads(summary.item(0, "issue_counts"))
    assert issue_counts["duplicate_rows"] > 0
    assert issue_counts["null_key_rows"] > 0
