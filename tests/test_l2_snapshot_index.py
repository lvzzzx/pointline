from datetime import date

import polars as pl

from pointline.l2_snapshot_index import build_snapshot_index


def test_build_snapshot_index_groups_by_message() -> None:
    df = pl.DataFrame(
        {
            "exchange": ["deribit"] * 6,
            "exchange_id": [21] * 6,
            "symbol_id": [1, 1, 1, 1, 2, 2],
            "ts_local_us": [100, 100, 100, 101, 100, 100],
            "date": [date(2025, 1, 1)] * 6,
            "file_id": [1, 1, 1, 1, 1, 2],
            "file_line_number": [1, 2, 3, 4, 1, 2],
            "is_snapshot": [True, True, True, True, True, True],
        }
    )

    result = build_snapshot_index(df.lazy())

    assert result.height == 4

    grouped = {
        (row["exchange_id"], row["symbol_id"], row["ts_local_us"], row["file_id"]): row
        for row in result.iter_rows(named=True)
    }

    assert grouped[(21, 1, 100, 1)]["file_line_number"] == 1
    assert grouped[(21, 1, 101, 1)]["file_line_number"] == 4
    assert grouped[(21, 2, 100, 1)]["file_line_number"] == 1
    assert grouped[(21, 2, 100, 2)]["file_line_number"] == 2
