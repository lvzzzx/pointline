from __future__ import annotations

import polars as pl

from pointline.ingestion.lineage import assign_lineage


def test_assign_lineage_prefers_existing_file_seq() -> None:
    df = pl.DataFrame({"file_seq": [10, 20]})
    out = assign_lineage(df, file_id=7)

    assert out["file_id"].to_list() == [7, 7]
    assert out["file_seq"].to_list() == [10, 20]


def test_assign_lineage_generates_seq_even_when_file_line_number_exists() -> None:
    df = pl.DataFrame({"file_line_number": [3, 8]})
    out = assign_lineage(df, file_id=11)

    assert out["file_id"].to_list() == [11, 11]
    assert out["file_seq"].to_list() == [1, 2]


def test_assign_lineage_generates_seq_when_missing() -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    out = assign_lineage(df, file_id=5)

    assert out["file_id"].to_list() == [5, 5, 5]
    assert out["file_seq"].to_list() == [1, 2, 3]
