"""Tests for lazy CSV scan + sort + streaming collect."""

import os

import polars as pl

try:
    import pytest
except ImportError:
    pytest = None


def test_lazy_scan_sort_collect_streaming(tmp_path):
    csv_path = tmp_path / "l2_updates_raw.csv"
    csv_path.write_text("ts_local_us,ingest_seq\n2,1\n1,1\n1,1\n")

    lf = pl.scan_csv(csv_path).with_row_index("file_line_number", offset=1)
    df = (
        lf.sort(["ts_local_us", "ingest_seq", "file_line_number"])
        .collect(engine="streaming")
    )

    assert df["ts_local_us"].to_list() == [1, 1, 2]
    assert df["ingest_seq"].to_list() == [1, 1, 1]
    assert df["file_line_number"].to_list() == [2, 3, 1]


def test_lazy_scan_sort_collect_streaming_real_file():
    path = os.environ.get(
        "POINTLINE_L2_UPDATES_TEST_INPUT",
        (
            "/Users/zjx/data/lake/tardis/exchange=binance-futures/"
            "type=incremental_book_L2/date=2024-05-01/symbol=BTCUSDT/"
            "binance-futures_incremental_book_L2_2024-05-01_BTCUSDT.csv.gz"
        ),
    )
    if not os.path.exists(path):
        if pytest:
            pytest.skip(f"test input not found: {path}")
        else:
            return

    lf = pl.scan_csv(path)
    is_valid = validate_sorted_batches(lf, sort_col="local_timestamp")
    assert is_valid, "Batches are not properly sorted"


def validate_sorted_batches(lf, sort_col: str = "local_timestamp"):
    """Validate that batches are sorted both internally and across batches."""
    prev_last_value = None
    batch_num = 0
    total_rows = 0

    print(f"Validating sorted batches by '{sort_col}'...")
    print("=" * 80)

    for df in lf.sort(sort_col).collect_batches():
        batch_num += 1
        batch_rows = df.height

        if batch_rows == 0:
            print(f"Batch {batch_num}: Empty (skipping)")
            continue

        total_rows += batch_rows

        # Check if batch is sorted internally
        is_sorted = df[sort_col].is_sorted()
        first_value = df[sort_col][0]
        last_value = df[sort_col][-1]

        # Check cross-batch ordering
        if prev_last_value is not None:
            cross_batch_valid = prev_last_value <= first_value
            if not cross_batch_valid:
                print(f"❌ Batch {batch_num}: Cross-batch ordering violation!")
                print(f"   Previous batch last: {prev_last_value}")
                print(f"   Current batch first: {first_value}")
                return False
        else:
            cross_batch_valid = True

        # Print batch info
        status = "✅" if is_sorted and cross_batch_valid else "❌"
        print(
            f"{status} Batch {batch_num}: {batch_rows:,} rows, "
            f"range [{first_value}, {last_value}], "
            f"sorted={is_sorted}, cross-batch={cross_batch_valid}"
        )

        if not is_sorted:
            print(f"   ❌ Batch {batch_num} is not sorted internally!")
            return False

        prev_last_value = last_value

    print("=" * 80)
    print(f"✅ All {batch_num} batches validated successfully!")
    print(f"   Total rows: {total_rows:,}")
    print("   All batches are sorted internally and in correct order.")
    return True


# Example usage:
if __name__ == "__main__":
    csv_path = (
        "/Users/zjx/data/lake/tardis/exchange=binance-futures/"
        "type=incremental_book_L2/date=2024-05-01/symbol=BTCUSDT/"
        "binance-futures_incremental_book_L2_2024-05-01_BTCUSDT.csv.gz"
    )
    lf = pl.scan_csv(csv_path)
    is_valid = validate_sorted_batches(lf, sort_col="local_timestamp")

    if not is_valid:
        print("\n❌ Validation failed - data is not properly sorted!")
        exit(1)
    else:
        print("\n✅ Validation passed - all data is properly sorted!")