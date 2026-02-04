"""Test headerless CSV handling to prevent data loss."""

import tempfile
from pathlib import Path

from pointline.io.vendors.utils import read_csv_with_lineage


def test_headerless_csv_no_data_loss():
    """Verify that headerless CSVs (Binance Vision klines) don't lose first row.

    Critical bug: Without proper handling, polars consumes first data row as header,
    causing data loss and malformed column names.
    """
    # Create a headerless CSV (like Binance Vision klines)
    data = """1704067200000,42000.0,42500.0,41800.0,42300.0,100.5,1704070800000,4230000.0,1500,60.3,2538000.0,0
1704070800000,42300.0,42800.0,42100.0,42600.0,120.3,1704074400000,5123000.0,1600,70.2,2990000.0,0
1704074400000,42600.0,43000.0,42400.0,42900.0,95.7,1704078000000,4105000.0,1400,55.1,2364000.0,0"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        temp_path = Path(f.name)

    try:
        # Read with has_header=False and proper column names
        # (metadata would be used by generic service, but we test the core logic directly)
        columns = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]
        df = read_csv_with_lineage(temp_path, has_header=False, columns=columns)

        # Verify no data loss: should have 3 rows
        assert (
            df.height == 3
        ), f"Expected 3 rows, got {df.height} (first row was consumed as header!)"

        # Verify column names are correct (not data values)
        assert "open_time" in df.columns, "Column names malformed"
        assert df["open_time"][0] == 1704067200000, "First row data is correct"

        # Verify the first row's data is intact (not lost)
        first_row = df.row(0, named=True)
        assert first_row["open_time"] == 1704067200000
        assert first_row["open"] == 42000.0
        assert first_row["close"] == 42300.0
        assert first_row["volume"] == 100.5

        print("✓ Headerless CSV handling correct: No data loss")

    finally:
        temp_path.unlink(missing_ok=True)


def test_header_csv_still_works():
    """Verify that CSVs with headers (Tardis) still work correctly."""
    # Create a CSV with headers (like Tardis)
    data = """local_timestamp,timestamp,trade_id,side,price,amount
1704067200000000,1704067200100000,t1,buy,42000.0,0.5
1704067201000000,1704067201100000,t2,sell,42100.0,0.3"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(data)
        temp_path = Path(f.name)

    try:
        # Read with default has_header=True (implicit)
        df = read_csv_with_lineage(temp_path, has_header=True)

        # Verify header row was properly consumed
        assert df.height == 2, "Header row should not be counted as data"
        assert "local_timestamp" in df.columns
        assert df["local_timestamp"][0] == 1704067200000000

        print("✓ Header CSV handling correct: Header consumed properly")

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    test_headerless_csv_no_data_loss()
    test_header_csv_still_works()
    print("\n✅ All headerless CSV tests pass")
