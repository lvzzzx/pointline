"""Tests for klines domain logic and fixed-point encoding."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pointline.io.vendors.binance_vision.parsers.klines import parse_binance_klines_csv
from pointline.tables.klines import (
    KLINE_SCHEMA,
    check_kline_completeness,
    decode_fixed_point,
    encode_fixed_point,
    normalize_klines_schema,
    validate_klines,
)
from pointline.validation_utils import DataQualityWarning

# --- Fixtures ---


@pytest.fixture
def sample_dim_symbol() -> pl.DataFrame:
    """Sample dim_symbol with known increments for testing."""
    return pl.DataFrame(
        {
            "symbol_id": [101, 102, 103],
            "exchange_id": [1, 1, 1],
            "exchange_symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "price_increment": [0.01, 0.01, 0.001],  # BTCUSDT: $0.01, ETHUSDT: $0.01, SOL: $0.001
            "amount_increment": [0.00001, 0.0001, 0.01],  # BTC: 0.00001, ETH: 0.0001, SOL: 0.01
        }
    )


@pytest.fixture
def raw_kline_data() -> pl.DataFrame:
    """Raw Binance kline CSV data (without headers)."""
    return pl.DataFrame(
        {
            "open_time": ["1609459200000", "1609462800000"],  # 2021-01-01 00:00:00, 01:00:00 UTC
            "open": ["29000.50", "29100.25"],
            "high": ["29200.75", "29300.00"],
            "low": ["28900.25", "29000.50"],
            "close": ["29100.00", "29200.50"],
            "volume": ["123.45678", "234.56789"],
            "close_time": ["1609462799999", "1609466399999"],
            "quote_volume": ["3582345.67", "6850123.45"],
            "trade_count": ["12345", "23456"],
            "taker_buy_base_volume": ["61.72839", "117.28394"],
            "taker_buy_quote_volume": ["1791172.83", "3425061.72"],
            "ignore": ["0", "0"],
        }
    )


@pytest.fixture
def raw_kline_data_with_header() -> pl.DataFrame:
    """Raw Binance kline CSV data WITH header row."""
    return pl.DataFrame(
        {
            "open_time": ["open_time", "1609459200000", "1609462800000"],
            "open": ["open", "29000.50", "29100.25"],
            "high": ["high", "29200.75", "29300.00"],
            "low": ["low", "28900.25", "29000.50"],
            "close": ["close", "29100.00", "29200.50"],
            "volume": ["volume", "123.45678", "234.56789"],
            "close_time": ["close_time", "1609462799999", "1609466399999"],
            "quote_volume": ["quote_volume", "3582345.67", "6850123.45"],
            "trade_count": ["trade_count", "12345", "23456"],
            "taker_buy_base_volume": ["taker_buy_base_volume", "61.72839", "117.28394"],
            "taker_buy_quote_volume": ["taker_buy_quote_volume", "1791172.83", "3425061.72"],
            "ignore": ["ignore", "0", "0"],
        }
    )


# --- Parsing Tests ---


def test_parse_binance_klines_csv_basic(raw_kline_data: pl.DataFrame):
    """Test parsing standard Binance klines CSV without headers."""
    result = parse_binance_klines_csv(raw_kline_data)

    assert not result.is_empty()
    assert result.height == 2

    # Check timestamp conversion (milliseconds → microseconds)
    assert result["ts_bucket_start_us"][0] == 1609459200000000
    assert result["ts_bucket_end_us"][0] == 1609462799999000

    # Check numeric conversions
    assert result["open_px"][0] == pytest.approx(29000.50)
    assert result["high_px"][0] == pytest.approx(29200.75)
    assert result["volume"][0] == pytest.approx(123.45678)
    assert result["quote_volume"][0] == pytest.approx(3582345.67)


def test_parse_klines_with_header(raw_kline_data_with_header: pl.DataFrame):
    """Test parsing CSV WITH header row - header should be filtered out."""
    result = parse_binance_klines_csv(raw_kline_data_with_header)

    # Should have 2 data rows (header filtered out)
    assert result.height == 2

    # First data row should be the actual data, not the header
    assert result["ts_bucket_start_us"][0] == 1609459200000000
    assert result["open_px"][0] == pytest.approx(29000.50)


def test_parse_klines_without_header(raw_kline_data: pl.DataFrame):
    """Test parsing CSV WITHOUT header row - all rows should be preserved."""
    result = parse_binance_klines_csv(raw_kline_data)

    # Should have all 2 rows
    assert result.height == 2
    assert result["ts_bucket_start_us"][0] == 1609459200000000


def test_parse_binance_klines_empty():
    """Test parsing empty DataFrame."""
    empty = pl.DataFrame()
    result = parse_binance_klines_csv(empty)
    assert result.is_empty()


# --- Fixed-Point Encoding Tests ---


def test_encode_quote_volume_with_computed_increment(sample_dim_symbol: pl.DataFrame):
    """Verify quote_volume is encoded using quote_increment = price_increment × amount_increment."""
    # Create test data with known values
    df = pl.DataFrame(
        {
            "symbol_id": [101],  # BTCUSDT
            "open_px": [29000.00],
            "high_px": [29100.00],
            "low_px": [28900.00],
            "close_px": [29050.00],
            "volume": [100.0],  # 100 BTC
            "quote_volume": [2905000.0],  # 100 BTC × $29050 = $2,905,000
            "taker_buy_base_volume": [50.0],
            "taker_buy_quote_volume": [1452500.0],
            "trade_count": [1000],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )

    result = encode_fixed_point(df, sample_dim_symbol)

    # Check computed quote_increment (price_increment × amount_increment)
    # BTCUSDT: 0.01 × 0.00001 = 0.0000001
    expected_quote_increment = 0.01 * 0.00001

    # Verify quote_volume encoding
    expected_quote_volume_int = round(2905000.0 / expected_quote_increment)
    assert result["quote_volume_int"][0] == expected_quote_volume_int

    # Verify taker_buy_quote_volume encoding
    expected_taker_int = round(1452500.0 / expected_quote_increment)
    assert result["taker_buy_quote_qty_int"][0] == expected_taker_int


def test_encode_fixed_point_multi_symbol(sample_dim_symbol: pl.DataFrame):
    """Test encoding with multiple symbols using per-symbol increments."""
    df = pl.DataFrame(
        {
            "symbol_id": [101, 102, 103],
            "open_px": [29000.0, 1800.0, 100.5],
            "high_px": [29100.0, 1850.0, 102.0],
            "low_px": [28900.0, 1750.0, 99.0],
            "close_px": [29050.0, 1825.0, 101.25],
            "volume": [100.0, 500.0, 10000.0],
            "quote_volume": [2905000.0, 912500.0, 1012500.0],
            "taker_buy_base_volume": [50.0, 250.0, 5000.0],
            "taker_buy_quote_volume": [1452500.0, 456250.0, 506250.0],
            "trade_count": [1000, 2000, 3000],
            "ts_bucket_start_us": [1609459200000000] * 3,
            "ts_bucket_end_us": [1609462799999000] * 3,
        }
    )

    result = encode_fixed_point(df, sample_dim_symbol)

    # Verify each symbol uses its own increments
    assert result.height == 3

    # BTCUSDT (symbol_id=101): price_inc=0.01, amount_inc=0.00001
    assert result["open_px_int"][0] == round(29000.0 / 0.01)
    assert result["volume_qty_int"][0] == round(100.0 / 0.00001)

    # ETHUSDT (symbol_id=102): price_inc=0.01, amount_inc=0.0001
    assert result["open_px_int"][1] == round(1800.0 / 0.01)
    assert result["volume_qty_int"][1] == round(500.0 / 0.0001)

    # SOLUSDT (symbol_id=103): price_inc=0.001, amount_inc=0.01
    assert result["open_px_int"][2] == round(100.5 / 0.001)
    assert result["volume_qty_int"][2] == round(10000.0 / 0.01)


def test_encode_fixed_point_missing_symbol_id():
    """Test encoding raises error when symbol_id column is missing."""
    df = pl.DataFrame({"open_px": [100.0], "volume": [10.0]})
    dim_symbol = pl.DataFrame(
        {"symbol_id": [101], "price_increment": [0.01], "amount_increment": [0.001]}
    )

    with pytest.raises(ValueError, match="must have 'symbol_id' column"):
        encode_fixed_point(df, dim_symbol)


def test_encode_fixed_point_symbol_not_in_dim():
    """Test encoding raises error when symbol_id not found in dim_symbol."""
    df = pl.DataFrame(
        {
            "symbol_id": [999],  # Not in dim_symbol
            "open_px": [100.0],
            "high_px": [101.0],
            "low_px": [99.0],
            "close_px": [100.5],
            "volume": [10.0],
            "quote_volume": [1005.0],
            "taker_buy_base_volume": [5.0],
            "taker_buy_quote_volume": [502.5],
            "trade_count": [100],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )
    dim_symbol = pl.DataFrame(
        {"symbol_id": [101], "price_increment": [0.01], "amount_increment": [0.001]}
    )

    with pytest.raises(ValueError, match="not found in dim_symbol"):
        encode_fixed_point(df, dim_symbol)


def test_encode_fixed_point_invalid_increments():
    """Test encoding raises error for invalid increments (<=0)."""
    df = pl.DataFrame(
        {
            "symbol_id": [101],
            "open_px": [100.0],
            "high_px": [101.0],
            "low_px": [99.0],
            "close_px": [100.5],
            "volume": [10.0],
            "quote_volume": [1005.0],
            "taker_buy_base_volume": [5.0],
            "taker_buy_quote_volume": [502.5],
            "trade_count": [100],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )
    dim_symbol = pl.DataFrame(
        {"symbol_id": [101], "price_increment": [0.0], "amount_increment": [0.001]}  # Invalid!
    )

    with pytest.raises(ValueError, match="Invalid increments"):
        encode_fixed_point(df, dim_symbol)


def test_quote_volume_overflow_detection():
    """Verify Int64 overflow is caught when quote_volume exceeds Int64 range."""
    # Create scenario that would cause Int64 overflow
    # Max Int64 is ~9.2e18, so (1e20 / 1e-24) = 1e44 would definitely overflow
    df = pl.DataFrame(
        {
            "symbol_id": [101],
            "open_px": [100.0],
            "high_px": [101.0],
            "low_px": [99.0],
            "close_px": [100.5],
            "volume": [10.0],
            "quote_volume": [1e20],  # Very large value
            "taker_buy_base_volume": [5.0],
            "taker_buy_quote_volume": [1e20],
            "trade_count": [100],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )
    dim_symbol = pl.DataFrame(
        {
            "symbol_id": [101],
            "price_increment": [1e-15],  # Very small
            "amount_increment": [1e-15],  # Combined: 1e-30 would overflow
        }
    )

    # Polars raises InvalidOperationError for conversion failures
    with pytest.raises((ValueError, pl.exceptions.InvalidOperationError)):
        encode_fixed_point(df, dim_symbol)


# --- Fixed-Point Decoding Tests ---


def test_decode_quote_volume_roundtrip(sample_dim_symbol: pl.DataFrame):
    """Verify encode → decode produces original values (within floating-point precision)."""
    original = pl.DataFrame(
        {
            "symbol_id": [101, 102],
            "open_px": [29000.50, 1800.25],
            "high_px": [29100.75, 1850.50],
            "low_px": [28900.25, 1750.10],
            "close_px": [29050.00, 1825.75],
            "volume": [123.45678, 456.78901],
            "quote_volume": [3582345.67, 834567.89],
            "taker_buy_base_volume": [61.72839, 228.39450],
            "taker_buy_quote_volume": [1791172.83, 417283.94],
            "trade_count": [12345, 23456],
            "ts_bucket_start_us": [1609459200000000, 1609462800000000],
            "ts_bucket_end_us": [1609462799999000, 1609466399999000],
        }
    )

    # Encode
    encoded = encode_fixed_point(original, sample_dim_symbol)

    # Decode
    decoded = decode_fixed_point(encoded, sample_dim_symbol, keep_ints=False)

    # Verify round-trip (within reasonable tolerance for floating-point)
    # Note: Fixed-point encoding rounds, so we use relative tolerance
    assert decoded["open_px"][0] == pytest.approx(original["open_px"][0], rel=1e-6)
    assert decoded["quote_volume"][0] == pytest.approx(original["quote_volume"][0], rel=1e-6)
    assert decoded["taker_buy_quote_volume"][0] == pytest.approx(
        original["taker_buy_quote_volume"][0], rel=1e-6
    )


def test_decode_fixed_point_keep_ints(sample_dim_symbol: pl.DataFrame):
    """Test decoding with keep_ints=True preserves integer columns."""
    encoded = pl.DataFrame(
        {
            "symbol_id": [101],
            "open_px_int": [2900000],
            "high_px_int": [2910000],
            "low_px_int": [2890000],
            "close_px_int": [2905000],
            "volume_qty_int": [10000000],
            "quote_volume_int": [29050000000000],
            "taker_buy_base_qty_int": [5000000],
            "taker_buy_quote_qty_int": [14525000000000],
            "trade_count": [1000],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )

    result = decode_fixed_point(encoded, sample_dim_symbol, keep_ints=True)

    # Should have both float and int columns
    assert "open_px" in result.columns
    assert "open_px_int" in result.columns
    assert "quote_volume" in result.columns
    assert "quote_volume_int" in result.columns


def test_decode_fixed_point_missing_columns():
    """Test decoding raises error when required columns are missing."""
    df = pl.DataFrame({"symbol_id": [101], "open_px_int": [2900000]})  # Missing other *_int columns
    dim_symbol = pl.DataFrame(
        {"symbol_id": [101], "price_increment": [0.01], "amount_increment": [0.001]}
    )

    with pytest.raises(ValueError, match="missing columns"):
        decode_fixed_point(df, dim_symbol)


def test_quote_increment_calculation(sample_dim_symbol: pl.DataFrame):
    """Verify quote_increment = price_increment × amount_increment in encode/decode."""
    df = pl.DataFrame(
        {
            "symbol_id": [101],
            "open_px": [29000.0],
            "high_px": [29000.0],
            "low_px": [29000.0],
            "close_px": [29000.0],
            "volume": [1.0],
            "quote_volume": [29000.0],  # 1 BTC × $29000
            "taker_buy_base_volume": [0.5],
            "taker_buy_quote_volume": [14500.0],  # 0.5 BTC × $29000
            "trade_count": [1],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
        }
    )

    # Encode
    encoded = encode_fixed_point(df, sample_dim_symbol)

    # Expected quote_increment for BTCUSDT (symbol_id=101)
    price_inc = 0.01
    amount_inc = 0.00001
    quote_inc = price_inc * amount_inc  # 0.0000001

    # Verify encoding used correct increment
    expected_quote_int = round(29000.0 / quote_inc)
    assert encoded["quote_volume_int"][0] == expected_quote_int

    # Verify decoding uses same increment
    decoded = decode_fixed_point(encoded, sample_dim_symbol)
    assert decoded["quote_volume"][0] == pytest.approx(29000.0, rel=1e-6)


# --- Validation Tests ---


def test_validate_klines_basic(sample_dim_symbol: pl.DataFrame):
    """Test validation with valid kline data."""
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)],
            "exchange": ["binance-futures"],
            "exchange_id": [2],  # binance-futures = 2
            "symbol_id": [101],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
            "open_px_int": [2900000],
            "high_px_int": [2910000],
            "low_px_int": [2890000],
            "close_px_int": [2905000],
            "volume_qty_int": [10000000],
            "quote_volume_int": [29050000000000],
            "taker_buy_base_qty_int": [5000000],
            "taker_buy_quote_qty_int": [14525000000000],
            "trade_count": [1000],
            "file_id": [1],
            "file_line_number": [1],
        }
    )

    result = validate_klines(df)
    assert result.height == 1  # All rows valid


def test_validate_klines_filters_invalid():
    """Test validation filters invalid rows."""
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1), date(2021, 1, 1)],
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_id": [2, 2],  # binance-futures = 2
            "symbol_id": [101, 101],
            "ts_bucket_start_us": [1609459200000000, 1609462800000000],
            "ts_bucket_end_us": [1609462799999000, 1609462799999000],  # Second: end < start!
            "open_px_int": [2900000, -100],  # Second: negative price!
            "high_px_int": [2910000, 2910000],
            "low_px_int": [2890000, 2890000],
            "close_px_int": [2905000, 2905000],
            "volume_qty_int": [10000000, 10000000],
            "quote_volume_int": [29050000000000, 29050000000000],
            "taker_buy_base_qty_int": [5000000, 5000000],
            "taker_buy_quote_qty_int": [14525000000000, 14525000000000],
            "trade_count": [1000, 1000],
            "file_id": [1, 1],
            "file_line_number": [1, 2],
        }
    )

    with pytest.warns(DataQualityWarning, match="validate_klines: filtered"):
        result = validate_klines(df)
    assert result.height == 1  # Only first row valid


def test_validate_klines_high_lt_low():
    """Test validation filters klines where high < low."""
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)],
            "exchange": ["binance-futures"],
            "exchange_id": [1],
            "symbol_id": [101],
            "ts_bucket_start_us": [1609459200000000],
            "ts_bucket_end_us": [1609462799999000],
            "open_px_int": [2900000],
            "high_px_int": [2890000],  # High < Low!
            "low_px_int": [2910000],
            "close_px_int": [2905000],
            "volume_qty_int": [10000000],
            "quote_volume_int": [29050000000000],
            "taker_buy_base_qty_int": [5000000],
            "taker_buy_quote_qty_int": [14525000000000],
            "trade_count": [1000],
            "file_id": [1],
            "file_line_number": [1],
        }
    )

    with pytest.warns(DataQualityWarning, match="validate_klines: filtered"):
        result = validate_klines(df)
    assert result.is_empty()  # Invalid row filtered


# --- Completeness Check Tests ---


def test_kline_completeness_full_day():
    """Verify completeness check for full day of 1h klines (24 rows)."""
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)] * 24,
            "symbol_id": [101] * 24,
            "ts_bucket_start_us": list(range(1609459200000000, 1609545600000000, 3600000000)),
        }
    )

    result = check_kline_completeness(df, interval="1h", warn_on_gaps=False)

    assert result.height == 1
    # Use indexing to extract scalar values
    assert result["is_complete"][0]
    assert result["row_count"][0] == 24
    assert result["expected_count"][0] == 24


def test_kline_completeness_partial_day():
    """Verify completeness check detects gaps."""
    # Only 20 klines instead of 24
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)] * 20,
            "symbol_id": [101] * 20,
            "ts_bucket_start_us": list(range(1609459200000000, 1609531200000000, 3600000000)),
        }
    )

    result = check_kline_completeness(df, interval="1h", warn_on_gaps=False)

    assert result.height == 1
    assert not result["is_complete"][0]
    assert result["row_count"][0] == 20
    assert result["expected_count"][0] == 24


def test_kline_completeness_multiple_symbols():
    """Verify completeness check per symbol."""
    df = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)] * 24 + [date(2021, 1, 1)] * 20,
            "symbol_id": [101] * 24 + [102] * 20,  # symbol 101: complete, 102: incomplete
            "ts_bucket_start_us": list(range(1609459200000000, 1609545600000000, 3600000000))
            + list(range(1609459200000000, 1609531200000000, 3600000000)),
        }
    )

    result = check_kline_completeness(df, interval="1h", warn_on_gaps=False)

    assert result.height == 2
    # Symbol 101 should be complete
    complete_row = result.filter(pl.col("symbol_id") == 101)
    assert complete_row["is_complete"][0]
    # Symbol 102 should be incomplete
    incomplete_row = result.filter(pl.col("symbol_id") == 102)
    assert not incomplete_row["is_complete"][0]


def test_kline_completeness_different_intervals():
    """Test completeness check for various intervals."""
    # 4h interval should have 6 rows per day
    df_4h = pl.DataFrame(
        {
            "date": [date(2021, 1, 1)] * 6,
            "symbol_id": [101] * 6,
            "ts_bucket_start_us": list(range(1609459200000000, 1609545600000000, 14400000000)),
        }
    )

    result = check_kline_completeness(df_4h, interval="4h", warn_on_gaps=False)
    assert result["is_complete"][0]
    assert result["expected_count"][0] == 6

    # 1d interval should have 1 row per day
    df_1d = pl.DataFrame(
        {"date": [date(2021, 1, 1)], "symbol_id": [101], "ts_bucket_start_us": [1609459200000000]}
    )

    result = check_kline_completeness(df_1d, interval="1d", warn_on_gaps=False)
    assert result["is_complete"][0]
    assert result["expected_count"][0] == 1


def test_kline_completeness_invalid_interval():
    """Test completeness check raises error for unknown interval."""
    df = pl.DataFrame(
        {"date": [date(2021, 1, 1)], "symbol_id": [101], "ts_bucket_start_us": [1609459200000000]}
    )

    with pytest.raises(ValueError, match="unknown interval"):
        check_kline_completeness(df, interval="99h", warn_on_gaps=False)


def test_kline_completeness_empty():
    """Test completeness check with empty DataFrame."""
    result = check_kline_completeness(
        pl.DataFrame(schema={"date": pl.Date, "symbol_id": pl.Int64}),
        interval="1h",
        warn_on_gaps=False,
    )

    assert result.is_empty()
    assert result.columns == ["date", "symbol_id", "row_count", "expected_count", "is_complete"]


# --- Schema Normalization Tests ---


def test_normalize_klines_schema():
    """Test schema normalization adds missing columns and casts types."""
    df = pl.DataFrame(
        {
            "date": ["2021-01-01"],  # String instead of Date
            "exchange": ["binance-futures"],
            "symbol_id": [101],
        }
    )

    result = normalize_klines_schema(df)

    # Should have all schema columns
    assert set(result.columns) == set(KLINE_SCHEMA.keys())

    # Should cast types correctly
    assert result.schema["date"] == pl.Date
    assert result.schema["symbol_id"] == pl.Int64

    # Missing columns should be null
    assert result["open_px_int"][0] is None


# --- Integration Test ---


def test_full_pipeline_parse_encode_decode(
    raw_kline_data: pl.DataFrame, sample_dim_symbol: pl.DataFrame
):
    """Test complete pipeline: parse → encode → decode."""
    # Parse
    parsed = parse_binance_klines_csv(raw_kline_data)
    assert parsed.height == 2

    # Add symbol_id for encoding
    with_symbol = parsed.with_columns(pl.lit(101, dtype=pl.Int64).alias("symbol_id"))

    # Encode
    encoded = encode_fixed_point(with_symbol, sample_dim_symbol)
    assert "quote_volume_int" in encoded.columns
    assert "quote_volume" not in encoded.columns  # Original dropped

    # Decode
    decoded = decode_fixed_point(encoded, sample_dim_symbol)
    assert "quote_volume" in decoded.columns
    assert "quote_volume_int" not in decoded.columns  # Integers dropped

    # Verify round-trip preserves values (within tolerance)
    assert decoded["quote_volume"][0] == pytest.approx(3582345.67, rel=1e-4)
