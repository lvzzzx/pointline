"""Tests for error message templates."""

from pointline._error_messages import (
    exchange_not_found_error,
    invalid_timestamp_range_error,
    symbol_id_required_error,
    symbol_not_found_error,
    table_not_found_error,
    timestamp_required_error,
)


def test_symbol_id_required_error():
    """Test symbol_id required error message."""
    error = symbol_id_required_error()

    assert "symbol_id is required" in error
    assert "partition pruning" in error
    assert "registry.find_symbol" in error
    assert "symbol_ids = symbols['symbol_id'].to_list()" in error


def test_timestamp_required_error():
    """Test timestamp required error message."""
    error = timestamp_required_error()

    assert "start_ts_us and end_ts_us are required" in error
    assert "Integer microseconds" in error
    assert "datetime objects" in error
    assert "timezone.utc" in error


def test_exchange_not_found_with_suggestions():
    """Test exchange error with close matches."""
    exchanges = ["binance-futures", "binance-coin-futures", "binance-us"]
    error = exchange_not_found_error("binance", exchanges)

    assert "binance" in error
    assert "not found in EXCHANGE_MAP" in error
    assert "Did you mean" in error
    # Should suggest binance-futures or binance-us
    assert any(exch in error for exch in ["binance-futures", "binance-us"])


def test_exchange_not_found_no_suggestions():
    """Test exchange error with no close matches."""
    exchanges = ["binance-futures", "coinbase", "kraken"]
    error = exchange_not_found_error("xyzzyx", exchanges)

    assert "xyzzyx" in error
    assert "not found in EXCHANGE_MAP" in error
    # Should list available exchanges
    assert "binance-futures" in error
    assert "coinbase" in error


def test_exchange_not_found_truncates_long_list():
    """Test that long exchange lists are truncated."""
    exchanges = [f"exchange{i}" for i in range(20)]
    error = exchange_not_found_error("test", exchanges)

    assert "... and 10 more" in error


def test_symbol_not_found_error():
    """Test symbol not found error message."""
    error = symbol_not_found_error(12345)

    assert "12345" in error
    assert "not found in dim_symbol registry" in error
    assert "Possible causes" in error
    assert "registry.find_symbol" in error


def test_table_not_found_with_suggestions():
    """Test table error with close matches."""
    tables = ["trades", "quotes", "book_snapshot_25"]
    error = table_not_found_error("trade", tables)

    assert "trade" in error
    assert "not found in TABLE_PATHS" in error
    assert "Did you mean" in error
    assert "trades" in error  # Should suggest "trades" for "trade"


def test_table_not_found_lists_all_tables():
    """Test that all tables are listed in error."""
    tables = ["trades", "quotes", "book_snapshot_25"]
    error = table_not_found_error("invalid", tables)

    assert "trades" in error
    assert "quotes" in error
    assert "book_snapshot_25" in error
    assert "research.list_tables()" in error


def test_invalid_timestamp_range_error():
    """Test invalid timestamp range error message."""
    start = 1700003600000000
    end = 1700000000000000
    error = invalid_timestamp_range_error(start, end)

    assert str(start) in error
    assert str(end) in error
    assert "must be greater than" in error
