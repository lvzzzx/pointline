from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pointline.research import (
    _normalize_timestamp,
    list_tables,
    load_trades,
    load_trades_decoded,
    scan_table,
)


@patch("pointline.research.core.resolve_symbols")
@patch("pointline.research.core.pl.scan_delta")
@patch("pointline.research.core.get_table_path")
def test_scan_table_filters(mock_get_path, mock_scan_delta, mock_resolve_symbols):
    # Setup mocks
    mock_get_path.return_value = "/fake/path"
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_delta.return_value = mock_lf

    mock_resolve_symbols.return_value = ["binance"]

    # Configure mock_lf to return itself on filter/select for chaining
    mock_lf.filter.return_value = mock_lf
    mock_lf.select.return_value = mock_lf
    mock_lf.schema = {
        "symbol_id": pl.Int64,
        "exchange": pl.Utf8,
        "date": pl.Date,
        "ts_local_us": pl.Int64,
    }

    # Call scan_table
    scan_table(
        "trades",
        symbol_id=[100, 200],
        start_ts_us=1_700_000_000_000_000,
        end_ts_us=1_700_000_100_000_000,
        columns=["ts_local_us", "price_int"],
    )

    # Verify pl.scan_delta was called with the path from get_table_path
    mock_scan_delta.assert_called_once_with("/fake/path")
    mock_resolve_symbols.assert_called_once_with([100, 200])

    # Verify filters were applied
    # Note: The order of filters depends on the implementation of _apply_filters
    assert mock_lf.filter.call_count == 4  # exchange, symbol_id, date range, time range
    mock_lf.select.assert_called_once_with(["ts_local_us", "price_int"])


@patch("pointline.research.core.resolve_symbols")
@patch("pointline.research.core.pl.scan_delta")
@patch("pointline.research.core.get_table_path")
def test_scan_table_time_range_prunes_date(mock_get_path, mock_scan_delta, mock_resolve_symbols):
    mock_get_path.return_value = "/fake/path"
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_delta.return_value = mock_lf
    mock_lf.filter.return_value = mock_lf
    mock_lf.select.return_value = mock_lf
    mock_lf.schema = {
        "symbol_id": pl.Int64,
        "exchange": pl.Utf8,
        "date": pl.Date,
        "ts_local_us": pl.Int64,
    }

    scan_table(
        "trades",
        symbol_id=[100],
        start_ts_us=1_700_000_000_000_000,
        end_ts_us=1_700_000_100_000_000,
        columns=["ts_local_us", "price_int"],
    )

    mock_scan_delta.assert_called_once_with("/fake/path")
    mock_resolve_symbols.assert_called_once_with([100])
    filter_args = [str(call.args[0]) for call in mock_lf.filter.call_args_list]
    assert any("date" in expr for expr in filter_args)
    assert any("ts_local_us" in expr for expr in filter_args)
    mock_lf.select.assert_called_once_with(["ts_local_us", "price_int"])


def test_list_tables():
    tables = list_tables()
    table_names = tables["table_name"].to_list()
    assert "trades" in table_names
    assert "quotes" in table_names
    assert "book_snapshot_25" in table_names


@patch("pointline.research.core.scan_table")
def test_load_trades_lazy(mock_scan_table):
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_table.return_value = mock_lf

    result = load_trades(
        symbol_id=[100],
        start_ts_us=1_700_000_000_000_000,
        end_ts_us=1_700_000_100_000_000,
        lazy=True,
    )

    assert result == mock_lf
    mock_scan_table.assert_called_once_with(
        "trades",
        symbol_id=[100],
        start_ts_us=1_700_000_000_000_000,
        end_ts_us=1_700_000_100_000_000,
        ts_col="ts_local_us",
        columns=None,
    )


@patch("pointline.research.core.decode_trades")
@patch("pointline.research.core.read_table")
def test_load_trades_decoded_keeps_ints_for_requested_columns(mock_read_table, mock_decode_trades):
    mock_read_table.return_value = pl.DataFrame(
        {"symbol_id": [1], "price_int": [10], "qty_int": [5]}
    )
    mock_decode_trades.return_value = pl.DataFrame(
        {"symbol_id": [1], "price_int": [10], "qty_int": [5], "price": [1.0], "qty": [0.05]}
    )

    dim_symbol = pl.DataFrame(
        {"symbol_id": [1], "price_increment": [0.1], "amount_increment": [0.01]}
    )

    result = load_trades_decoded(
        symbol_id=1,
        start_ts_us=1,
        end_ts_us=2,
        columns=["symbol_id", "price_int"],
        dim_symbol=dim_symbol,
    )

    assert result.shape[0] == 1
    assert "price_int" in result.columns
    assert mock_decode_trades.call_args.kwargs["keep_ints"] is True


def test_load_trades_decoded_lazy_returns_lazyframe():
    df = pl.DataFrame({"symbol_id": [1], "price_int": [10], "qty_int": [5]})
    dim_symbol = pl.DataFrame(
        {"symbol_id": [1], "price_increment": [0.1], "amount_increment": [0.01]}
    )

    with patch("pointline.research.core.scan_table") as mock_scan:
        mock_scan.return_value = df.lazy()

        result = load_trades_decoded(
            symbol_id=1,
            start_ts_us=1,
            end_ts_us=2,
            dim_symbol=dim_symbol,
            lazy=True,
        )

        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected["price"][0] == 1.0
        assert collected["qty"][0] == 0.05


def test_scan_table_requires_symbol_id():
    with pytest.raises(ValueError, match="symbol_id is required"):
        scan_table(
            "trades",
            start_ts_us=1_700_000_000_000_000,
            end_ts_us=1_700_000_100_000_000,
        )


def test_scan_table_requires_time_range():
    with pytest.raises(ValueError, match="start_ts_us and end_ts_us are required"):
        scan_table("trades", symbol_id=[100])


# ============================================================================
# Datetime Support Tests
# ============================================================================


def test_normalize_timestamp_with_int():
    """Test that _normalize_timestamp passes through int unchanged."""
    ts_us = 1700000000000000
    result = _normalize_timestamp(ts_us, "start_ts_us")
    assert result == ts_us
    assert isinstance(result, int)


def test_normalize_timestamp_with_datetime():
    """Test that _normalize_timestamp converts datetime to microseconds."""
    dt = datetime(2023, 11, 14, 12, 0, 0, tzinfo=timezone.utc)
    result = _normalize_timestamp(dt, "start_ts_us")

    expected = int(dt.timestamp() * 1_000_000)
    assert result == expected
    assert isinstance(result, int)


def test_normalize_timestamp_with_naive_datetime():
    """Test that naive datetime triggers a warning but is accepted as UTC."""
    dt = datetime(2023, 11, 14, 12, 0, 0)  # Naive (no tzinfo)

    with pytest.warns(UserWarning, match="naive datetime interpreted as UTC"):
        result = _normalize_timestamp(dt, "start_ts_us")

    # Should be treated as UTC
    dt_utc = dt.replace(tzinfo=timezone.utc)
    expected = int(dt_utc.timestamp() * 1_000_000)
    assert result == expected


def test_normalize_timestamp_with_none():
    """Test that _normalize_timestamp returns None for None input."""
    result = _normalize_timestamp(None, "start_ts_us")
    assert result is None


def test_normalize_timestamp_with_iso_date_string():
    """Test that _normalize_timestamp parses ISO date strings."""
    with pytest.warns(UserWarning, match="naive datetime interpreted as UTC"):
        result = _normalize_timestamp("2024-05-01", "start_ts_us")

    # ISO date without time is parsed as midnight UTC (with warning)
    expected_dt = datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    expected = int(expected_dt.timestamp() * 1_000_000)
    assert result == expected
    assert isinstance(result, int)


def test_normalize_timestamp_with_iso_datetime_string():
    """Test that _normalize_timestamp parses ISO datetime strings."""
    # With timezone
    result = _normalize_timestamp("2024-05-01T12:30:45+00:00", "start_ts_us")
    expected_dt = datetime(2024, 5, 1, 12, 30, 45, tzinfo=timezone.utc)
    expected = int(expected_dt.timestamp() * 1_000_000)
    assert result == expected

    # With Z suffix (common UTC indicator)
    result = _normalize_timestamp("2024-05-01T12:30:45Z", "start_ts_us")
    assert result == expected

    # Without timezone (should warn and assume UTC)
    with pytest.warns(UserWarning, match="naive datetime interpreted as UTC"):
        result = _normalize_timestamp("2024-05-01T12:30:45", "start_ts_us")
    assert result == expected


def test_normalize_timestamp_with_invalid_iso_string():
    """Test that _normalize_timestamp raises ValueError for invalid ISO strings."""
    with pytest.raises(ValueError, match="Invalid ISO datetime string"):
        _normalize_timestamp("not-a-date", "start_ts_us")

    with pytest.raises(ValueError, match="Invalid ISO datetime string"):
        _normalize_timestamp("2024-13-01", "start_ts_us")  # Invalid month


def test_normalize_timestamp_with_invalid_type():
    """Test that _normalize_timestamp raises TypeError for invalid types."""
    with pytest.raises(TypeError, match="must be int.*datetime.*or ISO string"):
        _normalize_timestamp(1.5, "start_ts_us")

    with pytest.raises(TypeError, match="must be int.*datetime.*or ISO string"):
        _normalize_timestamp([], "start_ts_us")


@patch("pointline.research.core.resolve_symbols")
@patch("pointline.research.core.pl.scan_delta")
@patch("pointline.research.core.get_table_path")
def test_scan_table_accepts_datetime(mock_get_path, mock_scan_delta, mock_resolve_symbols):
    """Test that scan_table accepts datetime objects."""
    start = datetime(2023, 11, 14, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 11, 14, 13, 0, 0, tzinfo=timezone.utc)

    # Setup mocks
    mock_get_path.return_value = "/fake/path"
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_delta.return_value = mock_lf
    mock_resolve_symbols.return_value = ["binance"]
    mock_lf.filter.return_value = mock_lf
    mock_lf.schema = {
        "symbol_id": pl.Int64,
        "exchange": pl.Utf8,
        "date": pl.Date,
        "ts_local_us": pl.Int64,
    }

    # Should not raise TypeError
    scan_table(
        "trades",
        symbol_id=101,
        start_ts_us=start,
        end_ts_us=end,
    )

    # Verify it was called
    mock_scan_delta.assert_called_once()


@patch("pointline.research.core.resolve_symbols")
@patch("pointline.research.core.pl.scan_delta")
@patch("pointline.research.core.get_table_path")
def test_load_trades_with_datetime(mock_get_path, mock_scan_delta, mock_resolve_symbols):
    """Test that load_trades works with datetime objects."""
    start = datetime(2023, 11, 14, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 11, 14, 13, 0, 0, tzinfo=timezone.utc)

    # Setup mocks
    mock_get_path.return_value = "/fake/path"
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_df = MagicMock(spec=pl.DataFrame)
    mock_scan_delta.return_value = mock_lf
    mock_resolve_symbols.return_value = ["binance"]
    mock_lf.filter.return_value = mock_lf
    mock_lf.collect.return_value = mock_df
    mock_lf.schema = {
        "symbol_id": pl.Int64,
        "exchange": pl.Utf8,
        "date": pl.Date,
        "ts_local_us": pl.Int64,
    }

    # Should not raise TypeError
    result = load_trades(
        symbol_id=101,
        start_ts_us=start,
        end_ts_us=end,
    )

    assert result == mock_df


def test_scan_table_enhanced_error_message_symbol_id():
    """Test enhanced error message when symbol_id is missing."""
    with pytest.raises(ValueError) as exc_info:
        scan_table(
            "trades",
            start_ts_us=1700000000000000,
            end_ts_us=1700003600000000,
        )

    error_msg = str(exc_info.value)
    assert "symbol_id is required for partition pruning" in error_msg
    assert "registry.find_symbol" in error_msg
    assert "symbol_ids = symbols['symbol_id'].to_list()" in error_msg


def test_scan_table_enhanced_error_message_timestamps():
    """Test enhanced error message when timestamps are missing."""
    with pytest.raises(ValueError) as exc_info:
        scan_table("trades", symbol_id=101)

    error_msg = str(exc_info.value)
    assert "start_ts_us and end_ts_us are required" in error_msg
    assert "Integer microseconds" in error_msg
    assert "datetime objects" in error_msg
    assert "timezone.utc" in error_msg


def test_scan_table_enhanced_error_message_invalid_range():
    """Test enhanced error message for invalid timestamp range."""
    with pytest.raises(ValueError) as exc_info:
        scan_table(
            "trades",
            symbol_id=101,
            start_ts_us=1700003600000000,
            end_ts_us=1700000000000000,  # Earlier than start
        )

    error_msg = str(exc_info.value)
    assert "1700003600000000" in error_msg
    assert "1700000000000000" in error_msg
    assert "must be greater than" in error_msg


def test_datetime_backwards_compatibility():
    """Test that existing int-based code still works."""
    # This test verifies backward compatibility
    with (
        patch("pointline.research.core.resolve_symbols"),
        patch("pointline.research.core.pl.scan_delta"),
        patch("pointline.research.core.get_table_path"),
    ):
        # Old-style API call with ints should still work
        scan_table(
            "trades",
            symbol_id=101,
            start_ts_us=1700000000000000,
            end_ts_us=1700003600000000,
        )
        # Should not raise any errors
