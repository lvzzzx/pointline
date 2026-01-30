from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pointline.research import list_tables, load_trades, scan_table


@patch("pointline.research.resolve_symbols")
@patch("pointline.research.pl.scan_delta")
@patch("pointline.research.get_table_path")
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


@patch("pointline.research.resolve_symbols")
@patch("pointline.research.pl.scan_delta")
@patch("pointline.research.get_table_path")
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
    assert "trades" in tables
    assert "quotes" in tables
    assert "book_snapshot_25" in tables


@patch("pointline.research.scan_table")
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
