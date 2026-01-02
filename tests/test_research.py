import pytest
import polars as pl
from datetime import date
from unittest.mock import patch, MagicMock
from pointline.research import scan_table, load_trades, list_tables

@patch("pointline.research.pl.scan_delta")
@patch("pointline.research.get_table_path")
def test_scan_table_filters(mock_get_path, mock_scan_delta):
    # Setup mocks
    mock_get_path.return_value = "/fake/path"
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_delta.return_value = mock_lf
    
    # Configure mock_lf to return itself on filter/select for chaining
    mock_lf.filter.return_value = mock_lf
    mock_lf.select.return_value = mock_lf

    # Call scan_table
    scan_table(
        "trades",
        exchange="binance",
        exchange_id=1,
        symbol_id=[100, 200],
        start_date="2025-01-01",
        end_date=date(2025, 1, 2),
        columns=["ts_local_us", "price_int"]
    )

    # Verify pl.scan_delta was called with the path from get_table_path
    mock_scan_delta.assert_called_once_with("/fake/path")
    
    # Verify filters were applied
    # Note: The order of filters depends on the implementation of _apply_filters
    assert mock_lf.filter.call_count == 4 # exchange, exchange_id, symbol_id, date range
    mock_lf.select.assert_called_once_with(["ts_local_us", "price_int"])

def test_list_tables():
    tables = list_tables()
    assert "trades" in tables
    assert "quotes" in tables
    assert "book_snapshots_top25" in tables

@patch("pointline.research.scan_table")
def test_load_trades_lazy(mock_scan_table):
    mock_lf = MagicMock(spec=pl.LazyFrame)
    mock_scan_table.return_value = mock_lf
    
    result = load_trades(exchange="binance", lazy=True)
    
    assert result == mock_lf
    mock_scan_table.assert_called_once_with(
        "trades",
        exchange="binance",
        exchange_id=None,
        symbol_id=None,
        start_date=None,
        end_date=None,
        columns=None
    )
