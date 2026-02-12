"""Tests for the research query convenience API."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl

from pointline.research import query


def test_query_trades_basic():
    """Test basic trades query delegates to core."""
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)

    with patch("pointline.research.query.core.load_trades") as mock_load:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        # Call query API
        result = query.trades(
            exchange="binance-futures",
            symbol="SOLUSDT",
            start=start,
            end=end,
        )

        # Verify load_trades was called with exchange and symbol
        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert call_kwargs["lazy"] is True
        assert result == mock_lf


def test_query_trades_decoded_calls_decoded_loader():
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)

    with patch("pointline.research.query.core.load_trades_decoded") as mock_load_decoded:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load_decoded.return_value = mock_lf

        result = query.trades(
            exchange="binance-futures",
            symbol="SOLUSDT",
            start=start,
            end=end,
            decoded=True,
            keep_ints=True,
        )

        mock_load_decoded.assert_called_once()
        call_kwargs = mock_load_decoded.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert call_kwargs["keep_ints"] is True
        assert result == mock_lf


def test_query_trades_with_string_dates():
    """Test trades query with ISO string dates."""
    with patch("pointline.research.query.core.load_trades") as mock_load:
        mock_load.return_value = MagicMock(spec=pl.LazyFrame)

        # ISO string dates should work (parsed in core._normalize_timestamp)
        result = query.trades(
            "binance-futures",
            "SOLUSDT",
            start="2024-05-01T00:00:00+00:00",
            end="2024-05-02T00:00:00+00:00",
        )

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]

        # Verify strings were converted to timestamps
        expected_start = int(
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000
        )
        expected_end = int(
            datetime(2024, 5, 2, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000
        )

        assert call_kwargs["start_ts_us"] == expected_start
        assert call_kwargs["end_ts_us"] == expected_end
        assert result == mock_load.return_value


def test_query_quotes():
    """Test quotes query delegates to core."""
    with patch("pointline.research.query.core.load_quotes") as mock_load:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        result = query.quotes(
            exchange="binance-futures",
            symbol="SOLUSDT",
            start=datetime(2024, 5, 1, tzinfo=timezone.utc),
            end=datetime(2024, 5, 2, tzinfo=timezone.utc),
            lazy=False,
        )

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert call_kwargs["lazy"] is False
        assert result == mock_lf


def test_query_quotes_decoded_calls_decoded_loader():
    with patch("pointline.research.query.core.load_quotes_decoded") as mock_load:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        result = query.quotes(
            exchange="binance-futures",
            symbol="SOLUSDT",
            start=datetime(2024, 5, 1, tzinfo=timezone.utc),
            end=datetime(2024, 5, 2, tzinfo=timezone.utc),
            decoded=True,
        )

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert result == mock_lf


def test_query_book_snapshot_25():
    """Test book_snapshot_25 query delegates to core."""
    with patch("pointline.research.query.core.load_book_snapshot_25") as mock_load:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        result = query.book_snapshot_25(
            "binance-futures",
            "SOLUSDT",
            datetime(2024, 5, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 2, tzinfo=timezone.utc),
        )

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert result == mock_lf


def test_query_book_snapshot_25_decoded_calls_decoded_loader():
    with patch("pointline.research.query.core.load_book_snapshot_25_decoded") as mock_load:
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        result = query.book_snapshot_25(
            "binance-futures",
            "SOLUSDT",
            datetime(2024, 5, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 2, tzinfo=timezone.utc),
            decoded=True,
        )

        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["exchange"] == "binance-futures"
        assert call_kwargs["symbol"] == "SOLUSDT"
        assert result == mock_lf


def test_query_with_custom_columns():
    """Test query with custom column selection."""
    with patch("pointline.research.query.core.load_trades") as mock_load:
        mock_load.return_value = MagicMock(spec=pl.LazyFrame)

        query.trades(
            "binance-futures",
            "SOLUSDT",
            datetime(2024, 5, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 2, tzinfo=timezone.utc),
            columns=["ts_local_us", "px_int", "qty_int"],
        )

        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["columns"] == ["ts_local_us", "px_int", "qty_int"]


def test_query_with_custom_ts_col():
    """Test query with custom timestamp column."""
    with patch("pointline.research.query.core.load_trades") as mock_load:
        mock_load.return_value = MagicMock(spec=pl.LazyFrame)

        query.trades(
            "binance-futures",
            "SOLUSDT",
            datetime(2024, 5, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 2, tzinfo=timezone.utc),
            ts_col="ts_exch_us",
        )

        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["ts_col"] == "ts_exch_us"


def test_backward_compatibility_import():
    """Test that old import patterns still work."""
    # Old pattern should still work
    from pointline import research

    assert hasattr(research, "load_trades")
    assert hasattr(research, "load_quotes")
    assert hasattr(research, "load_book_snapshot_25")
    assert hasattr(research, "query")


def test_query_module_accessible():
    """Test that query module is accessible."""
    from pointline.research import query

    assert hasattr(query, "trades")
    assert hasattr(query, "quotes")
    assert hasattr(query, "book_snapshot_25")
