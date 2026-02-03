"""Tests for the research query convenience API."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pointline.research import query


@pytest.fixture
def mock_dim_symbol_single():
    """Mock dim_symbol with single symbol (no metadata change)."""
    return pl.DataFrame(
        {
            "symbol_id": [101],
            "exchange_id": [2],
            "exchange": ["binance-futures"],
            "exchange_symbol": ["SOLUSDT"],
            "valid_from_ts": [0],
            "valid_until_ts": [2**63 - 1],
            "price_increment": [0.001],
            "tick_size": [0.001],
        }
    )


@pytest.fixture
def mock_dim_symbol_multiple():
    """Mock dim_symbol with multiple symbols (metadata changed)."""
    return pl.DataFrame(
        {
            "symbol_id": [101, 102],
            "exchange_id": [2, 2],
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_symbol": ["SOLUSDT", "SOLUSDT"],
            "valid_from_ts": [0, 1700000000000000],
            "valid_until_ts": [1700000000000000, 2**63 - 1],
            "price_increment": [0.001, 0.0001],
            "tick_size": [0.001, 0.0001],
        }
    )


def test_query_trades_basic():
    """Test basic trades query with auto-resolution."""
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)

    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades") as mock_load,
    ):
        # Mock symbol resolution
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

        # Mock load_trades
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_load.return_value = mock_lf

        # Call query API
        result = query.trades(
            exchange="binance-futures",
            symbol="SOLUSDT",
            start=start,
            end=end,
        )

        # Verify symbol resolution was called
        mock_find.assert_called_once_with("SOLUSDT", exchange="binance-futures")

        # Verify load_trades was called with resolved symbol_ids
        mock_load.assert_called_once()
        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["symbol_id"] == [101]
        assert call_kwargs["lazy"] is True
        assert result == mock_lf


def test_query_trades_decoded_calls_decoded_loader():
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 2, tzinfo=timezone.utc)

    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades_decoded") as mock_load_decoded,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
        assert call_kwargs["symbol_id"] == [101]
        assert call_kwargs["keep_ints"] is True
        assert result == mock_lf


def test_query_trades_with_string_dates():
    """Test trades query with ISO string dates."""
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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


def test_query_trades_warns_on_metadata_change():
    """Test that query warns when symbol metadata changed."""
    start = datetime(2024, 5, 1, tzinfo=timezone.utc)
    end = datetime(2024, 9, 30, tzinfo=timezone.utc)
    start_ts_us = int(start.timestamp() * 1_000_000)
    end_ts_us = int(end.timestamp() * 1_000_000)

    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades") as mock_load,
    ):
        # Mock multiple symbol versions - BOTH valid during query period
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101, 102],
                "exchange_id": [2, 2],
                "exchange": ["binance-futures", "binance-futures"],
                "exchange_symbol": ["SOLUSDT", "SOLUSDT"],
                "valid_from_ts": [0, start_ts_us + 1000],  # Both valid during query
                "valid_until_ts": [end_ts_us - 1000, 2**63 - 1],
                "price_increment": [0.001, 0.0001],
                "tick_size": [0.001, 0.0001],
            }
        )

        mock_load.return_value = MagicMock(spec=pl.LazyFrame)

        # Should warn about metadata change
        with pytest.warns(UserWarning, match="Symbol metadata changed"):
            query.trades("binance-futures", "SOLUSDT", start, end)

        # Should still call load_trades with both symbol_ids
        call_kwargs = mock_load.call_args[1]
        assert set(call_kwargs["symbol_id"]) == {101, 102}


def test_query_no_symbols_found():
    """Test error when no symbols match."""
    with patch("pointline.research.query.registry.find_symbol") as mock_find:
        # Empty result
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": pl.Series([], dtype=pl.Int64),
                "valid_from_ts": pl.Series([], dtype=pl.Int64),
                "valid_until_ts": pl.Series([], dtype=pl.Int64),
                "price_increment": pl.Series([], dtype=pl.Float64),
                "tick_size": pl.Series([], dtype=pl.Float64),
            }
        )

        with pytest.raises(ValueError, match="No symbols found"):
            query.trades(
                "binance-futures",
                "INVALID",
                datetime(2024, 5, 1, tzinfo=timezone.utc),
                datetime(2024, 5, 2, tzinfo=timezone.utc),
            )


def test_query_no_active_symbols_in_range():
    """Test error when symbols exist but not in the time range."""
    with patch("pointline.research.query.registry.find_symbol") as mock_find:
        # Symbol exists but not active in query range
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [1000000],  # Very early timestamp
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

        with pytest.raises(
            ValueError, match="No active symbols found.*in the specified time range"
        ):
            query.trades(
                "binance-futures",
                "SOLUSDT",
                datetime(2024, 5, 1, tzinfo=timezone.utc),
                datetime(2024, 5, 2, tzinfo=timezone.utc),
            )


def test_query_quotes():
    """Test quotes query with auto-resolution."""
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_quotes") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
        assert call_kwargs["symbol_id"] == [101]
        assert call_kwargs["lazy"] is False
        assert result == mock_lf


def test_query_quotes_decoded_calls_decoded_loader():
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_quotes_decoded") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
        assert call_kwargs["symbol_id"] == [101]
        assert result == mock_lf


def test_query_book_snapshot_25():
    """Test book_snapshot_25 query with auto-resolution."""
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_book_snapshot_25") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
        assert call_kwargs["symbol_id"] == [101]
        assert result == mock_lf


def test_query_book_snapshot_25_decoded_calls_decoded_loader():
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_book_snapshot_25_decoded") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
        assert call_kwargs["symbol_id"] == [101]
        assert result == mock_lf


def test_query_with_custom_columns():
    """Test query with custom column selection."""
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

        mock_load.return_value = MagicMock(spec=pl.LazyFrame)

        query.trades(
            "binance-futures",
            "SOLUSDT",
            datetime(2024, 5, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 2, tzinfo=timezone.utc),
            columns=["ts_local_us", "price_px_int", "qty_int"],
        )

        call_kwargs = mock_load.call_args[1]
        assert call_kwargs["columns"] == ["ts_local_us", "price_px_int", "qty_int"]


def test_query_with_custom_ts_col():
    """Test query with custom timestamp column."""
    with (
        patch("pointline.research.query.registry.find_symbol") as mock_find,
        patch("pointline.research.query.core.load_trades") as mock_load,
    ):
        mock_find.return_value = pl.DataFrame(
            {
                "symbol_id": [101],
                "valid_from_ts": [0],
                "valid_until_ts": [2**63 - 1],
                "price_increment": [0.001],
                "tick_size": [0.001],
            }
        )

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
