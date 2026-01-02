"""Tests for book snapshots domain logic and ingestion service."""

import polars as pl
import pytest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA, scd2_bootstrap
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.services.book_snapshots_service import BookSnapshotsIngestionService
from pointline.book_snapshots import (
    BOOK_SNAPSHOTS_SCHEMA,
    encode_fixed_point,
    normalize_book_snapshots_schema,
    parse_tardis_book_snapshots_csv,
    resolve_symbol_ids,
    validate_book_snapshots,
    required_book_snapshots_columns,
)


def _sample_tardis_book_snapshots_csv() -> pl.DataFrame:
    """Create a sample Tardis book snapshots CSV DataFrame.

    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema uses exact column names: asks[0].price, asks[0].amount, etc.
    """
    # 2024-05-01T10:00:00.000000Z = 1714557600000000 microseconds
    base_ts = 1714557600000000
    return pl.DataFrame({
        "exchange": ["binance", "binance"],
        "symbol": ["BTCUSDT", "BTCUSDT"],
        "timestamp": [
            base_ts + 100_000,  # +0.1 second
            base_ts + 1_100_000,  # +1.1 seconds
        ],
        "local_timestamp": [
            base_ts,
            base_ts + 1_000_000,  # +1 second
        ],
        # Asks: ascending prices (best ask first)
        "asks[0].price": [50000.5, 50001.5],
        "asks[0].amount": [0.15, 0.25],
        "asks[1].price": [50000.6, 50001.6],
        "asks[1].amount": [0.20, 0.30],
        "asks[2].price": [50000.7, 50001.7],
        "asks[2].amount": [0.25, 0.35],
        # Bids: descending prices (best bid first)
        "bids[0].price": [50000.0, 50001.0],
        "bids[0].amount": [0.1, 0.2],
        "bids[1].price": [49999.9, 50000.9],
        "bids[1].amount": [0.15, 0.25],
        "bids[2].price": [49999.8, 50000.8],
        "bids[2].amount": [0.20, 0.30],
    })


def _sample_tardis_book_snapshots_csv_full_25() -> pl.DataFrame:
    """Create a sample Tardis book snapshots CSV with all 25 levels."""
    base_ts = 1714557600000000
    data = {
        "exchange": ["binance"],
        "symbol": ["BTCUSDT"],
        "timestamp": [base_ts + 100_000],
        "local_timestamp": [base_ts],
    }
    # Add all 25 levels for asks and bids
    for i in range(25):
        data[f"asks[{i}].price"] = [50000.5 + i * 0.1]
        data[f"asks[{i}].amount"] = [0.1 + i * 0.01]
        data[f"bids[{i}].price"] = [50000.0 - i * 0.1]
        data[f"bids[{i}].amount"] = [0.1 + i * 0.01]
    return pl.DataFrame(data)


def _sample_dim_symbol() -> pl.DataFrame:
    """Create a sample dim_symbol DataFrame."""
    updates = pl.DataFrame({
        "exchange_id": [1],
        "exchange_symbol": ["BTCUSDT"],
        "base_asset": ["BTC"],
        "quote_asset": ["USDT"],
        "asset_type": [0],
        "tick_size": [0.01],
        "lot_size": [0.00001],
        "price_increment": [0.01],
        "amount_increment": [0.00001],
        "contract_size": [1.0],
        "valid_from_ts": [1000000000000000],  # Early timestamp
    })
    return scd2_bootstrap(updates)


def test_parse_tardis_book_snapshots_csv_basic():
    """Test parsing standard Tardis book snapshots CSV format."""
    raw_df = _sample_tardis_book_snapshots_csv()
    parsed = parse_tardis_book_snapshots_csv(raw_df)

    assert parsed.height == 2
    assert "ts_local_us" in parsed.columns
    assert "ts_exch_us" in parsed.columns
    assert "bids_px" in parsed.columns
    assert "bids_sz" in parsed.columns
    assert "asks_px" in parsed.columns
    assert "asks_sz" in parsed.columns

    # Check timestamps are parsed correctly
    assert parsed["ts_local_us"].dtype == pl.Int64
    assert parsed["ts_exch_us"].dtype == pl.Int64
    assert parsed["ts_local_us"].min() > 0

    # Check lists are created
    assert parsed["bids_px"].dtype == pl.List(pl.Float64)
    assert parsed["bids_sz"].dtype == pl.List(pl.Float64)
    assert parsed["asks_px"].dtype == pl.List(pl.Float64)
    assert parsed["asks_sz"].dtype == pl.List(pl.Float64)

    # Check list lengths (should be padded to 25)
    first_row_bids = parsed["bids_px"][0]
    assert len(first_row_bids) == 25
    # First 3 should have values, rest should be null
    assert first_row_bids[0] == 50000.0
    assert first_row_bids[1] == 49999.9
    assert first_row_bids[2] == 49999.8
    # Rest should be null
    assert all(v is None for v in first_row_bids[3:])


def test_parse_tardis_book_snapshots_csv_full_25():
    """Test parsing with all 25 levels."""
    raw_df = _sample_tardis_book_snapshots_csv_full_25()
    parsed = parse_tardis_book_snapshots_csv(raw_df)

    assert parsed.height == 1
    first_row_bids = parsed["bids_px"][0]
    first_row_asks = parsed["asks_px"][0]

    # All 25 levels should be present
    assert len(first_row_bids) == 25
    assert len(first_row_asks) == 25
    # All should have values (no nulls)
    assert all(v is not None for v in first_row_bids)
    assert all(v is not None for v in first_row_asks)


def test_parse_tardis_book_snapshots_csv_missing_required():
    """Test parsing fails when required columns are missing."""
    base_ts = 1714557600000000
    raw_df = pl.DataFrame({
        "exchange": ["binance"],
        "symbol": ["BTCUSDT"],
        # Missing timestamp columns
        "asks[0].price": [50000.5],
        "asks[0].amount": [0.15],
    })

    with pytest.raises(ValueError, match="missing required columns"):
        parse_tardis_book_snapshots_csv(raw_df)


def test_normalize_book_snapshots_schema():
    """Test schema normalization."""
    # Create a DataFrame with all required columns
    base_ts = 1714557600000000
    df = pl.DataFrame({
        "date": [date(2024, 5, 1), date(2024, 5, 1)],
        "exchange": ["binance", "binance"],
        "exchange_id": [1, 1],
        "symbol_id": [100, 100],
        "ts_local_us": [base_ts, base_ts + 1_000_000],
        "ts_exch_us": [base_ts + 100_000, base_ts + 1_100_000],
        "ingest_seq": [1, 2],
        "bids_px": [[50000.0, None] * 12 + [None], [50001.0, None] * 12 + [None]],
        "bids_sz": [[0.1, None] * 12 + [None], [0.2, None] * 12 + [None]],
        "asks_px": [[50000.5, None] * 12 + [None], [50001.5, None] * 12 + [None]],
        "asks_sz": [[0.15, None] * 12 + [None], [0.25, None] * 12 + [None]],
        "file_id": [1, 1],
        "file_line_number": [1, 2],
        "extra_col": ["extra", "extra"],  # Should be dropped
    })

    normalized = normalize_book_snapshots_schema(df)

    # Check all schema columns are present
    assert set(normalized.columns) == set(BOOK_SNAPSHOTS_SCHEMA.keys())
    assert "extra_col" not in normalized.columns

    # Check types match schema
    assert normalized["date"].dtype == pl.Date
    assert normalized["exchange"].dtype == pl.Utf8
    assert normalized["exchange_id"].dtype == pl.Int16
    assert normalized["symbol_id"].dtype == pl.Int64
    assert normalized["bids_px"].dtype == pl.List(pl.Int64)
    assert normalized["asks_px"].dtype == pl.List(pl.Int64)


def test_validate_book_snapshots_basic():
    """Test validation with valid data."""
    base_ts = 1714557600000000
    # Valid: bids descending, asks ascending, best bid < best ask
    df = pl.DataFrame({
        "exchange": ["binance", "binance"],
        "exchange_id": [1, 1],
        "symbol_id": [100, 100],
        "ts_local_us": [base_ts, base_ts + 1_000_000],
        "bids_px": [
            [50000.0, 49999.9, 49999.8] + [None] * 22,
            [50001.0, 50000.9, 50000.8] + [None] * 22,
        ],
        "bids_sz": [
            [0.1, 0.15, 0.2] + [None] * 22,
            [0.2, 0.25, 0.3] + [None] * 22,
        ],
        "asks_px": [
            [50000.5, 50000.6, 50000.7] + [None] * 22,
            [50001.5, 50001.6, 50001.7] + [None] * 22,
        ],
        "asks_sz": [
            [0.15, 0.2, 0.25] + [None] * 22,
            [0.25, 0.3, 0.35] + [None] * 22,
        ],
    })

    validated = validate_book_snapshots(df)
    assert validated.height == 2  # All rows should be valid


def test_validate_book_snapshots_crossed_book():
    """Test validation filters crossed book (bid >= ask)."""
    base_ts = 1714557600000000
    # Invalid: best bid >= best ask (crossed book)
    df = pl.DataFrame({
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [base_ts],
        "bids_px": [[50000.5, None] * 12 + [None]],  # Best bid = 50000.5
        "bids_sz": [[0.1, None] * 12 + [None]],
        "asks_px": [[50000.5, None] * 12 + [None]],  # Best ask = 50000.5 (same!)
        "asks_sz": [[0.15, None] * 12 + [None]],
    })

    validated = validate_book_snapshots(df)
    assert validated.height == 0  # Should filter out crossed book


def test_validate_book_snapshots_invalid_ordering():
    """Test validation filters invalid bid/ask ordering."""
    base_ts = 1714557600000000
    # Invalid: bids not descending
    df = pl.DataFrame({
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [base_ts],
        "bids_px": [[49999.9, 50000.0, None] * 8 + [None]],  # Ascending (wrong!)
        "bids_sz": [[0.1, 0.15, None] * 8 + [None]],
        "asks_px": [[50000.5, 50000.6, None] * 8 + [None]],
        "asks_sz": [[0.15, 0.2, None] * 8 + [None]],
    })

    validated = validate_book_snapshots(df)
    # Should filter out invalid ordering
    assert validated.height == 0


def test_encode_fixed_point():
    """Test fixed-point encoding for list columns."""
    dim_symbol = _sample_dim_symbol()
    base_ts = 1714557600000000

    # Create DataFrame with symbol_id and float lists
    df = pl.DataFrame({
        "symbol_id": [100, 100],
        "ts_local_us": [base_ts, base_ts + 1_000_000],
        "bids_px": [
            [50000.0, 49999.9, None] + [None] * 22,
            [50001.0, 50000.9, None] + [None] * 22,
        ],
        "bids_sz": [
            [0.1, 0.15, None] + [None] * 22,
            [0.2, 0.25, None] + [None] * 22,
        ],
        "asks_px": [
            [50000.5, 50000.6, None] + [None] * 22,
            [50001.5, 50001.6, None] + [None] * 22,
        ],
        "asks_sz": [
            [0.15, 0.2, None] + [None] * 22,
            [0.25, 0.3, None] + [None] * 22,
        ],
    })

    # Add symbol_id to dim_symbol for join
    dim_symbol = dim_symbol.with_columns(pl.lit(100, dtype=pl.Int64).alias("symbol_id"))

    encoded = encode_fixed_point(df, dim_symbol)

    # Check types are now Int64 lists
    assert encoded["bids_px"].dtype == pl.List(pl.Int64)
    assert encoded["bids_sz"].dtype == pl.List(pl.Int64)
    assert encoded["asks_px"].dtype == pl.List(pl.Int64)
    assert encoded["asks_sz"].dtype == pl.List(pl.Int64)

    # Check encoding: 50000.0 / 0.01 = 5000000
    first_bid_px = encoded["bids_px"][0]
    assert first_bid_px[0] == 5000000  # 50000.0 / 0.01
    assert first_bid_px[1] == 4999990  # 49999.9 / 0.01
    assert first_bid_px[2] is None  # Null preserved

    # Check sizes: 0.1 / 0.00001 = 10000
    first_bid_sz = encoded["bids_sz"][0]
    assert first_bid_sz[0] == 10000  # 0.1 / 0.00001


def test_resolve_symbol_ids():
    """Test symbol ID resolution."""
    dim_symbol = _sample_dim_symbol()
    base_ts = 1714557600000000

    df = pl.DataFrame({
        "ts_local_us": [base_ts, base_ts + 1_000_000],
        "bids_px": [
            [50000.0, None] * 12 + [None],
            [50001.0, None] * 12 + [None],
        ],
        "bids_sz": [
            [0.1, None] * 12 + [None],
            [0.2, None] * 12 + [None],
        ],
        "asks_px": [
            [50000.5, None] * 12 + [None],
            [50001.5, None] * 12 + [None],
        ],
        "asks_sz": [
            [0.15, None] * 12 + [None],
            [0.25, None] * 12 + [None],
        ],
    })

    resolved = resolve_symbol_ids(df, dim_symbol, exchange_id=1, exchange_symbol="BTCUSDT")

    assert "symbol_id" in resolved.columns
    assert resolved["symbol_id"].dtype == pl.Int64
    # Symbol ID should be resolved (non-null)
    assert resolved["symbol_id"].is_not_null().all()


def test_required_book_snapshots_columns():
    """Test required columns function."""
    required = required_book_snapshots_columns()
    assert isinstance(required, tuple)
    assert len(required) == len(BOOK_SNAPSHOTS_SCHEMA)
    assert all(col in BOOK_SNAPSHOTS_SCHEMA for col in required)


@pytest.fixture
def temp_table_path(tmp_path):
    """Create a temporary table path for testing."""
    return tmp_path / "test_book_snapshots"


@pytest.fixture
def sample_manifest_repo(tmp_path):
    """Create a sample manifest repository."""
    return DeltaManifestRepository(tmp_path / "manifest")


def test_book_snapshots_ingestion_service_ingest_file(
    temp_table_path, sample_manifest_repo, tmp_path
):
    """Test full ingestion flow."""
    # Create repositories
    repo = BaseDeltaRepository(temp_table_path, partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol = _sample_dim_symbol()
    dim_symbol_repo.write_full(dim_symbol)

    # Create service
    service = BookSnapshotsIngestionService(repo, dim_symbol_repo, sample_manifest_repo)

    # Create a sample CSV file
    bronze_path = tmp_path / "bronze" / "book_snapshots.csv"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    raw_df = _sample_tardis_book_snapshots_csv()
    raw_df.write_csv(bronze_path)

    # Create metadata
    meta = BronzeFileMetadata(
        exchange="binance",
        data_type="book_snapshots",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path=str(bronze_path.relative_to(tmp_path / "bronze")),
        file_size_bytes=1000,
        last_modified_ts=1714557600,
    )

    # Register file in manifest
    file_id = sample_manifest_repo.register_file(meta)

    # Ingest
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze")

    # Check result
    assert result.row_count > 0
    assert result.error_message is None
    assert result.ts_local_min_us > 0
    assert result.ts_local_max_us > 0

    # Check data was written
    written = repo.read_all()
    assert written.height == result.row_count
    assert "bids_px" in written.columns
    assert "asks_px" in written.columns
    assert written["bids_px"].dtype == pl.List(pl.Int64)
    assert written["asks_px"].dtype == pl.List(pl.Int64)


def test_book_snapshots_ingestion_service_empty_file(
    temp_table_path, sample_manifest_repo, tmp_path
):
    """Test handling of empty CSV file."""
    repo = BaseDeltaRepository(temp_table_path, partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol = _sample_dim_symbol()
    dim_symbol_repo.write_full(dim_symbol)

    service = BookSnapshotsIngestionService(repo, dim_symbol_repo, sample_manifest_repo)

    # Create empty CSV
    bronze_path = tmp_path / "bronze" / "empty.csv"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text("")  # Empty file

    meta = BronzeFileMetadata(
        exchange="binance",
        data_type="book_snapshots",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path=str(bronze_path.relative_to(tmp_path / "bronze")),
        file_size_bytes=0,
        last_modified_ts=1714557600,
    )

    file_id = sample_manifest_repo.register_file(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze")

    assert result.row_count == 0
    assert result.error_message is None


def test_book_snapshots_ingestion_service_quarantine(
    temp_table_path, sample_manifest_repo, tmp_path
):
    """Test quarantine handling for missing symbol."""
    repo = BaseDeltaRepository(temp_table_path, partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    # Empty dim_symbol (no coverage)
    dim_symbol_repo.write_full(pl.DataFrame(schema=DIM_SYMBOL_SCHEMA))

    service = BookSnapshotsIngestionService(repo, dim_symbol_repo, sample_manifest_repo)

    bronze_path = tmp_path / "bronze" / "book_snapshots.csv"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    raw_df = _sample_tardis_book_snapshots_csv()
    raw_df.write_csv(bronze_path)

    meta = BronzeFileMetadata(
        exchange="binance",
        data_type="book_snapshots",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path=str(bronze_path.relative_to(tmp_path / "bronze")),
        file_size_bytes=1000,
        last_modified_ts=1714557600,
    )

    file_id = sample_manifest_repo.register_file(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze")

    # Should be quarantined
    assert result.row_count == 0
    assert result.error_message is not None
    assert "missing_symbol" in result.error_message or "invalid_validity_window" in result.error_message
