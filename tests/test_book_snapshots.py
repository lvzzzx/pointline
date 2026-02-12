"""Tests for book snapshots domain logic and ingestion service."""

from datetime import date

import polars as pl
import pytest

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA
from pointline.dim_symbol import scd2_bootstrap
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata
from pointline.io.vendors.tardis.parsers.book_snapshots import parse_tardis_book_snapshots_csv
from pointline.tables.book_snapshots import (
    BOOK_SNAPSHOTS_SCHEMA,
    decode_fixed_point,
    encode_fixed_point,
    normalize_book_snapshots_schema,
    required_book_snapshots_columns,
    validate_book_snapshots,
)
from pointline.validation_utils import DataQualityWarning


def _sample_tardis_book_snapshots_csv() -> pl.DataFrame:
    """Create a sample Tardis book snapshots CSV DataFrame.

    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema uses exact column names: asks[0].price, asks[0].amount, etc.
    """
    # 2024-05-01T10:00:00.000000Z = 1714557600000000 microseconds
    base_ts = 1714557600000000
    return pl.DataFrame(
        {
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
        }
    )


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
    updates = pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange_symbol": ["BTCUSDT"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "asset_type": [0],
            "tick_size": [0.01],
            "lot_size": [0.00001],
            "contract_size": [1.0],
            "valid_from_ts": [1000000000000000],  # Early timestamp
        }
    )
    return scd2_bootstrap(updates)


def test_parse_tardis_book_snapshots_csv_basic():
    """Test parsing standard Tardis book snapshots CSV format."""
    raw_df = _sample_tardis_book_snapshots_csv()
    parsed = parse_tardis_book_snapshots_csv(raw_df)

    assert parsed.height == 2
    assert "ts_local_us" in parsed.columns
    assert "ts_exch_us" in parsed.columns
    assert "bids[0].price" in parsed.columns
    assert "bids[0].amount" in parsed.columns
    assert "asks[0].price" in parsed.columns
    assert "asks[0].amount" in parsed.columns

    # Check timestamps are parsed correctly
    assert parsed["ts_local_us"].dtype == pl.Int64
    assert parsed["ts_exch_us"].dtype == pl.Int64
    assert parsed["ts_local_us"].min() > 0

    # Check raw columns are cast to float
    assert parsed["bids[0].price"].dtype == pl.Float64
    assert parsed["bids[0].amount"].dtype == pl.Float64
    assert parsed["asks[0].price"].dtype == pl.Float64
    assert parsed["asks[0].amount"].dtype == pl.Float64


def test_parse_tardis_book_snapshots_csv_full_25():
    """Test parsing with all 25 levels."""
    raw_df = _sample_tardis_book_snapshots_csv_full_25()
    parsed = parse_tardis_book_snapshots_csv(raw_df)

    assert parsed.height == 1
    for i in range(25):
        assert f"asks[{i}].price" in parsed.columns
        assert f"asks[{i}].amount" in parsed.columns
        assert f"bids[{i}].price" in parsed.columns
        assert f"bids[{i}].amount" in parsed.columns
        assert parsed[f"asks[{i}].price"].dtype == pl.Float64
        assert parsed[f"bids[{i}].price"].dtype == pl.Float64


def test_parse_tardis_book_snapshots_csv_missing_required():
    """Test parsing fails when required columns are missing."""
    raw_df = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            # Missing timestamp columns
            "asks[0].price": [50000.5],
            "asks[0].amount": [0.15],
        }
    )

    with pytest.raises(ValueError, match="missing required columns"):
        parse_tardis_book_snapshots_csv(raw_df)


def test_normalize_book_snapshots_schema():
    """Test schema normalization."""
    # Create a DataFrame with all required columns
    base_ts = 1714557600000000
    df = pl.DataFrame(
        {
            "date": [date(2024, 5, 1), date(2024, 5, 1)],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "ts_local_us": [base_ts, base_ts + 1_000_000],
            "ts_exch_us": [base_ts + 100_000, base_ts + 1_100_000],
            "bids_px_int": [[5000000, None] * 12 + [None], [5000100, None] * 12 + [None]],
            "bids_sz_int": [[10000, None] * 12 + [None], [20000, None] * 12 + [None]],
            "asks_px_int": [[5000050, None] * 12 + [None], [5000150, None] * 12 + [None]],
            "asks_sz_int": [[15000, None] * 12 + [None], [25000, None] * 12 + [None]],
            "file_id": [1, 1],
            "file_line_number": [1, 2],
            "extra_col": ["extra", "extra"],  # Should be dropped
        }
    )

    normalized = normalize_book_snapshots_schema(df)

    # Check all schema columns are present
    assert set(normalized.columns) == set(BOOK_SNAPSHOTS_SCHEMA.keys())
    assert "extra_col" not in normalized.columns

    # Check types match schema
    assert normalized["date"].dtype == pl.Date
    assert normalized["exchange"].dtype == pl.Utf8
    assert normalized["symbol"].dtype == pl.Utf8
    assert normalized["bids_px_int"].dtype == pl.List(pl.Int64)
    assert normalized["asks_px_int"].dtype == pl.List(pl.Int64)


def test_validate_book_snapshots_basic():
    """Test validation with valid data."""
    base_ts = 1714557600000000
    # Valid: bids descending, asks ascending, best bid < best ask
    df = pl.DataFrame(
        {
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "ts_local_us": [base_ts, base_ts + 1_000_000],
            "bids_px_int": [
                [5000000, 4999990, 4999980] + [None] * 22,
                [5000100, 5000090, 5000080] + [None] * 22,
            ],
            "bids_sz_int": [
                [10000, 15000, 20000] + [None] * 22,
                [20000, 25000, 30000] + [None] * 22,
            ],
            "asks_px_int": [
                [5000050, 5000060, 5000070] + [None] * 22,
                [5000150, 5000160, 5000170] + [None] * 22,
            ],
            "asks_sz_int": [
                [15000, 20000, 25000] + [None] * 22,
                [25000, 30000, 35000] + [None] * 22,
            ],
        }
    )

    validated = validate_book_snapshots(df)
    assert validated.height == 2  # All rows should be valid


def test_validate_book_snapshots_crossed_book():
    """Test validation filters crossed book (bid >= ask)."""
    base_ts = 1714557600000000
    # Invalid: best bid >= best ask (crossed book)
    df = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            "ts_local_us": [base_ts],
            "bids_px_int": [[5000050, None] * 12 + [None]],  # Best bid = 50000.5
            "bids_sz_int": [[10000, None] * 12 + [None]],
            "asks_px_int": [[5000050, None] * 12 + [None]],  # Best ask = 50000.5 (same!)
            "asks_sz_int": [[15000, None] * 12 + [None]],
        }
    )

    with pytest.warns(DataQualityWarning, match="validate_book_snapshots: filtered"):
        validated = validate_book_snapshots(df)
    assert validated.height == 0  # Should filter out crossed book


def test_validate_book_snapshots_invalid_ordering():
    """Test validation filters invalid bid/ask ordering."""
    base_ts = 1714557600000000
    # Invalid: bids not descending
    df = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            "ts_local_us": [base_ts],
            "bids_px_int": [[4999990, 5000000, None] * 8 + [None]],  # Ascending (wrong!)
            "bids_sz_int": [[10000, 15000, None] * 8 + [None]],
            "asks_px_int": [[5000050, 5000060, None] * 8 + [None]],
            "asks_sz_int": [[15000, 20000, None] * 8 + [None]],
        }
    )

    with pytest.warns(DataQualityWarning, match="validate_book_snapshots: filtered"):
        validated = validate_book_snapshots(df)
    # Should filter out invalid ordering
    assert validated.height == 0


def test_encode_fixed_point():
    """Test fixed-point encoding from raw level columns using asset-class profile."""
    dim_symbol = _sample_dim_symbol()
    base_ts = 1714557600000000

    # Create DataFrame with symbol and raw float columns
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "ts_local_us": [base_ts, base_ts + 1_000_000],
            "bids[0].price": [50000.0, 50001.0],
            "bids[0].amount": [0.1, 0.2],
            "bids[1].price": [49999.9, 50000.9],
            "bids[1].amount": [0.15, 0.25],
            "asks[0].price": [50000.5, 50001.5],
            "asks[0].amount": [0.15, 0.25],
            "asks[1].price": [50000.6, 50001.6],
            "asks[1].amount": [0.2, 0.3],
        }
    )

    encoded = encode_fixed_point(df, dim_symbol, "binance-futures")

    # Check types are now Int64 lists
    assert encoded["bids_px_int"].dtype == pl.List(pl.Int64)
    assert encoded["bids_sz_int"].dtype == pl.List(pl.Int64)
    assert encoded["asks_px_int"].dtype == pl.List(pl.Int64)
    assert encoded["asks_sz_int"].dtype == pl.List(pl.Int64)

    # Check encoding with crypto profile (price scalar = 1e-9).
    # Bids use floor; due to IEEE 754 precision, floor(50000.0 / 1e-9) may
    # land 1 unit below the "exact" value. Verify within tolerance of 1.
    first_bid_px = encoded["bids_px_int"][0]
    assert len(first_bid_px) == 25
    assert abs(first_bid_px[0] - 50_000_000_000_000) <= 1  # 50000.0 / 1e-9 (floor)
    assert abs(first_bid_px[1] - 49_999_900_000_000) <= 1  # 49999.9 / 1e-9 (floor)
    assert first_bid_px[2] is None  # Null preserved

    # Check sizes with crypto profile (amount scalar = 1e-9):
    # 0.1 / 1e-9 = 100_000_000 (round mode, exact for powers of 10)
    first_bid_sz = encoded["bids_sz_int"][0]
    assert abs(first_bid_sz[0] - 100_000_000) <= 1  # 0.1 / 1e-9


def test_encode_fixed_point_multi_symbol():
    """Encode with multiple symbol values — same profile scalar for all."""
    updates = pl.DataFrame(
        {
            "exchange_id": [1, 1],
            "exchange_symbol": ["BTCUSDT", "ETHUSDT"],
            "base_asset": ["BTC", "ETH"],
            "quote_asset": ["USDT", "USDT"],
            "asset_type": [0, 0],
            "tick_size": [0.01, 0.1],
            "lot_size": [0.00001, 0.001],
            "contract_size": [1.0, 1.0],
            "valid_from_ts": [1000000000000000, 1000000000000000],
        }
    )
    dim_symbol = scd2_bootstrap(updates)

    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "ts_local_us": [1714557600000000, 1714557600000001],
            "asks[0].price": [50000.01, 250.01],
            "asks[0].amount": [0.15, 1.6],
            "bids[0].price": [50000.00, 249.99],
            "bids[0].amount": [0.10, 1.5],
        }
    )

    encoded = encode_fixed_point(df, dim_symbol, "binance-futures")

    # crypto profile: price scalar = 1e-9
    # Due to IEEE 754 precision, floor/ceil may land 1 unit off the "exact" value.
    # asks ceil: 50000.01 / 1e-9 ~ 50_000_010_000_000
    assert abs(encoded["asks_px_int"][0][0] - 50_000_010_000_000) <= 1
    # bids floor: 50000.00 / 1e-9 ~ 50_000_000_000_000
    assert abs(encoded["bids_px_int"][0][0] - 50_000_000_000_000) <= 1
    # asks ceil: 250.01 / 1e-9 ~ 250_010_000_000
    assert abs(encoded["asks_px_int"][1][0] - 250_010_000_000) <= 1
    # bids floor: 249.99 / 1e-9 ~ 249_990_000_000
    assert abs(encoded["bids_px_int"][1][0] - 249_990_000_000) <= 1


def test_decode_fixed_point():
    """Test decoding fixed-point list columns back to floats using asset-class profile."""
    base_ts = 1714557600000000

    # Encoded with crypto profile (price=1e-9, amount=1e-9):
    # 50000.0 → 50_000_000_000_000, 49999.9 → 49_999_900_000_000
    # 0.1 → 100_000_000, 0.15 → 150_000_000
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "exchange": ["binance-futures"],
            "ts_local_us": [base_ts],
            "bids_px_int": [[50_000_000_000_000, 49_999_900_000_000, None] + [None] * 22],
            "bids_sz_int": [[100_000_000, 150_000_000, None] + [None] * 22],
            "asks_px_int": [[50_000_500_000_000, 50_000_600_000_000, None] + [None] * 22],
            "asks_sz_int": [[150_000_000, 200_000_000, None] + [None] * 22],
        }
    )

    decoded = decode_fixed_point(df, exchange="binance-futures")

    assert decoded["bids_px"].dtype == pl.List(pl.Float64)
    assert decoded["bids_sz"].dtype == pl.List(pl.Float64)
    assert decoded["asks_px"].dtype == pl.List(pl.Float64)
    assert decoded["asks_sz"].dtype == pl.List(pl.Float64)

    first_bid_px = decoded["bids_px"][0]
    assert first_bid_px[0] == pytest.approx(50000.0)
    assert first_bid_px[1] == pytest.approx(49999.9)
    assert first_bid_px[2] is None


def test_decode_fixed_point_multi_symbol():
    """Decode with multiple symbol values — same profile scalar for all."""
    base_ts = 1714557600000000

    # Encoded with crypto profile (price=1e-9):
    # BTC 50000.0 → 50_000_000_000_000; ETH 250.0 → 250_000_000_000
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "exchange": ["binance-futures", "binance-futures"],
            "ts_local_us": [base_ts, base_ts + 1],
            "bids_px_int": [[50_000_000_000_000, None], [250_000_000_000, None]],
            "bids_sz_int": [[100_000_000, None], [1_500_000_000, None]],
            "asks_px_int": [[50_000_500_000_000, None], [250_100_000_000, None]],
            "asks_sz_int": [[150_000_000, None], [1_600_000_000, None]],
        }
    )

    decoded = decode_fixed_point(df, exchange="binance-futures")

    assert decoded["bids_px"][0][0] == pytest.approx(50000.0)
    assert decoded["asks_px"][0][0] == pytest.approx(50000.5)
    assert decoded["bids_px"][1][0] == pytest.approx(250.0)
    assert decoded["asks_px"][1][0] == pytest.approx(250.1)


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
    service = create_ingestion_service("book_snapshot_25", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    # Create a sample CSV file
    bronze_rel_path = (
        "exchange=binance/type=book_snapshot_25/date=2024-05-01/symbol=BTCUSDT/book_snapshots.csv"
    )
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    raw_df = _sample_tardis_book_snapshots_csv()
    raw_df.write_csv(bronze_path)

    # Create metadata
    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="book_snapshot_25",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=1000,
        last_modified_ts=1714557600,
        sha256="a" * 64,
        date=date(2024, 5, 1),
    )

    # Register file in manifest
    file_id = sample_manifest_repo.resolve_file_id(meta)

    # Ingest
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")

    # Check result
    assert result.row_count > 0
    assert result.error_message is None
    assert result.ts_local_min_us > 0
    assert result.ts_local_max_us > 0

    # Check data was written
    written = repo.read_all()
    assert written.height == result.row_count
    assert "bids_px_int" in written.columns
    assert "asks_px_int" in written.columns
    assert written["bids_px_int"].dtype == pl.List(pl.Int64)
    assert written["asks_px_int"].dtype == pl.List(pl.Int64)


def test_book_snapshots_ingestion_service_empty_file(
    temp_table_path, sample_manifest_repo, tmp_path
):
    """Test handling of empty CSV file."""
    repo = BaseDeltaRepository(temp_table_path, partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol = _sample_dim_symbol()
    dim_symbol_repo.write_full(dim_symbol)

    service = create_ingestion_service("book_snapshot_25", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    # Create empty CSV
    bronze_rel_path = (
        "exchange=binance/type=book_snapshot_25/date=2024-05-01/symbol=BTCUSDT/empty.csv"
    )
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text("")  # Empty file

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="book_snapshot_25",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=0,
        last_modified_ts=1714557600,
        sha256="b" * 64,
        date=date(2024, 5, 1),
    )

    file_id = sample_manifest_repo.resolve_file_id(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")

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

    service = create_ingestion_service("book_snapshot_25", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    bronze_rel_path = (
        "exchange=binance/type=book_snapshot_25/date=2024-05-01/symbol=BTCUSDT/book_snapshots.csv"
    )
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    raw_df = _sample_tardis_book_snapshots_csv()
    raw_df.write_csv(bronze_path)

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="book_snapshot_25",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=1000,
        last_modified_ts=1714557600,
        sha256="c" * 64,
        date=date(2024, 5, 1),
    )

    file_id = sample_manifest_repo.resolve_file_id(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")

    # Should be quarantined
    assert result.row_count == 0
    assert result.error_message is not None
    assert (
        "missing_symbol" in result.error_message
        or "invalid_validity_window" in result.error_message
        or "All symbols quarantined" in result.error_message
    )
