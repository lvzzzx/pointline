"""Tests for quotes domain logic and ingestion service."""

import polars as pl
import pytest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA, scd2_bootstrap
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.services.quotes_service import QuotesIngestionService
from pointline.quotes import (
    QUOTES_SCHEMA,
    encode_fixed_point,
    normalize_quotes_schema,
    parse_tardis_quotes_csv,
    resolve_symbol_ids,
    validate_quotes,
    required_quotes_columns,
)


def _sample_tardis_quotes_csv() -> pl.DataFrame:
    """Create a sample Tardis quotes CSV DataFrame.
    
    Tardis provides timestamps as microseconds since epoch (integers).
    Tardis schema uses exact column names.
    """
    # 2024-05-01T10:00:00.000000Z = 1714557600000000 microseconds
    base_ts = 1714557600000000
    return pl.DataFrame({
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
        "timestamp": [
            base_ts + 100_000,  # +0.1 second
            base_ts + 1_100_000,  # +1.1 seconds
            base_ts + 2_100_000,  # +2.1 seconds
        ],
        "local_timestamp": [
            base_ts,
            base_ts + 1_000_000,  # +1 second
            base_ts + 2_000_000,  # +2 seconds
        ],
        "bid_price": [50000.0, 50001.0, 50002.0],
        "bid_amount": [0.1, 0.2, 0.15],
        "ask_price": [50000.5, 50001.5, 50002.5],
        "ask_amount": [0.15, 0.25, 0.2],
    })


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


def test_parse_tardis_quotes_csv_basic():
    """Test parsing standard Tardis quotes CSV format."""
    raw_df = _sample_tardis_quotes_csv()
    parsed = parse_tardis_quotes_csv(raw_df)
    
    assert parsed.height == 3
    assert "ts_local_us" in parsed.columns
    assert "ts_exch_us" in parsed.columns
    assert "bid_price" in parsed.columns
    assert "bid_amount" in parsed.columns
    assert "ask_price" in parsed.columns
    assert "ask_amount" in parsed.columns
    
    # Check timestamps are parsed correctly
    assert parsed["ts_local_us"].dtype == pl.Int64
    assert parsed["ts_exch_us"].dtype == pl.Int64
    assert parsed["ts_local_us"].min() > 0
    
    # Check prices and amounts are floats
    assert parsed["bid_price"].dtype == pl.Float64
    assert parsed["bid_amount"].dtype == pl.Float64
    assert parsed["ask_price"].dtype == pl.Float64
    assert parsed["ask_amount"].dtype == pl.Float64


def test_parse_tardis_quotes_csv_missing_required():
    """Test parsing fails when required columns are missing."""
    base_ts = 1714557600000000
    raw_df = pl.DataFrame({
        "exchange": ["binance"],
        "symbol": ["BTCUSDT"],
        # Missing timestamp or local_timestamp
        "bid_price": [50000.0],
        "bid_amount": [0.1],
        "ask_price": [50000.5],
        "ask_amount": [0.15],
    })
    
    with pytest.raises(ValueError, match="missing required columns"):
        parse_tardis_quotes_csv(raw_df)


def test_parse_tardis_quotes_csv_empty_values():
    """Test parsing handles empty bid/ask values."""
    base_ts = 1714557600000000
    raw_df = pl.DataFrame({
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
        "timestamp": [base_ts + 100_000] * 3,
        "local_timestamp": [base_ts] * 3,
        "bid_price": [50000.0, None, ""],  # Empty string should become null
        "bid_amount": [0.1, None, ""],
        "ask_price": [50000.5, "", None],  # Empty string should become null
        "ask_amount": [0.15, "", None],
    })
    
    parsed = parse_tardis_quotes_csv(raw_df)
    
    assert parsed.height == 3
    # First row should have both bid and ask
    assert parsed["bid_price"][0] == 50000.0
    assert parsed["ask_price"][0] == 50000.5
    # Other rows should have nulls
    assert parsed["bid_price"][1] is None or parsed["bid_price"][1] == ""
    assert parsed["ask_price"][2] is None or parsed["ask_price"][2] == ""


def test_normalize_quotes_schema():
    """Test schema normalization."""
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "bid_px_int": [5000000],
        "bid_sz_int": [10000],
        "ask_px_int": [5000050],
        "ask_sz_int": [15000],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    normalized = normalize_quotes_schema(df)
    
    assert normalized["date"].dtype == pl.Date
    assert normalized["exchange_id"].dtype == pl.Int16
    assert normalized["symbol_id"].dtype == pl.Int64
    assert normalized["ts_local_us"].dtype == pl.Int64
    assert normalized["bid_px_int"].dtype == pl.Int64
    assert normalized["bid_sz_int"].dtype == pl.Int64
    assert normalized["ask_px_int"].dtype == pl.Int64
    assert normalized["ask_sz_int"].dtype == pl.Int64


def test_normalize_quotes_schema_missing_required():
    """Test that missing required columns raise error."""
    df = pl.DataFrame({
        "exchange_id": [1],
        # Missing other required columns
    })
    
    with pytest.raises(ValueError, match="missing required columns"):
        normalize_quotes_schema(df)


def test_validate_quotes_basic():
    """Test basic validation of quotes data."""
    df = pl.DataFrame({
        "bid_px_int": [5000000, 5000100, -100],  # Last one invalid
        "bid_sz_int": [10000, 20000, 5000],
        "ask_px_int": [5000050, 5000150, 5000200],
        "ask_sz_int": [15000, 25000, 30000],
        "ts_local_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "exchange": ["binance", "binance", "binance"],
        "exchange_id": [1, 1, 1],
        "symbol_id": [100, 100, 100],
    })
    
    validated = validate_quotes(df)
    
    # Should filter out the negative bid price
    assert validated.height == 2
    assert validated["bid_px_int"].min() > 0


def test_validate_quotes_crossed_book():
    """Test validation filters crossed book (bid >= ask)."""
    df = pl.DataFrame({
        "bid_px_int": [5000000, 5000100, 5000200],  # Last one crosses
        "bid_sz_int": [10000, 20000, 30000],
        "ask_px_int": [5000050, 5000150, 5000100],  # Last ask < bid (crossed)
        "ask_sz_int": [15000, 25000, 35000],
        "ts_local_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "exchange": ["binance", "binance", "binance"],
        "exchange_id": [1, 1, 1],
        "symbol_id": [100, 100, 100],
    })
    
    validated = validate_quotes(df)
    
    # Should filter out the crossed book
    assert validated.height == 2
    # All remaining should have bid < ask
    for row in validated.iter_rows(named=True):
        assert row["bid_px_int"] < row["ask_px_int"]


def test_validate_quotes_partial_quotes():
    """Test validation handles partial quotes (only bid or only ask)."""
    df = pl.DataFrame({
        "bid_px_int": [5000000, None, 5000200],
        "bid_sz_int": [10000, None, 30000],
        "ask_px_int": [5000050, 5000150, None],
        "ask_sz_int": [15000, 25000, None],
        "ts_local_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "exchange": ["binance", "binance", "binance"],
        "exchange_id": [1, 1, 1],
        "symbol_id": [100, 100, 100],
    })
    
    validated = validate_quotes(df)
    
    # Should keep all rows (each has at least one of bid or ask)
    assert validated.height == 3


def test_validate_quotes_both_missing():
    """Test validation filters rows where both bid and ask are missing."""
    df = pl.DataFrame({
        "bid_px_int": [5000000, None, 5000200],
        "bid_sz_int": [10000, None, 30000],
        "ask_px_int": [5000050, None, 5000250],
        "ask_sz_int": [15000, None, 35000],
        "ts_local_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "exchange": ["binance", "binance", "binance"],
        "exchange_id": [1, 1, 1],
        "symbol_id": [100, 100, 100],
    })
    
    # Set second row to have both null
    df = df.with_columns([
        pl.when(pl.int_range(0, df.height) == 1)
        .then(None)
        .otherwise(pl.col("bid_px_int"))
        .alias("bid_px_int"),
        pl.when(pl.int_range(0, df.height) == 1)
        .then(None)
        .otherwise(pl.col("bid_sz_int"))
        .alias("bid_sz_int"),
        pl.when(pl.int_range(0, df.height) == 1)
        .then(None)
        .otherwise(pl.col("ask_px_int"))
        .alias("ask_px_int"),
        pl.when(pl.int_range(0, df.height) == 1)
        .then(None)
        .otherwise(pl.col("ask_sz_int"))
        .alias("ask_sz_int"),
    ])
    # Ensure exchange column exists
    if "exchange" not in df.columns:
        df = df.with_columns(pl.lit("binance", dtype=pl.Utf8).alias("exchange"))
    
    validated = validate_quotes(df)
    
    # Should filter out the row with both missing
    assert validated.height == 2


def test_encode_fixed_point():
    """Test fixed-point encoding using dim_symbol metadata."""
    dim_symbol = _sample_dim_symbol()
    # dim_symbol already has symbol_id from scd2_bootstrap
    
    df = pl.DataFrame({
        "symbol_id": dim_symbol["symbol_id"].to_list() * 3,
        "bid_price": [50000.0, 50001.0, 50002.0],
        "bid_amount": [0.1, 0.2, 0.15],
        "ask_price": [50000.5, 50001.5, 50002.5],
        "ask_amount": [0.15, 0.25, 0.2],
    })
    
    encoded = encode_fixed_point(df, dim_symbol)
    
    assert "bid_px_int" in encoded.columns
    assert "bid_sz_int" in encoded.columns
    assert "ask_px_int" in encoded.columns
    assert "ask_sz_int" in encoded.columns
    
    # With price_increment=0.01, price=50000.0 should become 5000000
    assert encoded["bid_px_int"][0] == 5000000
    # With amount_increment=0.00001, amount=0.1 should become 10000
    assert encoded["bid_sz_int"][0] == 10000
    assert encoded["ask_px_int"][0] == 5000050
    assert encoded["ask_sz_int"][0] == 15000


def test_encode_fixed_point_with_nulls():
    """Test fixed-point encoding preserves nulls for empty bid/ask."""
    dim_symbol = _sample_dim_symbol()
    
    df = pl.DataFrame({
        "symbol_id": dim_symbol["symbol_id"].to_list() * 2,
        "bid_price": [50000.0, None],
        "bid_amount": [0.1, None],
        "ask_price": [50000.5, 50001.5],
        "ask_amount": [0.15, 0.25],
    })
    
    encoded = encode_fixed_point(df, dim_symbol)
    
    # First row should have all values
    assert encoded["bid_px_int"][0] == 5000000
    assert encoded["ask_px_int"][0] == 5000050
    # Second row should have null bid
    assert encoded["bid_px_int"][1] is None
    assert encoded["bid_sz_int"][1] is None
    assert encoded["ask_px_int"][1] == 5000150


def test_encode_fixed_point_missing_symbol():
    """Test that missing symbol_ids raise error."""
    dim_symbol = _sample_dim_symbol()
    
    # Use a symbol_id that doesn't exist
    df = pl.DataFrame({
        "symbol_id": [999999],  # Not in dim_symbol
        "bid_price": [50000.0],
        "bid_amount": [0.1],
        "ask_price": [50000.5],
        "ask_amount": [0.15],
    })
    
    with pytest.raises(ValueError, match="symbol_ids not found"):
        encode_fixed_point(df, dim_symbol)


def test_resolve_symbol_ids():
    """Test symbol ID resolution using as-of join."""
    dim_symbol = _sample_dim_symbol()
    
    # Create data with timestamps
    data = pl.DataFrame({
        "ts_local_us": [1714550400000000, 1714550401000000],
        "exchange_id": [1, 1],
        "exchange_symbol": ["BTCUSDT", "BTCUSDT"],
    })
    
    resolved = resolve_symbol_ids(data, dim_symbol, exchange_id=1, exchange_symbol="BTCUSDT")
    
    assert "symbol_id" in resolved.columns
    assert resolved.height == 2


def test_quotes_service_validate():
    """Test QuotesIngestionService.validate() method."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = QuotesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "bid_px_int": [5000000, -100],
        "bid_sz_int": [10000, 5000],
        "ask_px_int": [5000050, 5000100],
        "ask_sz_int": [15000, 20000],
        "ts_local_us": [1714550400000000, 1714550401000000],
        "exchange": ["binance", "binance"],
        "exchange_id": [1, 1],
        "symbol_id": [100, 100],
    })
    
    validated = service.validate(df)
    
    assert validated.height == 1  # Negative bid price filtered


def test_quotes_service_compute_state():
    """Test QuotesIngestionService.compute_state() method."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = QuotesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "bid_px_int": [5000000],
        "bid_sz_int": [10000],
        "ask_px_int": [5000050],
        "ask_sz_int": [15000],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    result = service.compute_state(df)
    
    assert result.height == 1
    assert "date" in result.columns


def test_quotes_service_write():
    """Test QuotesIngestionService.write() method."""
    repo = Mock(spec=BaseDeltaRepository)
    repo.append = Mock()
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = QuotesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "bid_px_int": [5000000],
        "bid_sz_int": [10000],
        "ask_px_int": [5000050],
        "ask_sz_int": [15000],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    service.write(df)
    
    repo.append.assert_called_once()


def test_quotes_service_ingest_file_quarantine():
    """Test that files are quarantined when symbol metadata is missing."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    dim_repo.read_all.return_value = pl.DataFrame(schema=DIM_SYMBOL_SCHEMA)  # Empty
    manifest_repo = Mock()
    
    service = QuotesIngestionService(repo, dim_repo, manifest_repo)
    
    meta = BronzeFileMetadata(
        exchange="binance",
        data_type="quotes",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path="test.csv.gz",
        file_size_bytes=1000,
        last_modified_ts=1000000,
    )
    
    # Create a temporary CSV file
    import tempfile
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z in microseconds
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("exchange,symbol,timestamp,local_timestamp,bid_price,bid_amount,ask_price,ask_amount\n")
        f.write(f"binance,BTCUSDT,{base_ts + 100_000},{base_ts},50000.0,0.1,50000.5,0.15\n")
        temp_path = Path(f.name)
    
    try:
        # Mock the bronze path
        from pointline.config import LAKE_ROOT
        bronze_path = LAKE_ROOT / meta.bronze_file_path
        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy(temp_path, bronze_path)
        
        result = service.ingest_file(meta, file_id=1)
        
        # Should be quarantined
        assert result.row_count == 0
        assert result.error_message is not None
        assert "missing_symbol" in result.error_message or "invalid_validity_window" in result.error_message
        
    finally:
        temp_path.unlink(missing_ok=True)
        bronze_path.unlink(missing_ok=True)


def test_quotes_service_ingest_file_success():
    """Test successful file ingestion."""
    repo = Mock(spec=BaseDeltaRepository)
    repo.append = Mock()
    
    dim_repo = Mock(spec=BaseDeltaRepository)
    dim_symbol = _sample_dim_symbol()
    # dim_symbol already has symbol_id from scd2_bootstrap
    dim_repo.read_all.return_value = dim_symbol
    
    manifest_repo = Mock()
    
    service = QuotesIngestionService(repo, dim_repo, manifest_repo)
    
    meta = BronzeFileMetadata(
        exchange="binance",
        data_type="quotes",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path="test.csv.gz",
        file_size_bytes=1000,
        last_modified_ts=1000000,
    )
    
    # Create a temporary CSV file
    import tempfile
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z in microseconds
    exch_ts = base_ts + 100_000  # +0.1 second
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("exchange,symbol,timestamp,local_timestamp,bid_price,bid_amount,ask_price,ask_amount\n")
        f.write(f"binance,BTCUSDT,{exch_ts},{base_ts},50000.0,0.1,50000.5,0.15\n")
        temp_path = Path(f.name)
    
    try:
        # Mock the bronze path
        from pointline.config import LAKE_ROOT
        bronze_path = LAKE_ROOT / meta.bronze_file_path
        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy(temp_path, bronze_path)
        
        result = service.ingest_file(meta, file_id=1)
        
        # Should succeed
        assert result.row_count == 1
        assert result.error_message is None
        assert result.ts_local_min_us > 0
        assert result.ts_local_max_us > 0
        
        # Verify append was called
        repo.append.assert_called_once()
        
    finally:
        temp_path.unlink(missing_ok=True)
        bronze_path.unlink(missing_ok=True)


def test_required_quotes_columns():
    """Test that required_quotes_columns() returns all schema columns."""
    cols = required_quotes_columns()
    assert len(cols) == len(QUOTES_SCHEMA)
    assert "date" in cols
    assert "exchange" in cols
    assert "exchange_id" in cols
    assert "symbol_id" in cols
    assert "bid_px_int" in cols
    assert "bid_sz_int" in cols
    assert "ask_px_int" in cols
    assert "ask_sz_int" in cols
