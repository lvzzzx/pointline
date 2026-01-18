"""Tests for trades domain logic and ingestion service."""

import polars as pl
import pytest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, MagicMock

from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA, scd2_bootstrap
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata, IngestionResult
from pointline.services.trades_service import TradesIngestionService
from pointline.tables.trades import (
    TRADES_SCHEMA,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_UNKNOWN,
    decode_fixed_point,
    encode_fixed_point,
    normalize_trades_schema,
    parse_tardis_trades_csv,
    resolve_symbol_ids,
    validate_trades,
    required_trades_columns,
)


def _sample_tardis_trades_csv() -> pl.DataFrame:
    """Create a sample Tardis trades CSV DataFrame.
    
    Tardis provides timestamps as microseconds since epoch (integers).
    """
    # 2024-05-01T10:00:00.000000Z = 1714557600000000 microseconds
    base_ts = 1714557600000000
    return pl.DataFrame({
        "local_timestamp": [
            base_ts,
            base_ts + 1_000_000,  # +1 second
            base_ts + 2_000_000,  # +2 seconds
        ],
        "timestamp": [
            base_ts + 100_000,  # +0.1 second
            base_ts + 1_100_000,  # +1.1 seconds
            base_ts + 2_100_000,  # +2.1 seconds
        ],
        "trade_id": ["t1", "t2", "t3"],
        "side": ["buy", "sell", "buy"],
        "price": [50000.0, 50001.0, 50002.0],
        "amount": [0.1, 0.2, 0.15],
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


def test_parse_tardis_trades_csv_basic():
    """Test parsing standard Tardis trades CSV format."""
    raw_df = _sample_tardis_trades_csv()
    parsed = parse_tardis_trades_csv(raw_df)
    
    assert parsed.height == 3
    assert "ts_local_us" in parsed.columns
    assert "ts_exch_us" in parsed.columns
    assert "trade_id" in parsed.columns
    assert "side" in parsed.columns
    assert "price" in parsed.columns
    assert "qty" in parsed.columns
    
    # Check timestamps are parsed correctly
    assert parsed["ts_local_us"].dtype == pl.Int64
    assert parsed["ts_exch_us"].dtype == pl.Int64
    assert parsed["ts_local_us"].min() > 0
    
    # Check side encoding
    assert parsed["side"].dtype == pl.UInt8
    assert set(parsed["side"].unique().to_list()) == {SIDE_BUY, SIDE_SELL}


def test_parse_tardis_trades_csv_preserves_file_line_number():
    """Ensure file_line_number is preserved when present."""
    raw_df = _sample_tardis_trades_csv().with_columns(
        pl.Series("file_line_number", [2, 3, 4], dtype=pl.Int32)
    )
    parsed = parse_tardis_trades_csv(raw_df)

    assert "file_line_number" in parsed.columns
    assert parsed["file_line_number"].to_list() == [2, 3, 4]


def test_parse_tardis_trades_csv_alternative_columns():
    """Test parsing with alternative column name variations."""
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z
    raw_df = pl.DataFrame({
        "localTimestamp": [base_ts],
        "tradeId": ["t1"],
        "takerSide": ["sell"],
        "tradePrice": [50000.0],
        "quantity": [0.1],
    })
    
    parsed = parse_tardis_trades_csv(raw_df)
    
    assert parsed.height == 1
    assert parsed["side"][0] == SIDE_SELL
    assert parsed["price"][0] == 50000.0
    assert parsed["qty"][0] == 0.1


def test_parse_tardis_trades_csv_missing_optional():
    """Test parsing when optional columns are missing."""
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z
    raw_df = pl.DataFrame({
        "local_timestamp": [base_ts],
        "side": ["buy"],
        "price": [50000.0],
        "amount": [0.1],
    })
    
    parsed = parse_tardis_trades_csv(raw_df)
    
    assert parsed.height == 1
    assert parsed["ts_exch_us"][0] is None
    assert parsed["trade_id"][0] is None


def test_parse_tardis_trades_csv_side_encoding():
    """Test side string to code mapping."""
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z
    raw_df = pl.DataFrame({
        "local_timestamp": [base_ts] * 4,
        "side": ["buy", "sell", "unknown", None],
        "price": [50000.0] * 4,
        "amount": [0.1] * 4,
    })
    
    parsed = parse_tardis_trades_csv(raw_df)
    
    assert parsed["side"][0] == SIDE_BUY
    assert parsed["side"][1] == SIDE_SELL
    assert parsed["side"][2] == SIDE_UNKNOWN
    assert parsed["side"][3] == SIDE_UNKNOWN


def test_normalize_trades_schema():
    """Test schema normalization."""
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "trade_id": ["t1"],
        "side": [0],
        "price_int": [5000000],
        "qty_int": [10000],
        "flags": [0],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    normalized = normalize_trades_schema(df)
    
    assert normalized["date"].dtype == pl.Date
    assert normalized["exchange_id"].dtype == pl.Int16  # Delta Lake stores as Int16 (not UInt16)
    assert normalized["symbol_id"].dtype == pl.Int64  # Delta Lake stores as Int64
    assert normalized["ts_local_us"].dtype == pl.Int64


def test_normalize_trades_schema_missing_required():
    """Test that missing required columns raise error."""
    df = pl.DataFrame({
        "exchange_id": [1],
        # Missing other required columns
    })
    
    with pytest.raises(ValueError, match="missing required columns"):
        normalize_trades_schema(df)


def test_validate_trades_basic():
    """Test basic validation of trades data."""
    df = pl.DataFrame({
        "price_int": [5000000, 5000100, -100],  # Last one invalid
        "qty_int": [10000, 20000, 5000],
        "ts_local_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "ts_exch_us": [1714550400000000, 1714550401000000, 1714550402000000],
        "side": [0, 1, 2],
        "exchange": ["binance", "binance", "binance"],
        "exchange_id": [1, 1, 1],
        "symbol_id": [100, 100, 100],
    })
    
    validated = validate_trades(df)
    
    # Should filter out the negative price
    assert validated.height == 2
    assert validated["price_int"].min() > 0


def test_validate_trades_invalid_side():
    """Test validation filters invalid side codes."""
    df = pl.DataFrame({
        "price_int": [5000000] * 3,
        "qty_int": [10000] * 3,
        "ts_local_us": [1714550400000000] * 3,
        "ts_exch_us": [1714550400000000] * 3,
        "side": [0, 1, 99],  # Last one invalid
        "exchange": ["binance"] * 3,
        "exchange_id": [1] * 3,
        "symbol_id": [100] * 3,
    })
    
    validated = validate_trades(df)
    
    assert validated.height == 2


def test_encode_fixed_point():
    """Test fixed-point encoding using dim_symbol metadata."""
    dim_symbol = _sample_dim_symbol()
    # dim_symbol already has symbol_id from scd2_bootstrap
    
    df = pl.DataFrame({
        "symbol_id": dim_symbol["symbol_id"].to_list() * 3,
        "price": [50000.0, 50001.0, 50002.0],
        "qty": [0.1, 0.2, 0.15],
    })
    
    encoded = encode_fixed_point(df, dim_symbol)
    
    assert "price_int" in encoded.columns
    assert "qty_int" in encoded.columns
    
    # With price_increment=0.01, price=50000.0 should become 5000000
    assert encoded["price_int"][0] == 5000000
    # With amount_increment=0.00001, qty=0.1 should become 10000
    assert encoded["qty_int"][0] == 10000


def test_encode_fixed_point_missing_symbol():
    """Test that missing symbol_ids raise error."""
    dim_symbol = _sample_dim_symbol()
    # dim_symbol already has symbol_id from scd2_bootstrap
    
    # Use a symbol_id that doesn't exist
    df = pl.DataFrame({
        "symbol_id": [999999],  # Not in dim_symbol
        "price": [50000.0],
        "qty": [0.1],
    })
    
    with pytest.raises(ValueError, match="symbol_ids not found"):
        encode_fixed_point(df, dim_symbol)


def test_decode_fixed_point():
    """Decode fixed-point integers back to float price/qty columns."""
    dim_symbol = _sample_dim_symbol()
    symbol_id = dim_symbol["symbol_id"][0]

    df = pl.DataFrame(
        {
            "symbol_id": [symbol_id],
            "price_int": [5000000],
            "qty_int": [10000],
        }
    )

    decoded = decode_fixed_point(df, dim_symbol)

    assert "price_int" not in decoded.columns
    assert "qty_int" not in decoded.columns
    assert decoded["price"].dtype == pl.Float64
    assert decoded["qty"].dtype == pl.Float64
    assert decoded["price"][0] == 50000.0
    assert decoded["qty"][0] == 0.1


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


def test_trades_service_validate():
    """Test TradesIngestionService.validate() method."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = TradesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "price_int": [5000000, -100],
        "qty_int": [10000, 5000],
        "ts_local_us": [1714550400000000, 1714550401000000],
        "ts_exch_us": [1714550400000000, 1714550401000000],
        "side": [0, 1],
        "exchange": ["binance", "binance"],
        "exchange_id": [1, 1],
        "symbol_id": [100, 100],
    })
    
    validated = service.validate(df)
    
    assert validated.height == 1  # Negative price filtered


def test_trades_service_compute_state():
    """Test TradesIngestionService.compute_state() method."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = TradesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "trade_id": ["t1"],
        "side": [0],
        "price_int": [5000000],
        "qty_int": [10000],
        "flags": [0],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    result = service.compute_state(df)
    
    assert result.height == 1
    assert "date" in result.columns


def test_trades_service_write():
    """Test TradesIngestionService.write() method."""
    repo = Mock(spec=BaseDeltaRepository)
    repo.append = Mock()
    dim_repo = Mock(spec=BaseDeltaRepository)
    manifest_repo = Mock()
    
    service = TradesIngestionService(repo, dim_repo, manifest_repo)
    
    df = pl.DataFrame({
        "date": [date(2024, 5, 1)],
        "exchange": ["binance"],
        "exchange_id": [1],
        "symbol_id": [100],
        "ts_local_us": [1714550400000000],
        "ts_exch_us": [1714550400100000],
        "ingest_seq": [1],
        "trade_id": ["t1"],
        "side": [0],
        "price_int": [5000000],
        "qty_int": [10000],
        "flags": [0],
        "file_id": [1],
        "file_line_number": [1],
    })
    
    service.write(df)
    
    repo.append.assert_called_once()


def test_trades_service_ingest_file_quarantine():
    """Test that files are quarantined when symbol metadata is missing."""
    repo = Mock(spec=BaseDeltaRepository)
    dim_repo = Mock(spec=BaseDeltaRepository)
    dim_repo.read_all.return_value = pl.DataFrame(schema=DIM_SYMBOL_SCHEMA)  # Empty
    manifest_repo = Mock()
    
    service = TradesIngestionService(repo, dim_repo, manifest_repo)
    
    meta = BronzeFileMetadata(
        vendor="tardis",
        exchange="binance",
        data_type="trades",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path="test.csv.gz",
        file_size_bytes=1000,
        last_modified_ts=1000000,
        sha256="a" * 64,
    )
    
    # Create a temporary CSV file
    import tempfile
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z in microseconds
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("local_timestamp,trade_id,side,price,amount\n")
        f.write(f"{base_ts},t1,buy,50000.0,0.1\n")
        temp_path = Path(f.name)
    
    try:
        # Mock the bronze path
        from pointline.config import get_bronze_root
        bronze_path = get_bronze_root("tardis") / meta.bronze_file_path
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


def test_trades_service_ingest_file_success():
    """Test successful file ingestion."""
    repo = Mock(spec=BaseDeltaRepository)
    repo.append = Mock()
    
    dim_repo = Mock(spec=BaseDeltaRepository)
    dim_symbol = _sample_dim_symbol()
    # dim_symbol already has symbol_id from scd2_bootstrap
    dim_repo.read_all.return_value = dim_symbol
    
    manifest_repo = Mock()
    
    service = TradesIngestionService(repo, dim_repo, manifest_repo)
    
    meta = BronzeFileMetadata(
        vendor="tardis",
        exchange="binance",
        data_type="trades",
        symbol="BTCUSDT",
        date=date(2024, 5, 1),
        bronze_file_path="test.csv.gz",
        file_size_bytes=1000,
        last_modified_ts=1000000,
        sha256="b" * 64,
    )
    
    # Create a temporary CSV file
    import tempfile
    base_ts = 1714557600000000  # 2024-05-01T10:00:00.000000Z in microseconds
    exch_ts = base_ts + 100_000  # +0.1 second
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("local_timestamp,timestamp,trade_id,side,price,amount\n")
        f.write(f"{base_ts},{exch_ts},t1,buy,50000.0,0.1\n")
        temp_path = Path(f.name)
    
    try:
        # Mock the bronze path
        from pointline.config import get_bronze_root
        bronze_path = get_bronze_root("tardis") / meta.bronze_file_path
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


def test_required_trades_columns():
    """Test that required_trades_columns() returns all schema columns."""
    cols = required_trades_columns()
    assert len(cols) == len(TRADES_SCHEMA)
    assert "date" in cols
    assert "exchange" in cols
    assert "exchange_id" in cols
    assert "symbol_id" in cols
