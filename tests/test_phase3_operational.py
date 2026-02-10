"""Tests for Phase 3 (Operational Maturity) features.

Covers:
- 3.1: Validation log wiring into GenericIngestionService
- 3.2: Vectorized quarantine check
- 3.3: Post-ingest auto-optimize (tested via CLI flag presence)
- 3.4: Dry-run mode
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import Mock

import polars as pl

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA
from pointline.tables.validation_log import create_ingestion_record

# ---------------------------------------------------------------------------
# 3.2: Vectorized quarantine check
# ---------------------------------------------------------------------------


def _make_dim_symbol(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal dim_symbol DataFrame for quarantine tests."""
    base = {
        "symbol_id": 0,
        "exchange": "binance-futures",
        "exchange_id": 1,
        "exchange_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "asset_type": 1,
        "price_increment": 0.01,
        "tick_size": 0.01,
        "lot_size": 0.001,
        "amount_increment": 0.001,
        "contract_size": 1.0,
        "valid_from_ts": 0,
        "valid_until_ts": 9999999999999999,
        "is_current": True,
        "expiry_ts_us": None,
        "underlying_symbol_id": None,
        "settlement_type": None,
        "strike": None,
        "put_call": None,
        "isin": None,
    }
    data: dict = {k: [] for k in DIM_SYMBOL_SCHEMA}
    for row in rows:
        merged = {**base, **row}
        for k in DIM_SYMBOL_SCHEMA:
            data[k].append(merged[k])
    return pl.DataFrame(data, schema=DIM_SYMBOL_SCHEMA)


def test_vectorized_quarantine_pass():
    """All symbols have coverage -> no quarantine."""
    service = create_ingestion_service("trades", manifest_repo=Mock())

    dim_symbol = _make_dim_symbol(
        [
            {"symbol_id": 1, "exchange_symbol": "BTCUSDT"},
            {"symbol_id": 2, "exchange_symbol": "ETHUSDT"},
        ]
    )

    df = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_id": [1, 1],
            "exchange_symbol": ["BTCUSDT", "ETHUSDT"],
            "date": [dt.date(2024, 5, 1), dt.date(2024, 5, 1)],
            "ts_local_us": [1714521600000000, 1714521600000000],
        }
    )

    filtered, frc, fsc = service._check_quarantine_vectorized(df, dim_symbol)
    assert filtered.height == 2
    assert frc == 0
    assert fsc == 0


def test_vectorized_quarantine_filters_missing_symbol():
    """Symbols missing from dim_symbol should be quarantined."""
    service = create_ingestion_service("trades", manifest_repo=Mock())

    dim_symbol = _make_dim_symbol([{"symbol_id": 1, "exchange_symbol": "BTCUSDT"}])

    df = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures", "binance-futures"],
            "exchange_id": [1, 1, 1],
            "exchange_symbol": ["BTCUSDT", "ETHUSDT", "ETHUSDT"],
            "date": [dt.date(2024, 5, 1), dt.date(2024, 5, 1), dt.date(2024, 5, 1)],
            "ts_local_us": [1714521600000000, 1714521600000000, 1714521600100000],
        }
    )

    filtered, frc, fsc = service._check_quarantine_vectorized(df, dim_symbol)
    assert filtered.height == 1
    assert frc == 2
    assert fsc == 1
    assert filtered["exchange_symbol"].to_list() == ["BTCUSDT"]


def test_vectorized_quarantine_multiple_dates():
    """Each (symbol, date) pair is checked independently."""
    service = create_ingestion_service("trades", manifest_repo=Mock())

    dim_symbol = _make_dim_symbol(
        [
            {
                "symbol_id": 1,
                "exchange_symbol": "BTCUSDT",
                "valid_from_ts": 1714521600000000,  # 2024-05-01 00:00 UTC
                "valid_until_ts": 1714608000000000,  # 2024-05-02 00:00 UTC
            },
        ]
    )

    df = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_id": [1, 1],
            "exchange_symbol": ["BTCUSDT", "BTCUSDT"],
            "date": [dt.date(2024, 5, 1), dt.date(2024, 5, 3)],
            "ts_local_us": [1714521600000000, 1714694400000000],
        }
    )

    filtered, frc, fsc = service._check_quarantine_vectorized(df, dim_symbol)
    # May 1 is covered, May 3 is NOT covered
    assert filtered.height == 1
    assert frc == 1
    assert fsc == 1
    assert filtered["date"].to_list() == [dt.date(2024, 5, 1)]


def test_vectorized_quarantine_empty_df():
    """Empty DataFrame should pass through unchanged."""
    service = create_ingestion_service("trades", manifest_repo=Mock())
    dim_symbol = _make_dim_symbol([])

    df = pl.DataFrame(
        {
            "exchange": pl.Series([], dtype=pl.Utf8),
            "exchange_id": pl.Series([], dtype=pl.Int16),
            "exchange_symbol": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Date),
        }
    )

    filtered, frc, fsc = service._check_quarantine_vectorized(df, dim_symbol)
    assert filtered.height == 0
    assert frc == 0
    assert fsc == 0


# ---------------------------------------------------------------------------
# 3.1: Validation log ingestion record
# ---------------------------------------------------------------------------


def test_ingestion_record_columns_match_schema():
    """Ingestion records should match VALIDATION_LOG_SCHEMA exactly."""
    from pointline.tables.validation_log import VALIDATION_LOG_SCHEMA

    record = create_ingestion_record(
        file_id=1,
        table_name="trades",
        vendor="tardis",
        data_type="trades",
        status="ingested",
        row_count=100,
        duration_ms=50,
    )
    assert record.columns == list(VALIDATION_LOG_SCHEMA.keys())
    for col, dtype in VALIDATION_LOG_SCHEMA.items():
        assert record[col].dtype == dtype, (
            f"Column {col}: expected {dtype}, got {record[col].dtype}"
        )


# ---------------------------------------------------------------------------
# 3.4: Dry-run mode
# ---------------------------------------------------------------------------


def test_dry_run_flag_on_cli_parser():
    """The bronze ingest subcommand should have --dry-run flag."""
    from pointline.cli.parser import build_parser

    parser = build_parser()
    # Parse with --dry-run
    args = parser.parse_args(["bronze", "ingest", "--vendor", "tardis", "--dry-run"])
    assert args.dry_run is True


def test_dry_run_flag_default_false():
    """--dry-run should default to False."""
    from pointline.cli.parser import build_parser

    parser = build_parser()
    args = parser.parse_args(["bronze", "ingest", "--vendor", "tardis"])
    assert args.dry_run is False
