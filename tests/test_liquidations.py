from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata
from pointline.io.vendors.tardis.parsers.liquidations import parse_tardis_liquidations_csv
from pointline.tables.liquidations import (
    LIQUIDATIONS_SCHEMA,
    encode_fixed_point,
    normalize_liquidations_schema,
    validate_liquidations,
)


def _sample_raw_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1714521600000000],
            "local_timestamp": [1714521600037264],
            "id": ["liq-1"],
            "side": ["sell"],
            "price": ["60651.1"],
            "amount": ["0.005"],
        }
    )


def _sample_raw_df_empty_id() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1714521600000000],
            "local_timestamp": [1714521600037264],
            "id": [""],
            "side": ["buy"],
            "price": ["60651.1"],
            "amount": ["0.005"],
        }
    )


def _sample_dim_symbol() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol_id": 1001,
                "exchange_id": 2,
                "exchange": "binance-futures",
                "exchange_symbol": "BTCUSDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "asset_type": 0,
                "tick_size": 0.1,
                "lot_size": 0.001,
                "price_increment": 0.1,
                "amount_increment": 0.001,
                "contract_size": 1.0,
                "expiry_ts_us": None,
                "underlying_symbol_id": None,
                "strike": None,
                "put_call": None,
                "valid_from_ts": 0,
                "valid_until_ts": 2**63 - 1,
                "is_current": True,
            }
        ],
        schema=DIM_SYMBOL_SCHEMA,
    )


def test_parse_tardis_liquidations_csv() -> None:
    parsed = parse_tardis_liquidations_csv(_sample_raw_df())
    assert set(parsed.columns) == {
        "exchange_symbol",
        "ts_local_us",
        "ts_exch_us",
        "liq_id",
        "side_raw",
        "price_px",
        "qty",
    }
    assert parsed["exchange_symbol"][0] == "BTCUSDT"
    assert parsed["liq_id"][0] == "liq-1"


def test_parse_tardis_liquidations_csv_empty_id_to_null() -> None:
    parsed = parse_tardis_liquidations_csv(_sample_raw_df_empty_id())
    assert parsed["liq_id"][0] is None


def test_parse_tardis_liquidations_csv_preserves_file_line_number() -> None:
    raw = _sample_raw_df().with_columns(pl.lit(42, dtype=pl.Int32).alias("file_line_number"))
    parsed = parse_tardis_liquidations_csv(raw)
    assert parsed.columns[0] == "file_line_number"
    assert parsed["file_line_number"][0] == 42


def test_normalize_encode_and_validate_liquidations() -> None:
    parsed = parse_tardis_liquidations_csv(_sample_raw_df())
    with_meta = parsed.with_columns(
        [
            pl.lit("binance-futures").alias("exchange"),
            pl.lit(2, dtype=pl.Int16).alias("exchange_id"),
            pl.lit(1001, dtype=pl.Int64).alias("symbol_id"),
            pl.lit(1, dtype=pl.Int32).alias("file_id"),
            pl.lit(1, dtype=pl.Int32).alias("file_line_number"),
            pl.lit(date(2024, 5, 1)).alias("date"),
        ]
    )
    encoded = encode_fixed_point(with_meta, _sample_dim_symbol())
    normalized = normalize_liquidations_schema(encoded)
    assert list(normalized.schema.keys()) == list(LIQUIDATIONS_SCHEMA.keys())
    assert normalized["px_int"][0] == 606511
    assert normalized["qty_int"][0] == 5
    validated = validate_liquidations(normalized)
    assert validated.height == 1


def test_validate_liquidations_filters_invalid_side() -> None:
    parsed = parse_tardis_liquidations_csv(_sample_raw_df().with_columns(pl.lit("x").alias("side")))
    df = parsed.with_columns(
        [
            pl.lit("binance-futures").alias("exchange"),
            pl.lit(2, dtype=pl.Int16).alias("exchange_id"),
            pl.lit(1001, dtype=pl.Int64).alias("symbol_id"),
            pl.lit(1, dtype=pl.Int64).alias("px_int"),
            pl.lit(1, dtype=pl.Int64).alias("qty_int"),
            pl.lit(1, dtype=pl.Int32).alias("file_id"),
            pl.lit(1, dtype=pl.Int32).alias("file_line_number"),
            pl.lit(date(2024, 5, 1)).alias("date"),
        ]
    )
    normalized = normalize_liquidations_schema(df)
    validated = validate_liquidations(normalized)
    assert validated.height == 0


@pytest.fixture
def sample_manifest_repo(tmp_path):
    return DeltaManifestRepository(tmp_path / "manifest")


def test_liquidations_ingestion_service_ingest_file(sample_manifest_repo, tmp_path):
    repo = BaseDeltaRepository(tmp_path / "liquidations", partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol_repo.write_full(_sample_dim_symbol())

    service = create_ingestion_service("liquidations", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    bronze_rel_path = (
        "exchange=binance-futures/type=liquidations/date=2024-05-01/symbol=BTCUSDT/file.csv"
    )
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    _sample_raw_df().write_csv(bronze_path)

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="liquidations",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=1000,
        last_modified_ts=1714557600,
        sha256="c" * 64,
        date=date(2024, 5, 1),
    )

    file_id = sample_manifest_repo.resolve_file_id(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")

    assert result.error_message is None
    assert result.row_count == 1
    written = repo.read_all()
    assert written.height == 1
    assert written["symbol_id"][0] == 1001
    assert written["px_int"][0] == 606511
    assert written["qty_int"][0] == 5
