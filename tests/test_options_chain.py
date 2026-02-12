from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.io.base_repository import BaseDeltaRepository
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.protocols import BronzeFileMetadata
from pointline.io.vendors.tardis.parsers.options_chain import parse_tardis_options_chain_csv
from pointline.tables.dim_symbol import SCHEMA as DIM_SYMBOL_SCHEMA
from pointline.tables.options_chain import (
    OPTIONS_CHAIN_DOMAIN,
    OPTIONS_CHAIN_SCHEMA,
)


def _sample_raw_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["deribit"],
            "symbol": ["BTC-30JUN24-60000-C"],
            "underlying_index": ["BTC"],
            "timestamp": [1714521600000000],
            "local_timestamp": [1714521600037264],
            "expiration_timestamp": [1719705600000000],
            "strike_price": ["60000"],
            "option_type": ["call"],
            "bid_price": ["100.5"],
            "ask_price": ["101.0"],
            "bid_amount": ["12"],
            "ask_amount": ["14"],
            "iv": ["0.55"],
            "mark_iv": ["0.56"],
            "delta": ["0.45"],
            "gamma": ["0.002"],
            "vega": ["10.2"],
            "theta": ["-2.1"],
            "rho": ["0.4"],
            "mark_price": ["100.8"],
            "underlying_price": ["60500"],
            "open_interest": ["1234"],
        }
    )


def _sample_dim_symbol(underlying_symbol_id: int | None = 1001) -> pl.DataFrame:
    rows = []
    rows.append(
        {
            "symbol_id": 1001,
            "exchange_id": 21,
            "exchange": "deribit",
            "exchange_symbol": "BTC",
            "base_asset": "BTC",
            "quote_asset": "USD",
            "asset_type": 0,
            "tick_size": 0.5,
            "lot_size": 1.0,
            "contract_size": 1.0,
            "expiry_ts_us": None,
            "underlying_symbol_id": None,
            "strike": None,
            "put_call": None,
            "valid_from_ts": 0,
            "valid_until_ts": 2**63 - 1,
            "is_current": True,
        }
    )
    rows.append(
        {
            "symbol_id": 2001,
            "exchange_id": 21,
            "exchange": "deribit",
            "exchange_symbol": "BTC-30JUN24-60000-C",
            "base_asset": "BTC",
            "quote_asset": "USD",
            "asset_type": 3,
            "tick_size": 0.5,
            "lot_size": 1.0,
            "contract_size": 1.0,
            "expiry_ts_us": 1719705600000000,
            "underlying_symbol_id": underlying_symbol_id,
            "strike": 60000.0,
            "put_call": "call",
            "valid_from_ts": 0,
            "valid_until_ts": 2**63 - 1,
            "is_current": True,
        }
    )
    return pl.DataFrame(rows, schema=DIM_SYMBOL_SCHEMA)


def test_parse_tardis_options_chain_csv() -> None:
    parsed = parse_tardis_options_chain_csv(_sample_raw_df())
    assert "exchange_symbol" in parsed.columns
    assert "option_type_raw" in parsed.columns
    assert parsed["exchange_symbol"][0] == "BTC-30JUN24-60000-C"
    assert parsed["strike_px"][0] == 60000.0
    assert parsed["bid_sz"][0] == 12.0


def test_normalize_and_validate_options_chain() -> None:
    parsed = parse_tardis_options_chain_csv(_sample_raw_df())
    df = parsed.with_columns(
        [
            pl.lit("deribit").alias("exchange"),
            pl.col("exchange_symbol").alias("symbol"),
            pl.lit(120000, dtype=pl.Int64).alias("strike_int"),
            pl.lit(201, dtype=pl.Int64).alias("bid_px_int"),
            pl.lit(202, dtype=pl.Int64).alias("ask_px_int"),
            pl.lit(12, dtype=pl.Int64).alias("bid_sz_int"),
            pl.lit(14, dtype=pl.Int64).alias("ask_sz_int"),
            pl.lit(1, dtype=pl.Int32).alias("file_id"),
            pl.lit(1, dtype=pl.Int32).alias("file_line_number"),
            pl.lit(date(2024, 5, 1)).alias("date"),
        ]
    )
    normalized = OPTIONS_CHAIN_DOMAIN.normalize_schema(df)
    assert list(normalized.schema.keys()) == list(OPTIONS_CHAIN_SCHEMA.keys())

    validated = OPTIONS_CHAIN_DOMAIN.validate(normalized)
    assert validated.height == 1


@pytest.fixture
def sample_manifest_repo(tmp_path):
    return DeltaManifestRepository(tmp_path / "manifest")


def test_options_chain_ingestion_service_ingest_file(sample_manifest_repo, tmp_path):
    repo = BaseDeltaRepository(tmp_path / "options_chain", partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol_repo.write_full(_sample_dim_symbol(underlying_symbol_id=None))

    service = create_ingestion_service("options_chain", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    bronze_rel_path = "exchange=deribit/type=options_chain/date=2024-05-01/symbol=BTC/file.csv"
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    _sample_raw_df().write_csv(bronze_path)

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="options_chain",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=1000,
        last_modified_ts=1714557600,
        sha256="a" * 64,
        date=date(2024, 5, 1),
    )

    file_id = sample_manifest_repo.resolve_file_id(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")

    assert result.error_message is None
    assert result.row_count == 1
    written = repo.read_all()
    assert written.height == 1
    assert written["symbol"][0] == "BTC-30JUN24-60000-C"
    assert written["underlying_index"][0] == "BTC"
    # crypto profile: price=1e-9 → 60000/1e-9 = 60000000000000
    assert written["strike_int"][0] == 60000000000000


def test_options_chain_ingestion_service_quarantine(sample_manifest_repo, tmp_path):
    repo = BaseDeltaRepository(tmp_path / "options_chain", partition_by=["exchange", "date"])
    dim_symbol_repo = BaseDeltaRepository(tmp_path / "dim_symbol")
    dim_symbol_repo.write_full(pl.DataFrame(schema=DIM_SYMBOL_SCHEMA))

    service = create_ingestion_service("options_chain", sample_manifest_repo)
    service.repo = repo
    service.dim_symbol_repo = dim_symbol_repo

    bronze_rel_path = "exchange=deribit/type=options_chain/date=2024-05-01/symbol=BTC/file.csv"
    bronze_path = tmp_path / "bronze" / "tardis" / bronze_rel_path
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    _sample_raw_df().write_csv(bronze_path)

    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="options_chain",
        bronze_file_path=bronze_rel_path,
        file_size_bytes=1000,
        last_modified_ts=1714557600,
        sha256="b" * 64,
        date=date(2024, 5, 1),
    )

    file_id = sample_manifest_repo.resolve_file_id(meta)
    result = service.ingest_file(meta, file_id, bronze_root=tmp_path / "bronze" / "tardis")
    assert result.row_count == 0
    assert result.error_message is not None
    assert "quarantined" in result.error_message.lower()
