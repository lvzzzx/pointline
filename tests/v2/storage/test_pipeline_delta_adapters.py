from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from pointline.io.protocols import BronzeFileMetadata
from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.v2.ingestion.pipeline import ingest_file
from pointline.v2.storage.delta import (
    DeltaDimensionStore,
    DeltaEventStore,
    DeltaManifestStore,
    DeltaQuarantineStore,
)


def _meta() -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=binance-futures/type=trades/date=2024-01-01/file.csv.gz",
        file_size_bytes=100,
        last_modified_ts=1000,
        sha256="a" * 64,
        date=date(2024, 1, 1),
    )


def _parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "ts_event_us": [1_700_000_000_000_000],
            "side": ["buy"],
            "is_buyer_maker": [False],
            "price": [123_000_000_000],
            "qty": [5_000_000_000],
        }
    )


def _write_dim_symbol(path: Path) -> None:
    df = pl.DataFrame(
        {
            "symbol_id": [7],
            "exchange": ["binance-futures"],
            "exchange_symbol": ["BTCUSDT"],
            "canonical_symbol": ["BTCUSDT"],
            "market_type": ["perpetual"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "valid_from_ts_us": [1_600_000_000_000_000],
            "valid_until_ts_us": [1_900_000_000_000_000],
            "is_current": [True],
            "tick_size": [None],
            "lot_size": [None],
            "contract_size": [None],
            "updated_at_ts_us": [1_700_000_000_000_000],
        },
        schema=DIM_SYMBOL.to_polars(),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), df.to_arrow(), mode="overwrite")


def test_pipeline_with_delta_adapters_success_path(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"

    _write_dim_symbol(silver_root / "dim_symbol")

    manifest_store = DeltaManifestStore(silver_root / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=silver_root)
    dimension_store = DeltaDimensionStore(silver_root=silver_root)
    quarantine_store = DeltaQuarantineStore(silver_root=silver_root)

    result = ingest_file(
        _meta(),
        parser=_parser,
        manifest_repo=manifest_store,
        writer=event_store,
        dim_symbol_df=dimension_store.load_dim_symbol(),
        quarantine_store=quarantine_store,
    )

    assert result.status == "success"
    assert result.rows_written == 1
    assert result.rows_quarantined == 0

    trades = pl.read_delta(str(silver_root / "trades"))
    assert trades.height == 1
    assert trades.item(0, "symbol_id") == 7

    manifest = pl.read_delta(str(silver_root / "ingest_manifest"))
    assert manifest.height == 1
    assert manifest.item(0, "status") == "success"


def test_pipeline_with_delta_adapters_writes_quarantine_rows(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"

    manifest_store = DeltaManifestStore(silver_root / "ingest_manifest")
    event_store = DeltaEventStore(silver_root=silver_root)
    quarantine_store = DeltaQuarantineStore(silver_root=silver_root)
    empty_dim = DeltaDimensionStore(silver_root=silver_root)

    result = ingest_file(
        _meta(),
        parser=_parser,
        manifest_repo=manifest_store,
        writer=event_store,
        dim_symbol_df=empty_dim.load_dim_symbol(),
        quarantine_store=quarantine_store,
    )

    assert result.status == "quarantined"
    assert result.rows_written == 0
    assert result.rows_quarantined == 1
    assert not (silver_root / "trades").exists()

    log_df = pl.read_delta(str(silver_root / "validation_log"))
    assert log_df.height == 1
    assert log_df.item(0, "rule_name") == "missing_pit_symbol_coverage"

    manifest = pl.read_delta(str(silver_root / "ingest_manifest"))
    assert manifest.item(0, "status") == "quarantined"
