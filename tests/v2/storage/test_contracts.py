from __future__ import annotations

from datetime import date

import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.v2.ingestion.models import IngestionResult
from pointline.v2.storage.contracts import (
    DimensionStore,
    EventStore,
    ManifestStore,
    QuarantineStore,
)
from pointline.v2.storage.models import ManifestIdentity


class _ManifestImpl:
    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        return 1

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        return candidates

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        return None


class _EventImpl:
    def append(self, table_name: str, df: pl.DataFrame) -> None:
        return None


class _DimensionImpl:
    def load_dim_symbol(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "symbol_id": pl.Int64,
                "exchange": pl.Utf8,
                "exchange_symbol": pl.Utf8,
                "canonical_symbol": pl.Utf8,
                "market_type": pl.Utf8,
                "base_asset": pl.Utf8,
                "quote_asset": pl.Utf8,
                "valid_from_ts_us": pl.Int64,
                "valid_until_ts_us": pl.Int64,
                "is_current": pl.Boolean,
                "tick_size": pl.Int64,
                "lot_size": pl.Int64,
                "contract_size": pl.Int64,
                "updated_at_ts_us": pl.Int64,
            }
        )


class _QuarantineImpl:
    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        *,
        reason: str,
        file_id: int,
    ) -> None:
        return None


def _meta() -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="quant360",
        data_type="order_new",
        bronze_file_path="exchange=sse/type=order_new/date=2024-01-02/symbol=600000/600000.csv.gz",
        file_size_bytes=123,
        last_modified_ts=456,
        sha256="a" * 64,
        date=date(2024, 1, 2),
    )


def test_protocols_are_runtime_checkable() -> None:
    assert isinstance(_ManifestImpl(), ManifestStore)
    assert isinstance(_EventImpl(), EventStore)
    assert isinstance(_DimensionImpl(), DimensionStore)
    assert isinstance(_QuarantineImpl(), QuarantineStore)


def test_manifest_identity_from_meta_is_stable() -> None:
    meta = _meta()
    identity = ManifestIdentity.from_meta(meta)
    assert identity.as_tuple() == (
        "quant360",
        "order_new",
        "exchange=sse/type=order_new/date=2024-01-02/symbol=600000/600000.csv.gz",
        "a" * 64,
    )
