from __future__ import annotations

from datetime import date

import polars as pl

from pointline.ingestion.manifest import build_manifest_identity
from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata


class FakeManifestRepo:
    def __init__(self) -> None:
        self._next_file_id = 1
        self.success_identities: set[tuple[str, str, str, str]] = set()

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        file_id = self._next_file_id
        self._next_file_id += 1
        return file_id

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        return [c for c in candidates if build_manifest_identity(c) not in self.success_identities]

    def update_status(
        self, file_id: int, status: str, meta: BronzeFileMetadata, result=None
    ) -> None:
        if status == "success":
            self.success_identities.add(build_manifest_identity(meta))


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
            "price": [1],
            "qty": [1],
        }
    )


def _dim_symbol() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "exchange_symbol": ["BTCUSDT"],
            "symbol_id": [7],
            "valid_from_ts_us": [1_600_000_000_000_000],
            "valid_until_ts_us": [1_900_000_000_000_000],
        }
    )


def test_build_manifest_identity_is_deterministic() -> None:
    meta = _meta()
    identity_a = build_manifest_identity(meta)
    identity_b = build_manifest_identity(meta)
    assert identity_a == identity_b
    assert identity_a == (meta.vendor, meta.data_type, meta.bronze_file_path, meta.sha256)


def test_force_true_reingests_success_identity() -> None:
    manifest = FakeManifestRepo()
    meta = _meta()
    manifest.success_identities.add(build_manifest_identity(meta))

    writes: list[pl.DataFrame] = []

    first = ingest_file(
        meta,
        parser=_parser,
        manifest_repo=manifest,
        writer=lambda _table, df: writes.append(df),
        dim_symbol_df=_dim_symbol(),
        force=False,
    )
    forced = ingest_file(
        meta,
        parser=_parser,
        manifest_repo=manifest,
        writer=lambda _table, df: writes.append(df),
        dim_symbol_df=_dim_symbol(),
        force=True,
    )

    assert first.skipped is True
    assert forced.skipped is False
    assert forced.status == "success"
    assert len(writes) == 1
