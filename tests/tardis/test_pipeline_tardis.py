from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from pointline.ingestion.pipeline import ingest_file
from pointline.protocols import BronzeFileMetadata
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE
from pointline.vendors.tardis import get_tardis_parser


@dataclass
class _ManifestCall:
    file_id: int
    status: str


class FakeManifestRepo:
    def __init__(self) -> None:
        self._next_file_id = 1
        self.success_identities: set[tuple[str, str, str, str]] = set()
        self.updated: list[_ManifestCall] = []

    def _identity(self, meta: BronzeFileMetadata) -> tuple[str, str, str, str]:
        return (meta.vendor, meta.data_type, meta.bronze_file_path, meta.sha256)

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        file_id = self._next_file_id
        self._next_file_id += 1
        return file_id

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        return [c for c in candidates if self._identity(c) not in self.success_identities]

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: Any | None = None,
    ) -> None:
        if status == "success":
            self.success_identities.add(self._identity(meta))
        self.updated.append(_ManifestCall(file_id=file_id, status=status))


class CapturingWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, pl.DataFrame]] = []

    def __call__(self, table_name: str, df: pl.DataFrame) -> None:
        self.calls.append((table_name, df))


def _meta(data_type: str, grouped_symbol: str = "PERPETUALS") -> BronzeFileMetadata:
    """Bronze metadata for a grouped-symbol file.

    The symbol= partition is a placeholder (e.g. PERPETUALS, SPOT);
    the CSV is self-contained with per-row exchange/symbol fields.
    """
    return BronzeFileMetadata(
        vendor="tardis",
        data_type=data_type,
        bronze_file_path=(
            f"exchange=binance-futures/type={data_type}"
            f"/date=2024-01-01/symbol={grouped_symbol}/{grouped_symbol}.csv.gz"
        ),
        file_size_bytes=123,
        last_modified_ts=1,
        sha256=f"{data_type:0<64}"[:64],
        date=date(2024, 1, 1),
        extra={"grouped_symbol": grouped_symbol},
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


def test_ingest_tardis_trades_via_dispatch_succeeds() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_000_100],
                "local_timestamp": [1_700_000_000_000_200],
                "id": ["t-1"],
                "side": ["buy"],
                "price": [42_000.0],
                "amount": [0.25],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("trades"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "trades"
    assert written["trade_id"][0] == "t-1"
    assert written["price"][0] == int(round(42_000.0 * PRICE_SCALE))
    assert written["qty"][0] == int(round(0.25 * QTY_SCALE))


def test_ingest_tardis_quotes_via_dispatch_succeeds() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_010_100],
                "bid_price": [99.9],
                "bid_amount": [4.0],
                "ask_price": [100.1],
                "ask_amount": [5.0],
                "sequence_number": [1001],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("quotes"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "quotes"
    assert written["seq_num"][0] == 1001
    assert written["bid_price"][0] == int(round(99.9 * PRICE_SCALE))
    assert written["ask_qty"][0] == int(round(5.0 * QTY_SCALE))


def test_ingest_tardis_incremental_l2_alias_routes_to_orderbook_updates() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_020_100],
                "local_timestamp": [1_700_000_000_020_200],
                "is_snapshot": [False],
                "side": ["bid"],
                "price": [100.0],
                "amount": [1.5],
                "update_id": [555],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("incremental_book_L2"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "orderbook_updates"
    assert written["book_seq"][0] == 555
    assert written["qty"][0] == int(round(1.5 * QTY_SCALE))
    assert written["is_snapshot"][0] is False
