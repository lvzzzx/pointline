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


def test_ingest_tardis_derivative_ticker_succeeds() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        """
        Create a parsed DataFrame for a derivative ticker Tardis stream.

        Parses a fabricated raw Tardis derivative-ticker payload (constructed from the provided metadata's data_type) and returns a Polars DataFrame containing derivative ticker fields such as `exchange`, `symbol`, `timestamp`, `local_timestamp`, `mark_price`, `index_price`, `last_price`, `open_interest`, `funding_rate`, `predicted_funding_rate`, and `funding_timestamp`.

        Parameters:
            meta (BronzeFileMetadata): File metadata whose `data_type` is used to select the appropriate Tardis stream parser.

        Returns:
            pl.DataFrame: Parsed derivative ticker rows ready for ingestion.
        """
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_000_100],
                "local_timestamp": [1_700_000_000_000_200],
                "mark_price": [43_210.5],
                "index_price": [43_200.0],
                "last_price": [43_215.0],
                "open_interest": [1234.5],
                "funding_rate": [0.0001],
                "predicted_funding_rate": [0.00012],
                "funding_timestamp": [1_700_003_600_000_000],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("derivative_ticker"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "derivative_ticker"
    assert written["mark_price"][0] == int(round(43_210.5 * PRICE_SCALE))
    assert written["funding_rate"][0] == 0.0001
    assert written["open_interest"][0] == int(round(1234.5 * QTY_SCALE))


def test_ingest_tardis_liquidations_succeeds() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        """
        Create and parse a sample liquidation record using the Tardis parser selected by the metadata.

        Parameters:
            meta (BronzeFileMetadata): Metadata whose `data_type` determines which Tardis parser to use.

        Returns:
            pl.DataFrame: Parsed DataFrame produced by the selected Tardis stream parser representing a single liquidation event.
        """
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_000_100],
                "local_timestamp": [1_700_000_000_000_200],
                "id": ["liq-1"],
                "side": ["sell"],
                "price": [42_500.0],
                "amount": [0.1],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("liquidations"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "liquidations"
    assert written["liquidation_id"][0] == "liq-1"
    assert written["side"][0] == "sell"
    assert written["price"][0] == int(round(42_500.0 * PRICE_SCALE))
    assert written["qty"][0] == int(round(0.1 * QTY_SCALE))


def test_ingest_tardis_options_chain_succeeds() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()

    def parser(meta: BronzeFileMetadata) -> pl.DataFrame:
        """
        Create and parse a sample options_chain DataFrame using the Tardis parser selected by meta.data_type.

        Parameters:
            meta (BronzeFileMetadata): Metadata whose data_type determines which Tardis stream parser to use.

        Returns:
            pl.DataFrame: Parsed options-chain records ready for ingestion, including fields such as exchange, symbol, timestamp and local_timestamp, option `type`, `strike_price`/strike, `expiration`/expiration_ts_us, prices (`last_price`, `mark_price`, `underlying_price`), bid/ask prices and amounts, implied vols (`bid_iv`, `ask_iv`, `mark_iv`), `open_interest`, and Greeks (`delta`, `gamma`, `vega`, `theta`, `rho`).
        """
        stream_parser = get_tardis_parser(meta.data_type)
        raw = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "symbol": ["BTCUSDT"],
                "timestamp": [1_700_000_000_000_100],
                "local_timestamp": [1_700_000_000_000_200],
                "type": ["call"],
                "strike_price": [42_000.0],
                "expiration": [1_706_284_800_000_000],
                "open_interest": [500.0],
                "last_price": [0.05],
                "bid_price": [0.045],
                "bid_amount": [10.0],
                "bid_iv": [0.55],
                "ask_price": [0.055],
                "ask_amount": [8.0],
                "ask_iv": [0.58],
                "mark_price": [0.05],
                "mark_iv": [0.56],
                "underlying_index": ["BTC"],
                "underlying_price": [43_000.0],
                "delta": [0.45],
                "gamma": [0.0001],
                "vega": [15.5],
                "theta": [-20.3],
                "rho": [0.5],
            }
        )
        return stream_parser(raw)

    result = ingest_file(
        _meta("options_chain"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "options_chain"
    assert written["option_type"][0] == "call"
    assert written["strike"][0] == int(round(42_000.0 * PRICE_SCALE))
    assert written["expiration_ts_us"][0] == 1_706_284_800_000_000
    assert written["delta"][0] == 0.45
    assert written["mark_iv"][0] == 0.56
