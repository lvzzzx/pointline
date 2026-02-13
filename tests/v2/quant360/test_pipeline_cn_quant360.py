from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from pointline.io.protocols import BronzeFileMetadata
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE
from pointline.v2.ingestion.pipeline import ingest_file


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


def _meta(data_type: str) -> BronzeFileMetadata:
    return BronzeFileMetadata(
        vendor="quant360",
        data_type=data_type,
        bronze_file_path="exchange=szse/type=order_new/date=2024-09-30/symbol=000001/000001.csv.gz",
        file_size_bytes=123,
        last_modified_ts=1,
        sha256="b" * 64,
        date=date(2024, 9, 30),
    )


def _dim_symbol(
    ts_event_us: int, *, exchange: str = "szse", symbol: str = "000001"
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange": [exchange],
            "exchange_symbol": [symbol],
            "symbol_id": [42],
            "valid_from_ts_us": [ts_event_us - 1],
            "valid_until_ts_us": [ts_event_us + 1],
        }
    )


def test_ingest_quant360_order_alias_routes_and_scales() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    ts_event_us = 1_704_580_200_123_000

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["szse"],
                "symbol": ["000001"],
                "ts_event_us": [ts_event_us],
                "appl_seq_num": [10],
                "channel_no": [3],
                "side_raw": ["1"],
                "ord_type_raw": ["2"],
                "order_action_raw": [None],
                "price_raw": [10.23],
                "qty_raw": [100],
                "biz_index_raw": [None],
                "order_index_raw": [None],
            }
        )

    result = ingest_file(
        _meta("order_new"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(ts_event_us),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1

    table_name, written = writer.calls[0]
    assert table_name == "cn_order_events"
    assert written["event_seq"][0] == 10
    assert written["channel_id"][0] == 3
    assert written["order_ref"][0] == 10
    assert written["event_kind"][0] == "ADD"
    assert written["side"][0] == "BUY"
    assert written["order_type"][0] == "LIMIT"
    assert written["price"][0] == int(round(10.23 * PRICE_SCALE))
    assert written["qty"][0] == int(round(100 * QTY_SCALE))


def test_ingest_quant360_l2_alias_routes_and_scales_price_levels() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    ts_event_us = 1_704_580_200_123_000

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["szse"],
                "symbol": ["000001"],
                "ts_event_us": [ts_event_us],
                "ts_local_us": [ts_event_us],
                "msg_seq_num": [100],
                "bid_price_levels": [
                    [11.63, 11.62, 11.61, 11.6, 11.59, 11.58, 11.57, 11.56, 11.55, 11.54]
                ],
                "bid_qty_levels": [[100, 90, 80, 70, 60, 50, 40, 30, 20, 10]],
                "ask_price_levels": [
                    [11.64, 11.65, 11.66, 11.67, 11.68, 11.69, 11.7, 11.71, 11.72, 11.73]
                ],
                "ask_qty_levels": [[110, 120, 130, 140, 150, 160, 170, 180, 190, 200]],
                "image_status": ["0"],
                "trading_phase_code_raw": ["T0"],
            }
        )

    result = ingest_file(
        _meta("L2_new"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(ts_event_us),
    )

    assert result.status == "success"
    assert len(writer.calls) == 1
    table_name, written = writer.calls[0]
    assert table_name == "cn_l2_snapshots"
    assert written["snapshot_seq"][0] == 100
    assert written["source_image_status_raw"][0] == "0"
    assert written["source_trading_phase_raw"][0] == "T0"
    assert written["bid_price_levels"][0][0] == int(round(11.63 * PRICE_SCALE))


def test_ingest_quant360_tick_invalid_exec_type_fails() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    ts_event_us = 1_704_580_200_123_000

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["szse"],
                "symbol": ["000001"],
                "ts_event_us": [ts_event_us],
                "appl_seq_num": [10],
                "channel_no": [3],
                "bid_appl_seq_num": [1],
                "offer_appl_seq_num": [2],
                "exec_type_raw": ["BAD"],
                "trade_bs_flag_raw": [None],
                "price_raw": [10.23],
                "qty_raw": [100],
                "biz_index_raw": [None],
                "trade_index_raw": [None],
            }
        )

    result = ingest_file(
        _meta("tick_new"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(ts_event_us),
    )

    assert result.status == "failed"
    assert result.failure_reason == "pipeline_error"
    assert result.error_message is not None
    assert "unsupported exec_type_raw" in result.error_message
    assert writer.calls == []


def test_ingest_quant360_sse_order_missing_aux_indices_partially_quarantines() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    ts_event_us = 1_704_580_200_123_000

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["sse", "sse"],
                "symbol": ["600000", "600000"],
                "ts_event_us": [ts_event_us, ts_event_us],
                "appl_seq_num": [10, 11],
                "channel_no": [3, 3],
                "side_raw": ["B", "S"],
                "ord_type_raw": ["A", "D"],
                "order_action_raw": ["A", "D"],
                "price_raw": [10.23, 10.24],
                "qty_raw": [100, 200],
                "biz_index_raw": [None, 1001],
                "order_index_raw": [100, 101],
            }
        )

    result = ingest_file(
        _meta("order_new"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(ts_event_us, exchange="sse", symbol="600000"),
    )

    assert result.status == "success"
    assert result.row_count == 2
    assert result.rows_written == 1
    assert result.rows_quarantined == 1
    assert len(writer.calls) == 1

    table_name, written = writer.calls[0]
    assert table_name == "cn_order_events"
    assert written["source_exchange_seq"][0] == 1001
    assert written["source_exchange_order_index"][0] == 101


def test_ingest_quant360_sse_tick_missing_aux_indices_quarantines_all() -> None:
    manifest = FakeManifestRepo()
    writer = CapturingWriter()
    ts_event_us = 1_704_580_200_123_000

    def parser(_meta: BronzeFileMetadata) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "exchange": ["sse"],
                "symbol": ["600000"],
                "ts_event_us": [ts_event_us],
                "appl_seq_num": [10],
                "channel_no": [3],
                "bid_appl_seq_num": [1],
                "offer_appl_seq_num": [2],
                "exec_type_raw": ["F"],
                "trade_bs_flag_raw": ["B"],
                "price_raw": [10.23],
                "qty_raw": [100],
                "biz_index_raw": [None],
                "trade_index_raw": [None],
            }
        )

    result = ingest_file(
        _meta("tick_new"),
        parser=parser,
        manifest_repo=manifest,
        writer=writer,
        dim_symbol_df=_dim_symbol(ts_event_us, exchange="sse", symbol="600000"),
    )

    assert result.status == "quarantined"
    assert result.row_count == 1
    assert result.rows_quarantined == 1
    assert result.failure_reason == "missing_sse_tick_sequence_fields"
    assert writer.calls == []
