from datetime import date

from pointline.cli.commands import ingest as ingest_cmd
from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.cli.parser import build_parser
from pointline.io.protocols import BronzeFileMetadata


def test_resolve_target_table_name_maps_data_types():
    trades_meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="trades",
        bronze_file_path="exchange=binance/type=trades/date=2024-05-01/symbol=BTCUSDT/file.csv.gz",
        file_size_bytes=1,
        last_modified_ts=1,
        sha256="a" * 64,
        date=date(2024, 5, 1),
    )
    assert ingest_cmd._resolve_target_table_name(trades_meta) == "trades"

    klines_meta = BronzeFileMetadata(
        vendor="binance_vision",
        data_type="klines",
        bronze_file_path=(
            "spot/exchange=binance/type=klines/date=2024-05-01/symbol=BTCUSDT/interval=1h/file.csv"
        ),
        file_size_bytes=1,
        last_modified_ts=1,
        sha256="b" * 64,
        date=date(2024, 5, 1),
        interval="1h",
    )
    assert ingest_cmd._resolve_target_table_name(klines_meta) == "kline_1h"


def test_extract_partition_filters_uses_path_and_date():
    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        bronze_file_path="exchange=binance-futures/type=quotes/date=2024-05-03/symbol=BTCUSDT/file.csv.gz",
        file_size_bytes=1,
        last_modified_ts=1,
        sha256="c" * 64,
        date=date(2024, 5, 3),
    )

    filters = ingest_cmd._extract_partition_filters(meta)
    assert filters == {"exchange": "binance-futures", "date": date(2024, 5, 3)}


def test_run_post_ingest_optimize_invokes_repo(monkeypatch):
    calls = []

    class FakeRepo:
        def __init__(self, table_path, partition_by=None):
            self.table_path = table_path
            self.partition_by = partition_by

        def optimize_partition(self, *, filters, target_file_size=None, z_order=None):
            calls.append(
                {
                    "table_path": str(self.table_path),
                    "partition_by": self.partition_by,
                    "filters": filters,
                    "target_file_size": target_file_size,
                    "z_order": z_order,
                }
            )
            return {"totalConsideredFiles": 1, "numFilesRemoved": 2, "numFilesAdded": 1}

    monkeypatch.setattr(ingest_cmd, "BaseDeltaRepository", FakeRepo)
    monkeypatch.setattr(ingest_cmd, "get_table_path", lambda table_name: f"/tmp/{table_name}")

    touched = {
        "quotes": {
            ("binance", date(2024, 5, 1)),
        }
    }

    failures = ingest_cmd._run_post_ingest_optimize(
        touched,
        target_file_size=123,
        zorder="symbol_id, ts_local_us",
    )

    assert failures == 0
    assert len(calls) == 1
    assert calls[0]["table_path"] == "/tmp/quotes"
    assert calls[0]["partition_by"] == ["exchange", "date"]
    assert calls[0]["filters"] == {"exchange": "binance", "date": date(2024, 5, 1)}
    assert calls[0]["target_file_size"] == 123
    assert calls[0]["z_order"] == ["symbol_id", "ts_local_us"]


def test_bronze_ingest_parser_accepts_optimize_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "bronze",
            "ingest",
            "--optimize-after-ingest",
            "--optimize-zorder",
            "symbol_id,ts_local_us",
            "--optimize-target-file-size",
            "1048576",
        ]
    )

    assert args.optimize_after_ingest is True
    assert args.optimize_zorder == "symbol_id,ts_local_us"
    assert args.optimize_target_file_size == 1048576


def test_ingestion_factory_uses_physical_table_names_for_logs():
    service_book = create_ingestion_service("book_snapshot_25", manifest_repo=object())
    assert service_book.table_name == "book_snapshot_25"

    service_options = create_ingestion_service("options_chain", manifest_repo=object())
    assert service_options.table_name == "options_chain"

    service_kline = create_ingestion_service("klines", manifest_repo=object(), interval="1h")
    assert service_kline.table_name == "kline_1h"
