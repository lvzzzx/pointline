from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

import polars as pl

from pointline.cli.commands import dim_symbol as dim_symbol_cmd
from pointline.cli.parser import build_parser
from pointline.io.protocols import BronzeFileMetadata


def test_capture_dim_symbol_api_response_writes_jsonl_gz(tmp_path):
    rows = [{"id": "A", "tick": 0.1}, {"id": "B", "tick": 0.2}]
    out_path = dim_symbol_cmd._capture_dim_symbol_api_response(
        vendor="tardis",
        exchange="binance-futures",
        records=rows,
        source_name="instruments",
        capture_root=str(tmp_path),
        snapshot_ts_us=1_700_000_000_000_000,
    )

    assert out_path.exists()
    assert "type=dim_symbol_metadata" in str(out_path)
    assert "exchange=binance-futures" in str(out_path)

    with gzip.open(out_path, "rt", encoding="utf-8") as handle:
        decoded = [json.loads(line) for line in handle if line.strip()]

    assert decoded == rows


def test_symbol_sync_tushare_uses_service_with_table_path(monkeypatch, tmp_path):
    class FakeTushareClient:
        def __init__(self, token=None):
            self.token = token

        def get_szse_stocks(self, include_delisted=False):
            return pl.DataFrame({"symbol": ["000001"], "exchange": ["SZSE"]})

    def fake_build_updates(_df):
        return pl.DataFrame(
            {
                "exchange_id": [30],
                "exchange_symbol": ["000001"],
                "base_asset": ["000001"],
                "quote_asset": ["CNY"],
                "asset_type": [1],
                "tick_size": [0.01],
                "lot_size": [100.0],
                "price_increment": [0.01],
                "amount_increment": [100.0],
                "contract_size": [1.0],
                "valid_from_ts": [1_700_000_000_000_000],
            }
        )

    repo_paths: list[Path] = []
    updates_seen: list[pl.DataFrame] = []

    class FakeRepo:
        def __init__(self, table_path):
            repo_paths.append(Path(table_path))

    class FakeService:
        def __init__(self, repo):
            self.repo = repo

        def update(self, updates):
            updates_seen.append(updates)

        def rebuild(self, updates):
            updates_seen.append(updates)

    import pointline.io.vendors.tushare as tushare_pkg
    import pointline.io.vendors.tushare.stock_basic_cn as stock_basic_module

    monkeypatch.setattr(tushare_pkg, "TushareClient", FakeTushareClient)
    monkeypatch.setattr(
        stock_basic_module, "build_dim_symbol_updates_from_stock_basic_cn", fake_build_updates
    )
    monkeypatch.setattr(dim_symbol_cmd, "BaseDeltaRepository", FakeRepo)
    monkeypatch.setattr(dim_symbol_cmd, "DimSymbolService", FakeService)

    args = argparse.Namespace(
        exchange="szse",
        include_delisted=False,
        token="x",
        table_path=str(tmp_path / "custom_dim_symbol"),
        rebuild=False,
        capture_api_response=False,
        capture_only=False,
        capture_root=None,
    )

    code = dim_symbol_cmd.cmd_dim_symbol_sync_tushare(args)
    assert code == 0
    assert repo_paths == [tmp_path / "custom_dim_symbol"]
    assert len(updates_seen) == 1
    assert updates_seen[0].height == 1


def test_symbol_sync_file_source_rejects_capture_flags(tmp_path):
    source = tmp_path / "updates.csv"
    source.write_text("exchange_id,exchange_symbol,valid_from_ts\n1,BTCUSDT,1\n", encoding="utf-8")
    args = argparse.Namespace(
        source=str(source),
        exchange=None,
        symbol=None,
        filter=None,
        api_key="",
        effective_ts="now",
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        capture_api_response=True,
        capture_only=False,
        capture_root=None,
    )
    assert dim_symbol_cmd.cmd_dim_symbol_sync(args) == 2


def test_symbol_sync_parser_accepts_capture_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "symbol",
            "sync",
            "--source",
            "api",
            "--exchange",
            "binance-futures",
            "--capture-api-response",
            "--capture-root",
            "/tmp/capture",
        ]
    )
    assert args.capture_api_response is True
    assert args.capture_root == "/tmp/capture"

    args_tushare = parser.parse_args(
        [
            "symbol",
            "sync-tushare",
            "--exchange",
            "szse",
            "--capture-only",
            "--rebuild",
        ]
    )
    assert args_tushare.capture_only is True
    assert args_tushare.rebuild is True


def test_symbol_ingest_metadata_parser_accepts_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "symbol",
            "ingest-metadata",
            "--vendor",
            "tardis",
            "--exchange",
            "binance-futures",
            "--manifest-path",
            "/tmp/manifest",
            "--table-path",
            "/tmp/dim_symbol",
            "--effective-ts",
            "1700000000000000",
            "--rebuild",
            "--force",
        ]
    )
    assert args.vendor == "tardis"
    assert args.exchange == "binance-futures"
    assert args.rebuild is True
    assert args.force is True


def test_symbol_ingest_metadata_success(monkeypatch, tmp_path):
    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="dim_symbol_metadata",
        bronze_file_path="type=dim_symbol_metadata/exchange=binance/date=2024-05-01/snapshot_ts=1/f.jsonl.gz",
        file_size_bytes=10,
        last_modified_ts=1,
        sha256="a" * 64,
        date=None,
    )

    class FakeManifestRepo:
        def __init__(self, _path):
            self.updated = []

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 1

        def update_status(self, file_id, status, _meta, result):
            self.updated.append((file_id, status, result.row_count))

    updates_seen: list[pl.DataFrame] = []

    class FakeRepo:
        def __init__(self, _path):
            pass

    class FakeService:
        def __init__(self, _repo):
            pass

        def update(self, updates):
            updates_seen.append(updates)

        def rebuild(self, updates):
            updates_seen.append(updates)

    fake_manifest = FakeManifestRepo(tmp_path / "manifest")
    monkeypatch.setattr(dim_symbol_cmd, "_discover_metadata_files", lambda **kwargs: [meta])
    monkeypatch.setattr(dim_symbol_cmd, "DeltaManifestRepository", lambda _p: fake_manifest)
    monkeypatch.setattr(dim_symbol_cmd, "BaseDeltaRepository", FakeRepo)
    monkeypatch.setattr(dim_symbol_cmd, "DimSymbolService", FakeService)
    monkeypatch.setattr(
        dim_symbol_cmd,
        "_build_updates_from_captured_metadata",
        lambda **kwargs: pl.DataFrame({"valid_from_ts": [100], "exchange_symbol": ["BTCUSDT"]}),
    )

    args = argparse.Namespace(
        vendor="tardis",
        bronze_root=str(tmp_path),
        glob="**/*.jsonl.gz",
        exchange=None,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        force=False,
        effective_ts=None,
    )

    code = dim_symbol_cmd.cmd_dim_symbol_ingest_metadata(args)
    assert code == 0
    assert len(updates_seen) == 1
    assert fake_manifest.updated == [(1, "success", 1)]


def test_symbol_ingest_metadata_failure(monkeypatch, tmp_path):
    meta = BronzeFileMetadata(
        vendor="tushare",
        data_type="dim_symbol_metadata",
        bronze_file_path="type=dim_symbol_metadata/exchange=szse/date=2024-05-01/snapshot_ts=1/f.jsonl.gz",
        file_size_bytes=10,
        last_modified_ts=1,
        sha256="b" * 64,
        date=None,
    )

    class FakeManifestRepo:
        def __init__(self, _path):
            self.updated = []

        def filter_pending(self, candidates):
            return candidates

        def resolve_file_id(self, _meta):
            return 2

        def update_status(self, file_id, status, _meta, result):
            self.updated.append((file_id, status, result.error_message))

    class FakeRepo:
        def __init__(self, _path):
            pass

    class FakeService:
        def __init__(self, _repo):
            pass

        def update(self, updates):
            return None

        def rebuild(self, updates):
            return None

    fake_manifest = FakeManifestRepo(tmp_path / "manifest")
    monkeypatch.setattr(dim_symbol_cmd, "_discover_metadata_files", lambda **kwargs: [meta])
    monkeypatch.setattr(dim_symbol_cmd, "DeltaManifestRepository", lambda _p: fake_manifest)
    monkeypatch.setattr(dim_symbol_cmd, "BaseDeltaRepository", FakeRepo)
    monkeypatch.setattr(dim_symbol_cmd, "DimSymbolService", FakeService)

    def _raise(**kwargs):
        raise ValueError("bad capture")

    monkeypatch.setattr(dim_symbol_cmd, "_build_updates_from_captured_metadata", _raise)

    args = argparse.Namespace(
        vendor="tushare",
        bronze_root=str(tmp_path),
        glob="**/*.jsonl.gz",
        exchange=None,
        manifest_path=str(tmp_path / "manifest"),
        table_path=str(tmp_path / "dim_symbol"),
        rebuild=False,
        force=False,
        effective_ts=None,
    )

    code = dim_symbol_cmd.cmd_dim_symbol_ingest_metadata(args)
    assert code == 1
    assert fake_manifest.updated == [(2, "failed", "bad capture")]
