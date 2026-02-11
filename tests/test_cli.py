from datetime import date

from pointline.cli import main
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import BronzeFileMetadata, IngestionResult


def _write_bronze_file(root, rel_path):
    path = root / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("dummy")
    return path


def test_bronze_discover_pending_only(tmp_path, capsys):
    bronze_root = tmp_path / "lake" / "tardis"
    _write_bronze_file(
        bronze_root,
        "exchange=binance/type=quotes/date=2024-05-01/symbol=BTCUSDT/file1.csv.gz",
    )
    _write_bronze_file(
        bronze_root,
        "exchange=binance/type=quotes/date=2024-05-02/symbol=BTCUSDT/file2.csv.gz",
    )

    manifest_path = tmp_path / "lake" / "silver" / "ingest_manifest"
    repo = DeltaManifestRepository(manifest_path)

    source = LocalBronzeSource(bronze_root)
    files = list(source.list_files("**/*.csv.gz"))
    meta = next(f for f in files if f.date == date(2024, 5, 1))
    file_id = repo.resolve_file_id(meta)
    repo.update_status(file_id, "success", meta, IngestionResult(1, 1, 1))

    exit_code = main(
        [
            "bronze",
            "discover",
            "--bronze-root",
            str(bronze_root),
            "--manifest-path",
            str(manifest_path),
            "--pending-only",
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr().out
    assert "pending files: 1" in captured
    assert "file2.csv.gz" in captured
    assert "file1.csv.gz" not in captured


def test_manifest_show_filters_vendor_exchange_and_symbol(tmp_path, capsys):
    manifest_path = tmp_path / "lake" / "silver" / "ingest_manifest"
    repo = DeltaManifestRepository(manifest_path)

    meta_1 = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        bronze_file_path="exchange=binance/type=quotes/date=2024-05-01/symbol=BTCUSDT/file1.csv.gz",
        file_size_bytes=100,
        last_modified_ts=1,
        sha256="a" * 64,
        date=date(2024, 5, 1),
    )
    meta_2 = BronzeFileMetadata(
        vendor="quant360",
        data_type="l3_ticks",
        bronze_file_path="exchange=szse/type=l3_ticks/date=2024-05-01/symbol=000001/file2.csv.gz",
        file_size_bytes=200,
        last_modified_ts=2,
        sha256="b" * 64,
        date=date(2024, 5, 1),
    )

    file_id_1 = repo.resolve_file_id(meta_1)
    repo.update_status(file_id_1, "success", meta_1, IngestionResult(10, 1, 2))
    file_id_2 = repo.resolve_file_id(meta_2)
    repo.update_status(file_id_2, "success", meta_2, IngestionResult(20, 1, 2))

    exit_code = main(
        [
            "manifest",
            "show",
            "--manifest-path",
            str(manifest_path),
            "--detailed",
            "--vendor",
            "tardis",
            "--exchange",
            "binance",
            "--symbol",
            "BTCUSDT",
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr().out
    assert "manifest entries (1 total)" in captured
    assert "tardis" in captured
    assert "quant360" not in captured
