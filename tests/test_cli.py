from datetime import date

from pointline.cli import main
from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import IngestionResult


def _write_bronze_file(root, rel_path):
    path = root / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("dummy")
    return path


def test_ingest_discover_pending_only(tmp_path, capsys):
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
            "ingest",
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
