from datetime import date

import pytest

from pointline.io.delta_manifest_repo import DeltaManifestRepository
from pointline.io.local_source import LocalBronzeSource
from pointline.io.protocols import BronzeFileMetadata, IngestionResult


@pytest.fixture
def temp_bronze_dir(tmp_path):
    """Creates a dummy bronze directory structure."""
    base = tmp_path / "lake" / "bronze" / "tardis"
    # Create file 1
    f1 = base / "exchange=binance/type=quotes/date=2024-05-01/symbol=BTCUSDT"
    f1.mkdir(parents=True)
    (f1 / "file1.csv.gz").write_text("dummy content")

    # Create file 2
    f2 = base / "exchange=binance/type=quotes/date=2024-05-02/symbol=BTCUSDT"
    f2.mkdir(parents=True)
    (f2 / "file2.csv.gz").write_text("dummy content 2")

    return base


@pytest.fixture
def manifest_repo(tmp_path):
    repo_path = tmp_path / "lake" / "silver" / "ingest_manifest"
    return DeltaManifestRepository(repo_path)


def test_local_source_scanning(temp_bronze_dir):
    source = LocalBronzeSource(temp_bronze_dir)
    files = list(source.list_files("**/*.csv.gz"))

    assert len(files) == 2

    f1 = next(f for f in files if f.date == date(2024, 5, 1))
    assert f1.vendor == "tardis"
    assert "file1.csv.gz" in f1.bronze_file_path
    assert len(f1.sha256) == 64


def test_manifest_workflow(manifest_repo):
    # Setup Dummy Metadata
    meta1 = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        date=date(2024, 1, 1),
        bronze_file_path="path/to/1",
        file_size_bytes=100,
        last_modified_ts=1000,
        sha256="a" * 64,
    )

    meta2 = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        date=date(2024, 1, 2),
        bronze_file_path="path/to/2",
        file_size_bytes=200,
        last_modified_ts=2000,
        sha256="b" * 64,
    )

    # 1. Resolve IDs (should mint new ones)
    id1 = manifest_repo.resolve_file_id(meta1)
    id2 = manifest_repo.resolve_file_id(meta2)

    assert id1 == 1
    assert id2 == 2

    # 2. Check Pending State
    # Both should be pending now (implied by resolve_file_id logic)
    # But let's verify via filter_pending.
    # Since they are NOT 'success', filter_pending should return them.
    pending = manifest_repo.filter_pending([meta1, meta2])
    assert len(pending) == 2

    # 3. Update Status (Success for #1)
    res1 = IngestionResult(row_count=50, ts_local_min_us=0, ts_local_max_us=10)
    manifest_repo.update_status(id1, "success", meta1, res1)

    # 4. Verify Filter
    # Now meta1 is success, so it should be filtered OUT.
    pending_after = manifest_repo.filter_pending([meta1, meta2])
    assert len(pending_after) == 1
    assert pending_after[0].bronze_file_path == "path/to/2"

    # 5. Idempotency Check (Retry meta1)
    # If we resolve meta1 again, we should get ID 1
    id1_retry = manifest_repo.resolve_file_id(meta1)
    assert id1_retry == 1


def test_skip_logic_modified_file(manifest_repo):
    """If content hash changes, it should NOT be skipped."""
    meta = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        date=date(2024, 1, 1),
        bronze_file_path="path/to/1",
        file_size_bytes=100,
        last_modified_ts=1000,
        sha256="c" * 64,
    )

    # Initial Success
    fid = manifest_repo.resolve_file_id(meta)
    manifest_repo.update_status(fid, "success", meta, IngestionResult(1, 1, 1))

    # Verify skipped
    assert len(manifest_repo.filter_pending([meta])) == 0

    # Modified file (content hash change)
    meta_mod = BronzeFileMetadata(
        vendor="tardis",
        data_type="quotes",
        date=date(2024, 1, 1),
        bronze_file_path="path/to/1",
        file_size_bytes=150,
        last_modified_ts=1000,
        sha256="d" * 64,
    )

    # Should NOT skip
    assert len(manifest_repo.filter_pending([meta_mod])) == 1

    fid_mod = manifest_repo.resolve_file_id(meta_mod)
    assert fid_mod != fid
