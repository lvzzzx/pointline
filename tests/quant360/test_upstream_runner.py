from __future__ import annotations

import gzip
from pathlib import Path

import py7zr

from pointline.vendors.quant360.upstream import runner as upstream_runner
from pointline.vendors.quant360.upstream.extract import ExtractionError
from pointline.vendors.quant360.upstream.runner import run


def _write_archive(path: Path, members: dict[str, str]) -> None:
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for member_path, content in members.items():
            archive.writestr(content, member_path)


def test_runner_processes_archives_and_tracks_success(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    ledger_path = tmp_path / "state" / "quant360_upstream.json"
    source_dir.mkdir()

    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {
            "order_new_STK_SZ_20240102/000001.csv": "a,b\n1,2\n",
            "order_new_STK_SZ_20240102/000002.csv": "a,b\n3,4\n",
        },
    )

    first = run(source_dir, bronze_root, ledger_path)
    assert first.processed_archives == 1
    assert first.total_members == 2
    assert first.published == 2
    assert first.skipped == 0
    assert first.failed == 0

    # Second run should skip (archive marked success)
    second = run(source_dir, bronze_root, ledger_path)
    assert second.total_members == 2
    assert second.published == 0
    assert second.skipped == 2
    assert second.failed == 0


def test_runner_reprocesses_failed_archive_on_rerun(tmp_path: Path, monkeypatch) -> None:
    """Failed archives are re-extracted and re-published on re-run (overwrite mode)."""
    source_dir = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    ledger_path = tmp_path / "state" / "quant360_upstream.json"
    source_dir.mkdir()

    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {
            "order_new_STK_SZ_20240102/000001.csv": "a,b\n1,2\n",
            "order_new_STK_SZ_20240102/000002.csv": "a,b\n3,4\n",
        },
    )

    # First run: simulate partial failure
    real_publish = upstream_runner.publish
    fail_once = {"enabled": True}

    def flaky_publish(payload, *, bronze_root):
        if payload.member_job.symbol == "000002" and fail_once["enabled"]:
            raise RuntimeError("simulated publish failure")
        return real_publish(payload, bronze_root=bronze_root)

    monkeypatch.setattr(upstream_runner, "publish", flaky_publish)

    first = run(source_dir, bronze_root, ledger_path)
    assert first.published == 1  # Only first member published
    assert first.failed == 1  # Archive marked failed
    assert first.failure_states[0].failure_reason == "publish_error"

    # Second run: re-process the failed archive (overwrite everything)
    fail_once["enabled"] = False
    second = run(source_dir, bronze_root, ledger_path)
    # Both members are re-published (overwritten)
    assert second.published == 2
    assert second.failed == 0
    assert len(second.failure_states) == 0

    # Verify both files exist with correct content
    output_one = (
        bronze_root / "exchange=szse/type=order_new/date=2024-01-02/symbol=000001/000001.csv.gz"
    )
    output_two = (
        bronze_root / "exchange=szse/type=order_new/date=2024-01-02/symbol=000002/000002.csv.gz"
    )
    assert output_one.exists()
    assert output_two.exists()
    with gzip.open(output_two, mode="rb") as f:
        assert f.read() == b"a,b\n3,4\n"


def test_runner_continues_when_one_archive_is_corrupted(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    ledger_path = tmp_path / "state" / "quant360_upstream.json"
    source_dir.mkdir()

    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {"order_new_STK_SZ_20240102/000001.csv": "a,b\n1,2\n"},
    )
    (source_dir / "tick_new_STK_SH_20240102.7z").write_bytes(b"not-a-valid-7z")

    result = run(source_dir, bronze_root, ledger_path)
    assert result.processed_archives == 2
    assert result.total_members == 1  # Only the valid archive
    assert result.published == 1
    assert result.failed == 1
    assert any(state.failure_reason == "discover_error" for state in result.failure_states)


def test_runner_marks_extract_failure_when_members_missing(tmp_path: Path, monkeypatch) -> None:
    """If extraction produces fewer files than expected, mark as extract_error."""
    source_dir = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    ledger_path = tmp_path / "state" / "quant360_upstream.json"
    source_dir.mkdir()

    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {"order_new_STK_SZ_20240102/000001.csv": "a,b\n1,2\n"},
    )

    # Simulate extraction validation failure
    def broken_iter(*args, **kwargs):
        raise ExtractionError("simulated extraction mismatch: expected 2 members, found 1")

    monkeypatch.setattr(upstream_runner, "iter_members", broken_iter)

    result = run(source_dir, bronze_root, ledger_path)
    assert result.failed == 1
    assert any(state.failure_reason == "extract_error" for state in result.failure_states)
