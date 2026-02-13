from __future__ import annotations

import gzip
from pathlib import Path

import py7zr

from pointline.vendors.quant360.upstream import runner as upstream_runner
from pointline.vendors.quant360.upstream.runner import run_quant360_upstream


def _write_archive(path: Path, members: dict[str, str]) -> None:
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for member_path, content in members.items():
            archive.writestr(content, member_path)


def test_runner_is_idempotent_across_reruns(tmp_path: Path) -> None:
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

    first = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert first.processed_archives == 1
    assert first.total_members == 2
    assert first.published == 2
    assert first.skipped == 0
    assert first.failed == 0

    second = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert second.total_members == 2
    assert second.published == 0
    assert second.skipped == 2
    assert second.failed == 0


def test_runner_recovers_from_partial_publish_failure(tmp_path: Path, monkeypatch) -> None:
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

    real_publish = upstream_runner.publish_member_payload
    fail_once = {"enabled": True}

    def flaky_publish(payload, *, bronze_root):
        if payload.member_job.symbol == "000002" and fail_once["enabled"]:
            raise RuntimeError("simulated publish failure")
        return real_publish(payload, bronze_root=bronze_root)

    monkeypatch.setattr(upstream_runner, "publish_member_payload", flaky_publish)

    first = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert first.published == 1
    assert first.failed == 1
    assert first.skipped == 0

    fail_once["enabled"] = False
    second = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert second.published == 1
    assert second.failed == 0
    assert second.skipped == 1

    output_one = (
        bronze_root / "exchange=szse/type=order_new/date=2024-01-02/symbol=000001/000001.csv.gz"
    )
    output_two = (
        bronze_root / "exchange=szse/type=order_new/date=2024-01-02/symbol=000002/000002.csv.gz"
    )
    assert output_one.exists()
    assert output_two.exists()
    with gzip.open(output_two, mode="rb") as handle:
        assert handle.read() == b"a,b\n3,4\n"


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

    result = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert result.processed_archives == 2
    assert result.total_members == 1
    assert result.published == 1
    assert result.failed == 1
    assert any(record.failure_reason == "discover_error" for record in result.failure_records)


def test_runner_marks_extract_failure_when_member_missing(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "source"
    bronze_root = tmp_path / "bronze"
    ledger_path = tmp_path / "state" / "quant360_upstream.json"
    source_dir.mkdir()

    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {"order_new_STK_SZ_20240102/000001.csv": "a,b\n1,2\n"},
    )

    real_iter = upstream_runner.iter_archive_members

    def broken_iter(*args, **kwargs):
        iterator = real_iter(*args, **kwargs)
        yield from iterator
        raise ValueError("simulated extract mismatch")

    monkeypatch.setattr(upstream_runner, "iter_archive_members", broken_iter)

    result = run_quant360_upstream(source_dir, bronze_root, ledger_path)
    assert result.failed == 1
    assert any(record.failure_reason == "extract_error" for record in result.failure_records)
