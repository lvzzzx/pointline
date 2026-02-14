from __future__ import annotations

from pathlib import Path

import py7zr
import pytest

from pointline.vendors.quant360.upstream.discover import (
    discover_archives,
    list_csv_members,
    plan_members,
)


def _write_archive(path: Path, members: dict[str, str]) -> None:
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for member_path, content in members.items():
            archive.writestr(content, member_path)


def test_discover_archives_and_members(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_archive(
        source_dir / "order_new_STK_SZ_20240102.7z",
        {
            "order_new_STK_SZ_20240102/000002.csv": "a,b\n1,2\n",
            "order_new_STK_SZ_20240102/000001.csv": "a,b\n3,4\n",
            "order_new_STK_SZ_20240102/readme.txt": "ignored",
        },
    )
    _write_archive(
        source_dir / "tick_new_STK_SH_20240102.7z",
        {
            "tick_new_STK_SH_20240102/600000.csv": "a,b\n1,2\n",
        },
    )

    archive_jobs = discover_archives(source_dir)
    assert [job.archive_path.name for job in archive_jobs] == [
        "tick_new_STK_SH_20240102.7z",
        "order_new_STK_SZ_20240102.7z",
    ]
    assert all(len(job.archive_sha256) == 64 for job in archive_jobs)

    csv_members = list_csv_members(archive_jobs[1])
    assert csv_members == [
        "order_new_STK_SZ_20240102/000001.csv",
        "order_new_STK_SZ_20240102/000002.csv",
    ]

    planned_members = plan_members(archive_jobs[1])
    assert [job.symbol for job in planned_members] == ["000001", "000002"]
    assert [job.member_path for job in planned_members] == csv_members


def test_discover_rejects_invalid_archive_name(tmp_path: Path) -> None:
    _write_archive(
        tmp_path / "bad_name.7z",
        {
            "bad_name/000001.csv": "a,b\n1,2\n",
        },
    )

    with pytest.raises(ValueError, match="Quant360 archive filename"):
        discover_archives(tmp_path)
