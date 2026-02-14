#!/usr/bin/env python3
"""Production runner for Quant360 upstream archive processing.

This script processes supported archives one-by-one with:
- live progress logs
- incremental ledger saves during the run
- final summary report

Default paths:
    archive dir: ~/data/lake/bronze/quant360/archive
    state dir:   ~/data/lake/bronze/quant360/state
    bronze root: ~/data/lake/bronze/quant360

Examples:
    uv run python scripts/run_quant360_upstream_prod.py
    uv run python scripts/run_quant360_upstream_prod.py --dry-run
    uv run python scripts/run_quant360_upstream_prod.py --limit 50
    uv run python scripts/run_quant360_upstream_prod.py --report-json /tmp/quant360_upstream_report.json
"""

from __future__ import annotations

import argparse
import fnmatch
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter

from pointline.vendors.quant360.filenames import parse_archive_filename
from pointline.vendors.quant360.upstream.discover import plan_members
from pointline.vendors.quant360.upstream.ledger import STATUS_FAILED, STATUS_SUCCESS, Ledger
from pointline.vendors.quant360.upstream.models import (
    ArchiveJob,
    ArchiveState,
    PublishedFile,
    RunResult,
)
from pointline.vendors.quant360.upstream.runner import process_archive
from pointline.vendors.quant360.upstream.utils import file_sha256

SUPPORTED_STREAMS = {"order_new", "tick_new", "L2_new"}


@dataclass(frozen=True)
class SelectionSummary:
    total_archives_found: int
    selected_supported: int
    skipped_unsupported: int
    skipped_invalid_name: int
    skipped_by_filter: int
    skipped_by_limit: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Quant360 upstream for production archive batches."
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("~/data/lake/bronze/quant360/archive"),
        help="Directory containing Quant360 .7z archives.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("~/data/lake/bronze/quant360/state"),
        help="State/ledger directory.",
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("~/data/lake/bronze/quant360"),
        help="Bronze destination root.",
    )
    parser.add_argument(
        "--ledger-name",
        default="quant360_upstream.json",
        help="Ledger filename under --state-dir.",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Optional glob pattern(s) to include (matched against archive filename). Repeatable.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Optional glob pattern(s) to exclude (matched against archive filename). Repeatable.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N selected archives (sorted by filename).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run planning/selection and upstream dry-run without writing outputs or ledger.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Optional path to write machine-readable run report JSON.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print selected archive names and detailed failure rows.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="Print progress every N archives (default: 1).",
    )
    parser.add_argument(
        "--save-every",
        type=int,
        default=1,
        help="Persist ledger every N archives while running (default: 1).",
    )
    return parser


def _resolve(path: Path) -> Path:
    return path.expanduser().resolve()


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in patterns)


def _select_archives(
    archive_dir: Path,
    *,
    includes: list[str],
    excludes: list[str],
    limit: int | None,
) -> tuple[list[Path], SelectionSummary, list[str], list[str]]:
    all_archives = sorted(archive_dir.glob("*.7z"))
    selected: list[Path] = []
    invalid_names: list[str] = []
    unsupported: list[str] = []
    skipped_by_filter = 0

    for archive in all_archives:
        name = archive.name
        if includes and not _matches_any(name, includes):
            skipped_by_filter += 1
            continue
        if excludes and _matches_any(name, excludes):
            skipped_by_filter += 1
            continue

        try:
            meta = parse_archive_filename(name)
        except ValueError:
            invalid_names.append(name)
            continue

        if meta.stream_type not in SUPPORTED_STREAMS:
            unsupported.append(name)
            continue

        selected.append(archive)

    skipped_by_limit = 0
    if limit is not None and limit >= 0 and len(selected) > limit:
        skipped_by_limit = len(selected) - limit
        selected = selected[:limit]

    summary = SelectionSummary(
        total_archives_found=len(all_archives),
        selected_supported=len(selected),
        skipped_unsupported=len(unsupported),
        skipped_invalid_name=len(invalid_names),
        skipped_by_filter=skipped_by_filter,
        skipped_by_limit=skipped_by_limit,
    )
    return selected, summary, invalid_names, unsupported


def _print_top_list(title: str, items: list[str], *, limit: int = 5) -> None:
    if not items:
        return
    print(f"{title} ({len(items)}):")
    for name in items[:limit]:
        print(f"  - {name}")
    if len(items) > limit:
        print(f"  ... and {len(items) - limit} more")


def _fmt_elapsed(seconds: float) -> str:
    whole = int(max(0, seconds))
    h, rem = divmod(whole, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _run_with_progress(
    archives: list[Path],
    bronze_root: Path,
    ledger_path: Path,
    *,
    dry_run: bool,
    progress_every: int,
    save_every: int,
) -> RunResult:
    ledger = Ledger(ledger_path)
    ledger.load()
    total_archives = len(archives)

    total_members = 0
    total_published = 0
    total_skipped = 0
    failed_archives = 0
    failure_states: list[ArchiveState] = []
    published_samples: list[PublishedFile] = []

    started = perf_counter()
    progress_every = max(1, progress_every)
    save_every = max(1, save_every)

    for idx, archive_path in enumerate(archives, start=1):
        meta = parse_archive_filename(archive_path.name)
        archive_job = ArchiveJob(
            archive_path=archive_path,
            archive_meta=meta,
            archive_sha256=file_sha256(archive_path),
        )
        key = archive_job.key
        archive_name = archive_path.name
        status = "ok"
        member_count = 0
        published_this = 0
        skipped_this = 0
        fail_reason = ""

        if ledger.is_success(key):
            status = "skip_success"
            try:
                members = plan_members(archive_job)
                member_count = len(members)
                total_members += member_count
                total_skipped += member_count
                skipped_this = member_count
            except Exception:
                member_count = 0
        else:
            try:
                members = plan_members(archive_job)
                member_count = len(members)
                total_members += member_count
            except Exception as exc:
                status = "fail_discover"
                fail_reason = str(exc)
                failed_archives += 1
                state = ArchiveState(
                    archive_key=key,
                    status=STATUS_FAILED,
                    member_count=0,
                    published_count=0,
                    failure_reason="discover_error",
                    error_message=str(exc),
                )
                failure_states.append(state)
                if not dry_run:
                    ledger.set_state(state)
            else:
                published_this, skipped_this, files, failure = process_archive(
                    archive_job,
                    bronze_root,
                    dry_run=dry_run,
                )
                total_published += published_this
                total_skipped += skipped_this
                if len(published_samples) < 10 and files:
                    remaining = 10 - len(published_samples)
                    published_samples.extend(files[:remaining])

                if failure:
                    status = "fail_process"
                    fail_reason = failure.error_message or failure.failure_reason or "unknown"
                    failed_archives += 1
                    failure_states.append(failure)
                    if not dry_run:
                        ledger.set_state(failure)
                elif not dry_run:
                    ledger.set_state(
                        ArchiveState(
                            archive_key=key,
                            status=STATUS_SUCCESS,
                            member_count=member_count,
                            published_count=published_this,
                        )
                    )

        if not dry_run and (idx % save_every == 0 or idx == total_archives):
            ledger.save()

        should_log = idx % progress_every == 0 or idx == total_archives or status.startswith("fail")
        if should_log:
            elapsed = perf_counter() - started
            rate = idx / elapsed if elapsed > 0 else 0.0
            remaining = total_archives - idx
            eta_seconds = remaining / rate if rate > 0 else 0.0
            line = (
                f"[{idx}/{total_archives}] {status} archive={archive_name} "
                f"members={member_count} published={published_this} skipped={skipped_this} "
                f"failed_total={failed_archives} elapsed={_fmt_elapsed(elapsed)} eta={_fmt_elapsed(eta_seconds)}"
            )
            if fail_reason:
                line += f" error={fail_reason[:180]}"
            print(line)

    if not dry_run:
        ledger.save()

    return RunResult(
        processed_archives=total_archives,
        total_members=total_members,
        published=total_published,
        skipped=total_skipped,
        failed=failed_archives,
        published_files=published_samples,
        failure_states=failure_states,
    )


def main() -> int:
    args = build_parser().parse_args()
    started = perf_counter()

    archive_dir = _resolve(args.archive_dir)
    state_dir = _resolve(args.state_dir)
    bronze_root = _resolve(args.bronze_root)
    ledger_path = state_dir / args.ledger_name

    if not archive_dir.exists():
        raise SystemExit(f"archive dir does not exist: {archive_dir}")
    state_dir.mkdir(parents=True, exist_ok=True)
    bronze_root.mkdir(parents=True, exist_ok=True)

    archives, selection, invalid_names, unsupported = _select_archives(
        archive_dir,
        includes=args.include,
        excludes=args.exclude,
        limit=args.limit,
    )

    print(f"archive_dir={archive_dir}")
    print(f"bronze_root={bronze_root}")
    print(f"ledger_path={ledger_path}")
    print(f"dry_run={args.dry_run}")
    print(f"selection={asdict(selection)}")
    _print_top_list("invalid_name_archives", invalid_names)
    _print_top_list("unsupported_stream_archives", unsupported)

    if not archives:
        print("No supported archives selected. Nothing to do.")
        return 0

    if args.verbose:
        print("selected_archives:")
        for archive in archives:
            print(f"  - {archive.name}")

    run_result = _run_with_progress(
        archives,
        bronze_root,
        ledger_path,
        dry_run=args.dry_run,
        progress_every=args.progress_every,
        save_every=args.save_every,
    )
    elapsed = perf_counter() - started

    report = {
        "archive_dir": str(archive_dir),
        "bronze_root": str(bronze_root),
        "ledger_path": str(ledger_path),
        "dry_run": bool(args.dry_run),
        "selection": asdict(selection),
        "run_result": {
            "processed_archives": run_result.processed_archives,
            "total_members": run_result.total_members,
            "published": run_result.published,
            "skipped": run_result.skipped,
            "failed": run_result.failed,
            "published_sample": [pf.bronze_rel_path for pf in run_result.published_files[:10]],
            "failure_sample": [
                {
                    "archive_key": str(state.archive_key),
                    "status": state.status,
                    "reason": state.failure_reason,
                    "error": state.error_message,
                    "member_count": state.member_count,
                    "published_count": state.published_count,
                }
                for state in run_result.failure_states[:10]
            ],
        },
        "elapsed_seconds": round(elapsed, 3),
    }

    print("run_summary=")
    print(json.dumps(report["run_result"], ensure_ascii=True, indent=2))
    print(f"elapsed_seconds={report['elapsed_seconds']}")

    if args.verbose and run_result.failure_states:
        print("all_failure_states:")
        for state in run_result.failure_states:
            print(
                json.dumps(
                    {
                        "archive_key": str(state.archive_key),
                        "reason": state.failure_reason,
                        "error": state.error_message,
                        "member_count": state.member_count,
                        "published_count": state.published_count,
                    },
                    ensure_ascii=True,
                )
            )

    if args.report_json is not None:
        report_path = _resolve(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
        print(f"report_json={report_path}")

    return 0 if run_result.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
