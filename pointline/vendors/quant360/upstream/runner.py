"""Function-first runner for Quant360 upstream archive processing."""

from __future__ import annotations

from pathlib import Path
from time import time_ns

from pointline.vendors.quant360.upstream.discover import discover_archives, plan_members
from pointline.vendors.quant360.upstream.extract import ExtractionError, iter_members
from pointline.vendors.quant360.upstream.ledger import STATUS_FAILED, STATUS_SUCCESS, Ledger
from pointline.vendors.quant360.upstream.models import ArchiveState, PublishedFile, RunResult
from pointline.vendors.quant360.upstream.publish import publish


def _now_us() -> int:
    return time_ns() // 1_000


def process_archive(
    archive_job,
    bronze_root: Path,
    *,
    dry_run: bool = False,
) -> tuple[int, int, list[PublishedFile], ArchiveState | None]:
    """Process a single archive.

    On re-processing (whether first time or retry), the archive is fully
    re-extracted and all members are published (overwriting any existing files).

    Returns:
        Tuple of (published_count, skipped_count, published_files, failure_state)
        If failure_state is not None, the archive failed.
    """
    key = archive_job.key

    # Plan members (fail early if archive is corrupt)
    try:
        members = plan_members(archive_job)
    except Exception as e:
        state = ArchiveState(
            archive_key=key,
            status=STATUS_FAILED,
            member_count=0,
            published_count=0,
            failure_reason="discover_error",
            error_message=str(e),
        )
        return 0, 0, [], state

    member_count = len(members)
    expected_member_paths = [m.member_path for m in members]

    if dry_run:
        return 0, 0, [], None

    published = 0
    skipped = 0
    published_files: list[PublishedFile] = []
    last_error: Exception | None = None
    failure_reason: str | None = None

    try:
        # Extract and validate against expected members
        for member_job, gz_path in iter_members(
            archive_job,
            member_jobs=members,
            expected_members=expected_member_paths,
        ):
            try:
                result = publish(member_job, gz_path=gz_path, bronze_root=bronze_root)
            except Exception as e:
                last_error = e
                failure_reason = "publish_error"
                continue

            if result.already_exists:
                # On re-process, we treat existing files as "re-published"
                # since we're effectively overwriting them
                published += 1
            else:
                published += 1
            published_files.append(result)

    except ExtractionError as e:
        last_error = e
        failure_reason = "extract_error"
    except Exception as e:
        last_error = e
        failure_reason = "extract_error"

    # Determine final state
    if last_error is not None:
        state = ArchiveState(
            archive_key=key,
            status=STATUS_FAILED,
            member_count=member_count,
            published_count=published,
            failure_reason=failure_reason,
            error_message=str(last_error),
        )
        return published, skipped, published_files, state

    # Check for partial failure (some members failed to publish)
    if published < member_count:
        failed_count = member_count - published
        state = ArchiveState(
            archive_key=key,
            status=STATUS_FAILED,
            member_count=member_count,
            published_count=published,
            failure_reason="publish_error",
            error_message=f"{failed_count} member(s) failed to publish",
        )
        return published, skipped, published_files, state

    # Full success
    return published, skipped, published_files, None


def run(
    source_dir: Path,
    bronze_root: Path,
    ledger_path: Path,
    *,
    dry_run: bool = False,
) -> RunResult:
    """Process all archives from source to Bronze.

    Archives are processed one at a time:
    - Successful archives (in ledger) are skipped entirely
    - Failed or new archives are fully re-extracted and re-published
    - If extraction produces fewer files than expected, archive is marked failed
    - Processing continues to the next archive even if one fails
    """
    ledger = Ledger(ledger_path)
    ledger.load()

    archives = discover_archives(source_dir)
    published_files: list[PublishedFile] = []
    failure_states: list[ArchiveState] = []
    total_members = 0
    total_published = 0
    total_skipped = 0
    failed_archives = 0

    for archive_job in archives:
        key = archive_job.key

        # Skip only if previously successful
        if ledger.is_success(key):
            # Count members for stats
            try:
                members = plan_members(archive_job)
                total_members += len(members)
                total_skipped += len(members)
            except Exception:
                pass
            continue

        # Plan members
        try:
            members = plan_members(archive_job)
            total_members += len(members)
        except Exception as e:
            # Record discover error
            failed_archives += 1
            state = ArchiveState(
                archive_key=key,
                status=STATUS_FAILED,
                member_count=0,
                published_count=0,
                failure_reason="discover_error",
                error_message=str(e),
            )
            failure_states.append(state)
            if not dry_run:
                ledger.set_state(state)
            continue

        # Process the archive (fresh extraction, overwrite existing)
        published, skipped, files, failure = process_archive(
            archive_job, bronze_root, dry_run=dry_run
        )

        total_published += published
        total_skipped += skipped
        published_files.extend(files)

        if failure:
            failed_archives += 1
            failure_states.append(failure)
            if not dry_run:
                ledger.set_state(failure)
        elif not dry_run:
            # Success: mark in ledger
            ledger.set_state(
                ArchiveState(
                    archive_key=key,
                    status=STATUS_SUCCESS,
                    member_count=len(members),
                    published_count=published,
                )
            )

    if not dry_run:
        ledger.save()

    return RunResult(
        processed_archives=len(archives),
        total_members=total_members,
        published=total_published,
        skipped=total_skipped,
        failed=failed_archives,
        published_files=published_files,
        failure_states=failure_states,
    )


def run_quant360_upstream(
    source_dir: Path,
    bronze_root: Path,
    ledger_path: Path,
    *,
    dry_run: bool = False,
) -> RunResult:
    """Compatibility alias for callers expecting run_quant360_upstream."""
    return run(source_dir, bronze_root, ledger_path, dry_run=dry_run)
