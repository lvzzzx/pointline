"""Function-first runner for Quant360 upstream archive processing."""

from __future__ import annotations

from pathlib import Path

from pointline.vendors.quant360.upstream.contracts import (
    FAILURE_REASON_DISCOVER,
    FAILURE_REASON_EXTRACT,
    FAILURE_REASON_PUBLISH,
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.vendors.quant360.upstream.discover import (
    discover_quant360_archives,
    plan_archive_members,
)
from pointline.vendors.quant360.upstream.extract import iter_archive_members
from pointline.vendors.quant360.upstream.ledger import Quant360UpstreamLedger, _now_us
from pointline.vendors.quant360.upstream.models import (
    Quant360LedgerRecord,
    Quant360UpstreamRunResult,
)
from pointline.vendors.quant360.upstream.publish import publish_member_payload


def run_quant360_upstream(
    source_dir: Path,
    bronze_root: Path,
    ledger_path: Path,
    *,
    dry_run: bool = False,
) -> Quant360UpstreamRunResult:
    ledger = Quant360UpstreamLedger(ledger_path)
    ledger.load()

    archive_jobs = discover_quant360_archives(source_dir)
    published_files = []
    failure_records = []
    total_members = 0
    published = 0
    skipped = 0
    failed = 0

    for archive_job in archive_jobs:
        archive_key = archive_job.archive_key
        try:
            member_jobs = plan_archive_members(archive_job)
        except Exception as exc:
            failed += 1
            record = Quant360LedgerRecord(
                archive_key=archive_key,
                status=LEDGER_STATUS_FAILED,
                updated_at_us=_now_us(),
                failure_reason=FAILURE_REASON_DISCOVER,
                error_message=str(exc),
            )
            ledger.mark_failure(record)
            failure_records.append(record)
            continue

        total_members += len(member_jobs)
        if ledger.should_skip(archive_key):
            skipped += len(member_jobs)
            continue

        archive_had_failure = False
        archive_published = 0
        archive_failure_reason: str | None = None
        archive_error_message: str | None = None

        if not dry_run:
            try:
                for payload in iter_archive_members(archive_job, member_jobs=member_jobs):
                    try:
                        published_file = publish_member_payload(payload, bronze_root=bronze_root)
                    except Exception as exc:
                        archive_had_failure = True
                        if archive_failure_reason is None:
                            archive_failure_reason = FAILURE_REASON_PUBLISH
                            archive_error_message = str(exc)
                        continue

                    archive_published += 1
                    if published_file.already_exists:
                        skipped += 1
                    else:
                        published += 1
                        published_files.append(published_file)
            except Exception as exc:
                archive_had_failure = True
                if archive_failure_reason is None:
                    archive_failure_reason = FAILURE_REASON_EXTRACT
                    archive_error_message = str(exc)

        if dry_run:
            continue

        if archive_had_failure:
            failed += 1
            record = Quant360LedgerRecord(
                archive_key=archive_key,
                status=LEDGER_STATUS_FAILED,
                updated_at_us=_now_us(),
                failure_reason=archive_failure_reason,
                error_message=archive_error_message,
                member_count=len(member_jobs),
                published_count=archive_published,
            )
            ledger.mark_failure(record)
            failure_records.append(record)
            continue

        ledger.mark_success(
            Quant360LedgerRecord(
                archive_key=archive_key,
                status=LEDGER_STATUS_SUCCESS,
                updated_at_us=_now_us(),
                member_count=len(member_jobs),
                published_count=archive_published,
            )
        )

    if not dry_run:
        ledger.save()

    return Quant360UpstreamRunResult(
        processed_archives=len(archive_jobs),
        total_members=total_members,
        published=published,
        skipped=skipped,
        failed=failed,
        published_files=published_files,
        failure_records=failure_records,
    )
