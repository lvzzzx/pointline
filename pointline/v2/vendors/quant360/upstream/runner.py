"""Function-first runner for Quant360 upstream archive processing."""

from __future__ import annotations

from pathlib import Path

from pointline.v2.vendors.quant360.upstream.contracts import (
    FAILURE_REASON_DISCOVER,
    FAILURE_REASON_EXTRACT,
    FAILURE_REASON_PUBLISH,
    LEDGER_STATUS_FAILED,
    LEDGER_STATUS_SUCCESS,
)
from pointline.v2.vendors.quant360.upstream.discover import (
    discover_quant360_archives,
    plan_archive_members,
)
from pointline.v2.vendors.quant360.upstream.extract import extract_member_payload
from pointline.v2.vendors.quant360.upstream.ledger import Quant360UpstreamLedger, _now_us
from pointline.v2.vendors.quant360.upstream.models import (
    Quant360LedgerRecord,
    Quant360MemberKey,
    Quant360UpstreamRunResult,
)
from pointline.v2.vendors.quant360.upstream.publish import publish_member_payload


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
        try:
            member_jobs = plan_archive_members(archive_job)
        except Exception as exc:
            failed += 1
            record = Quant360LedgerRecord(
                member_key=Quant360MemberKey(
                    archive_sha256=archive_job.archive_sha256,
                    member_path="__archive__",
                ),
                status=LEDGER_STATUS_FAILED,
                updated_at_us=_now_us(),
                failure_reason=FAILURE_REASON_DISCOVER,
                error_message=str(exc),
            )
            ledger.mark_failure(record)
            failure_records.append(record)
            continue

        for member_job in member_jobs:
            total_members += 1
            member_key = member_job.member_key
            if ledger.should_skip(member_key):
                skipped += 1
                continue

            if dry_run:
                continue

            try:
                payload = extract_member_payload(member_job)
            except Exception as exc:
                failed += 1
                record = Quant360LedgerRecord(
                    member_key=member_key,
                    status=LEDGER_STATUS_FAILED,
                    updated_at_us=_now_us(),
                    failure_reason=FAILURE_REASON_EXTRACT,
                    error_message=str(exc),
                )
                ledger.mark_failure(record)
                failure_records.append(record)
                continue

            try:
                published_file = publish_member_payload(payload, bronze_root=bronze_root)
            except Exception as exc:
                failed += 1
                record = Quant360LedgerRecord(
                    member_key=member_key,
                    status=LEDGER_STATUS_FAILED,
                    updated_at_us=_now_us(),
                    failure_reason=FAILURE_REASON_PUBLISH,
                    error_message=str(exc),
                )
                ledger.mark_failure(record)
                failure_records.append(record)
                continue

            published += 1
            published_files.append(published_file)
            ledger.mark_success(
                Quant360LedgerRecord(
                    member_key=member_key,
                    status=LEDGER_STATUS_SUCCESS,
                    updated_at_us=_now_us(),
                    bronze_rel_path=published_file.bronze_rel_path,
                    output_sha256=published_file.output_sha256,
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
