"""Reporting utilities for cross-table consistency checks."""

from __future__ import annotations

from typing import Any

from pointline.dq.cross_table import CheckReport, CheckResult, Severity


def format_check_result(result: CheckResult, verbose: bool = False) -> str:
    """Format a single check result as a string."""
    lines = []

    # Status icon
    icon = "✅" if result.passed else "❌"
    if not result.passed and result.severity == Severity.MEDIUM:
        icon = "⚠️"

    # Basic line
    status = "PASSED" if result.passed else "FAILED"
    lines.append(
        f"{icon} {result.check_name:<25} {status:<6} "
        f"({result.violation_count:,} / {result.total_count:,}) "
        f"[{result.severity.value}]"
    )

    if verbose and not result.passed:
        lines.append(f"   Violation rate: {result.violation_rate:.2%}")
        lines.append(f"   Duration: {result.duration_ms}ms")
        if result.recommendation:
            lines.append(f"   Recommendation: {result.recommendation}")

    return "\n".join(lines)


def format_check_report(
    report: CheckReport,
    verbose: bool = False,
    show_passed: bool = True,
) -> str:
    """Format a complete check report as a string."""
    lines = []

    # Header
    date_str = str(report.date_partition) if report.date_partition else "all dates"
    lines.append("=" * 70)
    lines.append(f"Cross-Table Consistency Report: {report.table_name} ({date_str})")
    lines.append("=" * 70)

    if not report.results:
        lines.append("\nNo checks were run.")
        return "\n".join(lines)

    # Categorize results
    failed = [r for r in report.results if not r.passed]
    passed = [r for r in report.results if r.passed]

    critical_failed = [r for r in failed if r.severity == Severity.CRITICAL]
    high_failed = [r for r in failed if r.severity == Severity.HIGH]
    medium_failed = [r for r in failed if r.severity == Severity.MEDIUM]

    # Failed checks first
    if failed:
        lines.append("\nFailed Checks:")
        lines.append("-" * 70)

        if critical_failed:
            lines.append("\n🔴 CRITICAL:")
            for result in critical_failed:
                lines.append(format_check_result(result, verbose))

        if high_failed:
            lines.append("\n🟠 HIGH:")
            for result in high_failed:
                lines.append(format_check_result(result, verbose))

        if medium_failed:
            lines.append("\n🟡 MEDIUM:")
            for result in medium_failed:
                lines.append(format_check_result(result, verbose))

    # Passed checks
    if show_passed and passed:
        lines.append("\nPassed Checks:")
        lines.append("-" * 70)
        for result in passed:
            lines.append(format_check_result(result, verbose))

    # Summary
    lines.append("\n" + "=" * 70)
    total = len(report.results)
    fail_count = len(failed)
    pass_count = len(passed)

    if report.overall_passed:
        lines.append(f"Overall: PASSED ✅ ({pass_count}/{total} checks passed)")
    else:
        lines.append(f"Overall: FAILED ❌ ({pass_count} passed, {fail_count} failed)")

    lines.append(f"Total duration: {report.overall_duration_ms}ms")

    # Recommendations for failed checks
    if failed:
        lines.append("\nRecommendations:")
        for i, result in enumerate(failed, 1):
            if result.recommendation:
                lines.append(f"{i}. [{result.check_name}] {result.recommendation}")

    return "\n".join(lines)


def format_compact_report(report: CheckReport) -> str:
    """Format a compact one-line report."""
    date_str = str(report.date_partition) if report.date_partition else "all"

    total = len(report.results)
    failed = len([r for r in report.results if not r.passed])
    critical = len([r for r in report.results if not r.passed and r.severity == Severity.CRITICAL])

    status_icon = "✅" if report.overall_passed else "❌"

    return f"{status_icon} {report.table_name} ({date_str}): {total - failed}/{total} passed" + (
        f" ({critical} critical)" if critical > 0 else ""
    )


def format_json_report(report: CheckReport) -> dict[str, Any]:
    """Format report as a JSON-serializable dictionary."""
    return {
        "table_name": report.table_name,
        "date_partition": (report.date_partition.isoformat() if report.date_partition else None),
        "overall_passed": report.overall_passed,
        "overall_duration_ms": report.overall_duration_ms,
        "results": [
            {
                "check_name": r.check_name,
                "severity": r.severity.value,
                "passed": r.passed,
                "violation_count": r.violation_count,
                "total_count": r.total_count,
                "violation_rate": r.violation_rate,
                "duration_ms": r.duration_ms,
                "details": r.details,
                "recommendation": r.recommendation,
            }
            for r in report.results
        ],
    }


def format_csv_report(report: CheckReport) -> str:
    """Format report as CSV."""
    lines = ["check_name,severity,passed,violation_count,total_count,violation_rate,duration_ms"]

    for r in report.results:
        lines.append(
            f"{r.check_name},{r.severity.value},{r.passed},"
            f"{r.violation_count},{r.total_count},"
            f"{r.violation_rate:.6f},{r.duration_ms}"
        )

    return "\n".join(lines)


def print_report(
    report: CheckReport,
    verbose: bool = False,
    show_passed: bool = True,
    format_type: str = "text",
) -> None:
    """Print a check report to stdout."""
    if format_type == "text":
        print(format_check_report(report, verbose, show_passed))
    elif format_type == "compact":
        print(format_compact_report(report))
    elif format_type == "json":
        import json

        print(json.dumps(format_json_report(report), indent=2))
    elif format_type == "csv":
        print(format_csv_report(report))
    else:
        raise ValueError(f"Unknown format: {format_type}")
