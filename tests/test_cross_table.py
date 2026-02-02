"""Tests for cross-table consistency checks."""

from datetime import date

import polars as pl
import pytest

from pointline.dq.cross_table import (
    CheckReport,
    CheckResult,
    Severity,
    SymbolIntegrityCheck,
    list_cross_table_checks,
    run_all_cross_table_checks,
    run_cross_table_check,
)
from pointline.dq.reporter import (
    format_check_report,
    format_compact_report,
    format_json_report,
)


class TestCheckResult:
    """Test CheckResult dataclass."""

    def test_basic_result(self):
        result = CheckResult(
            check_name="symbol_integrity",
            table_name="trades",
            date_partition=date(2024, 5, 1),
            severity=Severity.CRITICAL,
            passed=True,
            violation_count=0,
            total_count=1000,
            violation_rate=0.0,
            duration_ms=100,
        )

        assert result.check_name == "symbol_integrity"
        assert result.passed is True
        assert result.violation_rate == 0.0

    def test_violation_rate_clamping(self):
        """Test that violation_rate is clamped to 0-1 range."""
        result = CheckResult(
            check_name="test",
            table_name="trades",
            date_partition=None,
            severity=Severity.LOW,
            passed=False,
            violation_count=10,
            total_count=0,  # Division by zero case
            violation_rate=0.0,  # Will be set to 1.0
            duration_ms=10,
        )

        assert result.violation_rate == 1.0


class TestCheckReport:
    """Test CheckReport class."""

    def test_empty_report(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        assert report.overall_passed is True
        assert len(report.results) == 0

    def test_all_passed(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        report.add_result(
            CheckResult(
                check_name="check2",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.HIGH,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        assert report.overall_passed is True
        assert report.overall_duration_ms == 100

    def test_critical_failure(self):
        """Critical failure makes overall fail."""
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=False,
                violation_count=10,
                total_count=100,
                violation_rate=0.1,
                duration_ms=50,
            )
        )

        report.add_result(
            CheckResult(
                check_name="check2",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.LOW,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        assert report.overall_passed is False

    def test_high_failure(self):
        """High severity failure makes overall fail."""
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.HIGH,
                passed=False,
                violation_count=10,
                total_count=100,
                violation_rate=0.1,
                duration_ms=50,
            )
        )

        report.add_result(
            CheckResult(
                check_name="check2",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.MEDIUM,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        assert report.overall_passed is False

    def test_medium_failure_only(self):
        """Only medium severity failure - overall passes."""
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.MEDIUM,
                passed=False,
                violation_count=10,
                total_count=100,
                violation_rate=0.1,
                duration_ms=50,
            )
        )

        assert report.overall_passed is True  # Only medium failed

    def test_get_failed_checks(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=False,
                violation_count=10,
                total_count=100,
                violation_rate=0.1,
                duration_ms=50,
            )
        )

        report.add_result(
            CheckResult(
                check_name="check2",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.LOW,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        failed = report.get_failed_checks()
        assert len(failed) == 1
        assert failed[0].check_name == "check1"

    def test_to_dq_summary_records(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))

        report.add_result(
            CheckResult(
                check_name="symbol_integrity",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=False,
                violation_count=10,
                total_count=100,
                violation_rate=0.1,
                duration_ms=50,
            )
        )

        df = report.to_dq_summary_records()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 1


class TestListCrossTableChecks:
    """Test listing available checks."""

    def test_list_checks(self):
        checks = list_cross_table_checks()
        assert isinstance(checks, list)
        assert len(checks) > 0
        assert "symbol_integrity" in checks
        assert "manifest_consistency" in checks


class TestReporter:
    """Test reporter formatting functions."""

    def test_format_check_result_passed(self):
        result = CheckResult(
            check_name="symbol_integrity",
            table_name="trades",
            date_partition=date(2024, 5, 1),
            severity=Severity.CRITICAL,
            passed=True,
            violation_count=0,
            total_count=1000,
            violation_rate=0.0,
            duration_ms=100,
        )

        output = format_check_report(
            CheckReport(table_name="trades", date_partition=date(2024, 5, 1), results=[result])
        )

        assert "symbol_integrity" in output
        assert "PASSED" in output

    def test_format_compact_report(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))
        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        output = format_compact_report(report)
        assert "trades" in output
        assert "1/1 passed" in output

    def test_format_json_report(self):
        report = CheckReport(table_name="trades", date_partition=date(2024, 5, 1))
        report.add_result(
            CheckResult(
                check_name="check1",
                table_name="trades",
                date_partition=date(2024, 5, 1),
                severity=Severity.CRITICAL,
                passed=True,
                violation_count=0,
                total_count=100,
                violation_rate=0.0,
                duration_ms=50,
            )
        )

        json_data = format_json_report(report)
        assert json_data["table_name"] == "trades"
        assert json_data["overall_passed"] is True
        assert len(json_data["results"]) == 1


class TestIntegration:
    """Integration tests that require actual tables."""

    @pytest.mark.integration
    def test_symbol_integrity_with_missing_tables(self):
        """Test symbol integrity check when tables don't exist."""
        checker = SymbolIntegrityCheck()
        # Use valid table name that likely doesn't exist in lake
        result = checker.execute("trades", date(2099, 1, 1))

        # Should pass (no data) or fail (no dim_symbol), but not error
        assert isinstance(result.passed, bool)

    @pytest.mark.integration
    def test_run_cross_table_check_invalid_name(self):
        """Test running unknown check raises error."""
        with pytest.raises(ValueError, match="Unknown check"):
            run_cross_table_check("invalid_check", "trades", date(2024, 5, 1))

    @pytest.mark.integration
    def test_run_all_checks_no_tables(self):
        """Test running all checks when no tables exist."""
        # This should handle gracefully - use date far in future
        report = run_all_cross_table_checks("trades", date(2099, 1, 1))

        assert isinstance(report, CheckReport)
        assert len(report.results) == 5  # All 5 checks
        # Results depend on whether tables exist, but should not error
