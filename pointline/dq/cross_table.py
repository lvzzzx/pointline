"""Cross-table consistency checks for data quality.

This module provides checks that validate relationships between tables,
such as referential integrity, manifest consistency, and temporal alignment.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

import polars as pl

from pointline.config import TABLE_HAS_DATE, get_table_path
from pointline.tables.dq_summary import create_dq_summary_record


class Severity(Enum):
    """Severity levels for DQ issues."""

    CRITICAL = "critical"  # Data loss, corruption - blocks research
    HIGH = "high"  # Significant quality issues - warning required
    MEDIUM = "medium"  # Minor issues, observations
    LOW = "low"  # Informational only


@dataclass
class CheckResult:
    """Result of a single cross-table check."""

    check_name: str
    table_name: str
    date_partition: date | None
    severity: Severity
    passed: bool
    violation_count: int
    total_count: int
    violation_rate: float
    duration_ms: int
    details: dict[str, Any] | None = None
    recommendation: str | None = None

    def __post_init__(self):
        """Post-initialization to normalize violation rate."""
        if self.total_count == 0:
            # If total is 0, set violation_rate based on whether there are violations
            object.__setattr__(self, "violation_rate", 1.0 if self.violation_count > 0 else 0.0)
        elif not 0 <= self.violation_rate <= 1:
            object.__setattr__(self, "violation_rate", max(0.0, min(1.0, self.violation_rate)))


@dataclass
class CheckReport:
    """Report containing results from multiple checks."""

    table_name: str
    date_partition: date | None
    results: list[CheckResult] = field(default_factory=list)
    overall_passed: bool = True
    overall_duration_ms: int = 0

    def __post_init__(self):
        """Post-initialization to update overall status."""
        self._update_overall_status()

    def add_result(self, result: CheckResult) -> None:
        """Add a check result to the report."""
        self.results.append(result)
        self._update_overall_status()

    def _update_overall_status(self) -> None:
        """Update overall status based on results."""
        if not self.results:
            self.overall_passed = True
            return

        # Failed if any critical or high severity check failed
        critical_failed = any(
            r.severity == Severity.CRITICAL and not r.passed for r in self.results
        )
        high_failed = any(r.severity == Severity.HIGH and not r.passed for r in self.results)
        self.overall_passed = not (critical_failed or high_failed)

        self.overall_duration_ms = sum(r.duration_ms for r in self.results)

    def get_failed_checks(self) -> list[CheckResult]:
        """Get all failed checks."""
        return [r for r in self.results if not r.passed]

    def get_checks_by_severity(self, severity: Severity) -> list[CheckResult]:
        """Get checks with specified severity."""
        return [r for r in self.results if r.severity == severity]

    def to_dq_summary_records(self) -> pl.DataFrame:
        """Convert report to dq_summary record(s)."""
        records = []
        for result in self.results:
            issue_counts = {}
            if not result.passed:
                issue_counts[f"{result.check_name}_violations"] = result.violation_count

            status = "passed" if result.passed else "failed"

            # Create severity-specific issue type
            if not result.passed:
                issue_counts[f"severity_{result.severity.value}"] = 1

            record = create_dq_summary_record(
                table_name=f"{self.table_name}.{result.check_name}",
                date_partition=self.date_partition,
                row_count=result.total_count,
                duplicate_rows=result.violation_count,
                status=status,
                validation_duration_ms=result.duration_ms,
                issue_counts=issue_counts if issue_counts else None,
                run_id=None,  # Will be set by caller
            )
            records.append(record)

        if records:
            return pl.concat(records)
        return pl.DataFrame()


class BaseCrossTableCheck:
    """Base class for cross-table consistency checks."""

    CHECK_NAME: str = "base"
    SEVERITY: Severity = Severity.MEDIUM
    THRESHOLD: float = 0.0  # Default: zero tolerance

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute the check and return result."""
        raise NotImplementedError

    def _table_exists(self, table_path: str) -> bool:
        """Check if a Delta table exists."""
        from pathlib import Path

        return Path(table_path).exists()

    def _get_total_rows(self, table_name: str, date_partition: date | None) -> int:
        """Get total row count for a table/date."""
        try:
            path = get_table_path(table_name)
            lf = pl.scan_delta(str(path))

            if date_partition and TABLE_HAS_DATE.get(table_name, False):
                lf = lf.filter(pl.col("date") == pl.lit(date_partition))

            return lf.select(pl.len()).collect().item()
        except Exception:
            return 0


class SymbolIntegrityCheck(BaseCrossTableCheck):
    """Check that all symbols in data tables exist in dim_symbol."""

    CHECK_NAME = "symbol_integrity"
    SEVERITY = Severity.CRITICAL
    THRESHOLD = 0.0  # Zero tolerance for orphan rows

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute symbol integrity check."""
        import time

        start_ms = int(time.time() * 1000)

        # Check prerequisites
        table_path = get_table_path(table_name)
        dim_path = get_table_path("dim_symbol")

        if not self._table_exists(table_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": f"Table {table_name} does not exist"},
                recommendation=f"Verify table exists: {table_path}",
            )

        if not self._table_exists(dim_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": "dim_symbol table does not exist"},
                recommendation="Run dim_symbol initialization",
            )

        try:
            # Scan target table
            lf_target = pl.scan_delta(str(table_path))
            if date_partition and TABLE_HAS_DATE.get(table_name, False):
                lf_target = lf_target.filter(pl.col("date") == pl.lit(date_partition))

            # Get unique symbols from target event table
            target_symbols = lf_target.select(pl.col("symbol").alias("target_symbol")).unique()

            # Scan dim_symbol (current symbols only) — dim_symbol uses
            # ``exchange_symbol`` for the ticker string that event tables
            # now store in the ``symbol`` column.
            lf_dim = pl.scan_delta(str(dim_path))
            dim_symbols = lf_dim.select(pl.col("exchange_symbol").alias("dim_symbol")).unique()

            # Find orphans via anti-join
            orphans = (
                target_symbols.join(
                    dim_symbols,
                    left_on="target_symbol",
                    right_on="dim_symbol",
                    how="anti",
                )
                .select(pl.col("target_symbol").alias("orphan_symbol"))
                .collect()
            )

            orphan_count = orphans.height
            total_count = target_symbols.select(pl.len()).collect().item()

            violation_rate = orphan_count / total_count if total_count > 0 else 0.0
            passed = violation_rate <= self.THRESHOLD

            duration_ms = int(time.time() * 1000) - start_ms

            details = None
            recommendation = None

            if not passed:
                orphan_ids = orphans["orphan_symbol"].to_list()[:10]  # First 10
                details = {
                    "orphan_symbols": orphan_ids,
                    "orphan_count": orphan_count,
                }
                recommendation = (
                    f"Found {orphan_count:,} orphan rows with {len(orphan_ids)} "
                    f"unique invalid symbols: {orphan_ids}. "
                    "Check if dim_symbol needs updating or if symbols are corrupted."
                )

            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=passed,
                violation_count=orphan_count,
                total_count=total_count,
                violation_rate=violation_rate,
                duration_ms=duration_ms,
                details=details,
                recommendation=recommendation,
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": str(e)},
                recommendation=f"Check failed with error: {e}",
            )


class ManifestConsistencyCheck(BaseCrossTableCheck):
    """Check that manifest row counts match actual silver table counts."""

    CHECK_NAME = "manifest_consistency"
    SEVERITY = Severity.CRITICAL
    THRESHOLD = 0.001  # 0.1% tolerance for small differences

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute manifest consistency check."""
        import time

        from pointline.dq.registry import get_dq_config

        start_ms = int(time.time() * 1000)

        # Get manifest data_type for this table
        try:
            config = get_dq_config(table_name)
            data_type = config.manifest_data_type
        except Exception:
            data_type = None

        if not data_type:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=True,  # Skip check if no manifest mapping
                violation_count=0,
                total_count=0,
                violation_rate=0.0,
                duration_ms=duration_ms,
                details={"skipped": f"No manifest_data_type configured for {table_name}"},
                recommendation=None,
            )

        # Check tables exist
        table_path = get_table_path(table_name)
        manifest_path = get_table_path("ingest_manifest")

        if not self._table_exists(table_path) or not self._table_exists(manifest_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": "Required tables do not exist"},
                recommendation="Verify tables exist",
            )

        try:
            # Load manifest for this data_type
            lf_manifest = pl.scan_delta(str(manifest_path))
            lf_manifest = lf_manifest.filter(
                (pl.col("data_type") == pl.lit(data_type)) & (pl.col("status") == pl.lit("success"))
            )

            if date_partition:
                lf_manifest = lf_manifest.filter(pl.col("date") == pl.lit(date_partition))

            # Get manifest row counts per file
            manifest_counts = lf_manifest.select(
                [
                    pl.col("file_id"),
                    pl.col("row_count").alias("manifest_rows"),
                    pl.col("date"),
                ]
            )

            # Load actual counts from silver table
            lf_silver = pl.scan_delta(str(table_path))
            if date_partition and TABLE_HAS_DATE.get(table_name, False):
                lf_silver = lf_silver.filter(pl.col("date") == pl.lit(date_partition))

            # Count rows per file_id
            silver_counts = (
                lf_silver.group_by("file_id")
                .agg(pl.len().alias("silver_rows"))
                .select([pl.col("file_id"), pl.col("silver_rows")])
            )

            # Join and find mismatches, including silver-only file_ids
            comparison = manifest_counts.join(silver_counts, on="file_id", how="full").collect()

            # Calculate mismatches
            comparison = comparison.with_columns(
                pl.col("manifest_rows").fill_null(0).alias("manifest_rows_filled"),
                pl.col("silver_rows").fill_null(0).alias("silver_rows_filled"),
                pl.col("manifest_rows").is_null().alias("missing_manifest"),
            ).with_columns(
                (pl.col("manifest_rows_filled") - pl.col("silver_rows_filled")).alias("row_diff")
            )

            mismatches = comparison.filter(pl.col("row_diff") != 0)

            total_manifest_rows = comparison["manifest_rows_filled"].sum()
            silver_only_rows = (
                comparison.filter(pl.col("missing_manifest"))
                .select(pl.sum("silver_rows_filled"))
                .item()
            )
            total_diff = abs(mismatches["row_diff"]).sum()
            mismatch_files = mismatches.height

            total_reference_rows = total_manifest_rows + silver_only_rows
            violation_rate = total_diff / total_reference_rows if total_reference_rows > 0 else 0.0
            passed = violation_rate <= self.THRESHOLD

            duration_ms = int(time.time() * 1000) - start_ms

            details = None
            recommendation = None

            if not passed:
                mismatch_details = mismatches.head(5).to_dicts()
                details = {
                    "mismatch_count": mismatch_files,
                    "total_row_difference": int(total_diff),
                    "sample_mismatches": mismatch_details,
                }
                recommendation = (
                    f"Found {mismatch_files} files with row count mismatches. "
                    f"Total difference: {total_diff:,} rows. "
                    f"Sample file_ids: {mismatches['file_id'].to_list()[:3]}. "
                    "Re-run ingestion for affected files."
                )

            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=passed,
                violation_count=int(total_diff),
                total_count=int(total_manifest_rows),
                violation_rate=violation_rate,
                duration_ms=duration_ms,
                details=details,
                recommendation=recommendation,
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": str(e)},
                recommendation=f"Check failed with error: {e}",
            )


class TemporalAlignmentCheck(BaseCrossTableCheck):
    """Check temporal alignment between related tables (e.g., trades vs quotes)."""

    CHECK_NAME = "temporal_alignment"
    SEVERITY = Severity.HIGH
    THRESHOLD = 300.0  # 5 minutes tolerance in seconds

    # Table pairs to check for temporal alignment
    ALIGNMENT_PAIRS = {
        "trades": ["quotes", "book_snapshot_25"],
        "quotes": ["trades", "book_snapshot_25"],
    }

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute temporal alignment check."""
        import time

        start_ms = int(time.time() * 1000)

        # This check requires a date partition
        if not date_partition:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=True,  # Skip if no date
                violation_count=0,
                total_count=0,
                violation_rate=0.0,
                duration_ms=duration_ms,
                details={"skipped": "Date partition required for temporal alignment check"},
                recommendation=None,
            )

        # Get comparison tables
        comparison_tables = self.ALIGNMENT_PAIRS.get(table_name, [])
        if not comparison_tables:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=True,  # Skip if no alignment pairs defined
                violation_count=0,
                total_count=0,
                violation_rate=0.0,
                duration_ms=duration_ms,
                details={"skipped": f"No temporal alignment pairs for {table_name}"},
                recommendation=None,
            )

        table_path = get_table_path(table_name)
        if not self._table_exists(table_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": f"Table {table_name} does not exist"},
                recommendation="Verify table exists",
            )

        try:
            # Get time range for source table
            ts_col = self._get_ts_column(table_name)
            lf_source = pl.scan_delta(str(table_path))
            lf_source = lf_source.filter(pl.col("date") == pl.lit(date_partition))

            source_range = lf_source.select(
                [
                    pl.col(ts_col).min().alias("min_ts"),
                    pl.col(ts_col).max().alias("max_ts"),
                ]
            ).collect()

            if source_range.is_empty() or source_range["max_ts"][0] is None:
                duration_ms = int(time.time() * 1000) - start_ms
                return CheckResult(
                    check_name=self.CHECK_NAME,
                    table_name=table_name,
                    date_partition=date_partition,
                    severity=self.SEVERITY,
                    passed=True,  # Skip if no data
                    violation_count=0,
                    total_count=0,
                    violation_rate=0.0,
                    duration_ms=duration_ms,
                    details={"skipped": "No data in source table for date"},
                    recommendation=None,
                )

            source_max_ts = source_range["max_ts"][0]

            # Check each comparison table
            violations = []
            total_gaps = 0

            for comp_table in comparison_tables:
                comp_path = get_table_path(comp_table)
                if not self._table_exists(comp_path):
                    continue

                comp_ts_col = self._get_ts_column(comp_table)
                lf_comp = pl.scan_delta(str(comp_path))
                lf_comp = lf_comp.filter(pl.col("date") == pl.lit(date_partition))

                comp_range = lf_comp.select(
                    [
                        pl.col(comp_ts_col).min().alias("min_ts"),
                        pl.col(comp_ts_col).max().alias("max_ts"),
                    ]
                ).collect()

                if comp_range.is_empty() or comp_range["max_ts"][0] is None:
                    violations.append(
                        {
                            "table": comp_table,
                            "gap_seconds": float("inf"),
                            "issue": "No data",
                        }
                    )
                    total_gaps += 1
                    continue

                comp_max_ts = comp_range["max_ts"][0]
                gap_seconds = abs(source_max_ts - comp_max_ts) / 1_000_000

                if gap_seconds > self.THRESHOLD:
                    violations.append(
                        {
                            "table": comp_table,
                            "gap_seconds": gap_seconds,
                            "source_max_ts": source_max_ts,
                            "comp_max_ts": comp_max_ts,
                        }
                    )
                    total_gaps += 1

            violation_count = len(violations)
            # Rate: fraction of comparison tables with violations
            violation_rate = violation_count / len(comparison_tables) if comparison_tables else 0.0
            passed = violation_count == 0

            duration_ms = int(time.time() * 1000) - start_ms

            details = None
            recommendation = None

            if not passed:
                details = {"violations": violations, "threshold_seconds": self.THRESHOLD}
                gap_info = ", ".join(
                    [f"{v['table']}: {v.get('gap_seconds', 'N/A')}s" for v in violations]
                )
                recommendation = (
                    f"Temporal misalignment detected. Gaps: {gap_info}. "
                    f"Check if comparison tables have complete data for the date."
                )

            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=passed,
                violation_count=violation_count,
                total_count=len(comparison_tables),
                violation_rate=violation_rate,
                duration_ms=duration_ms,
                details=details,
                recommendation=recommendation,
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": str(e)},
                recommendation=f"Check failed with error: {e}",
            )

    def _get_ts_column(self, table_name: str) -> str:
        """Get timestamp column for a table."""
        from pointline.dq.registry import get_dq_config

        try:
            config = get_dq_config(table_name)
            return config.ts_column or "ts_local_us"
        except Exception:
            return "ts_local_us"


class ExchangeConsistencyCheck(BaseCrossTableCheck):
    """Check that (exchange, symbol) pairs in event tables exist in dim_symbol."""

    CHECK_NAME = "exchange_consistency"
    SEVERITY = Severity.HIGH
    THRESHOLD = 0.0  # Zero tolerance

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute exchange consistency check.

        Verifies that every (exchange, symbol) pair in the fact table has a
        corresponding entry in dim_symbol (via exchange → exchange_id mapping).
        """
        import time

        from pointline.config import get_exchange_id

        start_ms = int(time.time() * 1000)

        table_path = get_table_path(table_name)
        dim_path = get_table_path("dim_symbol")

        if not self._table_exists(table_path) or not self._table_exists(dim_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": "Required tables do not exist"},
                recommendation="Verify tables exist",
            )

        try:
            # Scan target table
            lf_target = pl.scan_delta(str(table_path))
            if date_partition and TABLE_HAS_DATE.get(table_name, False):
                lf_target = lf_target.filter(pl.col("date") == pl.lit(date_partition))

            # Get (exchange, symbol) pairs from target event table
            target_pairs = lf_target.select(["exchange", "symbol"]).unique().collect()

            # Derive exchange_id from exchange name for dim_symbol lookup
            exchange_id_map = {}
            for exch in target_pairs["exchange"].unique().to_list():
                with contextlib.suppress(ValueError):
                    exchange_id_map[exch] = get_exchange_id(exch)

            target_with_id = target_pairs.with_columns(
                pl.col("exchange")
                .replace_strict(exchange_id_map, default=None)
                .cast(pl.Int16)
                .alias("derived_exchange_id")
            )

            # Get exchange_id + exchange_symbol pairs from dim_symbol
            lf_dim = pl.scan_delta(str(dim_path))
            dim_pairs = (
                lf_dim.select(
                    [
                        pl.col("exchange_id").alias("dim_exchange_id"),
                        pl.col("exchange_symbol").alias("symbol"),
                    ]
                )
                .unique()
                .collect()
            )

            # Join and find symbols not in dim_symbol
            joined = target_with_id.join(dim_pairs, on="symbol", how="left")
            mismatches = joined.filter(
                pl.col("derived_exchange_id").is_null()
                | pl.col("dim_exchange_id").is_null()
                | (pl.col("derived_exchange_id") != pl.col("dim_exchange_id"))
            )

            mismatch_count = mismatches.height
            total_pairs = target_pairs.height

            violation_rate = mismatch_count / total_pairs if total_pairs > 0 else 0.0
            passed = violation_rate <= self.THRESHOLD

            duration_ms = int(time.time() * 1000) - start_ms

            details = None
            recommendation = None

            if not passed:
                sample = mismatches.select(["exchange", "symbol"]).head(5).to_dicts()
                details = {"mismatch_count": mismatch_count, "sample": sample}
                recommendation = (
                    f"Found {mismatch_count:,} (exchange, symbol) pairs not in dim_symbol. "
                    f"Check if dim_symbol was updated incorrectly or if symbols are missing."
                )

            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=passed,
                violation_count=mismatch_count,
                total_count=total_pairs,
                violation_rate=violation_rate,
                duration_ms=duration_ms,
                details=details,
                recommendation=recommendation,
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": str(e)},
                recommendation=f"Check failed with error: {e}",
            )


class DateIntegrityCheck(BaseCrossTableCheck):
    """Check that partition dates are valid and consistent with timestamps."""

    CHECK_NAME = "date_integrity"
    SEVERITY = Severity.MEDIUM
    THRESHOLD = 0.001  # 0.1% tolerance

    def execute(
        self,
        table_name: str,
        date_partition: date | None,
    ) -> CheckResult:
        """Execute date integrity check."""
        import time

        start_ms = int(time.time() * 1000)

        if not date_partition:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=True,
                violation_count=0,
                total_count=0,
                violation_rate=0.0,
                duration_ms=duration_ms,
                details={"skipped": "Date partition required"},
                recommendation=None,
            )

        # Check for future dates
        today = date.today()
        if date_partition > today:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=1,
                total_count=1,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": f"Future date: {date_partition} > {today}"},
                recommendation="Check ingestion pipeline for clock skew",
            )

        table_path = get_table_path(table_name)
        if not self._table_exists(table_path):
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": f"Table {table_name} does not exist"},
                recommendation="Verify table exists",
            )

        try:
            lf = pl.scan_delta(str(table_path))

            # Filter for the date partition
            lf = lf.filter(pl.col("date") == pl.lit(date_partition))

            # Get timestamp column
            from pointline.dq.registry import get_dq_config

            try:
                config = get_dq_config(table_name)
                ts_col = config.ts_column or "ts_local_us"
            except Exception:
                ts_col = "ts_local_us"

            # Calculate expected date from timestamp (assumes UTC for crypto)
            # Note: Full implementation would use exchange timezone for non-crypto
            ts_to_date = pl.col(ts_col).cast(pl.Datetime(time_unit="us", time_zone="UTC")).dt.date()

            # Find rows where partition date doesn't match timestamp date
            mismatches = (
                lf.with_columns(ts_to_date.alias("ts_date"))
                .filter(pl.col("ts_date") != pl.col("date"))
                .select([pl.col("date"), pl.col("ts_date"), pl.col(ts_col)])
                .collect()
            )

            mismatch_count = mismatches.height
            total_count = lf.select(pl.len()).collect().item()

            violation_rate = mismatch_count / total_count if total_count > 0 else 0.0
            passed = violation_rate <= self.THRESHOLD

            duration_ms = int(time.time() * 1000) - start_ms

            details = None
            recommendation = None

            if not passed:
                sample = mismatches.head(5).to_dicts()
                details = {"mismatch_count": mismatch_count, "sample": sample}
                recommendation = (
                    f"Found {mismatch_count:,} rows where partition date "
                    f"doesn't match timestamp date. Check timezone handling "
                    f"in ingestion pipeline."
                )

            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=passed,
                violation_count=mismatch_count,
                total_count=total_count,
                violation_rate=violation_rate,
                duration_ms=duration_ms,
                details=details,
                recommendation=recommendation,
            )

        except Exception as e:
            duration_ms = int(time.time() * 1000) - start_ms
            return CheckResult(
                check_name=self.CHECK_NAME,
                table_name=table_name,
                date_partition=date_partition,
                severity=self.SEVERITY,
                passed=False,
                violation_count=0,
                total_count=0,
                violation_rate=1.0,
                duration_ms=duration_ms,
                details={"error": str(e)},
                recommendation=f"Check failed with error: {e}",
            )


# Registry of all cross-table checks
CROSS_TABLE_CHECKS: dict[str, type[BaseCrossTableCheck]] = {
    "symbol_integrity": SymbolIntegrityCheck,
    "manifest_consistency": ManifestConsistencyCheck,
    "temporal_alignment": TemporalAlignmentCheck,
    "exchange_consistency": ExchangeConsistencyCheck,
    "date_integrity": DateIntegrityCheck,
}


def list_cross_table_checks() -> list[str]:
    """Return list of available cross-table check names."""
    return list(CROSS_TABLE_CHECKS.keys())


def run_cross_table_check(
    check_name: str,
    table_name: str,
    date_partition: date | None,
) -> CheckResult:
    """Run a single cross-table check."""
    if check_name not in CROSS_TABLE_CHECKS:
        raise ValueError(f"Unknown check: {check_name}")

    check_class = CROSS_TABLE_CHECKS[check_name]
    checker = check_class()
    return checker.execute(table_name, date_partition)


def run_all_cross_table_checks(
    table_name: str,
    date_partition: date | None,
) -> CheckReport:
    """Run all cross-table checks for a table."""
    report = CheckReport(table_name=table_name, date_partition=date_partition)

    for check_name in CROSS_TABLE_CHECKS:
        result = run_cross_table_check(check_name, table_name, date_partition)
        report.add_result(result)

    return report
