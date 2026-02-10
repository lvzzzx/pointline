# Data Quality & Monitoring Enhancement Plan

**Status:** Draft
**Priority:** High
**Estimated Effort:** 3-4 weeks

---

## Executive Summary

This plan proposes a robust, multi-layered Data Quality (DQ) and monitoring system for Pointline. The goal is to **detect data issues before they affect research**, provide **actionable alerts**, and maintain **auditability** for reproducible quantitative research.

### Current State Assessment

| Capability | Status | Gap |
|------------|--------|-----|
| Basic DQ checks (row counts, nulls, duplicates) | ✅ Implemented | Limited to static thresholds |
| Per-table profiling (min/max/mean) | ✅ Implemented | No anomaly detection |
| Re-ingestion validation | ✅ Implemented | Manual trigger only |
| Cross-table consistency | ❌ Missing | No referential integrity checks |
| Anomaly detection | ❌ Missing | No statistical outlier detection |
| Automated scheduling | ❌ Missing | No cron/airflow integration |
| Alerting/Notifications | ❌ Missing | No webhook/email/Slack alerts |
| DQ Trends & History | ⚠️ Partial | Basic storage, no visualization |
| Schema Drift Detection | ❌ Missing | No automatic schema change alerts |
| Data Lineage Impact | ❌ Missing | No downstream impact analysis |

---

## Phase 1: Core DQ Enhancements (Week 1)

### 1.1 Auto-Generated DQ Rules from Schema

**Problem:** DQ rules are manually defined in `registry.py` and can become stale.

**Solution:** Automatically generate base DQ rules from table schemas:

```python
# pointline/dq/rule_engine.py
class RuleEngine:
    """Automatically generate and manage DQ rules from schemas."""

    def generate_rules(self, table_name: str) -> list[DQRule]:
        rules = []
        schema = get_schema(table_name)

        # Auto-generate NOT NULL rules for key columns
        for col in schema.required_columns:
            rules.append(NotNullRule(table_name, col))

        # Auto-generate range rules for numeric columns
        for col in schema.numeric_columns:
            rules.append(RangeRule(table_name, col, min_val=None, max_val=None))

        # Auto-generate referential rules for foreign keys
        for fk in schema.foreign_keys:
            rules.append(ReferentialRule(table_name, fk.column, fk.ref_table, fk.ref_column))

        return rules
```

**Implementation:**
- [ ] Create `pointline/dq/rule_engine.py` with rule generation logic
- [ ] Add rule versioning and storage in `silver.dq_rules` table
- [ ] CLI command: `pointline dq rules generate --table trades`
- [ ] CLI command: `pointline dq rules validate` (check rules are current)

---

### 1.2 Cross-Table Consistency Checks

**Problem:** No validation that trades/quotes reference valid symbols or that manifests match silver data.

**Solution:** Implement referential integrity and consistency checks:

```python
# pointline/dq/cross_table_checks.py
CROSS_TABLE_CHECKS = {
    "trades_symbol_integrity": {
        "description": "All trades.symbol_id exist in dim_symbol",
        "query": """
            SELECT COUNT(*) as violation_count
            FROM silver.trades t
            LEFT JOIN silver.dim_symbol s ON t.symbol_id = s.symbol_id
            WHERE s.symbol_id IS NULL
              AND t.date >= '{start_date}' AND t.date <= '{end_date}'
        """
    },
    "manifest_silver_consistency": {
        "description": "Manifest row counts match silver counts per file_id",
        "query": """
            SELECT
                m.file_id,
                m.row_count as manifest_rows,
                COUNT(s.file_line_number) as silver_rows
            FROM silver.ingest_manifest m
            LEFT JOIN silver.trades s ON m.file_id = s.file_id
            WHERE m.status = 'success'
              AND m.date >= '{start_date}' AND m.date <= '{end_date}'
            GROUP BY m.file_id, m.row_count
            HAVING manifest_rows != silver_rows
        """
    },
    "quote_trade_temporal_alignment": {
        "description": "Quotes and trades timestamps align within reasonable bounds",
        "query": """
            -- Ensure max quote ts is within 1 hour of max trade ts per symbol/date
        """
    }
}
```

**Checks to Implement:**
- [ ] Symbol integrity (trades/quotes/book_snapshots ↔ dim_symbol)
- [ ] Manifest consistency (ingest_manifest row counts ↔ actual silver counts)
- [ ] Temporal alignment (trades vs quotes timestamp coverage)
- [ ] Exchange consistency (trades.exchange_id ↔ dim_symbol.exchange_id)
- [ ] Date partition integrity (no future dates, no gaps)

**CLI Commands:**
- [ ] `pointline dq cross-table --check symbol_integrity --table trades`
- [ ] `pointline dq cross-table --all`

---

### 1.3 Enhanced DQ Summary with Severity Levels

**Problem:** All DQ failures are treated equally. A single null is "failed" and so is 50% data loss.

**Solution:** Add severity levels and thresholds:

```python
# pointline/tables/dq_summary.py additions
class Severity(Enum):
    CRITICAL = "critical"    # Data loss, corruption
    HIGH = "high"           # Significant quality issues
    MEDIUM = "medium"       # Minor issues, warnings
    LOW = "low"             # Observations, info

@dataclass
class DQRule:
    name: str
    severity: Severity
    threshold: float  # Percentage (0.0 - 1.0)

    def evaluate(self, violation_count: int, total_count: int) -> bool:
        violation_rate = violation_count / total_count if total_count > 0 else 0
        return violation_rate <= self.threshold

# Default rules per table
DEFAULT_RULES = {
    "trades": [
        DQRule("null_key_rows", Severity.CRITICAL, threshold=0.0),
        DQRule("duplicate_rows", Severity.HIGH, threshold=0.001),  # 0.1%
        DQRule("price_outliers", Severity.MEDIUM, threshold=0.01),  # 1%
    ]
}
```

**Schema Changes:**
```python
DQ_SUMMARY_SCHEMA_V2 = {
    # ... existing fields ...
    "severity": pl.Utf8,  # "critical", "high", "medium", "low"
    "violation_rate": pl.Float64,  # 0.0 - 1.0
    "threshold": pl.Float64,  # Configured threshold
    "passed": pl.Boolean,  # Did it pass the threshold?
}
```

---

## Phase 2: Anomaly Detection (Week 2)

### 2.1 Statistical Anomaly Detection

**Problem:** No detection of unusual patterns (price spikes, volume drops, timestamp gaps).

**Solution:** Implement statistical anomaly detection using z-scores and IQR:

```python
# pointline/dq/anomaly_detection.py
class AnomalyDetector:
    """Detect statistical anomalies in market data."""

    def detect_price_anomalies(
        self,
        table_name: str,
        date_partition: date,
        z_threshold: float = 3.0
    ) -> pl.DataFrame:
        """Detect price jumps/spikes using z-score."""
        df = self._load_data(table_name, date_partition)

        # Calculate returns
        df = df.with_columns(
            (pl.col("price") / pl.col("price").shift(1) - 1)
            .alias("return")
        )

        # Z-score
        mean_return = df["return"].mean()
        std_return = df["return"].std()

        anomalies = df.filter(
            ((pl.col("return") - mean_return) / std_return).abs() > z_threshold
        )

        return anomalies

    def detect_volume_anomalies(
        self,
        table_name: str,
        lookback_days: int = 30
    ) -> pl.DataFrame:
        """Detect unusual volume patterns using historical median."""
        # Compare today's volume vs 30-day median
        pass

    def detect_timestamp_gaps(
        self,
        table_name: str,
        date_partition: date,
        max_gap_sec: int = 300  # 5 minutes
    ) -> pl.DataFrame:
        """Detect gaps in timestamp sequence."""
        df = self._load_data(table_name, date_partition)

        gaps = df.select([
            (pl.col("ts_local_us") - pl.col("ts_local_us").shift(1))
            .alias("gap_us")
        ]).filter(pl.col("gap_us") > max_gap_sec * 1_000_000)

        return gaps
```

**Anomaly Types to Detect:**

| Anomaly | Detection Method | Severity |
|---------|------------------|----------|
| Price spikes/jumps | Z-score > 3 on returns | HIGH |
| Volume drops | Below 5th percentile of 30-day median | MEDIUM |
| Timestamp gaps | Gap > 5 minutes in normally continuous data | HIGH |
| Crossed book (bid >= ask) | Direct check on quotes/book_snapshots | CRITICAL |
| Zero/null prices | Direct check | CRITICAL |
| Duplicate timestamps | Count duplicates | MEDIUM |
| Out-of-sequence timestamps | Check ts_local_us is monotonic | HIGH |

**CLI Commands:**
- [ ] `pointline dq anomalies detect --table trades --date 2024-05-01`
- [ ] `pointline dq anomalies scan --table trades --start 2024-05-01 --end 2024-05-31`

---

### 2.2 Time-Series Trend Analysis

**Problem:** Can't see DQ degradation over time.

**Solution:** Track DQ metrics as time series:

```python
# pointline/dq/trends.py
class DQTrendAnalyzer:
    """Analyze DQ metric trends over time."""

    def calculate_trends(
        self,
        table_name: str,
        metric: str,  # "row_count", "null_rate", "duplicate_rate"
        lookback_days: int = 30
    ) -> dict:
        """Calculate trend statistics for a DQ metric."""
        df = self._load_dq_history(table_name, lookback_days)

        return {
            "current": df[metric].last(),
            "mean": df[metric].mean(),
            "std": df[metric].std(),
            "trend_direction": "increasing" if df[metric].is_increasing() else "decreasing",
            "anomaly": self._is_anomalous(df[metric]),
        }
```

**Visualization (Future):**
- DQ dashboard showing trends over time
- Alerts when trend changes significantly

---

## Phase 3: Alerting & Notifications (Week 2-3)

### 3.1 Alerting System

**Problem:** DQ issues are only visible when manually running checks.

**Solution:** Event-driven alerting system:

```python
# pointline/dq/alerting.py
class AlertManager:
    """Manage DQ alerts and notifications."""

    ALERT_CHANNELS = {
        "slack": SlackNotifier,
        "email": EmailNotifier,
        "webhook": WebhookNotifier,
        "log": LogNotifier,
    }

    def send_alert(
        self,
        alert: DQAlert,
        channels: list[str] = None
    ) -> None:
        """Send alert through configured channels."""
        channels = channels or ["log"]

        for channel in channels:
            notifier = self.ALERT_CHANNELS[channel]()
            notifier.send(alert)

@dataclass
class DQAlert:
    severity: Severity
    table_name: str
    date_partition: date | None
    rule_name: str
    message: str
    violation_count: int
    total_count: int
    timestamp: int  # microseconds

class SlackNotifier:
    def send(self, alert: DQAlert) -> None:
        color = {
            Severity.CRITICAL: "#FF0000",
            Severity.HIGH: "#FF8C00",
            Severity.MEDIUM: "#FFD700",
            Severity.LOW: "#1E90FF",
        }[alert.severity]

        payload = {
            "attachments": [{
                "color": color,
                "title": f"DQ Alert: {alert.table_name}",
                "text": alert.message,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value, "short": True},
                    {"title": "Rule", "value": alert.rule_name, "short": True},
                    {"title": "Violations", "value": f"{alert.violation_count:,}", "short": True},
                    {"title": "Date", "value": str(alert.date_partition), "short": True},
                ]
            }]
        }
        requests.post(self.webhook_url, json=payload)
```

**Alert Routing Rules:**

```yaml
# ~/.config/pointline/dq_alerts.yaml
rules:
  - name: critical_trades_issues
    condition: table == "trades" and severity == "critical"
    channels: ["slack", "email"]

  - name: high_volume_anomalies
    condition: rule_name == "volume_anomaly" and violation_rate > 0.1
    channels: ["slack"]

  - name: all_failures
    condition: status == "failed"
    channels: ["log"]
```

**CLI Commands:**
- [ ] `pointline dq alerts config --set webhook.slack_url=https://hooks.slack.com/...`
- [ ] `pointline dq alerts test --channel slack`

---

### 3.2 Scheduled DQ Runs

**Problem:** DQ only runs manually.

**Solution:** Built-in scheduler and Airflow integration:

```python
# pointline/dq/scheduler.py
class DQScheduler:
    """Schedule and manage DQ runs."""

    def schedule_daily(self, table_name: str, hour: int = 2) -> None:
        """Schedule daily DQ run at specified hour."""
        # Generate cron expression
        cron = f"0 {hour} * * *"

        # Store in config
        self._save_schedule(table_name, cron, "daily")

    def run_scheduled(self) -> None:
        """Check and run due schedules."""
        for schedule in self._get_due_schedules():
            self._execute_schedule(schedule)
```

**CLI Commands:**
- [ ] `pointline dq schedule --table trades --cron "0 2 * * *"`
- [ ] `pointline dq schedule --list`
- [ ] `pointline dq schedule --remove --table trades`

**Airflow Integration:**
```python
# Example DAG
from pointline.dq.runner import run_dq_for_all_tables_partitioned

def dq_check_task(**context):
    run_dq_for_all_tables_partitioned(
        start_date=context['ds'],
        end_date=context['ds'],
        progress_cb=print
    )
```

---

## Phase 4: Schema Drift & Lineage (Week 3)

### 4.1 Schema Drift Detection

**Problem:** Vendor schema changes can break ingestion silently.

**Solution:** Detect and alert on schema changes:

```python
# pointline/dq/schema_drift.py
class SchemaDriftDetector:
    """Detect schema changes in bronze and silver tables."""

    def check_bronze_drift(
        self,
        vendor: str,
        data_type: str
    ) -> SchemaDriftReport:
        """Compare current bronze schema against baseline."""
        current_schema = self._sample_bronze_schema(vendor, data_type)
        baseline_schema = self._load_baseline(vendor, data_type)

        return self._compare_schemas(current_schema, baseline_schema)

    def _compare_schemas(
        self,
        current: Schema,
        baseline: Schema
    ) -> SchemaDriftReport:
        added = current.columns - baseline.columns
        removed = baseline.columns - current.columns
        type_changes = [
            col for col in current.columns & baseline.columns
            if current.types[col] != baseline.types[col]
        ]

        return SchemaDriftReport(
            added_columns=added,
            removed_columns=removed,
            type_changes=type_changes,
            is_breaking=len(removed) > 0 or len(type_changes) > 0
        )
```

**CLI Commands:**
- [ ] `pointline dq schema baseline --vendor tardis --data-type trades`
- [ ] `pointline dq schema check --vendor tardis --data-type trades`

---

### 4.2 Data Lineage Impact Analysis

**Problem:** When data is bad, don't know which research depends on it.

**Solution:** Track downstream dependencies:

```python
# pointline/dq/lineage.py
class LineageTracker:
    """Track data dependencies for impact analysis."""

    def get_downstream_tables(self, table_name: str) -> list[str]:
        """Get tables that depend on this table."""
        # Query gold table dependencies
        pass

    def get_affected_research(
        self,
        table_name: str,
        date_partition: date
    ) -> list[ResearchExperiment]:
        """Get research experiments using this data."""
        # Parse research/03_experiments/*/config.yaml files
        pass

    def generate_impact_report(
        self,
        dq_failure: DQAlert
    ) -> ImpactReport:
        """Generate impact analysis for a DQ failure."""
        return ImpactReport(
            affected_tables=self.get_downstream_tables(dq_failure.table_name),
            affected_research=self.get_affected_research(
                dq_failure.table_name,
                dq_failure.date_partition
            ),
            recommendation=self._generate_recommendation(dq_failure)
        )
```

---

## Phase 5: Monitoring Dashboard (Week 4)

### 5.1 DQ Health Dashboard

**CLI-Based Dashboard:**

```python
# pointline/dq/dashboard.py
class DQDashboard:
    """Generate DQ health reports."""

    def generate_health_report(self) -> str:
        """Generate a rich text health report."""
        tables = list_dq_tables()

        report = []
        report.append("=" * 60)
        report.append("POINTLINE DATA QUALITY HEALTH REPORT")
        report.append(f"Generated: {datetime.now(UTC)}")
        report.append("=" * 60)

        for table in tables:
            latest = self._get_latest_dq_summary(table)
            status_icon = "✅" if latest.status == "passed" else "❌"
            report.append(f"\n{status_icon} {table}")
            report.append(f"   Last check: {latest.validated_at}")
            report.append(f"   Status: {latest.status}")
            report.append(f"   Row count: {latest.row_count:,}")
            if latest.issue_counts:
                report.append(f"   Issues: {latest.issue_counts}")

        return "\n".join(report)
```

**CLI Commands:**
- [ ] `pointline dq health` - Show overall health
- [ ] `pointline dq health --table trades` - Show per-table details
- [ ] `pointline dq health --trend 7d` - Show 7-day trend

---

## Implementation Priority

### Must-Have (Phase 1)
1. Cross-table consistency checks (symbol integrity, manifest consistency)
2. Severity levels in DQ summary
3. Basic alerting (log + webhook)

### Should-Have (Phase 2)
4. Anomaly detection (price spikes, timestamp gaps)
5. Scheduled DQ runs

### Nice-to-Have (Phase 3-4)
6. Schema drift detection
7. Lineage impact analysis
8. Rich dashboard

---

## New CLI Commands Summary

| Command | Description | Phase |
|---------|-------------|-------|
| `pointline dq cross-table` | Run cross-table consistency checks | 1 |
| `pointline dq rules generate` | Auto-generate DQ rules from schema | 1 |
| `pointline dq anomalies detect` | Detect anomalies in data | 2 |
| `pointline dq anomalies scan` | Scan date range for anomalies | 2 |
| `pointline dq schedule` | Schedule recurring DQ runs | 3 |
| `pointline dq alerts config` | Configure alert channels | 3 |
| `pointline dq schema check` | Check for schema drift | 4 |
| `pointline dq health` | Show DQ health dashboard | 5 |
| `pointline dq trends` | Show DQ metric trends | 5 |

---

## New Tables/Schemas

### silver.dq_rules
Stores generated and custom DQ rules:
```python
{
    "rule_id": pl.Int64,
    "table_name": pl.Utf8,
    "rule_name": pl.Utf8,
    "rule_type": pl.Utf8,  # "not_null", "range", "referential", "custom"
    "column": pl.Utf8,
    "parameters": pl.Utf8,  # JSON
    "severity": pl.Utf8,
    "threshold": pl.Float64,
    "is_auto_generated": pl.Boolean,
    "created_at": pl.Int64,
    "updated_at": pl.Int64,
}
```

### silver.dq_alerts
Stores alert history:
```python
{
    "alert_id": pl.Int64,
    "rule_id": pl.Int64,
    "table_name": pl.Utf8,
    "date_partition": pl.Date,
    "severity": pl.Utf8,
    "message": pl.Utf8,
    "violation_count": pl.Int64,
    "violation_rate": pl.Float64,
    "channels_sent": pl.Utf8,  # JSON array
    "created_at": pl.Int64,
}
```

---

## Success Metrics

1. **Detection Speed:** DQ issues detected within 1 hour of data arrival
2. **Coverage:** 100% of silver tables have DQ rules
3. **Alert Response:** Critical alerts sent within 5 minutes
4. **False Positive Rate:** < 5% of alerts are false positives
5. **Research Impact:** Zero research experiments using bad data due to undetected issues

---

## Appendix: Configuration Example

```toml
# ~/.config/pointline/dq_config.toml

[alerts]
channels = ["slack", "log"]

[alerts.slack]
webhook_url = "https://hooks.slack.com/services/..."
channel = "#data-quality"

[alerts.email]
smtp_host = "smtp.gmail.com"
smtp_port = 587
username = "alerts@example.com"
password = "..."
recipients = ["data-team@example.com"]

[rules.trades]
price_z_threshold = 3.0
max_timestamp_gap_sec = 300

[rules.quotes]
crossed_book_threshold = 0.0  # Zero tolerance

[scheduling]
enabled = true
daily_check_time = "02:00"  # 2 AM UTC
```
