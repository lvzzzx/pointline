# Cross-Table Consistency Checks Explained

**Why this matters:** A trade with an invalid `symbol_id` is useless for research. These checks ensure data integrity across the entire lake.

---

## Real-World Problems These Checks Catch

### Problem 1: Orphan Trades (Silent Data Loss)

**Scenario:** A symbol gets delisted from `dim_symbol` but trades still reference the old `symbol_id`.

```python
# trades table
symbol_id | price_int | qty_int
----------|-----------|--------
12345     | 4500000   | 1000     # BTCUSDT on binance-futures
99999     | 100000    | 500      # ??? Invalid symbol_id!

# dim_symbol table
symbol_id | exchange_symbol | is_current
----------|-----------------|------------
12345     | BTCUSDT         | True
# 99999 doesn't exist!
```

**Impact:** Research query for BTC trades silently excludes the orphaned row. You think you have complete data, but you don't.

**Detection:** Symbol integrity check finds symbol_id 99999 in trades but not in dim_symbol.

---

### Problem 2: Manifest-Silver Mismatch (Data Corruption)

**Scenario:** ETL job reports success but doesn't write all rows.

```python
# ingest_manifest says:
file_id | row_count | status
--------|-----------|--------
1001    | 10000     | success

# But silver.trades actually has:
file_id | COUNT(*)
--------|---------
1001    | 8500     # Missing 1,500 rows!
```

**Impact:** You think you have 10K trades for that day, but you only have 8.5K. Backtests are wrong.

**Detection:** Manifest consistency check compares manifest row_count vs actual count.

---

### Problem 3: Temporal Misalignment (Lookahead Bias)

**Scenario:** Quotes data stops at 23:00 but trades continue until 23:59.

```python
# trades for 2024-05-01
min(ts_local_us) = 00:00:00
max(ts_local_us) = 23:59:59

# quotes for 2024-05-01
min(ts_local_us) = 00:00:00
max(ts_local_us) = 23:00:00  # Missing last hour!
```

**Impact:** If your strategy uses quotes to make trading decisions, it will have no quote data for the last hour but still see trades. This creates unrealistic backtests.

**Detection:** Temporal alignment check ensures timestamp coverage matches within tolerance.

---

## The Five Critical Checks

### Check 1: Symbol Integrity ⭐ HIGHEST PRIORITY

**What it does:** Verifies every `symbol_id` in data tables exists in `dim_symbol`.

**Tables checked:** trades, quotes, book_snapshot_25, derivative_ticker, kline_1h, szse_l3_orders, szse_l3_ticks

**SQL Logic:**
```sql
-- Find trades with invalid symbol_ids
SELECT
    t.exchange,
    t.date,
    COUNT(*) as orphan_count,
    COLLECT_LIST(DISTINCT t.symbol_id) as orphan_symbol_ids
FROM silver.trades t
LEFT JOIN silver.dim_symbol s
    ON t.symbol_id = s.symbol_id
WHERE s.symbol_id IS NULL
  AND t.date = '2024-05-01'
GROUP BY t.exchange, t.date
```

**Example Output:**
```
Check: symbol_integrity (trades)
Status: FAILED ❌
Orphan trades found: 15,234
Orphan symbol_ids: [99998, 99999]
Exchange: binance-futures
Date: 2024-05-01
Severity: CRITICAL
Action: Check if symbols were removed from dim_symbol incorrectly
```

**Common Causes:**
1. Symbol delisted but not handled properly (SCD Type 2 issue)
2. New symbol added to trades before dim_symbol is updated
3. Exchange ID mismatch (symbol_id collision between exchanges)
4. Corrupted ingestion (wrong symbol_id encoding)

---

### Check 2: Manifest Consistency ⭐ HIGH PRIORITY

**What it does:** Verifies that row counts in `ingest_manifest` match actual rows in silver tables.

**Why this matters:** The manifest is the source of truth for what was ingested. If it lies, you can't trust your data.

**SQL Logic:**
```sql
-- Compare manifest vs actual row counts per file
WITH manifest_counts AS (
    SELECT
        file_id,
        row_count as manifest_rows,
        exchange,
        date
    FROM silver.ingest_manifest
    WHERE status = 'success'
      AND date = '2024-05-01'
),
trade_counts AS (
    SELECT
        file_id,
        COUNT(*) as actual_rows
    FROM silver.trades
    WHERE date = '2024-05-01'
    GROUP BY file_id
)
SELECT
    m.file_id,
    m.exchange,
    m.date,
    m.manifest_rows,
    COALESCE(t.actual_rows, 0) as actual_rows,
    m.manifest_rows - COALESCE(t.actual_rows, 0) as missing_rows,
    CASE
        WHEN m.manifest_rows = 0 THEN 0
        ELSE (m.manifest_rows - COALESCE(t.actual_rows, 0)) * 100.0 / m.manifest_rows
    END as missing_pct
FROM manifest_counts m
LEFT JOIN trade_counts t ON m.file_id = t.file_id
WHERE m.manifest_rows != COALESCE(t.actual_rows, 0)
   OR t.actual_rows IS NULL
```

**Example Output:**
```
Check: manifest_consistency (trades)
Status: FAILED ❌
File ID: 1001
Exchange: binance-futures
Date: 2024-05-01
Manifest rows: 10,000
Actual rows: 8,500
Missing: 1,500 (15%)
Severity: CRITICAL
Action: Re-run ingestion for file_id 1001
```

**Common Causes:**
1. Partial write failure (Delta transaction rolled back but manifest committed)
2. Duplicate file_id assignment (two files with same ID)
3. Data quarantine during ingestion (rows removed after counting)
4. Delta table compaction changing row visibility

---

### Check 3: Exchange Consistency

**What it does:** Verifies that `exchange_id` values are consistent across tables.

**SQL Logic:**
```sql
-- Find trades where exchange_id doesn't match dim_symbol
SELECT
    t.exchange_id as trade_exchange_id,
    s.exchange_id as symbol_exchange_id,
    COUNT(*) as mismatch_count
FROM silver.trades t
JOIN silver.dim_symbol s ON t.symbol_id = s.symbol_id
WHERE t.exchange_id != s.exchange_id
  AND t.date = '2024-05-01'
GROUP BY t.exchange_id, s.exchange_id
```

**Common Causes:**
1. Exchange ID remapping in dim_symbol but not updated in silver tables
2. Cross-exchange symbol_id collisions
3. Manual data fixes that missed exchange_id updates

---

### Check 4: Temporal Alignment

**What it does:** Ensures timestamp coverage is consistent across related tables.

**Checks:**
- Trades and quotes should have similar time ranges per symbol/date
- No gaps > 5 minutes in normally continuous data
- No future timestamps

**SQL Logic:**
```sql
-- Compare timestamp coverage between trades and quotes
WITH trade_range AS (
    SELECT
        symbol_id,
        date,
        MIN(ts_local_us) as min_ts,
        MAX(ts_local_us) as max_ts,
        COUNT(*) as count
    FROM silver.trades
    WHERE date = '2024-05-01'
    GROUP BY symbol_id, date
),
quote_range AS (
    SELECT
        symbol_id,
        date,
        MIN(ts_local_us) as min_ts,
        MAX(ts_local_us) as max_ts,
        COUNT(*) as count
    FROM silver.quotes
    WHERE date = '2024-05-01'
    GROUP BY symbol_id, date
)
SELECT
    t.symbol_id,
    t.date,
    t.max_ts as trade_max_ts,
    q.max_ts as quote_max_ts,
    (t.max_ts - q.max_ts) / 1000000.0 as gap_seconds,
    CASE
        WHEN ABS(t.max_ts - q.max_ts) > 300000000 THEN 'CRITICAL'
        WHEN ABS(t.max_ts - q.max_ts) > 60000000 THEN 'HIGH'
        ELSE 'OK'
    END as severity
FROM trade_range t
LEFT JOIN quote_range q
    ON t.symbol_id = q.symbol_id AND t.date = q.date
WHERE q.max_ts IS NULL
   OR ABS(t.max_ts - q.max_ts) > 60000000  -- > 60 seconds gap
```

**Example Output:**
```
Check: temporal_alignment (trades vs quotes)
Status: WARNING ⚠️
Symbol ID: 12345 (BTCUSDT)
Date: 2024-05-01
Trade max time: 23:59:59
Quote max time: 23:00:00
Gap: 59 minutes 59 seconds
Severity: CRITICAL
Action: Check quotes ingestion - missing last hour of data
```

**Common Causes:**
1. Quotes ingestion job failed mid-day
2. Different data sources with different coverage
3. Timezone handling errors
4. Vendor data gaps

---

### Check 5: Date Partition Integrity

**What it does:** Validates date partitions are valid and consistent.

**Checks:**
- No future dates (date > today)
- No dates older than exchange inception
- Date matches derived date from ts_local_us
- No gaps in date sequence (for daily ingestion)

**SQL Logic:**
```sql
-- Find rows where partition date doesn't match timestamp date
SELECT
    date as partition_date,
    FROM_UNIXTIME(ts_local_us / 1000000) as ts_date,
    COUNT(*) as mismatch_count
FROM silver.trades
WHERE date != CAST(FROM_UNIXTIME(ts_local_us / 1000000) AS DATE)
  AND date = '2024-05-01'
GROUP BY date, FROM_UNIXTIME(ts_local_us / 1000000)
```

**Common Causes:**
1. Timezone conversion errors (UTC vs exchange-local)
2. Manual partition overrides during backfill
3. Clock skew in ingestion pipeline

---

## Implementation Architecture

### Directory Structure

```
pointline/dq/
├── __init__.py
├── registry.py           # Existing: DQ configs
├── runner.py             # Existing: DQ execution
├── cross_table.py        # NEW: Cross-table checks
├── reporter.py           # NEW: Report formatting
└line/rules/              # NEW: Rule definitions
    ├── __init__.py
    ├── symbol_integrity.py
    ├── manifest_consistency.py
    ├── temporal_alignment.py
    └── exchange_consistency.py
```

### Check Execution Flow

```python
# pointline/dq/cross_table.py
class CrossTableChecker:
    """Orchestrates cross-table consistency checks."""

    CHECKS = {
        "symbol_integrity": SymbolIntegrityCheck,
        "manifest_consistency": ManifestConsistencyCheck,
        "temporal_alignment": TemporalAlignmentCheck,
        "exchange_consistency": ExchangeConsistencyCheck,
        "date_integrity": DateIntegrityCheck,
    }

    def run_check(
        self,
        check_name: str,
        table_name: str,
        date_partition: date,
    ) -> CheckResult:
        check_class = self.CHECKS[check_name]
        checker = check_class()
        return checker.execute(table_name, date_partition)

    def run_all_checks(
        self,
        table_name: str,
        date_partition: date,
    ) -> CheckReport:
        results = []
        for check_name in self.CHECKS:
            result = self.run_check(check_name, table_name, date_partition)
            results.append(result)
        return CheckReport(results)
```

### Example Check Implementation

```python
# pointline/dq/rules/symbol_integrity.py
class SymbolIntegrityCheck:
    """Check that all symbol_ids exist in dim_symbol."""

    SEVERITY = Severity.CRITICAL
    THRESHOLD = 0.0  # Zero tolerance

    def execute(
        self,
        table_name: str,
        date_partition: date,
    ) -> CheckResult:
        # Build query
        query = f"""
            SELECT
                t.exchange,
                COUNT(*) as orphan_count,
                COUNT(DISTINCT t.symbol_id) as orphan_symbols
            FROM silver.{table_name} t
            LEFT JOIN silver.dim_symbol s
                ON t.symbol_id = s.symbol_id
            WHERE s.symbol_id IS NULL
              AND t.date = '{date_partition}'
            GROUP BY t.exchange
        """

        # Execute with Polars
        result = pl.sql(query).collect()

        total_rows = self._get_total_rows(table_name, date_partition)
        orphan_rows = result["orphan_count"].sum() if not result.is_empty() else 0
        orphan_rate = orphan_rows / total_rows if total_rows > 0 else 0

        passed = orphan_rate <= self.THRESHOLD

        return CheckResult(
            check_name="symbol_integrity",
            table_name=table_name,
            date_partition=date_partition,
            severity=self.SEVERITY,
            passed=passed,
            violation_count=orphan_rows,
            total_count=total_rows,
            violation_rate=orphan_rate,
            details=result.to_dicts() if not passed else None,
            recommendation=self._get_recommendation(result) if not passed else None,
        )

    def _get_recommendation(self, result: pl.DataFrame) -> str:
        orphan_symbols = result.select(
            pl.col("orphan_symbols").explode()
        ).to_series().to_list()

        return (
            f"Found {len(orphan_symbols)} orphan symbol_ids. "
            f"Check dim_symbol for missing entries: {orphan_symbols[:10]}. "
            "Run: pointline dim-symbol sync --check-missing"
        )
```

---

## CLI Usage Examples

### Run Single Check
```bash
# Check symbol integrity for trades on a specific date
$ pointline dq cross-table --check symbol_integrity --table trades --date 2024-05-01

Check: symbol_integrity (trades)
Date: 2024-05-01
Status: PASSED ✅
Orphan rows: 0 / 1,234,567
Time: 0.45s
```

### Run All Checks
```bash
# Run all cross-table checks for trades
$ pointline dq cross-table --all --table trades --date 2024-05-01

Cross-Table Consistency Report: trades (2024-05-01)
==================================================

✅ symbol_integrity        PASSED  (0 orphan rows)
❌ manifest_consistency     FAILED  (file_id 1001: -1,500 rows)
✅ exchange_consistency     PASSED  (0 mismatches)
⚠️  temporal_alignment      WARNING (BTCUSDT: 59min gap trades/quotes)
✅ date_integrity           PASSED  (0 mismatches)

Overall: FAILED (2 passed, 1 failed, 1 warning)

Recommendations:
1. Re-run ingestion for file_id 1001 (manifest mismatch)
2. Check quotes coverage for BTCUSDT (temporal gap)
```

### Auto-Fix Mode (Future)
```bash
# Attempt automatic fixes for fixable issues
$ pointline dq cross-table --all --table trades --date 2024-05-01 --auto-fix

Auto-fixing:
- Regenerating dim_symbol cache... Done
- Re-ingesting file_id 1001... Done

Re-running checks... All passed! ✅
```

---

## Integration with Existing DQ

Cross-table checks will write to the same `silver.dq_summary` table:

```python
# Add cross-table check results to dq_summary
issue_counts = {
    "symbol_integrity_orphans": 15,
    "manifest_mismatch_files": 1,
    "temporal_gaps": 2,
}

dq_record = create_dq_summary_record(
    table_name="trades",
    date_partition=date(2024, 5, 1),
    row_count=1234567,
    status="failed",
    issue_counts=issue_counts,
    # ... other fields
)
```

---

## Business Impact

| Check | Issues Caught / Year (Est.) | Research Impact Prevention |
|-------|----------------------------|---------------------------|
| Symbol Integrity | 5-10 symbol misconfigurations | Prevents silent data loss in backtests |
| Manifest Consistency | 2-5 partial ingestion failures | Prevents incomplete dataset usage |
| Temporal Alignment | 10-15 vendor data gaps | Prevents lookahead bias in strategies |
| Exchange Consistency | 1-2 exchange ID remaps | Prevents cross-exchange contamination |
| Date Integrity | 5-10 timezone issues | Prevents date-boundary errors |

**Estimated Value:** Prevents ~50 hours/year of debugging bad research results.
