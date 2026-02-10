# Ingest Quarantine Policy (File-Level)

This document defines the file-level quarantine policy for market-data ingestion when each
bronze file contains a single symbol for a single UTC day. The goal is to preserve PIT
correctness by blocking ingestion when required symbol metadata is missing or invalid.

## Assumptions
- One file = one symbol + one UTC day.
- Symbol metadata does not change within a single UTC day.

## Decision Rule (Per File)
Before ingesting a file, verify that `dim_symbol` contains valid row(s) that fully cover
the entire UTC day for the file's symbol. This accounts for intraday metadata changes
(e.g., tick size changes at 12:00 UTC).

Given:
- `exchange_id`, `exchange_symbol` (from file path metadata)
- `file_date` (UTC date from the file path, e.g., `date=2025-12-28`)
- `day_start_us` = UTC start of `file_date`
- `day_end_us` = UTC start of the next day

**Logic:**
1. Select all `dim_symbol` rows for the symbol where the validity interval `[valid_from_ts, valid_until_ts)` overlaps with `[day_start_us, day_end_us)`.
2. Sort rows by `valid_from_ts`.
3. Verify that:
   - The first row starts at or before `day_start_us`.
   - The last row ends at or after `day_end_us`.
   - All rows are contiguous (i.e., `row[i].valid_until_ts == row[i+1].valid_from_ts`).

If the check passes (coverage is complete and contiguous), ingest the file.
Otherwise, quarantine the file.

## Quarantine Outcomes
- **Ingested**: file is processed normally.
- **Quarantined**: file is skipped; no rows are written to Silver.

## Retry Strategy (Un-quarantine)
Files quarantined due to `missing_symbol` or `invalid_validity_window` should be
retried after `dim-symbol sync` has run.

The ingestion process should support a `--retry-quarantined` flag that:
1. Reads `silver.ingest_manifest` for files with `status = quarantined`.
2. Re-runs the **Decision Rule** check against the latest `dim_symbol`.
3. If the check passes, proceeds with ingestion and updates the manifest status to `success`.

## Manifest Recording (Recommended)
Record the decision in `silver.ingest_manifest`:
- `status = success` or `status = quarantined`
- `error_message` set to the reason:
  - `missing_symbol`
  - `invalid_validity_window`
- Optional fields:
  - `quarantine_at` (µs timestamp)
  - `quarantine_reason` (string)

## Notes
- If metadata can change intraday, this policy is insufficient. Switch to row-level checks
  or split files by validity windows.
