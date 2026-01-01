# Ingest Quarantine Policy (File-Level)

This document defines the file-level quarantine policy for market-data ingestion when each
bronze file contains a single symbol for a single UTC day. The goal is to preserve PIT
correctness by blocking ingestion when required symbol metadata is missing or invalid.

## Assumptions
- One file = one symbol + one UTC day.
- Symbol metadata does not change within a single UTC day.

## Decision Rule (Per File)
Before ingesting a file, verify that `dim_symbol` contains a valid row that covers the
entire day for the file's symbol.

Given:
- `exchange_id`, `exchange_symbol` (from file path metadata)
- `file_date` (UTC date from the file path, e.g., `date=2025-12-28`)

Compute:
- `day_start_us` = UTC start of `file_date`
- `day_end_us` = UTC start of the next day

Validity check:
```
valid_from_ts <= day_start_us
AND valid_until_ts > day_end_us
```

If the check passes, ingest the file. Otherwise, quarantine the file.

## Quarantine Outcomes
- **Ingested**: file is processed normally.
- **Quarantined**: file is skipped; no rows are written to Silver.

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
