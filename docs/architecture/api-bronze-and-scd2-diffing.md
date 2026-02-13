# API Bronze Format & SCD Type 2 Snapshot Diffing

**Status:** Draft (revised per design review 2026-02-11)
**Scope:** Bronze capture format for API-sourced data; snapshot-based SCD2 change detection
**Depends on:** [Architecture Design](design.md), [Schemas](../reference/schemas.md), [Vendor Plugin System](../vendor-plugin-system.md)

---

## 1) Problem Statement

Pointline ingests data from two fundamentally different transports:

| Transport | Examples | Current Support |
|-----------|----------|-----------------|
| **File download** | Tardis CSV.gz, Binance Vision CSV, Quant360 ZIP | Mature (bronze → silver pipeline) |
| **API response** | CoinGecko market stats, Tushare stock_basic, exchange info endpoints | Working but gaps remain |

Both transports must converge at bronze — the **capture boundary** where external data becomes immutable, timestamped, and reproducible. The current API capture works (JSONL.gz envelopes) but has structural inefficiencies and lacks explicit **snapshot diffing** for SCD Type 2 change detection.

This document specifies:
1. A refined bronze format for API-sourced data.
2. A snapshot-to-snapshot diff algorithm that produces SCD2 change sets.
3. How these fit into the existing ingestion pipeline.

---

## 2) Design Principles

These extend the core principles in [design.md](design.md):

- **Capture boundary universality:** Every byte entering the system — file or API — is persisted to bronze before any transformation. No direct-write to silver.
- **Raw fidelity:** Bronze records stay as close to the vendor's original format as possible. Normalization happens at ingestion, not capture.
- **Snapshot diffability:** Full snapshots are the primitive. The system diffs consecutive snapshots to detect changes, rather than relying on vendors to report changes.
- **Explicit change tracking:** SCD2 tracked columns are declared per dataset, not inferred at diff time.
- **Half-open validity intervals:** All SCD2 validity windows use `[valid_from_ts, valid_until_ts)` — inclusive start, exclusive end. A row is valid when `valid_from_ts <= t < valid_until_ts`. This eliminates off-by-one ambiguity at transition boundaries and aligns with the `[T_prev, T)` half-open convention used throughout the research contract.

---

## 3) Bronze Format for API Responses

### 3.1 Current Format (v1)

Each record is individually wrapped in an envelope:

```jsonl
{"schema_version":1,"vendor":"coingecko","dataset":"dim_asset_stats","captured_at_us":...,"snapshot_ts_us":...,"partitions":{...},"request":{...},"record":{...}}
{"schema_version":1,"vendor":"coingecko","dataset":"dim_asset_stats","captured_at_us":...,"snapshot_ts_us":...,"partitions":{...},"request":{...},"record":{...}}
```

**Problem:** Envelope metadata repeated per record. For a 10,000-symbol exchange info snapshot, that's 10,000 redundant copies of `vendor`, `dataset`, `request`, etc.

### 3.2 Proposed Format (v2): Manifest + Records

Separate capture metadata from record data:

```
bronze/{vendor}/type={data_type}/[exchange={exchange}/]date={date}/captured_ts={captured_at_us}/
├── _manifest.json          # capture metadata (once per snapshot)
└── records.jsonl.gz        # raw vendor records (no envelope wrapping)
```

Alternative for stable tabular schemas:

```
bronze/{vendor}/type={data_type}/[exchange={exchange}/]date={date}/captured_ts={captured_at_us}/
├── _manifest.json
└── records.parquet         # columnar, compressed, typed
```

#### `_manifest.json` Schema

```json
{
  "schema_version": 2,
  "vendor": "tushare",
  "dataset": "dim_symbol",
  "data_type": "dim_symbol_metadata",
  "capture_mode": "full_snapshot",
  "record_format": "jsonl.gz",
  "complete": true,
  "captured_at_us": 1714521600000000,
  "vendor_effective_ts_us": null,
  "api_endpoint": "stock_basic",
  "request_params": {
    "exchange": "SZSE",
    "list_status": "L"
  },
  "record_count": 2847,
  "expected_record_count": null,
  "records_content_sha256": "d4e5f6...",
  "records_file_sha256": "a1b2c3...",
  "partitions": {
    "exchange": "szse",
    "date": "2024-05-01"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | int | yes | Always `2` for this format |
| `vendor` | str | yes | Vendor plugin name |
| `dataset` | str | yes | Logical dataset name (e.g., `"dim_symbol"`, `"dim_asset_stats"`) |
| `data_type` | str | yes | Bronze data_type for discovery (e.g., `"dim_symbol_metadata"`) |
| `capture_mode` | str | yes | `"full_snapshot"` or `"incremental_append"` (see 3.3) |
| `record_format` | str | yes | `"jsonl.gz"` or `"parquet"` |
| `complete` | bool | yes | Whether the capture completed fully (see 3.6) |
| `captured_at_us` | int | yes | Wall-clock time of capture (when the API call returned), microseconds UTC |
| `vendor_effective_ts_us` | int\|null | no | Vendor-reported "as-of" time if available; null otherwise (see 4.2) |
| `api_endpoint` | str | yes | API endpoint or method called |
| `request_params` | dict | yes | Parameters passed to the API (for reproducibility audit) |
| `record_count` | int | yes | Actual number of records in the file |
| `expected_record_count` | int\|null | no | Expected count from API pagination metadata, if available |
| `records_content_sha256` | str | yes | SHA-256 of canonical uncompressed payload (see 3.5) — for logical dedup |
| `records_file_sha256` | str | yes | SHA-256 of on-disk file bytes — for artifact integrity |
| `partitions` | dict | yes | Hive partition keys extracted from path |

#### `records.jsonl.gz` Content

Raw API records, one per line, **no envelope wrapping**:

```jsonl
{"ts_code":"000001.SZ","symbol":"000001","name":"平安银行","exchange":"SZSE","list_date":"19910403",...}
{"ts_code":"000002.SZ","symbol":"000002","name":"万科A","exchange":"SZSE","list_date":"19910129",...}
```

These are the vendor's own field names and values. No Pointline normalization at this layer.

### 3.3 Capture Modes

| Mode | When to Use | Bronze Pattern | Downstream |
|------|-------------|----------------|------------|
| `full_snapshot` | API returns complete current state (e.g., `GET /exchangeInfo`) | One directory = full state at capture time | Diff against previous snapshot |
| `incremental_append` | API returns events/deltas (e.g., WebSocket, paginated change log) | Append within daily directory, seal at rollover | Records are already a change stream — apply directly |

Most metadata APIs are `full_snapshot`. The diff pipeline (Section 4) only applies to `full_snapshot` mode.

### 3.4 Record Format Selection

| Choose | When |
|--------|------|
| `jsonl.gz` | API schema is unstable, deeply nested, or vendor may add fields without notice |
| `parquet` | Schema is stable and tabular; you control the capture code; performance matters for large snapshots |

Both are valid bronze. The manifest's `record_format` field tells the replay pipeline how to read.

### 3.5 Idempotency and Integrity Hashing

Two hashes serve different purposes:

| Hash | Input | Purpose |
|------|-------|---------|
| `records_content_sha256` | **Canonical** uncompressed payload (see below) | Logical dedup — "is this the same data?" |
| `records_file_sha256` | Raw bytes of the on-disk file (`records.jsonl.gz` or `.parquet`) | Artifact integrity — "was the file corrupted?" |

**Canonical payload construction** (for `records_content_sha256`):

1. Sort records by natural key columns in deterministic order (e.g., `exchange_symbol` ascending).
2. For JSONL: serialize each record with sorted keys (`json.dumps(record, sort_keys=True)`), UTF-8 encoded, newline-separated.
3. For Parquet: serialize the sorted DataFrame as IPC bytes (`df.write_ipc(None)`).
4. Null values use a consistent sentinel (e.g., JSON `null`, not omitted keys).
5. Float values use fixed-precision formatting where applicable.
6. Hash the resulting byte stream with SHA-256.

**Why two hashes:** Hashing compressed `.gz` bytes is unstable — different gzip implementations, compression levels, or metadata headers produce different bytes for identical logical content. The canonical hash ensures equivalent snapshots dedup correctly regardless of compression artifacts.

**Dedup flow:**

1. After serializing records (before compression), compute `records_content_sha256`.
2. Check `ingest_manifest` for an existing entry with the same `(vendor, dataset, partitions, records_content_sha256)`.
3. If found → **still persist the bronze snapshot** (observation evidence), but record manifest status as `skipped_duplicate` with a reference to the prior snapshot. Do not enter the diff pipeline.
4. If not found → proceed with normal capture and subsequent replay.

This preserves the "we observed this state at time T" evidence while avoiding redundant SCD2 processing.

### 3.6 Snapshot Completeness Gate

For `full_snapshot` capture mode, an incomplete snapshot (e.g., pagination interrupted, rate-limit hit, API timeout) must **never** enter the diff pipeline. Diffing an incomplete snapshot against a complete one would produce false delistings for every symbol that was simply not fetched.

**Contract:** The `complete` field in `_manifest.json` is **required** and must be explicitly set.

**Completeness criteria:**

1. All API pages were fetched successfully (no HTTP errors, no timeouts).
2. If the API reports a total count (e.g., pagination metadata), `record_count == expected_record_count`.
3. Capture code explicitly sets `complete: true` only after all records are written and hashes computed.

**Enforcement:**

- `complete` defaults to `false` in the manifest template. Capture code must explicitly flip to `true` at the end of a successful capture.
- The replay pipeline **rejects** snapshots where `complete != true`:
  - Status recorded as `skipped_incomplete` in `ingest_manifest`.
  - Warning logged with `record_count` vs `expected_record_count` if available.
  - No diff computed, no SCD2 changes applied.
- Incomplete snapshots are still persisted to bronze (for debugging), but never used as a diff baseline.

**Finding the previous snapshot** (Section 6.3) must also filter to `complete == true` — an incomplete snapshot is never a valid baseline for diffing.

### 3.7 Backward Compatibility

The v1 envelope format continues to work. `ApiSnapshotService` detects format version:
- Directory contains `_manifest.json` → v2 path (read manifest, then records file).
- Directory contains `*.jsonl.gz` without `_manifest.json` → v1 path (extract from per-record envelopes).

No migration of existing bronze data required. New captures use v2.

---

## 4) SCD Type 2 Snapshot Diffing

### 4.1 Overview

Given two consecutive full snapshots of reference data, produce a structured diff that feeds into `scd2_upsert`:

```
Snapshot(T-1)  ──┐
                 ├──→  diff_snapshots()  ──→  SCD2Diff  ──→  apply_diff()  ──→  dim_symbol
Snapshot(T)    ──┘
```

### 4.2 Effective Timestamp

Two timestamps exist per snapshot. They serve different purposes:

| Field | Meaning | Source |
|-------|---------|--------|
| `captured_at_us` | Wall-clock time the API response was received | Always set by capture code |
| `vendor_effective_ts_us` | Vendor-reported "as-of" time, if the API provides one | Optional; set by vendor plugin if available |

The **effective timestamp** used for SCD2 transitions (`valid_from_ts` / `valid_until_ts`) is resolved as:

```
effective_ts_us = vendor_effective_ts_us ?? captured_at_us
```

**Contract:**
- **Never use start-of-day as effective timestamp.** A snapshot captured at 10:30 must not create metadata valid from 00:00 — that would backdate availability and introduce lookahead bias in as-of joins.
- **Forward-only monotonicity:** `effective_ts_us` must be strictly greater than the previous snapshot's effective timestamp for the same `(vendor, dataset, partitions)`. The existing `scd2_upsert` enforces `valid_from_ts > current_version.valid_from_ts`.
- `captured_at_us` is always recorded in the manifest for audit, regardless of which timestamp is used as effective.

**Examples:**
- Tushare `stock_basic` has no vendor-reported time → `effective_ts_us = captured_at_us`.
- Tardis instruments API reports `availableSince` per symbol → that becomes `vendor_effective_ts_us` for bootstrap; subsequent syncs use `captured_at_us`.
- CoinGecko `last_updated` field → can be used as `vendor_effective_ts_us` if it represents the data's logical time.

### 4.3 Tracked vs Untracked Columns

Not every column change should trigger a new SCD2 version. Each dataset declares its tracked columns explicitly:

```python
# In the vendor plugin or dataset spec
SCD2_TRACKED_COLUMNS = {
    "dim_symbol": [
        "tick_size",
        "lot_size",
        "price_increment",
        "amount_increment",
        "contract_size",
        "asset_type",
        "expiry_ts_us",
        "underlying_symbol_id",
        "strike",
        "put_call",
    ],
}
```

**Tracked columns** (changes create a new SCD2 version):
- Columns that affect backtest correctness: tick_size, lot_size, contract_size, asset_type.
- Columns that define the instrument identity: expiry, strike, put_call.

**Untracked columns** (updated in place on current version, no new version):
- Display names, cosmetic metadata.
- `base_asset`, `quote_asset` if they are renames without semantic change.

### 4.4 Diff Algorithm

#### Input

```python
def diff_snapshots(
    prev: pl.DataFrame | None,              # previous full snapshot (None for bootstrap)
    curr: pl.DataFrame,                     # current full snapshot
    natural_key: list[str],                 # e.g., ["exchange_id", "exchange_symbol"]
    tracked_cols: list[str],                # columns that trigger SCD2 versioning
    effective_ts_us: int,                   # effective timestamp for changes (see 4.2)
    col_tolerances: dict[str, float] | None = None,  # per-column float tolerance (see 4.4)
) -> SCD2Diff:
```

#### Output

```python
@dataclass
class SCD2Diff:
    new_listings: pl.DataFrame      # in curr but not prev
    modifications: pl.DataFrame     # in both, tracked_cols differ
    delistings: pl.DataFrame        # in prev but not curr
    unchanged_count: int            # for audit logging
    effective_ts_us: int            # effective timestamp for SCD2 transitions
```

#### Logic

```
For each natural key (exchange_id, exchange_symbol):

1. BOOTSTRAP (prev is None):
   → All records in curr are new_listings.

2. NEW LISTING (key in curr, not in prev):
   → Add to new_listings.
   → Will be inserted with valid_from_ts = effective_ts_us.

3. DELISTING (key in prev, not in curr):
   → Add to delistings.
   → Current dim_symbol row will be closed: valid_until_ts = effective_ts_us, is_current = False.
   → No new row inserted.

4. PRESENT IN BOTH:
   a. Compare tracked_cols with tolerance for floats.
   b. If any tracked column changed → add to modifications.
      → Current row closed, new row opened at effective_ts_us.
   c. If only untracked columns changed → no SCD2 action (optionally update in place).
   d. If nothing changed → increment unchanged_count.
```

#### Numeric Comparison Strategy

**Preferred: Fixed-point integer comparison.** Convert float metadata to integers before comparison, eliminating floating-point ambiguity entirely. For `tick_size` and `lot_size`, this means comparing the integer representation (e.g., `tick_size=0.01` → `int(0.01 / 1e-10) = 100_000_000`) rather than the float.

```python
def to_fixed_int(val: float, precision: int = 10) -> int:
    """Convert float to fixed-point integer for exact comparison."""
    return round(val * 10**precision)
```

**Fallback: Per-column tolerance.** When fixed-point conversion is not practical (e.g., `contract_size` with arbitrary precision), use column-specific absolute tolerances:

```python
TRACKED_COLUMN_TOLERANCES = {
    "tick_size": 1e-12,        # price precision — very tight
    "lot_size": 1e-12,         # quantity precision — very tight
    "price_increment": 1e-12,
    "amount_increment": 1e-12,
    "contract_size": 1e-6,     # contract sizes can be large — slightly looser
    "strike": 1e-6,
}
```

**Never use a single global tolerance.** A tolerance of `1e-12` may miss meaningful changes on large-magnitude columns, while being too loose on small-precision columns.

Non-float tracked columns (int, string, bool) use exact equality.

### 4.5 Applying the Diff

```python
def apply_scd2_diff(
    dim_symbol: pl.DataFrame,     # current dim_symbol table
    diff: SCD2Diff,
) -> pl.DataFrame:
```

**Interval contract:** All validity windows are half-open `[valid_from_ts, valid_until_ts)`.

A row is valid when: `valid_from_ts <= t < valid_until_ts`.

At a transition point `T`, the old version's `valid_until_ts = T` and the new version's `valid_from_ts = T`. Because the old interval is exclusive at `T` and the new is inclusive at `T`, there is exactly one valid version at every point in time — no overlap, no gap.

| Diff Category | Action on dim_symbol |
|---------------|---------------------|
| `new_listings` | Insert new row: `valid_from_ts = effective_ts_us`, `valid_until_ts = MAX_INT64`, `is_current = True`. Compute `symbol_id` from `(exchange_id, exchange_symbol, valid_from_ts)`. |
| `modifications` | Close current row: `valid_until_ts = effective_ts_us`, `is_current = False`. Insert new row with updated tracked columns, `valid_from_ts = effective_ts_us`, new `symbol_id`. |
| `delistings` | Close current row: `valid_until_ts = effective_ts_us`, `is_current = False`. No new row. |

All as-of join predicates must use the same half-open convention:

```sql
WHERE valid_from_ts <= t AND t < valid_until_ts
-- NOT: valid_from_ts <= t AND t <= valid_until_ts
```

This maps directly onto the existing `scd2_upsert` in `dim_symbol.py`, with the addition of delisting handling. The existing `resolve_symbol_ids` as-of join must be verified to use `<` (not `<=`) for `valid_until_ts`.

### 4.6 Delisting and Re-listing

Symbols can delist and later re-list (common in crypto). The SCD2 model handles this naturally:

```
symbol: BTCUSDT on exchange X

Row 1: valid_from=T0, valid_until=T5 (delisted at T5), is_current=False
Row 2: valid_from=T8, valid_until=MAX (re-listed at T8), is_current=True
```

- Gap between T5 and T8: no valid version exists.
- As-of join during the gap correctly returns no match → ingestion/research correctly fails for that period.
- Re-listing creates a new `symbol_id` (hash includes `valid_from_ts = T8`).

No special handling needed. The diff naturally produces a delisting at T5 and a new listing at T8.

### 4.7 Bootstrap: First Snapshot

When no previous snapshot exists for a `(vendor, dataset, partition)` combination:

1. `diff_snapshots` receives `prev=None`.
2. All records in `curr` become `new_listings`.
3. `apply_scd2_diff` inserts all as new rows.

The existing `scd2_bootstrap()` function handles this case. The diff pipeline should detect "no prior snapshot" and route through bootstrap automatically.

### 4.8 Vendor-Provided Change History

Some vendors (e.g., Tardis instruments API) provide a `changes` array with historical metadata. This is a richer signal than snapshot diffing.

When available, prefer `rebuild_from_history()` (already in `dim_symbol.py`) over snapshot diffing. The decision tree:

```
Vendor provides change history? (e.g., Tardis `changes` array)
├─ Yes → rebuild_from_history() — most accurate
└─ No  → diff_snapshots() between consecutive bronze snapshots
```

Both paths produce the same SCD2 output format. The diff pipeline is the **general-purpose fallback** that works for any vendor without requiring change history support.

---

## 5) Change Audit Log

Every SCD2 change should be traceable to the bronze snapshots that caused it. This doesn't bloat `dim_symbol` itself but provides an audit trail.

### Option A: Extend `ingest_manifest`

Record snapshot processing in the existing manifest with additional metadata:

```
file_id | vendor  | data_type              | bronze_file_path                    | status  | scd2_new | scd2_modified | scd2_delisted | scd2_unchanged
  42    | tushare | dim_symbol_metadata    | tushare/type=.../snapshot_ts=.../   | success |   3      |     1         |     0         |    2844
```

### Option B: Dedicated `scd2_changelog` table

A separate table logging each version transition:

| Column | Type | Description |
|--------|------|-------------|
| `change_id` | i64 | Auto-increment |
| `target_table` | str | `"dim_symbol"` |
| `natural_key` | str | `"exchange_id=2,exchange_symbol=BTCUSDT"` |
| `change_type` | str | `"new_listing"`, `"modification"`, `"delisting"` |
| `prev_symbol_id` | i64 | Null for new listings |
| `new_symbol_id` | i64 | Null for delistings |
| `effective_ts_us` | i64 | When the change took effect |
| `changed_columns` | str | JSON: `{"tick_size": [0.01, 0.001]}` (old → new) |
| `prev_snapshot_path` | str | Bronze path of previous snapshot |
| `curr_snapshot_path` | str | Bronze path of current snapshot |

**Recommendation:** Start with Option A (lighter, no new table). Add Option B if debugging SCD2 issues becomes frequent.

---

## 6) Full Pipeline: Capture → Diff → Apply

### 6.1 Capture Phase (runs on schedule or on-demand)

```
1. Vendor plugin calls API endpoint.
2. Serialize response to canonical form (sorted keys/rows).
3. Compute records_content_sha256 on canonical payload.
4. Compress → records.jsonl.gz (or write .parquet).
5. Compute records_file_sha256 on the on-disk artifact.
6. Determine completeness:
   a. All pages fetched? No errors/timeouts?
   b. record_count matches expected_record_count (if available)?
   c. Set complete = true only if all checks pass; false otherwise.
7. Write _manifest.json (captured_at_us = now, vendor_effective_ts_us if available).
8. Check ingest_manifest for duplicate records_content_sha256
   → if duplicate: record as skipped_duplicate, stop.
   → if new: persist to bronze path with status = pending.
```

### 6.2 Replay Phase (runs on ingest)

```
1. Discover new snapshot directories in bronze (manifest status = pending).
2. Read _manifest.json.
3. Gate: reject if complete != true → status = skipped_incomplete, stop.
4. Load records from records file (format per record_format field).
5. Vendor plugin normalizes records → silver-compatible columns.
6. Resolve effective_ts_us = vendor_effective_ts_us ?? captured_at_us.
7. Route by capture_mode:
   a. full_snapshot:
      i.  find_previous_snapshot() → load prev (complete=true only).
      ii. diff_snapshots(prev, curr, tracked_cols, effective_ts_us).
   b. incremental_append:
      Records are already changes → wrap as SCD2Diff directly.
8. apply_scd2_diff() against current dim_symbol (or target table).
9. Write updated table.
10. Update ingest_manifest with outcome + SCD2 change counts.
```

### 6.3 Finding the Previous Snapshot

For `full_snapshot` diffing, the pipeline needs the most recent successfully processed **complete** snapshot:

```python
def find_previous_snapshot(
    vendor: str,
    dataset: str,
    partitions: dict,
    before_ts_us: int,
) -> Path | None:
    """Query ingest_manifest for the latest successful snapshot where:
    - vendor, dataset, partitions match
    - effective_ts_us < before_ts_us
    - complete == true
    - status == 'success'
    Returns None if no prior snapshot exists (triggers bootstrap)."""
```

If no previous snapshot exists → bootstrap path (all records are new listings).

---

## 7) Impact on Existing Code

### Vendor Plugin Protocol (`base.py`)

New optional methods:

```python
class VendorPlugin(Protocol):
    # ... existing methods ...

    def get_scd2_tracked_columns(self, dataset: str) -> list[str]:
        """Return columns that trigger SCD2 versioning for this dataset."""
        ...

    def normalize_snapshot_records(
        self, dataset: str, records: pl.DataFrame
    ) -> pl.DataFrame:
        """Convert raw API records to dim_symbol-compatible columns.
        Called during replay, not capture."""
        ...
```

### `ApiSnapshotService`

Refactor to support v2 bronze format:
- Capture: write `_manifest.json` + `records.*` instead of per-record envelopes.
- Replay: detect v1 vs v2, load accordingly, run diff pipeline.

### `dim_symbol.py`

Add delisting support to `scd2_upsert`:
- Current implementation only handles new listings and modifications.
- Add a `delistings` parameter that closes rows without opening new ones.

### `ingest_manifest`

Add optional columns for SCD2 audit (Option A from Section 5):
- `scd2_new`, `scd2_modified`, `scd2_delisted`, `scd2_unchanged` (all nullable Int32).

---

## 8) Migration Path

1. **No breaking changes.** v1 bronze format continues to work.
2. New captures use v2 format. Controlled by a `bronze_format_version` config (default: 2).
3. Existing `scd2_upsert` and `scd2_bootstrap` remain; `diff_snapshots` + `apply_scd2_diff` are additive.
4. Delisting detection is new behavior — first run after upgrade may produce delistings for symbols that disappeared in past snapshots but were never closed.

---

## 9) Contract Decisions (Resolved)

These were open questions in the initial draft, now resolved per design review.

### 9.1 Effective Timestamp Source

**Decision:** `effective_ts_us = vendor_effective_ts_us ?? captured_at_us`. Never use start-of-day. See Section 4.2.

### 9.2 Snapshot Completeness

**Decision:** `complete: bool` is required in the manifest. Incomplete snapshots are persisted to bronze but never enter the diff pipeline. See Section 3.6.

### 9.3 Untracked Column Updates

**Decision:** **Ignore.** Changes to untracked columns do not mutate existing dim_symbol rows. This preserves strict append-only semantics for SCD2 rows — once a row is written, only `valid_until_ts` and `is_current` may change (to close the version).

Rationale: In-place mutation of dimension rows complicates auditing and breaks the assumption that a `symbol_id` uniquely identifies an immutable attribute set. If untracked values need to be queryable, they belong in a separate non-SCD2 table (e.g., `dim_symbol_metadata`) that can be freely overwritten.

### 9.4 Multi-Vendor Authority

**Decision:** One authoritative vendor per `(exchange, dataset)`, configured explicitly:

```python
VENDOR_AUTHORITY = {
    # (exchange, dataset) → vendor name
    ("binance-futures", "dim_symbol"): "tardis",
    ("szse", "dim_symbol"): "tushare",
    ("sse", "dim_symbol"): "tushare",
}
```

If a non-authoritative vendor captures a snapshot for the same `(exchange, dataset)`, it is persisted to bronze (useful for cross-validation) but does not enter the diff/apply pipeline. This prevents conflicting SCD2 updates from different vendors with potentially different observation times.

### 9.5 Dedup Policy

**Decision:** Persist all captures to bronze (observation evidence preserved). Dedup at replay: identical `records_content_sha256` → manifest status `skipped_duplicate`, no diff computed. See Section 3.5.

### 9.6 Snapshot Retention

**Decision:** Keep all bronze API snapshots indefinitely. They are small (typically KB–low MB) and provide irreplaceable historical evidence. Retention review only if bronze storage exceeds a configured threshold (not expected for metadata snapshots).

---

## 10) Acceptance Tests

These tests validate the contracts established in this document. All must pass before the implementation is considered complete.

### PIT Correctness

1. **No lookahead from capture timing:** A snapshot captured at `10:30` with `effective_ts_us = 10:30` must not affect as-of joins at `09:00`. Specifically: `resolve_symbol_ids(ts=09:00)` returns the version that was valid before the snapshot, not the newly captured one.

2. **Forward-only enforcement:** Attempting to apply a snapshot with `effective_ts_us <= previous_effective_ts_us` raises an error and produces no SCD2 changes.

### Hashing and Dedup

3. **Canonical dedup works across runs:** Two captures of byte-identical logical payload (same records, possibly different gzip metadata or compression levels) produce the same `records_content_sha256` and the second is recorded as `skipped_duplicate`.

4. **Different content is not falsely deduped:** A snapshot with one field changed produces a different `records_content_sha256` and enters the diff pipeline normally.

### Completeness Gate

5. **Incomplete snapshot never diffs:** A snapshot with `complete: false` is persisted to bronze, recorded as `skipped_incomplete` in the manifest, and never used as input to `diff_snapshots` (neither as `curr` nor as `prev` baseline).

6. **Incomplete snapshot is not a valid baseline:** `find_previous_snapshot()` never returns a snapshot where `complete != true`.

### SCD2 Interval Correctness

7. **Half-open boundary at transition:** When a modification occurs at timestamp `T`, the old version has `valid_until_ts = T` and the new version has `valid_from_ts = T`. Query at `t = T` returns the **new** version (because `T < T` is false for the old row, `T <= T` is true for the new row).

8. **No overlap, no gap:** For any natural key, the union of all `[valid_from_ts, valid_until_ts)` intervals covers a contiguous range with no overlaps.

### Numeric Comparison

9. **Fixed-point comparison prevents false churn:** `tick_size` values `0.01` and `0.01000000000000000020816681711721685228` (IEEE 754 representation) are treated as equal and do not trigger a new SCD2 version.

10. **Meaningful changes are detected:** `tick_size` changing from `0.01` to `0.001` triggers a new SCD2 version.

### Delisting and Re-listing

11. **Delisting closes version:** Symbol present in previous snapshot but absent from current snapshot → current dim_symbol row closed with `is_current = False`, no new row created.

12. **Re-listing after gap:** Symbol delisted at `T5`, then appears again in a snapshot at `T8` → new row with `valid_from_ts = T8`. As-of join during `[T5, T8)` returns no match.
