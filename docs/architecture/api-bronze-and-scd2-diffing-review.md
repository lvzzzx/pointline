# Review Notes: API Bronze Format & SCD2 Snapshot Diffing

**Reviewed document:** `docs/architecture/api-bronze-and-scd2-diffing.md`
**Review type:** Design review (PIT correctness, SCD2 behavior, operational safety)
**Date:** 2026-02-11

## Overall Assessment

The design direction is strong: manifest/records separation, explicit tracked columns, and a generic
snapshot-diff fallback are all good foundations. Before implementation, a few contracts should be
tightened to avoid PIT regressions and false SCD2 transitions.

## Findings (Ordered by Severity)

### 1) High: PIT Risk From Start-of-Day `snapshot_ts_us`

The current rule allows scheduled snapshots to use the start of partition date instead of the actual
observation time. This can backdate metadata availability and introduce lookahead bias.

**Risk**
- Metadata becomes valid earlier than when it was actually observable.
- Replay and as-of joins may consume future knowledge.

**Recommendation**
- Define effective timestamp as:
  - `effective_ts_us = vendor_effective_ts_us ?? captured_at_us`
- Do not use date-start as default effective time.
- Keep monotonic forward-only checks relative to prior snapshot.

### 2) High: Dedup Hash Must Be Canonicalized

The document proposes SHA-256 dedup but does not define canonical hashing input. Hashing compressed
`jsonl.gz` bytes is unstable due to compression metadata and serialization variability.

**Risk**
- Equivalent snapshots may produce different hashes (false negatives).
- Cross-run reproducibility is weakened.

**Recommendation**
- Hash canonical uncompressed payload bytes:
  - stable field ordering
  - deterministic row order
  - deterministic null/number formatting
- Optionally store two hashes:
  - `records_payload_sha256` (dedup identity)
  - `records_file_sha256` (artifact integrity)

### 3) High: Delisting-by-Absence Requires Hard Completeness Gate

The diff logic closes symbols missing from current snapshot. Without strict completeness checks,
interrupted pagination/rate-limit failures can create mass false delistings.

**Risk**
- Incorrectly closing active symbols.
- Cascading impact to joins and ingestion validity.

**Recommendation**
- Make snapshot completeness explicit and required in manifest:
  - `complete: bool`
  - expected/actual page or record counts
  - optional response checksum metadata
- Reject diffing when `complete != true`.
- Persist failure status in manifest for operator visibility.

### 4) Medium: Interval Boundary Semantics Need Explicit Contract

The design sets `valid_until_ts = effective_ts_us` but does not define interval inclusivity.

**Risk**
- Off-by-one boundary bugs at transition timestamps.
- Inconsistent join predicates across modules.

**Recommendation**
- Standardize on half-open intervals:
  - valid when `valid_from_ts <= t < valid_until_ts`
- Document this contract and align all as-of join predicates/tests.

### 5) Medium: Global Float Tolerance Is Brittle

A single `float_atol=1e-12` for tracked comparisons may be too strict/loose depending on column scale.

**Risk**
- Spurious version churn or missed meaningful updates.

**Recommendation**
- Prefer fixed-point/int comparison for tracked numeric metadata.
- If float compare is needed, use per-column tolerance rules.

### 6) Medium: Dedup Policy Should Preserve Observation Intent

Current dedup guidance can skip unchanged snapshots entirely.

**Risk**
- Loss of "state observed unchanged at time T" evidence if this is operationally important.

**Recommendation**
- Choose one policy explicitly:
  1. Physical dedup + lightweight observation manifest row (`observed_at_us`, reference to prior snapshot), or
  2. Keep all captures in bronze; dedup only at replay.

## Contract Decisions To Finalize Before Implementation

1. **Effective timestamp source:** vendor-effective vs captured time precedence.
2. **Completeness criteria:** exact checks required for `complete=true`.
3. **Untracked updates behavior:** in-place mutate vs ignore (append-only implications).
4. **Multi-vendor authority:** deterministic precedence by `(exchange, dataset)`.

## Suggested Acceptance Tests

- Snapshot captured at 10:30 does not affect joins at 09:00 (PIT guard).
- Byte-identical logical payload with different gzip metadata dedups correctly.
- Incomplete snapshot never enters diff path and never emits delistings.
- SCD2 transition at `t` respects half-open validity with no overlap.
- Numeric tracked changes only version when fixed-point value actually differs.

## Recommended Next Step

Update the design doc with these contracts first, then implement `diff_snapshots` and replay wiring
behind the finalized timestamp/completeness semantics.
