# Design Rationale — Deep Dive

## Table of Contents
- [Function-First Architecture](#function-first-architecture)
- [Fixed-Point Encoding](#fixed-point-encoding)
- [Quarantine Strategy](#quarantine-strategy)
- [No Backward Compatibility](#no-backward-compatibility)
- [Delta Lake Selection](#delta-lake-selection)
- [SCD Type 2 for Symbols](#scd-type-2-for-symbols)
- [Protocol-Based Contracts](#protocol-based-contracts)
- [Single-Machine Constraint](#single-machine-constraint)

## Function-First Architecture

**Choice:** Core business logic implemented as pure functions, not class methods.

**Why:**
- **Testability**: Pure functions need no setup/teardown. Input → output. Assert on output.
- **Composability**: Pipeline stages are independent functions chained in `ingest_file()`. Easy to add/remove/reorder stages.
- **Debuggability**: No hidden state. Every intermediate DataFrame is inspectable.
- **Concurrency-safe**: No shared mutable state between calls.

**Trade-off accepted:**
- No method dispatch on object state. Pipeline flow is explicit in `ingest_file()`.
- Some duplication of parameter passing (silver_root, dim_symbol_df appear in multiple signatures).

**When to break this rule:** Storage layer uses classes (DeltaEventStore, etc.) because it wraps stateful I/O resources (file handles, Delta transaction logs). The protocol/implementation split keeps the stateful boundary narrow.

## Fixed-Point Encoding

**Choice:** All prices and quantities stored as `Int64` scaled by `10^9`.

**Why 10^9:**
- 9 decimal places covers the highest precision seen across all integrated exchanges
- BTC at $100,000 → `100_000_000_000_000` (100 trillion) — well within Int64 range (max 9.2 × 10^18)
- Single scale factor means no per-column lookup. `PRICE_SCALE == QTY_SCALE == 10^9` everywhere.

**Why not Float64:**
- `0.1 + 0.2 != 0.3` in IEEE 754. Research pipelines do millions of comparisons and aggregations. Accumulated rounding errors cause non-determinism.
- Float equality comparison is fragile. Fixed-point equality is exact.
- GroupBy on float columns can produce duplicate groups from rounding.

**Why not Decimal128:**
- 16 bytes vs 8 bytes per value → 2x storage and memory.
- Polars Decimal support is less mature than Int64.
- Decimal arithmetic is slower than integer arithmetic.
- No precision advantage over Int64 at 10^9 scale for our value ranges.

**Decode rule:** `decode_scaled_columns()` only at final research output. Never mid-pipeline. This ensures all intermediate computations maintain exact precision.

## Quarantine Strategy

**Choice:** Invalid rows written to `validation_log` with reason, never silently dropped.

**Why:**
- **Coverage reporting**: Know exactly how many rows from each file were quarantined and why.
- **Debug pipeline**: When a researcher sees unexpected gaps, quarantine log reveals what was removed.
- **Audit trail**: Regulators or internal audit can verify data quality decisions.
- **Iterative improvement**: Quarantine reasons reveal parser bugs or unexpected vendor data formats.

**Trade-off accepted:**
- Extra I/O for writing quarantined rows.
- `validation_log` table grows with every ingestion.
- Slight pipeline complexity (every validation stage returns `(valid, quarantined)` tuple).

**When quarantine is wrong:** If 99%+ of rows in a file are invalid, the file itself is likely corrupt. The manifest should be marked with `status_reason` explaining the failure, rather than quarantining millions of rows.

## No Backward Compatibility

**Choice:** Schema changes require rebuild from Bronze. No migration scripts, no version negotiation.

**Why:**
- Migration scripts compound: each new schema version adds a migration. After N versions, the migration chain is N steps long and fragile.
- Bronze is immutable. Re-ingestion from Bronze always works. Bounded cost.
- Schema-as-code (versioned by git) means the current spec is always the source of truth.
- Single-machine → rebuild latency is hours, not days.

**Trade-off accepted:**
- Rebuild cost for large datasets (days of crypto data = hours of re-ingestion).
- Must keep Bronze files available indefinitely.
- Researchers lose Delta Lake time-travel across schema versions.

**When this hurts:** Adding a column to a high-volume table (trades across 26 exchanges × years). Mitigation: plan schema changes carefully, batch multiple changes into one rebuild.

## Delta Lake Selection

**Choice:** Delta Lake for Silver storage layer.

**Why Delta Lake over raw Parquet:**
- ACID transactions prevent corrupted reads during concurrent write + read.
- Schema enforcement catches mistyped columns at write time.
- Time travel enables debugging ("what did this partition look like before today's ingestion?").
- Compaction merges small files from append-heavy ingestion.

**Why not DuckDB:**
- DuckDB excels at OLAP queries but lacks partitioned append workflows.
- No native partition pruning on Hive-style layouts.
- Single-file format doesn't suit append-heavy ingestion.

**Why not PostgreSQL / TimescaleDB:**
- Columnar storage outperforms row-store for analytical queries.
- No external service dependency (single-machine constraint).
- Polars → Parquet pipeline is zero-copy efficient.

## SCD Type 2 for Symbols

**Choice:** `dim_symbol` tracks all metadata changes with validity windows.

**Why Type 2 over Type 1 (overwrite):**
- PIT queries need historical metadata. "What was BTCUSDT's tick_size on 2023-06-15?"
- As-of joins on validity windows give exact PIT-correct metadata for any event timestamp.

**Why not snapshot tables (one snapshot per day):**
- Snapshot tables require full outer joins across dates — expensive.
- SCD2 as-of join is O(log n) per event row (binary search on valid_from).
- SCD2 naturally handles mid-day metadata changes (rare but possible).

**Validity semantics:**
```
valid_from_ts_us <= event_ts < valid_until_ts_us  (half-open interval)
```
- Current rows: `valid_until_ts_us = 2^63 - 1`
- Closed rows: `valid_until_ts_us = effective_ts_us` of the superseding update

## Protocol-Based Contracts

**Choice:** Storage abstractions defined as `typing.Protocol`, not `abc.ABC`.

**Why:**
- Structural typing: any object with the right methods satisfies the protocol. No inheritance required.
- Test doubles are plain objects/dataclasses — no mock frameworks needed.
- Adding a new implementation (e.g., SQLite-backed ManifestStore) requires zero changes to existing code.

**Trade-off accepted:**
- No runtime isinstance checking (by design — duck typing).
- IDE support for Protocol is slightly less mature than ABC.
- Must rely on mypy/pyright for contract verification at type-check time.

## Single-Machine Constraint

**Choice:** No distributed compute, no cluster, no external services.

**Why:**
- Research workloads are embarrassingly parallelizable across symbols/dates, not within a single query.
- Polars saturates a single machine's cores efficiently.
- No ops burden: no cluster management, no network partitions, no distributed transactions.
- Data volume fits on NVMe: ~1TB for 2 years of crypto L2 + CN L3.

**When this breaks:** If data volume exceeds single-machine storage or if research requires cross-exchange joins across the full dataset. Mitigation: partition research by exchange, sample before full-scale.
