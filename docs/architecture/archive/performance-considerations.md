# Performance Considerations (Local-Host)

This document covers ingestion performance on a single machine and how to tune it.

Environment assumptions:
- local SSD
- Polars + Delta (`delta-rs`)
- no distributed workers by default

---

## 1) Current Throughput Profile (Book Snapshots)

For a typical `book_snapshot_25` file (~790k rows, ~58MB gzipped), current behavior is:

- CSV read: ~0.7s
- parse/list assembly (`pl.concat_list`): ~0.9s
- symbol resolution: ~0.4s
- fixed-point encoding (`list.eval`): ~0.6s
- validation: ~30-35s (dominant cost)
- Delta write: ~1-2s

Expected total:
- ~30-40 seconds per file depending on validation and hardware

The dominant bottleneck remains row-wise validation logic for list ordering checks.

---

## 2) What Is Fast vs Slow

Fast (vectorized):
- scalar-to-list conversion with `pl.concat_list`
- fixed-point transformations via `list.eval`
- partitioned Delta writes

Slow (non-vectorized):
- list-adjacency order checks using Python `map_elements`

---

## 3) Practical Tuning Priorities

1. Keep validation enabled for correctness-sensitive ingestion.
2. Use partition-scoped ingest jobs to bound memory and retry cost.
3. Run Delta optimize on large/hot partitions after ingestion batches.
4. Avoid over-parallelizing on one machine if disk becomes the bottleneck.

---

## 4) Operational Guidance for Localhost

Monitor:
- end-to-end time per file
- validation time share
- memory pressure during parse + validation
- file counts per partition (before/after optimize)

If ingestion is too slow:
1. profile validation first
2. reduce validation strictness only if explicitly accepted
3. test chunked processing for very large files
4. batch optimize/vacuum after major backfills

---

## 5) Future Optimization Targets

1. Replace row-wise ordering checks with vectorized equivalents when Polars supports them.
2. Add optional validation profiles (`strict`, `default`, `fast`) with clear risk labels.
3. Introduce bounded local parallelism per data type with partition-level isolation.

---

## 6) Performance Policy

This project optimizes for correctness first and throughput second.

- PIT and lineage guarantees are not relaxed by default.
- Any fast-path mode must be explicit and documented.
