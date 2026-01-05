# L2 Replay Engine (Rust + PyO3) Design

This document proposes a high-performance L2 order book replay engine with a Rust core and
Python bindings (PyO3). It preserves Point-in-Time (PIT) semantics, deterministic ordering,
and the `silver.l2_updates` update rules used in this repository.

## 1. Goals

- Deterministic, PIT-correct replay using `ts_local_us`.
- Full-depth reconstruction from incremental updates.
- Performance suitable for both infra batch builds and researcher usage.
- Clean Python interface that integrates with existing data lake helpers.

## 2. Non-negotiable Semantics

- **Replay timeline:** `ts_local_us` (arrival time) only.
- **Stable ordering:** `(ts_local_us, ingest_seq, file_line_number)` ascending.
- **Snapshot rules:** `is_snapshot = true` resets state before applying rows.
- **Update rules:** `size_int == 0` deletes a level; sizes are absolute (not deltas).
- **Fixed-point:** keep `price_int` and `size_int` as `i64` until output.

## 3. Component Layout

### 3.1 Rust Core (`l2_replay`)
Responsibilities:
- Parse and apply ordered updates.
- Maintain full-depth bid/ask maps.
- Emit checkpoints and on-demand snapshots.
- Enforce deterministic ordering and reset semantics.

### 3.2 Infra CLI (`l2_cli`)
Responsibilities:
- Build `gold.l2_snapshot_index` and `gold.l2_state_checkpoint`.
- Scan Delta/Parquet partitions with partition-first filters.
- Write Delta tables with partitioned overwrite or delete-then-append.

### 3.3 Python Bindings (`l2_py`)
Responsibilities:
- Provide a stable researcher API.
- Accept Arrow/Polars batches to avoid extra copies.
- Return snapshots in Arrow/Polars-friendly formats.

## 4. Core Data Structures

```rust
struct L2Update {
    ts_local_us: i64,
    ingest_seq: i32,
    file_line_number: i32,
    is_snapshot: bool,
    side: u8,        // 0 = bid, 1 = ask
    price_int: i64,
    size_int: i64,
    file_id: i32,
}

struct OrderBook {
    bids: std::collections::BTreeMap<i64, i64>,
    asks: std::collections::BTreeMap<i64, i64>,
}
```

`BTreeMap` provides deterministic iteration order for full-depth output. Bids should be emitted
descending (reverse iteration), asks ascending.

## 5. Replay Engine API (Rust)

```rust
fn replay<I: Iterator<Item = L2Update>>(
    updates: I,
    on_snapshot: impl FnMut(&OrderBook, &StreamPos),
    on_checkpoint: impl FnMut(&OrderBook, &StreamPos),
)
```

Engine responsibilities:
- Reset on `is_snapshot`.
- Apply updates in order.
- Emit checkpoints on a time or update cadence.
- Optionally validate monotonic ordering (debug mode).

## 6. Snapshot Index and Checkpoints (Infra Build)

### Snapshot Index
Build `gold.l2_snapshot_index` from `silver.l2_updates`:
- Filter `is_snapshot = true`.
- Group by `(exchange_id, symbol_id, ts_local_us, file_id)`.
- Keep the minimum `file_line_number` per group.

### State Checkpoints
Build `gold.l2_state_checkpoint` by replaying from the latest snapshot and writing the full book
state on a cadence (time or update count). Each checkpoint records the exact stream position.

## 7. IO and Performance

- **Input:** Delta/Parquet via `delta-rs` with Arrow memory layout.
- **Filtering:** always apply `date`, then `exchange`, then `symbol_id`.
- **Parallelism:** per-symbol or per-partition parallelism; never parallelize within a single
  symbol stream.
- **Emission:** materialize `Vec<(price,size)>` only at checkpoint time to reduce overhead.

## 8. Python API (Researcher)

Researcher-facing APIs should be high-level and hide batch management.

Suggested API surface (recommended):

```python
import l2_replay

snapshot = l2_replay.snapshot_at(
    exchange_id=21,
    symbol_id=1234,
    ts_local_us=1700000000000000,
)

for snap in l2_replay.replay_between(
    exchange_id=21,
    symbol_id=1234,
    start_ts_local_us=1700000000000000,
    end_ts_local_us=1700003600000000,
    every_us=1_000_000,  # optional cadence for snapshots
):
    ...
```

Guidance:
- `snapshot_at(...)` should read from `gold.l2_state_checkpoint` when available, then replay
  forward to the target `ts_local_us`.
- `replay_between(...)` should hide input scans and emit full-depth snapshots on a cadence.
- Return snapshots as Arrow Tables (or Polars DataFrames) for zero-copy integration.

Advanced / internal API (infra or power users only):

```python
engine = l2_replay.Engine(exchange_id=21, symbol_id=1234)
engine.apply_batch(arrow_table)   # pre-sorted Arrow/Polars batch
snapshot = engine.snapshot()      # full-depth bids/asks
```

`apply_batch` is a low-level hook intended for infra pipelines; it should not be promoted in
researcher documentation.

## 9. Determinism Guardrails

- Enforce sorted input; optionally assert monotonicity in debug mode.
- Include `(file_id, ingest_seq, file_line_number)` in checkpoints to make replays
  provably reproducible.
- Never use `ts_exch_us` for replay ordering.

## 10. Integration Notes

- Symbol resolution remains in Python (`silver.dim_symbol`), then pass `symbol_id` to the engine.
- `pointline.research.scan_table` can provide Arrow/Polars batches for the bindings.
- Output should remain fixed-point for storage; convert to real values at presentation.
