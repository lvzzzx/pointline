# Incremental L2 Replay + Caching Strategy

This document describes a deterministic, research-friendly approach to reconstructing full-depth
Level 2 order books from `silver.l2_updates` and caching the results for long time ranges.

## 1. Goals

- **PIT-correct replay:** match what a trading system would have known at time `ts_local_us`.
- **Deterministic results:** replays must be stable across runs and partitions.
- **Scalable for long ranges:** minimize full-history scans via anchors and checkpoints.
- **Reproducible state:** every cached state ties to a precise position in the event stream.

## 2. Source Data and Semantics

**Primary input:** `silver.l2_updates`

Key fields (see `docs/schemas.md`):
- `ts_local_us`: primary replay timeline
- `ingest_seq`: stable ordering within a file
- `file_line_number`: deterministic tie-breaker
- `is_snapshot`: marks snapshot rows
- `side`: 0 = bid, 1 = ask
- `price_int`, `size_int`: fixed-point (absolute sizes)

**Semantics:**
- `size_int == 0` means delete the price level.
- `is_snapshot == true` means **reset** book state before applying rows.
- Apply updates in **strict order**:
  `(ts_local_us ASC, ingest_seq ASC, file_line_number ASC)`.

## 3. Replay Model

Maintain two maps per `(exchange_id, symbol_id)`:
- `bids: price_int -> size_int`
- `asks: price_int -> size_int`

Apply updates deterministically:
1. If `is_snapshot == true`, clear both maps.
2. For each row:
   - if `size_int == 0`, delete `(side, price_int)`
   - else set `(side, price_int) = size_int`

**Output state** can be emitted:
- at every update (full trace), or
- at periodic intervals, or
- at requested query timestamps only.

## 4. Anchor Selection

For any query range `[start, end]`:
1. Find the **latest snapshot at or before** `start` using `ts_local_us`.
2. If none found within a configured lookback window:
   - mark book as **unknown** until the first snapshot inside `[start, end]`.

**Rule:** never anchor with `book_snapshot_25` when full depth is required.

## 5. Chunking Strategy

- Process data in daily partitions (`date`), but anchor by snapshot boundaries.
- If a new `is_snapshot` row appears mid-stream, **reset** immediately and continue.
- Prefer per-symbol replay to keep memory bounded and simplify state isolation.

## 6. Caching Layers

### 6.1 Snapshot Anchor Index
A lightweight index for fast snapshot lookups.

Suggested table: `gold.l2_snapshot_index`
- `exchange_id`, `symbol_id`
- `ts_local_us`, `date`
- `file_id`, `file_line_number`

Purpose: find `max(ts_local_us) <= start` without scanning full L2 updates.
Tardis can emit multiple price levels in one message, so `ts_local_us` is the message
group key (per symbol + file).

**Schema:** See `docs/schemas.md` (`gold.l2_snapshot_index`).

**Construction (DuckDB, one row per snapshot group):**
```sql
CREATE TABLE IF NOT EXISTS gold.l2_snapshot_index AS
SELECT
  exchange_id,
  symbol_id,
  ts_local_us,
  date,
  file_id,
  MIN(file_line_number) AS file_line_number
FROM delta_scan('${LAKE_ROOT}/silver/l2_updates')
WHERE is_snapshot
GROUP BY exchange_id, symbol_id, ts_local_us, date, file_id;
```

**Anchor lookup:**
```sql
SELECT *
FROM gold.l2_snapshot_index
WHERE exchange_id = ?
  AND symbol_id = ?
  AND ts_local_us <= ?
ORDER BY ts_local_us DESC, file_line_number ASC
LIMIT 1;
```

### 6.2 State Checkpoints (Full Depth)
Checkpoint full book state to avoid long backfills.

Suggested table: `gold.l2_state_checkpoint`
- `exchange_id`, `symbol_id`
- `ts_local_us`
- `bids`, `asks` as `list<struct<price_int, size_int>>`
- `file_id`, `ingest_seq`, `file_line_number`

Recommended cadence:
- every **1 minute** or **10k updates** (whichever comes first), configurable.

**Schema:** See `docs/schemas.md` (`gold.l2_state_checkpoint`).

**Construction (pipeline concept):**
```python
def maybe_checkpoint(state, stream_pos, now_us, last_checkpoint_us):
    if now_us - last_checkpoint_us < 60_000_000 and state.update_count < 10_000:
        return None

    bids = [{"price_int": p, "size_int": s} for p, s in sorted(state.bids.items(), reverse=True)]
    asks = [{"price_int": p, "size_int": s} for p, s in sorted(state.asks.items())]
    return {
        "exchange_id": state.exchange_id,
        "symbol_id": state.symbol_id,
        "ts_local_us": now_us,
        "bids": bids,
        "asks": asks,
        "file_id": stream_pos.file_id,
        "ingest_seq": stream_pos.ingest_seq,
        "file_line_number": stream_pos.file_line_number,
        "checkpoint_kind": "periodic",
    }
```

Store checkpoints in a Delta table. They can be written in batch at the end of each day or
streamed incrementally during replay.

### 6.3 In-Memory Replay Cache (Optional)
- LRU keyed by `(exchange_id, symbol_id, date)` or `(exchange_id, symbol_id, minute)`.
- Stores the last replayed state to accelerate repeated interactive queries.

## 7. Incremental Replay Flow

1. **Resolve symbols** via `silver.dim_symbol` for the query timeline.
2. **Find anchor** via `gold.l2_snapshot_index`.
3. **Use checkpoint** if available and closer than the anchor.
4. **Replay forward** to reach `start`.
5. **Emit state** until `end`, updating checkpoints as needed.

## 8. Build Ownership

Building the snapshot index and state checkpoints is a **data-infra responsibility**. See
`docs/architecture/design.md` for the build recipes and operational guidance.

## 9. Consistency and Invalidation

- Checkpoints must store a **stream position**:
  `(ts_local_us, ingest_seq, file_id, file_line_number)`.
- If ingestion is re-run, rebuild checkpoints for affected `(exchange_id, symbol_id, date)`.
- Always sort by `(ts_local_us, ingest_seq, file_line_number)` to ensure deterministic replay.

## 10. Validation Checks

Recommended invariants during replay:
- No negative sizes.
- Bid/ask separation (best bid < best ask), unless venue rules allow crossing.
- Snapshot reset applied before first snapshot row.

Optional sanity checks:
- Compare top-25 derived from full book vs `silver.book_snapshot_25` for overlap.

## 11. Practical Defaults

- **Anchor lookback:** 3–7 days.
- **Checkpoint cadence:** 1 minute or 10k updates.
- **Checkpoint retention:** 30–90 days for active symbols.
- **Emit format:** fixed-point integers; convert to real values only at presentation.

## 12. Implementation Notes

- Use `ts_local_us` for replay; reserve `ts_exch_us` for latency analysis.
- Filter partitions by `date`, `exchange_id`, then `symbol_id` to avoid full scans.
- Keep `price_int` and `size_int` as integers until the final step.

## 13. Pseudocode

```python
state = {"bids": {}, "asks": {}}

for row in rows_sorted_by_ts_local_us:
    if row.is_snapshot:
        state["bids"].clear()
        state["asks"].clear()

    side_map = state["bids"] if row.side == 0 else state["asks"]
    if row.size_int == 0:
        side_map.pop(row.price_int, None)
    else:
        side_map[row.price_int] = row.size_int
```

This model is deterministic, PIT-correct, and designed to scale across large time ranges by
leveraging anchors and checkpoints.
