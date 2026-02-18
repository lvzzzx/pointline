# Pointline-Compatible Simulation Tape Architecture

> Design note for deterministic order book replay in ultra-HFT research while preserving Pointline lake guarantees.

## 0) Positioning in Pointline

- Keep Pointline Silver/Delta as the **research warehouse + audit layer**.
- Add a replay tape store as a **separate runtime-optimized layer** for deterministic simulation.
- Use a **dual-store architecture** (common industry pattern):
  - Delta Lake for analytics, feature generation, reproducibility/audit.
  - Tape store for low-latency sequential replay.

---

## 1) One Logical Event Model, Two Adapters

## 1.1 Logical canonical replay event model (shared by live + simulation)

Canonical fields (fixed-point, deterministic):

- Identity:
  - `exchange`
  - `symbol` (or `instrument_id`)
  - `channel_id`
- Timing:
  - `ts_event_us`
  - `ts_local_us`
- Sequencing:
  - `seq_exchange` (nullable for venues without strict sequence)
- Event type:
  - `BOOK_DELTA`
  - `TRADE`
  - `SNAPSHOT`
  - `GAP`
  - `RESET`
  - `HEARTBEAT`
  - `SESSION_BOUNDARY`
- Payload:
  - Book delta: `side`, `price_int`, `qty_int`, `is_snapshot`
  - Trade: `price_int`, `qty_int`, `aggressor_side`, `trade_id`
- Lineage:
  - `file_id`
  - `file_seq`

Deterministic total-order key:

`(exchange, symbol, channel_id, seq_exchange, ts_event_us, ts_local_us, file_id, file_seq)`

## 1.2 Two adapters

1. **Live adapter**
   - Normalizes real-time feed messages into canonical replay events.
2. **Tape adapter**
   - Reads/writes canonical replay events using tape segment/index/snapshot files.

Strategy code consumes one canonical event interface and can switch source without changing business logic.

---

## 2) Tape Physical Format (Simulation Store)

## 2.1 Physical partitioning

Per partition:

`(exchange, symbol, trading_date, channel_id)`

Example layout:

```text
tapes/exchange=binance-futures/symbol=BTCUSDT/trading_date=2024-05-01/channel=1/
  segment_000001.bin
  segment_000001.idx
  segment_000001.meta.json
  snapshot_000010.bin
  partition_manifest.json
```

## 2.2 Why this partitioning

- Preserves day/channel sequence semantics.
- Enables independent rebuild/recovery of one shard.
- Supports parallel tape compilation.
- Keeps file sizes manageable and operationally predictable.

---

## 3) Multi-Day Continuous Replay (Stitch Layer)

To replay multiple days continuously, add a logical manifest above day partitions.

Example:

```text
tapes/exchange=binance-futures/symbol=BTCUSDT/
  symbol_manifest.json
```

`symbol_manifest.json` should include:

- Ordered list of `(trading_date, channel)` partitions.
- Boundary metadata.
- Reset/gap declarations.
- Format version + checksums.

Replay engine uses this manifest to stream a deterministic continuous timeline across days.

---

## 4) Snapshot + Incremental Chain

- Write periodic snapshots per partition (e.g., every N events or every M seconds).
- Replay startup:
  1. Seek nearest snapshot.
  2. Apply subsequent deltas.

This is required for practical iteration speed on long replay windows.

---

## 5) Gap / Reset Policy (Explicit)

Never hide feed pathologies. Encode control events directly:

- `GAP`
- `RESET`
- `SESSION_BOUNDARY`

Replay should expose explicit policy knobs:

- `on_gap = halt | warn | reset`
- `on_seq_reset = accept | halt`

---

## 6) Build Pipeline (from Pointline Silver)

Compiler flow:

1. Load Silver sources (`orderbook_updates`, `trades`, optional quotes).
2. Normalize rows to canonical replay event model.
3. Deterministic merge-sort by total-order key.
4. Write immutable segments + sparse index + snapshots + manifests.
5. Persist checksums/ranges/build metadata.

Invariant: same Silver snapshot => byte-identical tape artifacts.

---

## 7) Researcher UX

Build tape:

```bash
pointline replay build-tape \
  --silver-root /data/lake/silver \
  --exchange binance-futures \
  --symbol BTCUSDT \
  --start 2024-05-01 \
  --end 2024-05-08 \
  --out /data/lake/tapes
```

Replay multi-day:

```python
source = open_tape_source(
    root="/data/lake/tapes",
    exchange="binance-futures",
    symbol="BTCUSDT",
    start="2024-05-01",
    end="2024-05-08",
    stitch=True,
)

engine = ReplayEngine(source=source, mode="as_fast_as_possible")
for evt in engine:
    strategy.on_event(evt)
```

Switch to live by replacing only the source adapter; strategy callback contract remains unchanged.

---

## 8) Proposed module layout (Pointline-friendly)

```text
pointline/replay/
  model.py
  ordering.py
  adapters/
    live.py
    tape.py
  tape/
    format.py
    writer.py
    reader.py
    index.py
    snapshot.py
    manifest.py
  compiler/
    from_silver.py
```

This keeps replay concerns separate from ingestion/storage/research core paths and avoids dependency-cycle pressure.

---

## 9) Non-Negotiable Invariants

- Deterministic replay order.
- Fixed-point numeric fidelity (no float in tape payloads).
- Lineage retained (`file_id`, `file_seq`).
- Explicit gap/reset/session boundary semantics.
- Immutable artifacts + versioned tape format.

---

## 10) Scope Boundary

This design is for **offline deterministic replay for research/simulation**.
It does not imply live execution stack ownership by Pointline core.
