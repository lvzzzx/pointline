# Plan: Rust Feature Engine for L2 Replay

## Goal
Implement a high-performance feature extraction engine within the `l2_replay` Rust crate. This engine will consume L2 delta updates, maintain the Order Book state, and compute derived microstructure features (OIB, Depth, Spread, etc.) *in-process*, returning only the lightweight feature vector to Python. The design must clearly support **resample + aggregate** for all features (including snapshot features) and make **stateful** features first-class (e.g., OFI), even if V1 only ships a minimal subset.

## Why? (The "Leo" Critique)
- **Current State:** `replay_between` returns raw order book snapshots (or dense arrays) to Python.
- **Problem:** Python/Polars cannot efficiently iterate through 100 levels of depth for millions of seconds to compute `Weighted OIB`. Serializing the full book to Arrow is a memory bottleneck.
- **Solution:** Compute `sum(price * size * weight)` inside the Rust loop. Reduce 200MB of book data to 10MB of features.

## Architecture

### 1. Resample + Aggregate Model
All features are treated as **signals** that are resampled into fixed time windows and then
**aggregated**. Snapshot features are just book-based signals; stateful features are delta-based
signals. The default aggregation depends on the feature type, but can be overridden per feature.

```rust
trait FeatureSignal {
    fn name(&self) -> &str;
    fn on_update(
        &mut self,
        update: &L2Update,
        book_before: &OrderBook,
        book_after: &OrderBook,
        mid_price: f64,
    ) -> Option<f64>; // emit a per-update signal value
}

trait WindowAggregator {
    fn push(&mut self, value: f64);
    fn emit_and_reset(&mut self) -> f64;
}
```

Windowing is time-based and driven by `start_ts`, `end_ts`, `step_us`, and an explicit alignment rule
(`StartAligned` vs `EpochAligned`). For snapshot features with `agg=last`, we can optionally compute
once at window end (`sample=window_end`) instead of on every update.

### 2. Implemented Features
We will implement the following extractors matching `exp_2026-01-20`:

#### A. Book-Based Signals (default `agg=last`)
- **`MidPrice`**: `(best_bid + best_ask) / 2` (default `agg=last`)
- **`Spread`**: `best_ask - best_bid` (default `agg=last`)
- **`WeightedDepth`**:
  - Params: `side` (Bid/Ask), `halflife_bps` (Lambda).
  - Logic: Exponential decay weighting based on distance from mid.
- **`OIB` (Order Imbalance)**:
  - Params: `halflife_bps`.
  - Logic: `(WeightedBid - WeightedAsk) / (WeightedBid + WeightedAsk)`

#### B. Delta-Based Signals (default `agg=sum`)
These emit per-update deltas and are aggregated over the window. Example: **OFI**.

**OFI example (delta-based):**
- For each L2 update at a level: `delta = new_size - old_size`
- Contribution: `+delta` for bids, `-delta` for asks
- Optional weighting by distance to mid (e.g., `halflife_bps` or depth cutoff)
- Window value: sum of contributions within the `step_us` window

This provides a concrete stateful example without hard-coding a single OFI definition.

*V1 decision:* snapshot features are required. Stateful support must be present in the design,
and can ship with a minimal example (OFI or simple event counts) while more complex stateful
features can remain in Polars temporarily.

### 3. Configuration Schema (JSON)
Python passes a JSON config to Rust defining both signals and their window aggregation.

```json
[
  { "type": "OIB", "halflife_bps": 25, "agg": "last", "alias": "oib_25bps" },
  { "type": "OIB", "halflife_bps": 50, "agg": "last", "alias": "oib_50bps" },
  { "type": "Depth", "side": "bid", "bps": 100, "agg": "last", "alias": "depth_bid_100bps" },
  { "type": "Spread", "agg": "mean", "alias": "spread_mean_1s" },
  { "type": "OFI", "mode": "delta", "depth_bps": 50, "agg": "sum", "alias": "ofi_50bps" }
]
```

Optional `sample` can override when the signal is computed (e.g., `sample=window_end` for
`agg=last` to avoid per-update evaluation).

### 4. Python API
New function signature in `python_binding.rs`:

```rust
#[pyfunction]
fn extract_features(
    updates_path: String,
    config_json: String,
    start_ts: i64,
    end_ts: i64,
    step_us: i64,
    // ... filters
) -> PyResult<PyObject> // Returns RecordBatch of [ts, feat1, feat2, ...]
```

## Implementation Steps

1.  **Define `features.rs`**:
    - Structs for `OIB`, `Depth`, `Spread`.
    - Logic to iterate `OrderBook` levels efficiently.
2.  **Update `ops.rs`**:
    - Create a new `replay_features_delta` function.
    - Inside the replay loop:
        - Apply L2 Update.
        - For each feature with `sample=event`, call `on_update` and push values to its aggregator.
        - If window boundary:
            - For each feature with `sample=window_end`, compute its value from the current book
              and push once to the aggregator.
            - Emit all aggregators, append results to `Arrow` builders, and reset aggregators.
3.  **Python Binding**:
    - Expose `extract_features`.
    - Map JSON config to Rust structs.

## Timeline
- **Phase 1:** Core `FeatureExtractor` trait + OIB/Depth logic.
- **Phase 2:** Integration into `ops.rs` replay loop.
- **Phase 3:** Python binding and Polars test.

## Verification
- Compare Rust windowed outputs (OIB, Depth, Spread) against a slow Python reference that applies
  the same resample + aggregate rules for a single 1-minute slice.
- For a stateful example (OFI), compare Rust windowed output against a reference that processes
  the same L2 deltas and resampling rules.
