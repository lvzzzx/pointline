# Plan: Rust Feature Engine for L2 Replay

## Goal
Implement a high-performance feature extraction engine within the `l2_replay` Rust crate. This engine will consume L2 delta updates, maintain the Order Book state, and compute derived microstructure features (OIB, Depth, Spread, Shock Counts) *in-process*, returning only the lightweight feature vector to Python.

## Why? (The "Leo" Critique)
- **Current State:** `replay_between` returns raw order book snapshots (or dense arrays) to Python.
- **Problem:** Python/Polars cannot efficiently iterate through 100 levels of depth for millions of seconds to compute `Weighted OIB`. Serializing the full book to Arrow is a memory bottleneck.
- **Solution:** Compute `sum(price * size * weight)` inside the Rust loop. Reduce 200MB of book data to 10MB of features.

## Architecture

### 1. `FeatureExtractor` Trait
Define a trait that runs on every snapshot tick.

```rust
trait FeatureExtractor {
    fn name(&self) -> &str;
    fn calculate(&mut self, book: &OrderBook, mid_price: f64) -> f64;
}
```

### 2. Implemented Features
We will implement the following extractors matching `exp_2026-01-20`:

#### A. Snapshot Features (Instantaneous)
- **`MidPrice`**: `(best_bid + best_ask) / 2`
- **`Spread`**: `best_ask - best_bid`
- **`WeightedDepth`**:
  - Params: `side` (Bid/Ask), `halflife_bps` (Lambda).
  - Logic: Exponential decay weighting based on distance from mid.
- **`OIB` (Order Imbalance)**:
  - Params: `halflife_bps`.
  - Logic: `(WeightedBid - WeightedAsk) / (WeightedBid + WeightedAsk)`

#### B. Stateful Features (The "Scar Tissue")
These require maintaining internal state across ticks within a window (e.g., 1H).

*Note: For V1, we might compute instantaneous features at 1s resolution in Rust, and then aggregate "Shocks" in Polars. This keeps Rust stateless and simple.*
*Decision: V1 = Stateless 1s snapshots. Polars does the `rolling().sum()` for shocks.*

### 3. Configuration Schema (JSON)
Python will pass a JSON config to Rust to define what to extract.

```json
[
  { "type": "OIB", "halflife_bps": 25, "alias": "oib_25bps" },
  { "type": "OIB", "halflife_bps": 50, "alias": "oib_50bps" },
  { "type": "Depth", "side": "bid", "bps": 100, "alias": "depth_bid_100bps" }
]
```

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
        - If `ts % step == 0`:
            - Run all Extractors.
            - Append results to `Arrow` builders.
3.  **Python Binding**:
    - Expose `extract_features`.
    - Map JSON config to Rust structs.

## Timeline
- **Phase 1:** Core `FeatureExtractor` trait + OIB/Depth logic.
- **Phase 2:** Integration into `ops.rs` replay loop.
- **Phase 3:** Python binding and Polars test.

## Verification
- Compare Rust `OIB` output against a slow Python reference implementation for a single 1-minute slice.
