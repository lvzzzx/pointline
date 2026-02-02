# Profile: HFT & Market Microstructure

**Focus:** Low-latency trading, tick-to-trade analysis, order book dynamics, and Level 3 data processing.
**Triggers:** "latency", "queue position", "imbalance", "L3", "tick-level", "cancellation", "sequence".

## Priority Data Sources
- **`szse_l3_orders` / `szse_l3_ticks`**: Essential for queue simulation and granular flow analysis.
- **`book_snapshot_25`**: For state reconstruction and depth pressure.
- **`trades` (Raw)**: Use `decoded=True` but keep full fidelity (do not downsample).

## Analysis Patterns
- **Queue Position Simulation:** Reconstructing FIFO queues from L3 data.
- **Tick-to-Trade Latency:** Measuring reaction times between signal (book update) and action (order).
- **Adverse Selection (Tick):** Price movement < 10ms after execution.

## Critical Checks
- [ ] **Sequence Continuity:** Are there gaps in `seq_id` or `msg_seq_num`? (Packet loss invalidates HFT research).
- [ ] **Timestamp Precision:** Are you using `ts_local_us` (arrival) for reaction time?
- [ ] **Auction States:** Are you filtering out Call Auction periods where logic differs?
