# Execution Simulation for L2-Only Crypto MM

## 1) Goal and Constraint

Goal: estimate realistic MM performance when only incremental L2 data is available.
Constraint: no true L3 queue position visibility.

## 2) Minimum Simulator Components

1. Quote lifecycle: place, amend, cancel, expire
2. Exchange constraints: tick size, lot size, min notional
3. Latency model: decision -> send -> exchange ack delay
4. Fill model: probabilistic maker fills at quoted levels
5. Partial fills: support split fills across events
6. Inventory and cash accounting
7. Fee model: maker/taker per venue/tier

## 3) L2 Fill Modeling (Practical)

Because queue rank is unknown, use probabilistic fills:

- Fill probability increases with:
  - traded volume through your level
  - time resting in book
  - tighter quote competitiveness
- Fill probability decreases with:
  - wider spread placement
  - adverse price drift away from quote
  - high cancel churn regime

Recommended: calibrate a simple hazard/logistic fill model from historical L2+trade statistics.

## 4) Conservative Assumption Set (Default)

- Penalize expected fills by a safety haircut.
- Add adverse-selection penalty on fills when short-term drift continues through quote.
- Assume non-zero cancel/replace delay; no instantaneous queue refresh.
- Use taker fees for emergency hedge fills.

## 5) Advanced Mode (100ms-5s)

Only enable with explicit caveats:

- Run sensitivity grid over latency and fill probability.
- Report performance under pessimistic assumptions as primary.
- Reject policies whose edge disappears under modest degradation.

## 6) Required Sensitivity Tests

- Latency +50% and +100%
- Fill probability -20% and -40%
- Spread widening during stress (2x)
- Slippage +50%
- Fee tier worsening by one level

## 7) Validation Checks

- Inventory conservation and cashflow sanity checks
- PnL decomposition: spread capture, fees, slippage, adverse selection, funding
- Reproducibility: same inputs => same outputs
- Compare simulator output against a naive fill model to quantify optimism bias

## 8) Common Failure Modes

- Implicitly assuming first-in-queue behavior
- Ignoring partial fills and stale quote risk
- Using mid-only execution assumptions for maker fills
- Under-modeling downtime/throttle effects on fast refresh strategies