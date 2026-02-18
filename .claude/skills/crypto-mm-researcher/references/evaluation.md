# Evaluation Framework for Crypto MM

## 1) Core Metrics

### PnL and Risk
- Net PnL (after fees, spread, slippage, funding)
- Inventory-adjusted Sharpe
- Max drawdown and drawdown duration
- Tail loss metrics (e.g., 1%/5% worst periods)

### Fill Quality
- Maker fill rate
- Quote-to-fill ratio
- Cancel ratio
- Effective spread captured per fill

### Adverse Selection
- Post-fill short-horizon markout (e.g., 1s/5s/15s)
- Adverse-selection cost per fill
- Toxic fill fraction (fills followed by adverse drift)

### Inventory Control
- Mean and variance of inventory
- Percent time near soft/hard limits
- Inventory breach count and duration

## 2) Required Breakdowns

- By session: Asia / EU / US
- By volatility regime: low / medium / high
- By liquidity bucket: top pairs vs thinner pairs
- By event windows: funding settlement proximity, liquidation spikes

## 3) Baselines

- Baseline A: static two-sided spread, static inventory limits
- Baseline B: inventory skew only, no toxicity control
- Candidate must beat both on net risk-adjusted basis

## 4) Acceptance Heuristics (Skill Guidance)

A candidate policy is promising when:
- Net Sharpe remains positive under conservative costs
- Inventory tails are controlled (no persistent hard-limit pinning)
- Adverse-selection cost does not dominate spread capture
- Performance is not concentrated in one session only

## 5) Rejection Signals

Reject or rework if:
- Gross PnL positive but net PnL negative after realistic costs
- Edge disappears under mild fill/latency stress
- Inventory breaches are frequent and long-lived
- PnL depends on a single narrow regime

## 6) Reporting Template (Minimum)

1. Configuration summary (fees, latency, fill model version)
2. Aggregate metrics table
3. PnL decomposition chart
4. Markout and adverse-selection analysis
5. Inventory distribution and breach diagnostics
6. Sensitivity table (latency/fill/slippage/cost stress)
7. Decision: promote / iterate / archive