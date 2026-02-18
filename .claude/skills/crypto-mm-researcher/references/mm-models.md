# Crypto MM Models (L2-First)

## 1) Baseline Policy Stack

Start with a transparent stack before ML-heavy policies:

1. Fair value estimate (`mid`, optional microprice adjustment)
2. Reservation price with inventory skew
3. Dynamic spread rule
4. Quote placement / refresh cadence
5. Inventory breach hedge rules

## 2) Reservation Price and Inventory Skew

A practical formulation:

- `r_t = fair_t - k_inv * inv_t`
- `inv_t`: signed inventory in base units or notional-normalized units
- `k_inv`: inventory aversion coefficient

Interpretation:
- Long inventory (`inv_t > 0`) shifts reservation price down, biasing sells.
- Short inventory shifts reservation up, biasing buys.

## 3) Dynamic Spread Model

Decompose half-spread:

- `half_spread_t = base_spread + vol_term + tox_term + inv_term`

Where:
- `base_spread`: minimum viable spread under fee/rebate structure
- `vol_term`: rises with short-horizon realized volatility
- `tox_term`: rises with toxicity proxies (OFI shocks, sweep intensity)
- `inv_term`: widens one side when inventory nears soft limit

## 4) Quote Construction

- Bid quote: `q_bid = r_t - half_spread_t`
- Ask quote: `q_ask = r_t + half_spread_t`
- Tick-align and enforce min spread constraints.

Optional side-specific widening:
- If too long: reduce bid aggressiveness, improve ask competitiveness.
- If too short: mirror behavior.

## 5) Strategy Styles

### A) Two-sided passive quoting (default)

- Quote both sides with inventory-aware skew.
- Target spread capture with maker-heavy fills.

### B) Inventory-first single-sided mode

- In stress/inventory breach states, quote mostly one side.
- Use optional taker rebalancing when hard thresholds hit.

## 6) Feature Families for MM Policies

### Core microstructure (L2-derived)
- Top-k depth imbalance
- OFI over 1s/5s/15s windows
- Microprice-mid divergence
- Spread regime and spread jump frequency
- Short-term cancel intensity proxies

### Regime/context features
- Short-horizon realized vol
- Funding countdown / predicted funding extremity
- OI change and liquidation intensity
- Session flags (Asia/EU/US)

## 7) Practical Tuning Defaults

- Keep parameter count low in v1.
- Tune in this order:
  1. `base_spread`
  2. `k_inv`
  3. toxicity widening multiplier
  4. refresh cadence / cancel thresholds
- Reject settings that improve gross PnL but worsen inventory tails materially.

## 8) Model Governance

- Always compare against a simple static-spread baseline.
- Require stability by session and volatility regime.
- Treat any “too good” result as a potential simulator assumption bug first.