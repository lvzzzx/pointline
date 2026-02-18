# Crypto Market Structure Notes for MM

## 1) Why L2-First Matters

In most crypto CEX feeds, practitioners receive:
- incremental L2 order book updates
- trades and top-of-book quotes
- derivatives context (funding/OI/liquidations)

But usually not full L3 queue state for all venues/symbols. MM research must therefore avoid queue-perfect assumptions.

## 2) Perp-Dominated Microstructure

- Perps often lead spot price discovery.
- Funding cycles (00/08/16 UTC on many venues) can alter short-horizon flow.
- OI and liquidation dynamics can abruptly change toxicity and spread behavior.

## 3) Session Effects

- US session often has highest volume and volatility.
- Weekend liquidity is thinner; spreads/slippage worsen.
- MM policies should include session-aware guards.

## 4) Fee and Rebate Reality

- Maker/taker economics can flip viability.
- VIP tier assumptions materially change expected edge.
- Promotional fee regimes should not be treated as permanent.

## 5) Event Regimes Relevant to MM

- Funding settlement windows
- Macro data releases (CPI/FOMC)
- liquidation cascades
- venue incidents/outages

These regimes justify dynamic spread widening and temporary risk-off behavior.

## 6) Practical Horizon Choice

### 100ms-5s
- Potentially higher gross edge
- Highly sensitive to latency and queue uncertainty
- Fragile without robust fill modeling and infra assumptions

### 5s-60s (recommended default)
- Better robustness under L2 constraints
- Emphasizes inventory/risk discipline and toxicity filtering
- Faster research iteration with fewer hidden queue assumptions

## 7) Recommended v1 Skill Default

- Instrument focus: CEX linear perps
- Strategy style: two-sided passive quoting + optional taker hedge
- Horizon: 5s-60s primary, 100ms-5s as advanced appendix
- Validation: cost-aware, stress-tested, and explicitly conservative