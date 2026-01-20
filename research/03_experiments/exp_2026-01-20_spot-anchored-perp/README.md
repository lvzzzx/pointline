# exp_2026-01-20_spot-anchored-perp

Goal
Design and validate a spot-anchored, perp-dominant microstructure framework that
produces interpretable 1-hour signals net of funding, fees, and slippage.

Core thesis
Short-horizon price discovery in crypto is dominated by perps (leverage,
positioning, liquidation dynamics), while spot anchors value via the index/mark
construction. The edge comes from jointly modeling spot liquidity and perp
positioning/carry, then aggregating microstructure features conditionally by
perp-state regimes.

Data scope
- Venue: Binance USDT-M futures + Binance index/spot methodology
- Streams:
  - L2 order book updates (microsecond granularity, resampled to 1s snapshots)
  - Trades (aggressive flow)
  - Derivative tickers (mark, index, funding, open interest)
  - Liquidations via WS forceOrder (since 2021-04-27, snapshot updates up to 1s)
- Normalization: per-symbol (no cross-sectional pooling in v1)
- ts_col: ts_local_us

Prediction target
- Primary: 1-hour forward return (perp mid or VWAP)
- Secondary: multi-horizon (2h/4h) once stable
- PnL decomposition (required):
  - Price PnL
  - Funding PnL
  - Fees/slippage

Research paradigm
Microstructure + derivatives state -> regime identification -> conditional
feature aggregation -> 1h tradable signals (carry-aware).

Feature families
1) Spot anchor block (demand + reference)
   - L2: spread, depth, OIB, slope, update intensity
   - Trades: notional volume, trade imbalance
   - Stability: tail spread, depth minima, OIB persistence

2) Perp state block (positioning + carry)
   - OI: level, dOI, dOI z-score, OI/volume
   - Funding: level, change, acceleration, z-score
   - Basis: mark-index and/or perp-spot spread, change, z-score

3) Cross-market interaction block (alpha candidates)
   - OIB_gap: OIB_perp - OIB_spot
   - Liquidity_gap: RelSpread_perp - RelSpread_spot, Depth_perp / Depth_spot
   - Crowding: funding_z x OI_shock
   - Fragility: OI_shock x tail_spread or OI_shock / depth

4) Event and tail-risk labels
   - Liquidation spikes (forceOrder) as regime confirmation
   - Funding settlement windows as event-time features

Regime logic (ex-ante, no leakage)
Define a small, interpretable regime set per 1-hour bar using only perp state
variables (exclude dP from labels):
- Crowded carry: funding_z high + dOI positive
- Deleveraging stress: large negative dOI or liquidation spike + liquidity thin
- Neutral/other: remaining states

Within each regime, compute conditional aggregates, e.g.:
- RelSpread_q90 | regime=crowded_carry
- Depth_min | regime=deleveraging
- OIB_persistence | regime=crowded_carry

OIB (order-book imbalance) definition
Compute OIB from 1s resampled snapshots.

Mid:
  m_t = (best_bid + best_ask) / 2

Depth within symmetric band around mid (quote notional):
  D_bid(t) = sum_{p_i >= m_t(1-delta)} size_i * p_i * w(p_i)
  D_ask(t) = sum_{p_i <= m_t(1+delta)} size_i * p_i * w(p_i)

OIB:
  OIB(t) = (D_bid - D_ask) / (D_bid + D_ask)

Distance weight (recommended):
  w(p_i) = exp(-lambda * |p_i/m_t - 1|)

Half-life parametrization:
  lambda = ln(2) / (h * 1e-4)

Defaults (Binance USDT-M):
  - Tier 1 (BTC/ETH, high-liq): delta = 25 bps, h = 25 bps, lambda = 277.3
  - Tier 2 (thin alts): delta = 50 bps, h = 50 bps, lambda = 138.6

Aggregation:
  - OIB_ewma_30s
  - OIB_q10/q50/q90 over 1m and 1h
  - OIB_persistence (fraction of snapshots with unchanged sign)

Mark price and index context (Binance USDT-M)
Price Index:
  Price Index = sum_i (Weight_i * SpotPrice_i)
  Weight_i = Weight_i / sum_j Weight_j

Mark Price:
  Mark Price = median(Price1, Price2, Contract Price)
  Price1 = Price Index * (1 + LastFundingRate * (TimeUntilNextFunding / FundingPeriod))

Evaluation
1) Predictive
   - 1h IC / rank IC by symbol
   - Regime-conditioned IC
2) Economic
   - Strategy PnL including funding, fees, slippage
   - Capacity sensitivity via depth
3) Robustness
   - Stability across regimes, volatility buckets, market phases
   - Performance after controlling for BTC/ETH beta and liquidity exposure

Baselines
- Spot-only features
- Perp-only features
- Combined features
- Compare to naive averages (non-regime-aware)

Success criteria
- Signals remain predictive after funding and fee adjustments
- Regime-conditioned signals outperform unconditional aggregates
- Results are interpretable in terms of perp state + microstructure

Open questions
- Exact OIB band calibration per symbol vs tiered defaults
- Best representation of basis (mark-index vs perp-spot vs futures curve)
- Liquidation data coverage and timestamp alignment quality

Run checklist
- Resolve symbol_id(s) once and store in config
- Use ts_local_us for PIT correctness
- Keep price_int/qty_int as integers until final decode
- Log run metadata in logs/runs.jsonl

Next steps
- Fill symbol_ids and date_range for an initial run (BTCUSDT, ETHUSDT)
- Implement OIB and perp-state features in 02_pipelines
- Run baseline models: spot-only, perp-only, combined
- Evaluate IC and PnL net of funding/fees/slippage by regime
