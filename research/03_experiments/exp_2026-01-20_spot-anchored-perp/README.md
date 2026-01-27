# exp_2026-01-20_spot-anchored-perp

Goal
Design and validate a spot-anchored, perp-dominant microstructure framework that
produces interpretable 1-hour signals net of funding, fees, and slippage.

Core thesis
Short-horizon price discovery in crypto is dominated by perps (leverage,
positioning, liquidation dynamics), while spot anchors value via the index/mark
construction.

**The Bridge:** Alpha exists in the tension between micro-scale events (seconds)
and macro-scale price discovery (hours). A naive average of L2 features washes
out signal ("The 1H Cliff"). Instead, we model the *cumulative impact* ("scar
tissue") of microstructure battles—shocks, absorption, and persistence—to
predict 1H returns net of carry.

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
   - L2 State: spread, depth, OIB (Order Imbalance), slope
   - **Micro-to-Macro Bridge (The "Scar Tissue"):**
     - `OIB_shock_count`: Count of 1s snapshots where OIB > threshold (e.g., 0.7).
     - `OIB_persistence`: Duration of sustained imbalance (filter out spoofing).
     - `Impulse_Response`: Correlation(OIB_t, Price_{t+k}) within the hour (did buyers win?).
   - Stability: tail spread, depth minima

2) Perp state block (positioning + carry)
   - **Open Interest (The "Fuel"):**
     - `OI_Norm`: OI / 24h_Volume (Days to Cover). High = Fragile.
     - `dOI_Type`: Classify 1H changes based on Price direction:
       - Price Up + OI Up = **Long Build** (Aggressive)
       - Price Down + OI Down = **Long Liquidation** (Forced)
       - Price Down + OI Up = **Short Build** (Aggressive)
       - Price Up + OI Down = **Short Squeeze** (Forced)
     - `OI_Shock`: Z-score of dOI (is this a standard rebalance or a regime shift?).

   - **Funding Rate (The "Cost" & "Sentiment"):**
     - `Predicted_Funding_Z`: Z-score of the *real-time predicted* rate (Sentiment). 
       - *Note:* Do not use settled rate for signal; it lags by up to 8h.
     - `Funding_Acceleration`: `d(Predicted_Funding) / dt`. Rapid rises often mark local tops.
     - `Carry_Yield`: Annualized `Settled_Funding` (The hurdle rate for any directional trade).

   - **Basis:**
     - `Basis_Fair`: (Mark - Index) / Index. (Pure perp demand).
     - `Basis_Venue`: (Perp - Binance_Spot) / Binance_Spot. (Venue-specific dislocations).

3) Cross-market interaction block (alpha candidates)
   - `OIB_Divergence` (Snapshot): `OIB_spot - OIB_perp`.
     - *Signal:* Positive = Bullish Divergence; Negative = Bearish Divergence.
   - **Cross-Market Scar Tissue (Cumulative):**
     - `Lead_Lag_Accumulator`: Count of 1s snapshots where `Spot_OIB > Thresh` but `Perp_OIB < Thresh`.
       - *Insight:* Captures "Spot-led impulses" that vanish before the hourly close.
     - `Basis_Dislocation_Duration`: Minutes per hour where `|Basis_Z| > 2.0`.
       - *Insight:* Distinguishes persistent inefficiency (tradeable) from transient noise.
   - `Liquidity_Ratio`: `Depth_perp / Depth_spot`.
     - *Insight:* Low ratio (< 1.0) indicates perp liquidity is drying up relative to spot; often a **Volatility Precursor**.
   - `Crowding_Ratio`: `OI_Shock / Volume_Shock`.
     - *Insight:* If OI rises faster than Volume, positions are becoming "locked" (illiquid).
   - `Cascade_Risk`: `dOI / Depth_perp`.
     - *Insight:* Measures "Price impact if recent positioning tried to close immediately." High values = Liquidation Risk.

4) Event and tail-risk labels
   - Liquidation spikes (forceOrder) as regime confirmation
   - Funding settlement windows as event-time features

Data Alignment Strategy: State vs. Flow
We face a "State vs. Flow" problem when combining continuous market states (OI, Funding) with discrete time bars.

**The Problem:**
- **Bars (Flow):** Aggregate activity *over* the hour (Open, High, Low, Close, Vol).
- **Ticker (State):** Snapshots of the system state *at* specific moments (Funding, OI).

**The Solution:**
1. **For Signal (State variables):** Use **As-Of Join (Backward)**.
   - *Logic:* Take the Last Known Value at `bar_close_ts`.
   - *Reason:* This represents the state of the world *at the moment of decision*.
   - *Implementation:* `join_asof(strategy="backward")` on `ts_local_us`.

2. **For Cashflow (Settled Funding):** Use **Window Sum**.
   - *Logic:* Sum all realized funding payments where `funding_ts` falls *within* `[bar_open, bar_close)`.
   - *Reason:* This represents the actual cost incurred.

3) The "Rollover" Edge Case:
   - At 00:00, 08:00, 16:00 UTC, `predicted_funding_rate` resets to baseline.
   - *Fix:* Mask `funding_acceleration` calculations at these exact timestamps to prevent artificial "crashes" in the signal.

Sampling Methodology (V1 vs. Roadmap)
**Current: 1s Time-Sampling**
- *Method:* Snapshot the order book state at the top of every second (`t`, `t+1s`...).
- *Justification:* 1s resolution is sufficient to capture "sustained pressure" (Regimes) for a 1H strategy. Events shorter than 1s are likely HFT noise irrelevant to our execution horizon.
- *Constraint:* Uses "Last Known State" (Point-in-Time).

**Future Upgrade: Volume Clocks**
- *Concept:* Snapshot every `$N` notional traded (e.g., every $1M).
- *Benefit:* Adapts to volatility. Fast markets = high sampling rate; Slow markets = low sampling rate.
- *Why not V1?* significantly increases alignment complexity with time-based funding/tickers.

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
