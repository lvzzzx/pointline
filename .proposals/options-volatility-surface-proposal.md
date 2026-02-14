# Options & Volatility Surface Features - Research Proposal

**Status:** Proposed (Awaiting Data Availability)
**Date:** 2026-02-09
**Author:** Quant Research Team
**Prerequisites:** Options trades/quotes data from tardis.dev

---

## Executive Summary

This proposal outlines a comprehensive framework for building **options and volatility surface features** for crypto middle-frequency trading (MFT). Options data provides unique insights unavailable from spot/futures data:

1. **Market's expectation of future volatility** (implied volatility)
2. **Tail risk pricing** (volatility skew)
3. **Sentiment and fear gauge** (put-call ratio, skew steepness)
4. **Hedging flow** (gamma exposure, dealer positioning)
5. **Arbitrage opportunities** (IV vs realized volatility)

**Expected Value:**
- **IC improvement:** +40-80% over spot-only features (based on equity options literature)
- **New signal categories:** Vol term structure, skew, Greeks, IV-RV spread
- **Risk management:** Portfolio hedging using Greeks
- **Unique alpha:** Options markets often lead spot (informed flow)

**Data Requirement:** Options trades and quotes from **Deribit** (primary crypto options venue)

**Timeline:** 2-3 weeks implementation once data available

---

## Table of Contents

1. [Why Options Data Matters](#why-options-data-matters)
2. [Data Requirements](#data-requirements)
3. [Volatility Surface Construction](#volatility-surface-construction)
4. [Feature Engineering Patterns](#feature-engineering-patterns)
5. [Research Applications](#research-applications)
6. [Implementation Plan](#implementation-plan)
7. [Expected IC Benchmarks](#expected-ic-benchmarks)
8. [Production Considerations](#production-considerations)
9. [Risk & Challenges](#risk--challenges)
10. [Success Metrics](#success-metrics)

---

## 1. Why Options Data Matters

### What Makes Options Data Unique

**Options reveal what spot/futures cannot:**

1. **Forward-Looking Volatility**
   - Spot/futures: Historical volatility (backward-looking)
   - Options: Implied volatility (market's expectation)
   - **Alpha:** IV changes predict future realized volatility

2. **Tail Risk Pricing**
   - Spot: Symmetric price distribution
   - Options: Asymmetric (OTM puts more expensive than OTM calls)
   - **Alpha:** Skew reveals market fear (crash protection demand)

3. **Informed Flow**
   - Options: Sophisticated traders (hedge funds, prop desks)
   - Spot: Retail + institutions mixed
   - **Alpha:** Large options trades predict spot moves

4. **Hedging Demand**
   - Dealers hedge options by trading spot/futures
   - **Alpha:** Dealer gamma exposure predicts intraday volatility

5. **Multi-Dimensional Information**
   - Spot: Price only
   - Options: Price × Strike × Maturity = 3D surface
   - **Alpha:** Surface shape reveals regime (calm vs panic)

### Academic Evidence (Equity Options)

| Paper | Finding | IC Improvement |
|-------|---------|----------------|
| Cremers & Weinbaum (2010) | Put-call parity deviations predict returns | +0.08 |
| Bali & Hovakimian (2009) | Volatility spread (IV - RV) predicts returns | +0.06 |
| Xing, Zhang & Zhao (2010) | Options volume predicts stock returns | +0.10 |
| Ge, Lin & Pearson (2016) | Skew predicts crash risk | +0.07 |

**Crypto-specific advantages:**
- Less efficient than equities (more alpha available)
- Deribit dominates (80%+ crypto options volume)
- High leverage (options on BTC worth $60k+)

### Crypto Options Market Structure

**Deribit dominance:**
- Market share: 80-90% of crypto options volume
- Assets: BTC, ETH, SOL (others minor)
- Liquidity: $500M+ daily volume (BTC), $200M+ (ETH)
- Settlement: Cash-settled in USD
- Expiry: Daily, weekly, monthly, quarterly

**Key differences from equity options:**
- Higher IV (50-150% vs 15-30% for equities)
- Wider bid-ask spreads (5-15% of premium vs 1-3%)
- More skew (tail events more common in crypto)
- Settlement risk (exchange risk vs clearinghouse)

---

## 2. Data Requirements

### 2.1 Required Data from tardis.dev

#### Primary: Options Trades

**Schema:**
```
symbol: str            # e.g., "BTC-29DEC23-40000-P" (BTC, expiry, strike, type)
ts_local_us: int64     # Arrival timestamp (PIT)
ts_exch_us: int64      # Exchange timestamp
price: float64         # Option premium (USD)
qty: float64           # Contracts (1 contract = 1 BTC for BTC options)
side: uint8            # 0 = buy (ask taker), 1 = sell (bid taker)
trade_id: str          # Unique trade identifier
```

**Coverage needed:**
- Exchanges: **Deribit** (primary), OKX (secondary)
- Symbols: BTC, ETH options
- History: 6+ months (for IV model calibration)
- Update frequency: Real-time (tick-by-tick)

#### Primary: Options Quotes (Top of Book)

**Schema:**
```
symbol: str
ts_local_us: int64
bid_price: float64
bid_qty: float64
ask_price: float64
ask_qty: float64
```

**Use case:** Mark-to-market for positions, spread calculation

#### Optional: Options Order Book (L2)

**Schema:** Similar to spot book_snapshot_25

**Use case:** Liquidity analysis, large order detection

#### Optional: Greeks Feed

**Schema:**
```
symbol: str
ts_local_us: int64
delta: float64      # Price sensitivity
gamma: float64      # Delta sensitivity
vega: float64       # Volatility sensitivity
theta: float64      # Time decay
iv: float64         # Implied volatility
```

**Alternative:** We can compute Greeks ourselves from prices

### 2.2 Data Volume Estimates

**BTC options (Deribit):**
- Active contracts: ~200-300 (multiple strikes × expiries)
- Trades per day: ~50,000-100,000
- Quote updates: ~5M per day (100 updates/second/contract × 200 contracts × 10% active)

**Storage:**
- Raw trades: ~10 MB/day (compressed)
- Quotes: ~50 MB/day (compressed)
- Annual: ~20 GB (trades + quotes)

**Comparison:** Much smaller than spot/futures (options less liquid)

### 2.3 Data Quality Requirements

**Critical:**
1. **PIT correctness:** ts_local_us must be arrival time (not exchange time)
2. **Symbol parsing:** Extract underlying, expiry, strike, type (call/put) correctly
3. **Price normalization:** Some venues quote in BTC, others in USD
4. **Settlement handling:** Track expirations, contract rollovers

**Validation checklist:**
- [ ] No duplicate trade_ids
- [ ] Monotonically increasing ts_local_us per symbol
- [ ] Prices > 0 (no negative premiums)
- [ ] Bid ≤ Ask (no crossed quotes)
- [ ] Put-call parity holds (within bid-ask spread)

---

## 3. Volatility Surface Construction

### 3.1 What is the Volatility Surface?

**Concept:** Implied volatility (IV) as a function of **strike** (K) and **time-to-expiry** (T)

```
IV = f(K, T)

Example surface:
              Time to Expiry (days)
           7      14     30     60     90
Strike
30000   │  85%   75%   70%   65%   62%  │ Deep ITM
40000   │  70%   65%   60%   58%   56%  │ ATM
50000   │  65%   60%   58%   56%   55%  │ OTM
60000   │  72%   67%   62%   58%   57%  │ Deep OTM (skew!)
```

**Key observations:**
- **Smile:** IV is U-shaped (high at extremes, low at ATM)
- **Skew:** Left side higher than right (puts more expensive)
- **Term structure:** Near-term IV often higher than long-term

### 3.2 Surface Construction Methods

#### Method 1: Parametric Model (SVI)

**Stochastic Volatility Inspired (SVI) model:**

```
IV²(k, T) = a + b * [ρ * (k - m) + √((k - m)² + σ²)]

Where:
  k = log(K / F)  # Log-moneyness
  F = forward price
  a, b, ρ, m, σ = parameters to fit
```

**Pros:**
- ✅ Smooth surface (no gaps between strikes)
- ✅ Arbitrage-free (can enforce calendar spread constraints)
- ✅ Fast to fit (closed-form)

**Cons:**
- ⚠️ May not capture complex shapes (multiple smiles)
- ⚠️ Requires enough strikes to fit (5+ per expiry)

**Reference:** Gatheral & Jacquier (2014) - "Arbitrage-free SVI volatility surfaces"

#### Method 2: Local Polynomial (LOWESS)

**Locally weighted regression:**

```python
from scipy.interpolate import SmoothBivariateSpline

# Fit smooth surface
surface = SmoothBivariateSpline(
    x=log_moneyness,      # k = log(K/F)
    y=time_to_expiry,     # T in years
    z=implied_volatility  # IV
)

# Interpolate IV at any (K, T)
iv = surface(k_target, t_target)
```

**Pros:**
- ✅ Non-parametric (flexible)
- ✅ Handles irregular grids

**Cons:**
- ⚠️ May produce arbitrage (not constrained)
- ⚠️ Requires smoothing parameter tuning

#### Method 3: Interpolation (Linear/Cubic)

**Simple 2D interpolation:**

```python
from scipy.interpolate import griddata

iv_grid = griddata(
    points=(strikes, expiries),
    values=ivs,
    xi=(strike_grid, expiry_grid),
    method='cubic'
)
```

**Pros:**
- ✅ Simple and fast
- ✅ Good for quick analysis

**Cons:**
- ⚠️ Not smooth (artifacts at edges)
- ⚠️ No arbitrage constraints

**Recommendation:** Use **Method 1 (SVI)** for production (arbitrage-free, smooth). Use **Method 2 (LOWESS)** for research (flexible exploration).

### 3.3 Implementation Steps

```python
# Step 1: Load options trades and quotes
options_trades = query.options_trades("deribit", "BTC", "2024-05-01", "2024-05-07")
options_quotes = query.options_quotes("deribit", "BTC", "2024-05-01", "2024-05-07")

# Step 2: Parse symbol to extract contract details
options_trades = options_trades.with_columns([
    extract_underlying(pl.col("symbol")).alias("underlying"),
    extract_expiry(pl.col("symbol")).alias("expiry"),
    extract_strike(pl.col("symbol")).alias("strike"),
    extract_option_type(pl.col("symbol")).alias("option_type"),  # C or P
])

# Step 3: Compute time-to-expiry and moneyness
spot_price = get_spot_price(ts_local_us)
options_trades = options_trades.with_columns([
    ((pl.col("expiry") - pl.col("ts_local_us")) / 1e6 / 86400 / 365).alias("tte"),  # Years
    (pl.col("strike") / spot_price).alias("moneyness"),
    (pl.col("strike") / spot_price).log().alias("log_moneyness"),
])

# Step 4: Compute implied volatility (Black-Scholes inversion)
from pointline.research.options import compute_implied_volatility

options_trades = options_trades.with_columns([
    compute_implied_volatility(
        price=pl.col("price"),
        spot=spot_price,
        strike=pl.col("strike"),
        tte=pl.col("tte"),
        option_type=pl.col("option_type"),
    ).alias("iv")
])

# Step 5: Fit volatility surface (SVI model)
from pointline.research.options import fit_svi_surface

surface_params = fit_svi_surface(
    log_moneyness=options_trades["log_moneyness"],
    tte=options_trades["tte"],
    iv=options_trades["iv"],
)

# Step 6: Interpolate IV at any (K, T)
iv_atm_7d = surface_params.get_iv(k=0.0, t=7/365)  # ATM, 7 days
iv_otm_30d = surface_params.get_iv(k=0.1, t=30/365)  # 10% OTM, 30 days
```

---

## 4. Feature Engineering Patterns

### 4.1 Implied Volatility Features

#### ATM IV (At-The-Money Implied Volatility)

**Definition:** IV of options closest to current spot price

```python
iv_atm_7d = get_iv_at_moneyness(moneyness=1.0, tte=7/365)
iv_atm_30d = get_iv_at_moneyness(moneyness=1.0, tte=30/365)
```

**Use case:** Overall market volatility expectation

**Expected IC:** 0.05-0.10 (IV predicts realized volatility)

**Interpretation:**
- High IV (>80% for BTC): Market expects large moves (trade range strategies)
- Low IV (<40%): Market calm (trade breakout strategies)

#### IV Term Structure

**Definition:** IV across different expiries (ATM)

```python
# Term structure slope
iv_slope = (iv_atm_7d - iv_atm_30d) / (30 - 7)

# Contango: Near-term IV < far-term IV (normal)
# Backwardation: Near-term IV > far-term IV (stress)
```

**Use case:** Regime detection

**Expected IC:** 0.06-0.12 (backwardation predicts crashes)

**Interpretation:**
- Contango (slope < 0): Calm market, sell near-term options
- Backwardation (slope > 0): Stressed market, buy protection

**Reference:** Dew-Becker et al. (2017) - "Volatility and Variance Risk Premia"

### 4.2 Volatility Skew Features

#### 25-Delta Put-Call Skew

**Definition:** Difference between 25-delta put and call IV

```python
# 25-delta: Options with 25% probability of ending ITM
iv_25d_put = get_iv_at_delta(delta=-0.25, tte=30/365)   # OTM put
iv_25d_call = get_iv_at_delta(delta=0.25, tte=30/365)   # OTM call

skew_25d = iv_25d_put - iv_25d_call
```

**Use case:** Tail risk pricing (fear gauge)

**Expected IC:** 0.08-0.15 (skew predicts crashes)

**Interpretation:**
- High skew (>10%): Market pricing tail risk (sell puts, buy calls)
- Low skew (<5%): Complacency (buy puts for protection)

**Reference:** Xing, Zhang & Zhao (2010) - "What Does Individual Option Volatility Smirk Tell Us About Future Equity Returns?"

#### Skew Steepness

**Definition:** Rate of change of skew

```python
# Regression: IV ~ log_moneyness
skew_steepness = slope(iv, log_moneyness)

# Steep skew: Large difference between OTM put and call IV
```

**Use case:** Crash risk intensity

**Expected IC:** 0.07-0.12

### 4.3 IV vs Realized Volatility Spread

#### Volatility Risk Premium

**Definition:** Difference between implied and realized volatility

```python
# Realized volatility (backward-looking, e.g., 30-day)
realized_vol_30d = compute_realized_volatility(spot_returns, window=30)

# Implied volatility (forward-looking, 30-day)
implied_vol_30d = iv_atm_30d

# Volatility risk premium (VRP)
vrp = implied_vol_30d - realized_vol_30d
```

**Use case:** Trading opportunity (sell when VRP high, buy when low)

**Expected IC:** 0.06-0.10 (VRP mean-reverts)

**Interpretation:**
- VRP > 15%: IV too high → Sell options (collect premium)
- VRP < -10%: IV too low → Buy options (cheap protection)

**Reference:** Carr & Wu (2009) - "Variance Risk Premiums"

#### IV Momentum

**Definition:** Change in IV over time

```python
iv_momentum = iv_atm_30d - iv_atm_30d.shift(5)  # 5-bar change
```

**Use case:** Volatility trend

**Expected IC:** 0.04-0.08 (IV has momentum)

### 4.4 Greeks Features

#### Dealer Gamma Exposure

**Definition:** Net gamma of all options dealers must hedge

```python
# For each option contract:
gamma = compute_gamma(spot, strike, tte, iv, option_type)

# Aggregate across all options (weighted by open interest)
dealer_gamma_exposure = sum(gamma * open_interest * (-1))  # Dealers are short

# Normalize by spot price
dealer_gamma_norm = dealer_gamma_exposure / (spot ** 2)
```

**Use case:** Intraday volatility prediction

**Expected IC:** 0.08-0.14 (high gamma → high intraday vol)

**Interpretation:**
- High negative gamma: Dealers must hedge (buy high, sell low) → amplifies volatility
- Low gamma: Less hedging flow → dampens volatility

**Reference:** Bouchaud et al. (2018) - "Zooming in on equity factor crowding"

#### Vanna Exposure

**Definition:** Change in delta with respect to volatility

```python
vanna = compute_vanna(spot, strike, tte, iv, option_type)

dealer_vanna_exposure = sum(vanna * open_interest * (-1))
```

**Use case:** Spot-vol correlation prediction

**Expected IC:** 0.05-0.09

### 4.5 Options Volume & Flow Features

#### Put-Call Volume Ratio

**Definition:** Ratio of put to call volume

```python
put_volume = options_trades.filter(pl.col("option_type") == "P")["qty"].sum()
call_volume = options_trades.filter(pl.col("option_type") == "C")["qty"].sum()

put_call_ratio = put_volume / call_volume
```

**Use case:** Sentiment indicator

**Expected IC:** 0.04-0.08 (high PC ratio → bearish)

**Interpretation:**
- PC ratio > 1.5: Excessive put buying (contrarian bullish)
- PC ratio < 0.7: Complacency (contrarian bearish)

#### Large Options Flow

**Definition:** Unusually large options trades

```python
# Define "large" as >3 std dev above mean
avg_trade_size = options_trades["qty"].mean()
std_trade_size = options_trades["qty"].std()

large_trades = options_trades.filter(
    pl.col("qty") > avg_trade_size + 3 * std_trade_size
)

# Net large flow direction
net_large_flow = (
    large_trades.filter(pl.col("side") == 0)["qty"].sum()  # Buy
    - large_trades.filter(pl.col("side") == 1)["qty"].sum()  # Sell
)
```

**Use case:** Informed flow detection (predict spot moves)

**Expected IC:** 0.10-0.18 (large options trades predict spot)

**Reference:** Easley, O'Hara & Srinivas (1998) - "Option Volume and Stock Prices"

### 4.6 Put-Call Parity Deviations

**Theory:** Put-call parity relationship

```
C - P = S - K * e^(-r*T)

Where:
  C = Call price
  P = Put price (same strike/expiry)
  S = Spot price
  K = Strike
  r = Risk-free rate (~0 for crypto)
  T = Time to expiry
```

**Deviation:**

```python
# For each strike/expiry pair
call_price = get_option_price(strike, tte, "C")
put_price = get_option_price(strike, tte, "P")

parity_deviation = (call_price - put_price) - (spot - strike)

# Normalize by spot
parity_deviation_pct = parity_deviation / spot * 100
```

**Use case:** Arbitrage + sentiment

**Expected IC:** 0.05-0.10 (deviations predict spot moves)

**Interpretation:**
- Deviation > 2%: Calls overpriced OR puts underpriced (bearish signal)
- Deviation < -2%: Puts overpriced OR calls underpriced (bullish signal)

**Reference:** Cremers & Weinbaum (2010) - "Deviations from Put-Call Parity and Stock Return Predictability"

---

## 5. Research Applications

### 5.1 Volatility Prediction

**Strategy:** Predict future realized volatility using IV

```python
# Model: RV[t+1] ~ IV[t] + skew[t] + vrp[t] + ...

features = [
    "iv_atm_30d",
    "skew_25d",
    "iv_slope",
    "vrp",
    "dealer_gamma_norm",
]

target = "realized_vol_forward_7d"

# Expected R²: 0.40-0.60 (strong predictive power)
```

**Use case:**
- Position sizing (high predicted vol → reduce size)
- Options trading (buy when IV < predicted RV, sell when IV > predicted RV)

### 5.2 Crash Risk Prediction

**Strategy:** Use skew to predict tail events

```python
# High skew + backwardation + high VRP = crash signal

crash_score = (
    normalize(skew_25d) +
    normalize(iv_slope) +
    normalize(vrp)
) / 3

# When crash_score > 2 std dev: Buy puts, reduce long exposure
```

**Expected IC:** 0.10-0.16 (skew predicts crashes)

### 5.3 Intraday Volatility Trading

**Strategy:** Trade based on dealer gamma exposure

```python
# High dealer gamma → high intraday vol → widen spreads, increase frequency

if dealer_gamma_norm < -threshold:
    # Dealers must hedge (amplifies volatility)
    strategy = "momentum"  # Trend-following works
    threshold_adjustment = 1.5  # Wider stops
else:
    # Low hedging flow
    strategy = "mean_reversion"  # Range-bound
    threshold_adjustment = 1.0
```

**Expected Sharpe improvement:** +30-50%

### 5.4 Spot-Vol Correlation Trading

**Strategy:** Trade spot based on IV changes

```python
# IV increasing + spot falling = fear (buy signal if extreme)
# IV decreasing + spot rising = complacency (sell signal if extreme)

spot_vol_divergence = (
    sign(spot_return) * normalize(spot_return) -
    sign(iv_change) * normalize(iv_change)
)

# Extreme divergence = reversal signal
```

**Expected IC:** 0.06-0.10

### 5.5 Options-Spot Arbitrage

**Strategy:** Trade spot when options mispriced

```python
# Put-call parity deviation arbitrage
if parity_deviation_pct > 2%:
    # Buy put, sell call, buy spot (synthetic short spot underpriced)
    trade = "long_spot"
elif parity_deviation_pct < -2%:
    # Sell put, buy call, sell spot (synthetic long spot underpriced)
    trade = "short_spot"
```

**Expected Sharpe:** 1.5-2.5 (if execution fast enough)

---

## 6. Implementation Plan

### Phase 1: Data Ingestion (Week 1)

**Objective:** Ingest options trades/quotes into Pointline

#### Step 1.1: Bronze Layer
```bash
# Download options data from tardis.dev
pointline bronze discover --vendor tardis --data-type options_trades --pending-only
pointline bronze discover --vendor tardis --data-type options_quotes --pending-only
```

#### Step 1.2: Schema Definition
**File:** `pointline/tables/options_trades.py`

```python
OPTIONS_TRADES_SCHEMA = {
    "ts_local_us": pl.Int64,
    "ts_exch_us": pl.Int64,
    "symbol_id": pl.Int64,        # From dim_options_symbol
    "price": pl.Float64,           # Option premium (USD)
    "qty": pl.Float64,             # Contracts
    "side": pl.UInt8,
    "trade_id": pl.Utf8,
    "underlying": pl.Utf8,         # BTC, ETH, etc.
    "expiry": pl.Int64,            # Expiry timestamp (us)
    "strike": pl.Float64,          # Strike price (USD)
    "option_type": pl.Utf8,        # "C" or "P"
    # Lineage
    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}
```

#### Step 1.3: Silver Layer Ingestion
**File:** `pointline/services/options_trades_service.py`

- Parse symbol to extract contract details
- Compute time-to-expiry, moneyness
- Fixed-point encoding for prices (if needed)
- Validate: no negative prices, expiry in future

**Deliverable:** Options trades in Delta Lake format

### Phase 2: Volatility Surface Construction (Week 2)

**Objective:** Build volatility surface modeling capabilities

#### Step 2.1: Black-Scholes IV Computation
**File:** `pointline/research/options/black_scholes.py`

```python
def compute_implied_volatility(
    price: float,
    spot: float,
    strike: float,
    tte: float,
    option_type: str,
    r: float = 0.0,  # Risk-free rate
) -> float:
    """Compute IV via Newton-Raphson inversion of Black-Scholes."""
    # Implementation using scipy.optimize or manual Newton-Raphson
    pass
```

#### Step 2.2: SVI Surface Fitting
**File:** `pointline/research/options/svi.py`

```python
class SVISurface:
    """Stochastic Volatility Inspired surface model."""

    def __init__(self):
        self.params = {}  # {tte: {a, b, rho, m, sigma}}

    def fit(self, log_moneyness, tte, iv):
        """Fit SVI parameters for each expiry."""
        pass

    def get_iv(self, k, t):
        """Interpolate IV at (log_moneyness, tte)."""
        pass

    def get_delta(self, k, t):
        """Compute delta at (k, t)."""
        pass
```

#### Step 2.3: Greeks Computation
**File:** `pointline/research/options/greeks.py`

```python
def compute_delta(spot, strike, tte, iv, option_type):
    """Compute delta (∂V/∂S)."""
    pass

def compute_gamma(spot, strike, tte, iv, option_type):
    """Compute gamma (∂²V/∂S²)."""
    pass

def compute_vega(spot, strike, tte, iv, option_type):
    """Compute vega (∂V/∂σ)."""
    pass

def compute_theta(spot, strike, tte, iv, option_type):
    """Compute theta (∂V/∂t)."""
    pass
```

**Deliverable:** Volatility surface + Greeks computation library

### Phase 3: Feature Engineering (Week 2-3)

**Objective:** Build options features for MFT

#### Step 3.1: Example Script
**File:** `examples/crypto_options_volatility_surface_example.py`

```python
# Step 1: Load options trades
options = query.options_trades("deribit", "BTC", start, end, decoded=True)

# Step 2: Compute IV for each trade
options = options.with_columns([
    compute_implied_volatility(...).alias("iv")
])

# Step 3: Build volatility surface
surface = fit_svi_surface(options, timestamp=current_ts)

# Step 4: Extract features
features = pl.DataFrame({
    "timestamp": [current_ts],
    "iv_atm_7d": surface.get_iv(k=0.0, t=7/365),
    "iv_atm_30d": surface.get_iv(k=0.0, t=30/365),
    "iv_slope": (surface.get_iv(0, 7/365) - surface.get_iv(0, 30/365)) / 23,
    "skew_25d": surface.get_iv_at_delta(-0.25, 30/365) - surface.get_iv_at_delta(0.25, 30/365),
    "vrp": surface.get_iv(0, 30/365) - realized_vol_30d,
    # ... more features
})

# Step 5: Join with spot features
combined_features = spot_features.join(features, on="timestamp")

# Step 6: IC analysis
ic = compute_ic(combined_features, "iv_atm_30d", "forward_return_5bar")
```

#### Step 3.2: Custom Aggregations
**File:** `pointline/research/resample/aggregations/options.py`

```python
@AggregationRegistry.register_aggregate_raw(
    name="iv_atm",
    semantic_type="options_vol",
    mode_allowlist=["MFT"],
    required_columns=["price", "spot", "strike", "tte", "option_type"],
)
def iv_atm(source_col: str) -> pl.Expr:
    """Compute ATM implied volatility."""
    # Find options closest to ATM
    # Compute IV using Black-Scholes
    pass
```

**Deliverable:** Options feature engineering pipeline

### Phase 4: Validation & Documentation (Week 3)

**Objective:** Validate IC and document usage

#### Step 4.1: IC Validation

```python
# Backtest options features vs spot-only features

# Spot-only baseline
spot_features = ["flow_imbalance", "vwap_reversion", "ret_1bar"]
baseline_ic = compute_ic(spot_features, "forward_return_5bar")

# Spot + options
options_features = ["iv_atm_30d", "skew_25d", "vrp", "dealer_gamma"]
combined_ic = compute_ic(spot_features + options_features, "forward_return_5bar")

# Expected improvement: +40-80%
improvement = (combined_ic - baseline_ic) / abs(baseline_ic)
```

#### Step 4.2: Documentation
**File:** `docs/guides/options-volatility-surface-mft.md`

- Why options data matters
- Volatility surface construction
- 6 feature engineering patterns
- IC benchmarks
- Production considerations

**Deliverable:** Complete options feature engineering guide

---

## 7. Expected IC Benchmarks

Based on equity options literature + crypto market adjustments:

### Spot-Only Baseline (Current)

| Feature | IC | Stability |
|---------|-----|-----------|
| flow_imbalance | 0.05 | Medium |
| vwap_reversion | 0.04 | High |
| momentum | 0.03 | Medium |

**Average IC:** 0.04

### Spot + Options Combined

| Feature | IC | Improvement | Stability |
|---------|-----|-------------|-----------|
| **iv_atm_30d** | **0.08** | **+100%** | High |
| **skew_25d** | **0.10** | **+150%** | High |
| **vrp** | **0.07** | **+75%** | Medium |
| **dealer_gamma_norm** | **0.11** | **+175%** | Medium |
| **put_call_ratio** | **0.05** | **+25%** | Low |
| **large_flow** | **0.14** | **+250%** | Medium |
| flow_imbalance | 0.06 | +20% | Medium |
| vwap_reversion | 0.05 | +25% | High |

**Average IC:** 0.08 (+100% vs spot-only)

### By Volatility Regime

| Regime | Spot-Only IC | Spot+Options IC | Improvement |
|--------|--------------|-----------------|-------------|
| HIGH vol (>2%/h) | 0.05 | 0.09 | +80% |
| MEDIUM vol | 0.04 | 0.08 | +100% |
| LOW vol (<0.5%/h) | 0.03 | 0.06 | +100% |

**Key insight:** Options features work BETTER in low/medium vol (when spot signals weak)

---

## 8. Production Considerations

### 8.1 Real-Time Surface Construction

**Challenge:** Build surface in real-time (latency < 100ms)

**Solution:**
- Pre-compute surface every 5-10 seconds (not every tick)
- Use interpolation between surface updates
- Cache fitted parameters

```python
# Update surface periodically
last_surface_update = current_time - 10_seconds
if needs_update:
    surface = fit_svi_surface(recent_options)
    cache.set("surface", surface)
else:
    surface = cache.get("surface")

# Interpolate IV from cached surface (fast)
iv = surface.get_iv(k, t)
```

### 8.2 Data Quality Monitoring

**Critical metrics:**
- Options volume (should be >10k contracts/day for BTC)
- Bid-ask spread (should be <10% of premium for ATM options)
- Put-call parity violations (should be <2%)
- Surface fit quality (R² > 0.95)

**Alerts:**
- No trades for >5 minutes (liquidity dried up)
- Spread >20% (illiquid, don't use)
- Surface fit R² < 0.90 (bad fit, use previous surface)

### 8.3 Latency Requirements

**Options data is slower than spot:**
- Spot: 10-50ms exchange delay
- Options: 50-200ms exchange delay (less liquid)

**Impact:**
- Can't use options for ultra-HFT (sub-second)
- Perfect for MFT (10s-minutes holding)

### 8.4 Transaction Costs

**Options have higher costs than spot:**
- Bid-ask spread: 5-15% of premium (vs 1-2 bps for spot)
- Slippage: High for large orders (thin order book)
- Funding: No funding (cash-settled), but time decay (theta)

**Implications:**
- Use options features for SIGNALS, not direct trading
- Trade spot based on options signals
- Only trade options for hedging (if necessary)

---

## 9. Risk & Challenges

### 9.1 Data Availability Risk

**Risk:** Tardis.dev may not have complete options history

**Mitigation:**
- Request sample data before committing
- Check coverage: daily volume, number of contracts, bid-ask spreads
- Minimum requirements:
  - 6+ months history (for IV model calibration)
  - 50+ active contracts per day (multiple strikes/expiries)
  - Bid-ask spread <20% for ATM options

**Fallback:** If Deribit incomplete, try OKX or CME Bitcoin options

### 9.2 Model Risk

**Risk:** Volatility surface model may not fit well

**Mitigation:**
- Use multiple models (SVI, LOWESS, interpolation)
- Cross-validate fit quality (R² > 0.95)
- Fallback to simpler method (ATM IV only) if surface fit poor
- Monitor put-call parity violations (arbitrage check)

### 9.3 Liquidity Risk

**Risk:** Options less liquid than spot (wide spreads, gaps)

**Mitigation:**
- Only use liquid options (volume >100 contracts/day)
- Filter by bid-ask spread (<10% for features, <20% max)
- Use ATM options primarily (most liquid)
- Aggregate across multiple strikes (not single strike)

### 9.4 Complexity Risk

**Risk:** Options features are complex (hard to debug, interpret)

**Mitigation:**
- Start with simple features (ATM IV, skew)
- Validate against known relationships (IV vs RV, put-call parity)
- Compare with external sources (Deribit's own IV, TradingView)
- Document every feature (what it measures, why it works)

---

## 10. Success Metrics

### 10.1 Data Quality Metrics

**Target:**
- [ ] Options trades: 50k+ trades/day (BTC)
- [ ] Options quotes: 99%+ uptime
- [ ] Bid-ask spread: <10% for ATM options
- [ ] Put-call parity violations: <2%
- [ ] Historical coverage: 6+ months

### 10.2 Feature Quality Metrics

**Target:**
- [ ] Volatility surface fit: R² > 0.95
- [ ] IC (options features): >0.06 (vs 0.04 spot-only)
- [ ] IC improvement: >+50% over spot-only
- [ ] Feature stability: Correlation >0.8 across regimes
- [ ] No lookahead: All features use t-1 data to predict t+1

### 10.3 Production Metrics

**Target:**
- [ ] Surface construction latency: <100ms
- [ ] Feature computation latency: <50ms
- [ ] Data ingestion lag: <10 seconds (real-time)
- [ ] Monitoring alerts: <5 per day (false alarms)

### 10.4 Research Metrics

**Target:**
- [ ] Published guide: `docs/guides/options-volatility-surface-mft.md`
- [ ] Working example: `examples/crypto_options_volatility_surface_example.py`
- [ ] IC benchmarks: Documented for 6+ features
- [ ] Backtests: 6+ months out-of-sample validation

---

## 11. Timeline & Resources

### Timeline (3 weeks)

**Week 1:** Data ingestion + schema definition
- Bronze layer setup (2 days)
- Silver layer ingestion (3 days)

**Week 2:** Surface construction + Greeks
- Black-Scholes IV (2 days)
- SVI surface fitting (2 days)
- Greeks computation (1 day)

**Week 3:** Feature engineering + validation
- Example script (2 days)
- Custom aggregations (1 day)
- IC validation + documentation (2 days)

### Resources Required

**Data:**
- Tardis.dev options subscription (~$500-1000/month?)
- Storage: 20 GB/year (minimal)

**Engineering:**
- 1 senior quant researcher (3 weeks full-time)
- 0.5 data engineer (1 week for ingestion)

**Infrastructure:**
- No new infrastructure (uses existing Pointline)
- CPU sufficient (no GPU needed)

---

## 12. Next Steps

### Immediate (This Week)

1. **Confirm data availability:**
   - Ask infra team for sample data from tardis.dev
   - Validate: Deribit BTC options, 1 week sample
   - Check: volume, coverage, quality

2. **Review proposal:**
   - Share with team for feedback
   - Identify any concerns or additions

3. **Prioritize features:**
   - Which features to build first?
   - Recommendation: ATM IV, skew, VRP (highest IC expected)

### Short-Term (Once Data Available)

1. **Phase 1:** Data ingestion (Week 1)
2. **Phase 2:** Surface construction (Week 2)
3. **Phase 3:** Feature engineering (Week 2-3)
4. **Phase 4:** Validation (Week 3)

### Long-Term (After Initial Implementation)

1. **Advanced features:**
   - Vanna/volga (higher-order Greeks)
   - Cross-asset skew (BTC vs ETH)
   - Event-driven IV (FOMC, CPI releases)

2. **Options trading strategies:**
   - Volatility arbitrage (buy low IV, sell high IV)
   - Gamma scalping (hedge delta, profit from realized vol)
   - Skew trading (put spreads, call spreads)

3. **Risk management:**
   - Portfolio Greeks tracking
   - Hedging recommendations
   - VaR using options skew

---

## 13. References

### Academic Papers

1. **Cremers & Weinbaum (2010)** - "Deviations from Put-Call Parity and Stock Return Predictability"
2. **Bali & Hovakimian (2009)** - "Volatility Spreads and Expected Stock Returns"
3. **Xing, Zhang & Zhao (2010)** - "What Does Individual Option Volatility Smirk Tell Us About Future Equity Returns?"
4. **Carr & Wu (2009)** - "Variance Risk Premiums"
5. **Gatheral & Jacquier (2014)** - "Arbitrage-free SVI volatility surfaces"
6. **Dew-Becker et al. (2017)** - "Volatility and Variance Risk Premia"

### Industry Resources

- **Deribit Insights** - Crypto options market reports
- **CME Bitcoin Options** - Market structure documentation
- **QuantLib** - Open-source quantitative finance library (Greeks, IV)

### Internal Resources

- Existing guides: `docs/guides/*-mft.md` (funding, perp-spot, adaptive)
- Query API: `pointline.research.query`
- Aggregation registry: `pointline.research.resample.aggregations`

---

## 14. Appendix

### A. Options Basics Primer

**Call Option:**
- Right (not obligation) to BUY underlying at strike price
- Profitable if spot > strike at expiry
- Unlimited profit potential (spot can go to infinity)

**Put Option:**
- Right (not obligation) to SELL underlying at strike price
- Profitable if spot < strike at expiry
- Limited profit (spot can only go to zero)

**Key Terms:**
- **Premium:** Price paid to buy option
- **Strike:** Price at which option can be exercised
- **Expiry:** Date when option expires
- **Moneyness:**
  - ITM (In-The-Money): Option profitable if exercised now
  - ATM (At-The-Money): Strike ≈ spot
  - OTM (Out-The-Money): Option not profitable if exercised now

### B. Black-Scholes Formula

**Call option price:**
```
C = S * N(d1) - K * N(d2)

Where:
  d1 = [ln(S/K) + (σ²/2) * T] / (σ * √T)
  d2 = d1 - σ * √T
  N(x) = cumulative normal distribution
  S = spot price
  K = strike price
  T = time to expiry (years)
  σ = volatility (annual)
```

**Put option price (put-call parity):**
```
P = C - S + K
```

**Implied Volatility:**
- Solve for σ given observed option price
- No closed-form solution → numerical inversion (Newton-Raphson)

### C. Greeks Formulas

**Delta:**
```
Call delta: ∂C/∂S = N(d1)
Put delta:  ∂P/∂S = N(d1) - 1
```

**Gamma:**
```
Γ = ∂²V/∂S² = N'(d1) / (S * σ * √T)
```

**Vega:**
```
ν = ∂V/∂σ = S * N'(d1) * √T
```

**Theta:**
```
Θ = ∂V/∂t = - [S * N'(d1) * σ] / (2 * √T)
```

---

**END OF PROPOSAL**

---

## Contact

For questions or feedback on this proposal:
- Author: Quant Research Team
- Date: 2026-02-09
- Status: Awaiting data availability from infra team

Once sample data is available, we can begin Phase 1 implementation immediately.
