# Feature Engineering Catalog for Crypto MFT

## Table of Contents
1. [Derivatives & Positioning Features](#derivatives--positioning-features)
2. [Order Book / LOB Features](#order-book--lob-features)
3. [Trade Flow Features](#trade-flow-features)
4. [Microstructure Features](#microstructure-features)
5. [Cross-Asset Features](#cross-asset-features)
6. [Volatility Features](#volatility-features)
7. [Options-Derived Features](#options-derived-features)
8. [Temporal / Calendar Features](#temporal--calendar-features)
9. [Regime / State Features](#regime--state-features)
10. [Feature Construction Best Practices](#feature-construction-best-practices)

---

## Derivatives & Positioning Features

Crypto is a perp-dominated market. On most pairs, perp volume is 5-20x spot. Derivatives positioning data (funding, OI, liquidations) often carries more alpha than LOB features, especially at 1min+ horizons.

### Funding Rate
- **Raw funding rate:** Current 8h (Binance, OKX, Bybit) or 1h (some exchanges) rate. Positive = longs pay shorts, negative = shorts pay longs.
- **Predicted funding rate:** Real-time TWAP of premium index before next settlement. Available via API on most exchanges. More timely than settled rate.
- **Funding rate momentum:** `funding_rate - EMA(funding_rate, N)` for N = 3, 7, 21 periods. Persistence signal — extreme funding tends to persist then snap back.
- **Funding rate acceleration:** `d(funding_rate)/dt`. Captures direction of crowding pressure.
- **Cross-exchange funding spread:** `funding_A - funding_B` for same pair across exchanges. Arb signal when spread exceeds transfer cost. Also detects exchange-specific positioning.
- **Funding-price divergence:** Funding rate direction vs price direction. Divergence (price up but funding falling) signals weakening conviction — contrarian.
- **Annualized funding yield:** `funding_rate * 3 * 365` (for 8h). Extreme values (>50% APR or <-50% APR) are strong mean-reversion signals.
- **Funding rate term structure:** Compare predicted next-period funding with 7d/30d average. Steep = crowding into one direction.
- **Cumulative funding:** `sum(funding_rate)` over window. Total cost of holding position. Affects optimal holding period.

### Open Interest (OI)
- **OI level:** Total open interest in USD/contracts. Baseline positioning.
- **OI change (delta OI):** `OI_t - OI_{t-w}` over window. Rising OI = new positions entering. Falling OI = positions closing.
- **OI change rate:** `d(OI)/dt` normalized by rolling OI. Captures velocity of position building.
- **OI-price divergence:** Price rises + OI falls = short covering rally (weak). Price rises + OI rises = genuine buying (strong). Key regime signal.
- **OI-volume ratio:** `OI / daily_volume`. High = crowded (slow churn). Low = fast turnover. Crowded markets are more fragile.
- **OI concentration (Herfindahl):** When available (Binance top trader data), measures how concentrated OI is among large traders. High concentration = whale-driven, fragile.
- **Cross-exchange OI distribution:** Fraction of total OI on each exchange. Shifts in distribution signal migration and different margining regimes.
- **OI by expiry (for dated futures):** Distribution of OI across maturities. Concentration near expiry = potential pinning/settlement effects.
- **OI-weighted funding:** `OI * funding_rate`. Dollar cost of crowded positioning. Extreme values predict forced unwind.
- **OI as % of market cap:** Position leverage relative to underlying asset. Higher = more leveraged, more fragile. Tokens with OI > 5% of market cap are liquidation-prone.

### Liquidation Data
- **Liquidation volume:** Total forced liquidation volume (buy + sell) over window. Spike detection.
- **Net liquidation flow:** `long_liq_volume - short_liq_volume`. Net direction of forced selling. Large imbalance = cascading into one side.
- **Liquidation intensity:** `liq_volume / total_volume`. Fraction of flow that is forced. High = market stress, potential overshoot + reversal.
- **Liquidation clustering:** Count/volume of liquidations within X seconds. Cascades are clustered. Detect cascade onset.
- **Liquidation-price impact:** Rolling regression of `price_change ~ liq_volume`. Measures how much liquidations move price. Higher impact = thinner book, more fragile.
- **Large liquidation events:** Liquidations > $1M (BTC) or > $100K (alts). Binary or count. These move markets.
- **Liquidation heatmap distance:** Distance from current price to estimated liquidation clusters (from leverage distribution). Prices are attracted to liquidation clusters.
- **Cumulative liquidation imbalance:** Running sum of net liquidation flow. Persistent imbalance signals ongoing deleveraging.

### Long/Short Ratio & Positioning
- **Top trader long/short ratio (Binance):** Ratio of long vs short positions among top 20% of traders by margin. Sentiment of large traders.
- **Global long/short ratio (Binance):** Ratio across all traders. Retail sentiment proxy.
- **Long/short ratio divergence:** Top trader vs global ratio. When they disagree, follow top traders.
- **Long/short ratio momentum:** `LS_ratio - EMA(LS_ratio, N)`. Captures crowding direction.
- **Long/short ratio extreme:** Z-score of LS ratio. Extremes (|z| > 2) are contrarian signals.
- **Buy/sell taker volume ratio:** `taker_buy_volume / taker_sell_volume`. Aggressor-side measure of directional pressure on perps specifically.
- **Taker volume ratio momentum:** Change in taker ratio over window. Shift in aggressive flow direction.

### Perp-Spot Basis
- **Basis (raw):** `(perp_price - spot_price) / spot_price` in bps. Positive = contango (longs paying premium).
- **Basis momentum:** `basis_t - EMA(basis, N)`. Basis widening = increasing speculative demand.
- **Basis-funding coherence:** `sign(basis) == sign(funding)`. When coherent, trend is strong. Divergence = mixed signals.
- **Basis term structure:** Basis across maturities (perp, front-month, quarterly). Slope of term structure = cost of carry curve.
- **Basis volatility:** `std(basis)` over window. Low basis vol = stable carry, high = unstable (don't trade carry).
- **Cross-exchange basis:** Same pair's basis on different exchanges. Divergence = fragmented positioning.

### Data Sources for Derivatives Features
| Feature | Binance | OKX | Bybit | Deribit | Tardis |
|---|---|---|---|---|---|
| Funding rate (current + predicted) | REST + WS | REST + WS | REST + WS | REST + WS | derivative_ticker |
| Open interest | REST + WS | REST + WS | REST + WS | REST + WS | derivative_ticker |
| Liquidations | WS (forceOrder) | WS | WS | WS | liquidations |
| Long/short ratio (top trader) | REST (5min) | REST | REST | N/A | N/A |
| Long/short ratio (global) | REST (5min) | REST | REST | N/A | N/A |
| Taker buy/sell volume | REST (kline) | REST | REST | N/A | trades (compute) |

## Order Book / LOB Features

### Depth & Imbalance
- **Book imbalance (BIR):** `(bid_qty_top_k - ask_qty_top_k) / (bid_qty_top_k + ask_qty_top_k)` at levels k=1,3,5,10,20. Most predictive at narrow levels for ultra-short horizons, deeper levels for longer.
- **Weighted mid-price:** `P_wmid = P_ask * Q_bid / (Q_bid + Q_ask) + P_bid * Q_ask / (Q_bid + Q_ask)`. Deviation from raw mid is itself a feature.
- **Depth ratio:** `sum(bid_qty[1:k]) / sum(ask_qty[1:k])` for k=5,10,20. Log-transform recommended.
- **Cumulative depth delta:** Change in total depth at k levels over window w. Captures refill/pull patterns.
- **Queue position features:** Estimated queue position change rate at best bid/ask.

### Order Flow
- **Order flow imbalance (OFI):** Net signed order flow: `sum(signed_qty)` over window. Sign from aggressor side.
- **Trade flow toxicity (VPIN):** Volume-synchronized probability of informed trading. Bucket by volume bars, classify buy/sell, compute abs imbalance.
- **Lambda (Kyle's):** Slope of `delta_P ~ delta_OFI` regression over rolling window. Price impact per unit flow.
- **Trade arrival rate:** Poisson intensity `lambda_t` estimated over exponential windows. Regime shifts in activity.
- **Aggressor ratio:** `buy_volume / (buy_volume + sell_volume)` over window.

### Book Dynamics
- **Spread dynamics:** Spread level changes, spread crossing frequency, time-at-spread.
- **Book renewal rate:** Fraction of book replaced per unit time at each level.
- **Large order detection:** Orders > N*median_size at top K levels. Binary or count feature.
- **Cancellation rate:** Cancel/place ratio over rolling window. High values suggest spoofing or HFT activity.

## Trade Flow Features

### Volume Features
- **Volume bars:** Aggregate by fixed volume (not time). More uniform information content per bar.
- **Dollar bars:** Aggregate by fixed dollar value. Normalizes across price regimes.
- **Tick bars:** Aggregate by fixed trade count. Captures activity intensity.
- **Buy/sell volume:** Signed by aggressor. `buy_vol`, `sell_vol`, `net_vol = buy - sell`.
- **Volume acceleration:** `d(volume)/dt` over exponential window. Detects volume surges.
- **Relative volume:** Current volume / rolling median volume. Detects unusual activity.

### Price Impact
- **Amihud illiquidity:** `|return| / dollar_volume` over window. Higher = less liquid.
- **Realized spread:** `2 * sign * (trade_price - mid_price_at_t+delta)`. Measures adverse selection.
- **Effective spread:** `2 * |trade_price - mid_price| / mid_price`. Actual cost of trading.
- **Roll measure:** `2 * sqrt(-cov(delta_p_t, delta_p_{t-1}))`. Spread proxy from autocov.

### Trade Size Distribution
- **Trade size entropy:** Shannon entropy of trade size distribution over window. Low entropy = concentrated.
- **Large trade ratio:** Volume from trades > 95th percentile / total volume.
- **Trade clustering:** Count of trades within X ms of each other. Detects iceberg/algo patterns.

## Microstructure Features

- **Quoted spread:** `(ask - bid) / mid` in bps.
- **Effective half-spread:** `|trade_price - mid| / mid`.
- **Realized volatility (tick):** `sqrt(sum(log_return^2))` from tick data over window.
- **Microstructure noise:** Estimated from bid-ask bounce. `2 * var(noise) = -2 * cov(r_t, r_{t-1})`.
- **Price discreteness:** Fraction of price changes that are 1-tick vs multi-tick.
- **Inter-trade duration:** Mean, std, skew of time between trades. Carries information content (ACD models).

## Cross-Asset Features

Funding rate, basis, and perp-specific positioning features are in [Derivatives & Positioning Features](#derivatives--positioning-features) above.

### Cross-Exchange
- **Cross-exchange spread:** `(price_A - price_B) / price_A` between same pair on different exchanges.
- **Exchange lead-lag:** Which exchange moves first for same symbol. Granger causality or IC at lags.
- **Volume concentration:** Fraction of total volume on dominant exchange. Shifts in concentration are informative.

### Cross-Asset
- **BTC beta:** Rolling regression beta of altcoin returns vs BTC returns.
- **Sector momentum:** Equal-weighted return of tokens in same sector (DeFi, L1, L2, etc.).
- **Correlation regime:** Rolling correlation to BTC. Spikes in correlation = risk-off.
- **Relative strength:** `return(asset) - return(BTC)` over window. Pairs trading signal.

## Volatility Features

- **Realized vol (close-close):** `std(log_returns) * sqrt(periods_per_day)` over window.
- **Parkinson vol:** `sqrt(1/(4*N*ln2) * sum((ln(H/L))^2))`. Range-based, more efficient.
- **Garman-Klass vol:** Uses OHLC. More efficient than close-close.
- **Yang-Zhang vol:** Combines overnight and intraday components. Best for gapped markets (less relevant for 24/7 crypto but useful for session analysis).
- **Vol of vol:** Rolling std of realized vol. Captures vol regime uncertainty.
- **Vol term structure slope:** Ratio of short-term to long-term realized vol. Mean-reverting.
- **Vol skew (realized):** Skewness of return distribution over window. Fat tails signal.
- **High-low range ratio:** `(H-L)/mid` over window. Simple vol proxy, robust.

## Options-Derived Features

### Implied Volatility Surface
- **ATM IV:** At-the-money implied vol for nearest expiry. Forward-looking vol consensus.
- **IV term structure:** ATM IV slope across expiries. Steep = event risk priced in.
- **IV skew (25-delta):** `IV(25d put) - IV(25d call)`. Crash fear premium.
- **IV smile curvature:** `(IV(25d put) + IV(25d call)) / 2 - IV(ATM)`. Tail risk pricing.
- **IV-RV spread:** `IV(ATM) - RV`. Volatility risk premium. Persistently positive, mean-reverting.

### Flow & Positioning
- **Put-call ratio (volume):** `put_volume / call_volume`. Sentiment proxy.
- **Put-call ratio (OI):** `put_OI / call_OI`. Positioning proxy.
- **Max pain:** Strike with max aggregate pain for option sellers. Magnet near expiry.
- **Gamma exposure (GEX):** Estimated dealer gamma. Large GEX = vol suppression, negative GEX = vol amplification.
- **Vanna exposure:** Dealer sensitivity to spot-vol correlation. Drives vol-of-vol dynamics.

### Greeks as Features
- **Delta-adjusted volume:** `sum(|delta| * volume)`. Dollar-weighted directional flow.
- **Gamma-weighted OI:** Open interest weighted by gamma. Measures convexity exposure in market.
- **Vega-weighted flow:** Net vega of traded options. Captures vol directional bets.

## Temporal / Calendar Features

- **Time-of-day (sin/cos encoded):** `sin(2*pi*hour/24)`, `cos(2*pi*hour/24)`. Captures intraday seasonality.
- **Day-of-week (sin/cos encoded):** Weekend patterns differ (lower volume, wider spreads).
- **Time to funding reset:** Countdown to next 8h or 1h funding settlement. Predictive near reset.
- **Time to options expiry:** Distance to next major expiry (weekly/monthly/quarterly). Max pain effect intensifies.
- **Seconds since last trade:** Captures lull/activity alternation.
- **Session flag:** Asian/European/US overlap sessions. Different participants, different dynamics.

## Regime / State Features

- **Hidden Markov Model states:** 2-3 state HMM on returns + vol. Bull/bear/neutral regimes.
- **Change-point detection:** Online Bayesian change-point detection on return distribution. Detects regime transitions.
- **Liquidity regime:** Cluster on (spread, depth, volume) features. Different microstructure regimes.
- **Correlation regime:** Rolling BTC correlation + vol cluster. Risk-on vs risk-off.
- **Trend strength (ADX-like):** Smoothed directional movement ratio. Trend vs mean-reversion regime indicator.

## Feature Construction Best Practices

### Normalization
- **Z-score:** `(x - rolling_mean) / rolling_std`. Standard for cross-sectional.
- **Rank transform:** Map to uniform [0,1] via rank. Robust to outliers. Preferred for tree models.
- **Quantile clip:** Winsorize at 1st/99th percentile before z-score. Handles fat tails.
- **Log transform:** For volume, depth, spread, and other right-skewed features.

### Window Selection
- Use **exponential** windows (half-life parameterization) over rolling windows for MFT. More responsive.
- Common half-lives for 10s-1hr: 30s, 1m, 5m, 15m, 30m, 1hr. Multi-scale features.
- **Lookback ratio rule:** Feature lookback should be 3-10x the prediction horizon.

### Feature Interactions
- **Spread x volume:** Low spread + high volume = informed flow.
- **Imbalance x volatility:** Imbalance more predictive in low-vol regimes.
- **Funding x basis:** Divergence between funding and basis signals arb pressure.
- Let tree models discover interactions; for linear models, add explicit crosses.

### Pitfalls
- **Lookahead in normalization:** Rolling stats must use only past data. No future min/max.
- **Survivorship in cross-sectional:** Only include symbols that existed at feature computation time.
- **Stale features:** Book features from illiquid pairs can be stale. Filter by last-update recency.
- **Exchange-specific quirks:** Different tick sizes, lot sizes, fee tiers across exchanges affect microstructure features. Normalize per-exchange.
