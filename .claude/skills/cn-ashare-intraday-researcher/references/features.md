# Feature Engineering Catalog for CN A-Share Intraday

## Table of Contents
1. [Book State Features](#book-state-features)
2. [Event Stream Features](#event-stream-features)
3. [Auction Features](#auction-features)
4. [Price Limit Features](#price-limit-features)
5. [Intraday Volume & Liquidity Features](#intraday-volume--liquidity-features)
6. [Cross-Sectional Features](#cross-sectional-features)
7. [Microstructure Features](#microstructure-features)
8. [Index Futures & ETF Options Features](#index-futures--etf-options-features)
9. [Temporal / Calendar Features](#temporal--calendar-features)
10. [Regime / State Features](#regime--state-features)
11. [Feature Construction Best Practices](#feature-construction-best-practices)

---

## Book State Features

Features computed from periodic book snapshots (L2) or continuously reconstructed book state (L3).

### From L2 Snapshots (10-level depth, SSE ~3s / SZSE ~3s but may vary by subscription tier)
- **Book imbalance (BIR):** `(bid_qty_top_k - ask_qty_top_k) / (bid_qty_top_k + ask_qty_top_k)` at levels k=1,3,5,10. Deeper levels more informative at 1min+ horizons.
- **Weighted mid-price:** `P_wmid = P_ask * Q_bid / (Q_bid + Q_ask) + P_bid * Q_ask / (Q_bid + Q_ask)`. Deviation from raw mid is a feature.
- **Depth ratio:** `sum(bid_qty[1:k]) / sum(ask_qty[1:k])` for k=5,10. Log-transform recommended.
- **Cumulative depth delta:** Change in total depth at k levels over window. Captures refill/pull.
- **Book slope:** `sum(qty[i] * (i - 1)) / sum(qty[i])` for each side. Measures how concentrated liquidity is near top.
- **Depth decay profile:** Exponential fit to `qty(level)`. Steeper = thinner book, more impact.
- **Snapshot-to-snapshot depth change:** Since L2 snapshots are ~3s intervals, delta between consecutive snapshots captures fast book changes.

### From L3-Reconstructed Book
Reconstructing the book from L3 order events (new orders + cancels) provides continuous book state at event-level resolution, not limited to snapshot intervals.
- **Sub-snapshot book imbalance:** BIR computed from reconstructed book at every order/trade event. Captures fast imbalance shifts missed by ~3s L2 snapshots.
- **Event-resolution depth changes:** Track depth additions and withdrawals at each price level between L2 snapshots. Reveals transient liquidity that snapshots miss entirely.
- **Reconstruction drift:** Divergence between L3-reconstructed book and next L2 snapshot. Large drift = missed events or data quality issue. Use as data-quality filter.
- **Cross-validation:** Periodically align reconstructed book to L2 snapshots. Required for correctness. Discard features from intervals with reconstruction errors.

## Event Stream Features

Features computed from L3 order events and/or tick-by-tick trade events. Both streams share a single timeline and are linked via order references (`OrderNo`/`ApplSeqNum`). Source stream tagged per feature.

### Data Stream Notes

**Three event types on a single timeline:**
- **New orders [L3 order stream]:** Order placement events. Fields: order ID, price, qty, side, timestamp.
- **Cancellations:** SSE: `OrdType='D'` in **order stream**. SZSE: `ExecType='4'` in **tick stream**. Cancel location differs by exchange — this is a key reason to process both streams together.
- **Trades [tick stream]:** Execution events with `BuyNo`/`SellNo` (SSE) or `BidApplSeqNum`/`OfferApplSeqNum` (SZSE) linking back to the two matched orders.

**Aggressor determination:** Higher order sequence number = later-arriving order = aggressor (crossed the spread). SSE also provides explicit `TradeBSFlag` (B/S/N). Auction trades have no meaningful aggressor.

**Why unified:** SZSE cancels are in the tick stream, both streams share order ID linkage, and the most valuable features require joining both. Process on a single timeline.

### Order Flow [L3]
- **Order flow imbalance (OFI):** Net signed order flow from new orders: `sum(buy_new_qty - sell_new_qty)` over window.
- **Cancel imbalance:** `(bid_cancel_qty - ask_cancel_qty) / total_cancel_qty`. High one-sided cancellation = potential manipulation or informed withdrawal.
- **Add-cancel ratio:** `new_order_count / cancel_count` per side over window. Low ratio (heavy cancellation) = spoofing signal.
- **Order size distribution:** Entropy or Gini of order sizes. Low entropy = concentrated (institutional-like).
- **Large order detection:** Orders > N*median_size. Count and direction. Institutional footprint.
- **Queue position dynamics:** For a given price level, track queue depth changes. Rapid queue shortening = imminent level consumption.
- **Order arrival rate asymmetry:** `buy_order_rate / sell_order_rate` over window. Directional pressure from order placement alone, before any executions occur.
- **Fleeting liquidity detection:** Orders cancelled within very short time after placement (< N events or < X ms). High fleeting rate at touch = unreliable depth, HFT quoting activity.
- **Order size surprise:** `(median_order_size_window - EMA_median_order_size) / std`. Detects shifts in participant mix (e.g., sudden institutional entry into a retail-dominated name).

### Trade Flow [Tick-by-Tick]
- **Aggressor-signed volume:** Use quant360 aggressor rules (higher sequence number = aggressor). `buy_aggressor_vol`, `sell_aggressor_vol`, `net_aggressor_vol`.
- **Volume bars:** Aggregate by fixed volume threshold (not time). More uniform information per bar.
- **Dollar bars:** Aggregate by fixed CNY value. Normalizes across price levels.
- **Volume acceleration:** `d(volume)/dt` over exponential window. Detects sudden activity surges.
- **Relative volume:** Current volume / rolling median intraday volume for same time-of-day. Detects unusual activity.
- **Intraday cumulative volume ratio:** Actual cumulative volume / expected (from historical intraday profile). >1 = above-average activity.
- **Trade direction runs:** Length of consecutive same-side aggressor trades. Long buy-runs = persistent buying pressure. Run breaks signal potential reversal points.
- **Signed trade autocorrelation:** `corr(sign_t, sign_{t-k})` for lags k=1,5,10 trades. Positive autocorrelation = trending flow. Negative = mean-reverting. Regime indicator.
- **Trade arrival intensity (Hawkes):** Self-excitation parameter from Hawkes process fit. High self-excitation = trades beget trades (cascade/liquidation-like dynamics). Low = independent arrivals.

### Trade Size Analysis [Tick-by-Tick]
- **Large trade ratio:** Volume from trades > 95th percentile / total volume. Institutional proxy.
- **Trade size entropy:** Shannon entropy of trade size distribution. Low entropy = concentrated, likely institutional.
- **Small trade intensity:** Count of minimum-lot (100 or 200 shares) trades / total count. Retail activity proxy. CN market is ~60% retail.
- **Trade clustering:** Count of trades within X ms. Detects algo/iceberg patterns.

### VPIN [Tick-by-Tick, Enhanced with L3]
- Bucket trades by volume (not time). Classify buy/sell via aggressor flag or tick rule.
- `VPIN = mean(|buy_vol - sell_vol| / total_vol)` over N buckets.
- Elevated VPIN predicts short-term volatility. Particularly useful pre-announcement.
- **L3 enhancement:** When L3 order refs are available, use true aggressor classification instead of tick rule. Tick rule misclassifies ~15% of trades; L3 aggressor flags are exact (higher sequence number = aggressor). More accurate VPIN especially during fast markets where tick rule degrades.

### Order-Trade Joint Features [L3 + Tick-by-Tick]
These features capture the conversion from intention (orders) to execution (trades) — often the most informative for MFT horizons.
- **Order-to-trade ratio:** `order_count / trade_count` over window. High ratio = low conversion, HFT activity. CN regulators monitor this metric.
- **Passive fill rate:** Fraction of limit orders that get filled vs cancelled. Low fill rate near touch = fleeting liquidity. Requires joining L3 order IDs with tick-stream trade references.
- **Hidden order detection:** Trades executing at prices with no visible depth in L2 snapshots or reconstructed book. Indicates iceberg/hidden orders. SSE supports hidden orders.
- **Order-to-fill conversion rate:** At best bid/ask, what fraction of resting order volume converts to trades within window? High conversion = genuine liquidity being consumed. Low = quote stuffing or flickering depth.
- **Trade-order flow divergence:** `sign(net_trade_aggressor) != sign(net_new_order_flow)`. When net new orders are buy-biased but trades are sell-aggressor-dominated, signals passive institutional accumulation (placing buy limits, not crossing spread).
- **Informed order detection:** Orders placed within N events before a significant price move (> X bps). Track by order size and timing. Clusters of informed orders reveal detectable institutional footprint.

### Price Impact [Tick + Book State]
- **Kyle's lambda:** Slope of `delta_P ~ delta_OFI` regression over rolling window. Uses OFI from L3 and price changes from tick stream. Price impact per unit flow. Higher = less liquid.
- **Amihud illiquidity:** `|return| / CNY_volume` over window. Tick-by-tick trades only.
- **Realized spread:** `2 * sign * (trade_price - mid_at_t+delta)`. Measures adverse selection. Requires trade price (tick) + mid-price (L2 or reconstructed book).
- **Effective spread:** `2 * |trade_price - mid| / mid`. Actual cost of taking liquidity. Same source requirement as realized spread.
- **Price impact asymmetry:** `lambda_buy / lambda_sell` where lambda is estimated separately for buy-aggressor and sell-aggressor trades. Asymmetry signals directional pressure: higher buy-side impact = thin ask side, bullish setup.

## Auction Features

CN call auctions create unique intraday signals. These features are specific to A-shares and have no direct equivalent in 24/7 crypto markets.

### Opening Call Auction (09:15-09:25)
- **Indicative price trajectory:** Track virtual matching price at 1s intervals during 09:15-09:25. Direction and stability signal opening sentiment.
- **Indicative volume trajectory:** Virtual matched volume over time. Rising volume = converging interest.
- **Order imbalance at close of auction:** Final `(buy_qty - sell_qty) / total_qty` at indicative price. Strong imbalance predicts early AM direction.
- **Price gap:** `(auction_price - prev_close) / prev_close`. Large gaps tend to partially revert in first 5-30min.
- **Gap-volume interaction:** Large gap + high auction volume = more informed, less reversion. Large gap + low volume = more likely to revert.
- **Cancel phase behavior (09:20-09:25):** Orders can be cancelled during 09:20-09:25. Heavy cancellation signals manipulation or uncertainty.

### Closing Call Auction (SZSE 14:57-15:00, SSE no closing auction)
- **Pre-closing price deviation:** `(price_at_14:56 - closing_auction_price) / price_at_14:56`. SZSE closing auction can deviate from continuous trading price.
- **Closing auction volume concentration:** Fraction of daily volume in closing auction. High = institutional rebalancing (index tracking).
- **Closing imbalance:** Order imbalance during closing auction. Predicts overnight gap direction.
- **Index rebalance days:** On CSI300/500/1000 rebalance dates, closing auction volume spikes massively. Separate regime.

### Pre-Open Phase (09:25-09:30)
- No trading occurs but order book state is visible. Analyze order accumulation pattern before continuous trading starts at 09:30.

## Price Limit Features

Price limits create discontinuities and non-linear dynamics unique to CN A-shares. Main Board: +/-10%, ChiNext/STAR: +/-20%, new listings: no limit for first 5 days.

### Distance Features
- **Distance to upper limit:** `(upper_limit - current_price) / current_price` in bps. Key regime-change feature.
- **Distance to lower limit:** `(current_price - lower_limit) / current_price` in bps.
- **Normalized limit distance:** `(current_price - prev_close) / (upper_limit - prev_close)`. 0 = at prev close, 1 = at limit up.

### Limit Dynamics
- **Magnet effect:** Prices tend to accelerate toward limits once within ~2-3%. Feature: speed of approach (return/minute) when within X% of limit.
- **Limit-up board (zhang ting ban) features:**
  - **Queue depth at limit:** Total unfilled buy orders at limit price. Measures conviction.
  - **Queue stability:** Duration of continuous limit-up. Opens that quickly break = weak. Sustained = strong.
  - **Seal-break count:** Number of times price hits limit then retreats. More breaks = weaker conviction.
  - **Seal volume ratio:** `volume_at_limit / total_daily_volume`. High ratio = strong lock.
- **Limit-down board (die ting ban) features:** Mirror of above for sell side.
- **Consecutive limit days:** Count of consecutive limit-up or limit-down days. Momentum/reversal signal.
- **Cross-stock limit contagion:** Fraction of sector peers at limit. Sector-wide limit events have different dynamics than idiosyncratic ones.

### Limit Impact on Feature Engineering
- **Truncation bias:** When price is at limit, many features become degenerate (spread = 0, imbalance = max). Must handle:
  - Flag limit-locked periods and either exclude or use separate feature set.
  - Use order queue features instead of price-based features during limit-lock.

## Intraday Volume & Liquidity Features

### Volume Profile
- **Intraday volume curve deviation:** `actual_volume_bin / expected_volume_bin` where expected is historical average for that time-of-day. CN stocks have distinctive U-shape (high at open/close, low at lunch).
- **Volume surprise:** `(volume_window - EMA_volume) / std_volume`. Spike detection.
- **Session volume ratio:** `AM_volume / PM_volume`. Deviations from normal ratio signal regime.
- **Pre/post-lunch volume:** Volume in 11:20-11:30 (pre-lunch) vs 13:00-13:10 (post-lunch). Information arrival over lunch break.
- **Turnover rate:** `volume / free_float`. Normalized activity level.

### Liquidity Features
- **Realized liquidity:** `volume / (high - low)`. Higher = more liquid for given range.
- **Depth-at-touch persistence:** How long best bid/ask depth survives. Short = fragile.
- **Spread regime:** Rolling quantile of quoted spread. Compare to its own history (10th/50th/90th pctile).

## Cross-Sectional Features

### Market-Level
- **Market breadth:** `advancing_stocks / (advancing + declining)` updated in real-time. Strong breadth = genuine rally, weak breadth = narrow.
- **Up/down limit count:** Number of stocks at limit-up vs limit-down. Extreme values signal market stress/euphoria.
- **Sector money flow:** Net aggressor-signed volume per sector (e.g., TMT, finance, consumer). Detects sector rotation intraday.
- **Index futures premium:** `(IF_price - CSI300_fair) / CSI300_fair`. Positive premium = bullish sentiment. Lead-lag with spot.

### Stock-Level Relative Features
- **Intraday relative strength:** `return(stock) - return(sector_index)` over window. Pairs/mean-reversion signal.
- **Beta-adjusted return:** `return(stock) - beta * return(CSI300)`. Intraday alpha component.
- **Rank return:** Percentile rank of stock's intraday return within its sector. Cross-sectional momentum/reversal.
- **Correlated pair spread:** For highly correlated pairs (e.g., Ping An / China Life), track spread deviation. Mean-reversion at intraday horizon.

### Northbound (Stock Connect) Flow
- **Northbound net buy/sell:** Real-time intraday cumulative net foreign buy via Stock Connect (沪深港通). Extreme values tend to mean-revert. Strong sentiment indicator widely used in CN quant.
- **Northbound stock-level flow:** Per-stock foreign holding changes (delayed T+1 disclosure). Persistent buying/selling signals institutional conviction.
- **Northbound flow acceleration:** `d(cumulative_net_buy)/dt` over window. Detects surges in foreign flow.

### ETF/Index Signals
- **ETF premium/discount:** `(ETF_price - NAV) / NAV`. For SSE 50 ETF, CSI 300 ETF. Arb signal and sentiment proxy.
- **ETF creation/redemption flow:** When available, net creation = bullish, net redemption = bearish.

## Microstructure Features

- **Quoted spread:** `(ask_1 - bid_1) / mid` in bps. CN stocks typically 2-20bps depending on price level and liquidity.
- **Effective half-spread:** `|trade_price - mid| / mid`. Actual cost.
- **Tick-size constraint:** At 0.01 CNY, low-priced stocks (e.g., 3 CNY) have 3.3bps minimum spread while high-priced stocks (e.g., 300 CNY) have 0.03bps. This creates very different microstructure dynamics by price level. Normalize accordingly.
- **Realized volatility (tick):** `sqrt(sum(log_return^2))` from trade data over window.
- **Microstructure noise:** `2 * var(noise) = -2 * cov(r_t, r_{t-1})` from bid-ask bounce.
- **Price discreteness:** Fraction of price changes that are 1-tick vs multi-tick. More informative for low-priced stocks.
- **Inter-trade duration:** Mean, std, skew of time between trades. Carries information content.
- **Hasbrouck information share:** For stocks trading on both SSE and SZSE (rare, mainly via ETFs), or between stock and its futures/options.

## Index Futures & ETF Options Features

Index futures (CFFEX) and ETF options provide market-level sentiment and hedging flow signals.

### Index Futures (IF/IH/IC/IM on CFFEX)
- **Basis:** `(futures_price - index_value) / index_value`. Reflects cost of carry + sentiment.
- **Basis momentum:** `basis_t - EMA(basis, N)`. Widening basis = increasing speculative demand.
- **Futures lead:** Index futures often lead spot by 1-5 minutes. Use futures return as predictive feature for constituent stocks.
- **Futures volume spike:** Abnormal futures volume precedes spot moves. Feature: `futures_volume / EMA(futures_volume)`.
- **Open interest change:** Rising OI + rising price = new longs (bullish). Rising OI + falling price = new shorts (bearish).
- **Delivery convergence:** Near contract expiry, basis converges to zero. Arbitrage pressure affects constituent stocks.

### ETF Options (50ETF, 300ETF on SSE/SZSE)
- **Put-call ratio:** `put_volume / call_volume`. Sentiment proxy. Extreme values contrarian.
- **IV skew:** `IV(OTM put) - IV(OTM call)`. Crash fear premium.
- **IV level change:** Delta IV(ATM) over window. Rising IV = uncertainty increasing.
- **Max pain:** Strike with maximum aggregate open interest pain. Magnet near expiry.
- **GEX (Gamma Exposure):** Estimated dealer gamma. Positive GEX suppresses volatility, negative amplifies.

## Temporal / Calendar Features

- **Trading phase flag:** Opening auction / continuous AM / lunch break / continuous PM / closing auction. Encode as categorical or one-hot.
- **Minutes since open:** Continuous feature. Many effects are time-dependent (e.g., opening reversion decays).
- **Minutes to close:** Captures end-of-day effects (position squaring, closing auction preparation).
- **Minutes to/from lunch:** Pre-lunch position reduction, post-lunch information arrival.
- **Day of week:** Monday effect (weekend information), Friday effect (position unwinding). Sin/cos encode.
- **Month-end / quarter-end:** Window dressing, fund rebalancing. Binary flag.
- **Index rebalance proximity:** Days to next CSI300/500/1000 rebalance. Affects constituents being added/removed.
- **Options expiry proximity:** Days to next 50ETF/300ETF options expiry (monthly, 4th Wednesday). Max pain effect.
- **IPO lock-up expiry:** Days to lock-up period end for individual stocks. Known sell pressure catalyst.
- **Earnings announcement proximity:** Days to/from earnings release. Vol expansion.

## Regime / State Features

- **Volatility regime:** Cluster on realized vol (e.g., HMM 2-3 states). Different features work in different vol regimes.
- **Market regime:** Bull/bear/neutral from CSI300 rolling return. Affects mean-reversion vs momentum dynamics.
- **Liquidity regime:** Cluster on (spread, depth, turnover). Different microstructure in thin vs thick markets.
- **Limit-proximity regime:** Binary — is stock within X% of any price limit? Feature behavior changes near limits.
- **Auction vs continuous phase:** Many features behave differently during auction. Separate models or conditional features.
- **Lunch-break state:** Post-lunch first 5-10 minutes often behave like a mini-open. Separate regime.
- **Market stress indicator:** Composite of VIX-equivalent (iVIX), futures discount, breadth. Conditions all feature interpretation.

## Feature Construction Best Practices

### Normalization
- **Z-score:** `(x - rolling_mean) / rolling_std`. Standard for regression targets.
- **Rank transform:** Map to uniform [0,1] via rank. Robust to outliers. Preferred for tree models.
- **Quantile clip:** Winsorize at 1st/99th percentile before z-score. CN stocks have fat tails.
- **Log transform:** For volume, depth, spread, and other right-skewed features.
- **Cross-sectional z-score:** Normalize across stocks at same timestamp. Essential for cross-sectional features.

### Window Selection
- Use **exponential** windows (half-life parameterization) for responsiveness.
- Common half-lives for 1min-2hr: 1m, 5m, 15m, 30m, 1hr, 2hr. Multi-scale features.
- **Lookback ratio rule:** Feature lookback should be 3-10x the prediction horizon.
- **Lunch break handling:** DO NOT span windows across 11:30-13:00 lunch break. Either reset at lunch or use separate AM/PM windows.

### Feature Interactions
- **Imbalance x volatility:** Book imbalance more predictive in low-vol regimes.
- **Volume x spread:** High volume + tight spread = informed institutional flow.
- **Limit distance x imbalance:** Near-limit imbalance behavior is non-linear. Interaction captures this.
- Let tree models discover interactions; for linear models, add explicit crosses.

### CN-Specific Pitfalls
- **Lunch break discontinuity:** Features computed across 11:30-13:00 mix two sessions. Always handle lunch break.
- **Auction contamination:** Opening/closing auction mechanics differ from continuous trading. Features computed during 09:25-09:30 or 14:57-15:00 (SZSE) are different regimes.
- **Price limit truncation:** At limit price, spread=0, imbalance=max, many features degenerate. Detect and handle.
- **Tick size effects:** 0.01 CNY tick creates dramatically different microstructure for 3 CNY vs 300 CNY stocks. Normalize features by tick-size-relative metrics.
- **Stale L2 snapshots:** L2 snapshots are ~3s intervals (SSE; SZSE may vary by tier). Features may be stale if computed from last snapshot vs current trade price.
- **T+1 constraint:** Cannot sell same-day purchases. Affects feature-label alignment for short signals (must already hold).
- **Board differences:** ChiNext/STAR have wider limits (+/-20%), different lot sizes (STAR: 200), and often different microstructure than Main Board. Consider board as a categorical feature or train separate models.
