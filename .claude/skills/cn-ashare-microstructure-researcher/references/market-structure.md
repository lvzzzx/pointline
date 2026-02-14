# CN A-Share Market Structure for Intraday Research

This reference covers market structure aspects relevant to **research design and feature engineering**. For L2/L3 data format schemas, field definitions, and exchange-specific parsing rules, see the quant360 skill.

## Table of Contents
1. [Market Overview](#market-overview)
2. [T+1 Settlement & Research Implications](#t1-settlement--research-implications)
3. [Intraday Session Dynamics](#intraday-session-dynamics)
4. [Price Limit Dynamics](#price-limit-dynamics)
5. [Participant Structure](#participant-structure)
6. [Index Futures & Derivatives](#index-futures--derivatives)
7. [Liquidity Landscape](#liquidity-landscape)
8. [Regulatory Environment](#regulatory-environment)

---

## Market Overview

### Exchanges
| Exchange | Abbrev | Boards | Notes |
|---|---|---|---|
| Shanghai Stock Exchange | SSE | Main Board, STAR Market (科创板) | No closing auction. L2 snapshots + L3 order-by-order + tick-by-tick. |
| Shenzhen Stock Exchange | SZSE | Main Board, ChiNext (创业板) | Closing auction 14:57-15:00. L2 snapshots + L3 order-by-order + tick-by-tick. |
| Beijing Stock Exchange | BSE | Main Board | Smaller, less liquid. +/-30% limits. Less relevant for MFT. |

### Board Characteristics
| Board | Exchange | Price Limit | Lot Size | Min Order | Typical Market Cap | Microstructure |
|---|---|---|---|---|---|---|
| SSE Main | SSE | +/-10% | 100 shares | 100 shares | Large cap | Deep book, tight spread |
| SZSE Main | SZSE | +/-10% | 100 shares | 100 shares | Mid-large cap | Similar to SSE Main |
| ChiNext 创业板 | SZSE | +/-20% | 100 shares | 100 shares | Growth/tech, mid-small cap | Wider limits, more volatile |
| STAR 科创板 | SSE | +/-20% | 200 shares | 200 shares | Tech/innovation | Wider limits, higher tick-to-price ratio |
| BSE | BSE | +/-30% | 100 shares | 100 shares | Small cap | Thin liquidity |

### Key Structural Properties
- **T+1 settlement:** Cannot sell shares bought today. Fundamental constraint for intraday research.
- **Price limits:** Hard limits on daily price movement. Creates truncated distributions and limit-specific dynamics.
- **Retail-dominated:** ~60-70% of turnover from retail investors. Different microstructure than institutional markets (US, HK).
- **Centralized exchange:** No dark pools, no internalization. All flow visible on-exchange (L3 data captures everything).
- **Tick size:** 0.01 CNY universal. Creates very different tick-to-price ratios (large for cheap stocks, tiny for expensive stocks).
- **No short selling for most stocks:** Short selling (融券) is restricted and expensive. Effectively long-only for most participants.

## T+1 Settlement & Research Implications

T+1 is the single most important structural constraint for CN A-share intraday research.

### What T+1 Means
- Shares purchased on day T cannot be sold until day T+1 (next trading day).
- Shares already held (purchased on T-1 or earlier) can be sold on day T.
- This is **asymmetric:** selling existing holdings is unrestricted, but new purchases are locked for 1 day.

### Research Design Implications
- **Intraday long signals:** Buy today → must hold overnight → sell tomorrow at earliest. Overnight gap risk is unavoidable. Intraday signal must be strong enough to survive overnight noise.
- **Intraday sell signals:** Only useful if you already hold the stock. Universe of sellable stocks = existing portfolio. Reduces effective short-side universe.
- **Strategy design patterns:**
  - **T+0 with ETFs:** Some ETFs (e.g., SSE 50 ETF, CSI 300 ETF) allow same-day buy+sell. Legitimate intraday vehicle but limited universe.
  - **Intraday timing, overnight holding:** Use intraday signals to time entries, accept overnight holding. Separate overnight risk from intraday alpha.
  - **Pairs/hedged:** Buy stock + short index futures (IF/IH/IC/IM) to hedge overnight gap risk. Isolates intraday alpha.
  - **Portfolio rotation:** Sell yesterday's buys, buy today's signals. Daily turnover = 100%. Net portfolio size constant.

### Backtest Realism
- **Naive intraday backtest (T+0):** Assumes buy and sell within day. **Invalid for stocks.** Only valid for T+0 ETFs.
- **Realistic backtest (T+1):** Buy at signal, sell next-day open/VWAP/optimal. Must include overnight PnL.
- **Hybrid backtest:** Use intraday signal for entry timing, evaluate over T+1 minimum hold. Most practical.

## Intraday Session Dynamics

### Trading Session Timeline (All Times CST/UTC+8)
| Phase | SSE | SZSE | Activity |
|---|---|---|---|
| Pre-open orders | 09:15-09:25 | 09:15-09:25 | Order submission for opening auction |
| Cancel allowed | 09:15-09:20 | 09:15-09:20 | Orders can be cancelled |
| No cancel | 09:20-09:25 | 09:20-09:25 | Orders locked, no cancellation |
| Pre-open (no trade) | 09:25-09:30 | 09:25-09:30 | Order accumulation, no matching |
| AM continuous | 09:30-11:30 | 09:30-11:30 | Normal continuous trading |
| Lunch break | 11:30-13:00 | 11:30-13:00 | No trading (90 minutes) |
| PM continuous | 13:00-14:57 (SSE: to 15:00) | 13:00-14:57 | Normal continuous trading |
| Closing auction | N/A | 14:57-15:00 | SZSE only: closing call auction |
| SSE close | 14:59-15:00 | N/A | SSE: closing price = VWAP of last minute (14:59:00-15:00:00) |

### Session-Specific Dynamics
- **First 30 minutes (09:30-10:00):** Highest volatility, widest spreads, most volume. Overnight information gets priced in. Gap fill/continuation dynamics.
- **Mid-morning (10:00-11:00):** Volume settles. Microstructure features most predictive here.
- **Pre-lunch (11:00-11:30):** Position reduction, spread widening. Some participants unwind before lunch.
- **Post-lunch (13:00-13:30):** Mini-open effect. Information arrived during lunch (news, macro). Similar to morning open but smaller magnitude.
- **Mid-afternoon (13:30-14:30):** Lowest volume period. Thin liquidity, wider spreads. Signal quality may degrade.
- **Last 30 minutes (14:30-15:00):** Rising volume, position squaring. Institutional rebalancing. Closing auction effect (SZSE).

### Lunch Break as Research Feature
- 90-minute gap creates information discontinuity.
- **Pre-lunch signal persistence:** Measure if AM signals carry over to PM. Microstructure signals typically don't. Fundamental signals may.
- **Lunch news effect:** Major announcements sometimes timed for lunch break. Post-lunch open captures this.
- **Feature design:** Never compute rolling features across lunch break.

## Price Limit Dynamics

### Limit Rules by Board
| Board | Daily Limit | New Listing | ST Stocks |
|---|---|---|---|
| SSE/SZSE Main (registration) | +/-10% | +44%/-36% day 1, then +/-10% | +/-5% |
| SSE/SZSE Main (legacy approval) | +/-10% | +44%/-36% day 1, then +/-10% | +/-5% |
| ChiNext | +/-20% | No limit for first 5 days, then +/-20% | +/-20% |
| STAR | +/-20% | No limit for first 5 days, then +/-20% | +/-20% |
| BSE | +/-30% | No limit for first day, then +/-30% | +/-30% |

Limits are calculated from previous close price. `upper_limit = ceil(prev_close * 1.1, 0.01)`, `lower_limit = floor(prev_close * 0.9, 0.01)`.

### Limit-Up (涨停) Dynamics
- **Approach phase:** As price approaches limit-up, buying pressure accelerates (magnet effect). Time-to-limit and acceleration are features.
- **Lock phase:** At limit-up, only buy orders queue (no ask side). Large queue depth = strong conviction.
- **Break and re-lock:** Price may briefly drop below limit then re-lock. Count of seal-breaks measures conviction strength.
- **Limit-up board (打板 da ban):** Popular trading strategy — buy at limit-up expecting next-day continuation. Queue position matters.
- **Next-day dynamics:** Stocks at limit-up today have bimodal next-day outcome: continuation (+another limit-up) or reversal. Premium/discount to limit is a signal.

### Limit-Down (跌停) Dynamics
- Mirror of limit-up. Only sell orders queue.
- **Panic cascading:** Limit-down can cascade to correlated stocks or sector. Cross-stock contagion is a feature.
- **Inability to exit:** Sellers at limit-down may not get filled (no buyers). Liquidity risk.

### Research Implications
- **Truncated returns:** Forward returns are censored when stock hits limit. Affects label design.
- **Feature degeneracy at limit:** Many features become meaningless (spread=0, imbalance=max). Must detect and handle.
- **Limit events as features:** Distance-to-limit, limit-up/down count in sector, consecutive limit days — all valuable features.

## Participant Structure

### Who Trades CN A-Shares
| Participant | ~% of Turnover | Typical Behavior | Microstructure Signature |
|---|---|---|---|
| Retail investors | 60-70% | Short-term, momentum-chasing, small orders | High order-to-trade ratio, small lot sizes (100-1000 shares) |
| Mutual funds | 10-15% | Longer-term, fundamentals, larger blocks | Block trades, VWAP algorithms, end-of-day rebalancing |
| Hedge funds / quant | 5-10% | Intraday alpha, systematic | Algo patterns, high cancellation rate, maker flow |
| Proprietary desks | 5-10% | Market-making, arbitrage | Tight quotes, high cancellation, cross-instrument |
| Insurance / pension | 3-5% | Very long-term, low turnover | Minimal intraday signal |
| QFII/RQFII (foreign) | 2-5% | Mix of fundamental and quant | Northbound (Stock Connect) flow visible |

### Implications for Feature Design
- **Retail dominance = behavioral alpha:** Retail herding, overreaction, and momentum-chasing create exploitable patterns.
- **Institutional footprint:** Large orders, VWAP patterns, block trades visible in L3. Detecting institutional flow is valuable.
- **Northbound flow (陆股通):** Real-time northbound net buy/sell data is published. Sentiment signal from foreign investors.
- **Low short selling:** Most stocks cannot be effectively shorted. Market is structurally long-biased. Affects mean-reversion dynamics.

## Index Futures & Derivatives

### CFFEX Index Futures
| Contract | Underlying | Margin | Trading Hours | Settlement |
|---|---|---|---|---|
| IF | CSI 300 | ~12% | 09:30-11:30, 13:00-15:00 | T+0 |
| IH | SSE 50 | ~12% | 09:30-11:30, 13:00-15:00 | T+0 |
| IC | CSI 500 | ~14% | 09:30-11:30, 13:00-15:00 | T+0 |
| IM | CSI 1000 | ~14% | 09:30-11:30, 13:00-15:00 | T+0 |

**Research relevance:**
- Futures allow T+0 intraday trading (unlike stocks).
- Futures lead spot by 1-5 minutes (price discovery).
- Basis = sentiment indicator. Discount = bearish positioning.
- Can hedge overnight risk from stock positions with futures shorts.

### ETF Options (SSE, SZSE)
| Contract | Underlying | Exchange | Expiries |
|---|---|---|---|
| 50ETF Options | SSE 50 ETF (510050) | SSE | Monthly (4th Wednesday) |
| 300ETF Options | CSI 300 ETF (510300/159919) | SSE/SZSE | Monthly |
| 500ETF Options | CSI 500 ETF | SSE/SZSE | Monthly |
| Sci-Tech 50 ETF Options | STAR 50 ETF | SSE | Monthly |

**Research relevance:**
- Put-call ratio, IV skew, GEX as sentiment signals for underlying constituents.
- Options allow T+0 trading.
- Max pain effect near expiry dates.

### Stock Connect (沪深港通)
- **Northbound:** Foreign investors buying CN A-shares via Hong Kong.
- **Southbound:** CN investors buying HK stocks.
- **Data signal:** Northbound net buy/sell data published in near real-time. Used widely as sentiment indicator.
- **Daily quota:** Rarely binding. More relevant as flow signal than constraint.

## Liquidity Landscape

### Typical Liquidity by Stock Type
| Stock Type | Typical Spread (bps) | Daily Volume (CNY) | Top-of-Book Depth | L3 Data Quality |
|---|---|---|---|---|
| CSI 300 constituents | 3-10 | 500M-5B | 500K-5M CNY | Rich, high-frequency |
| CSI 500 constituents | 5-20 | 100M-1B | 100K-1M CNY | Good |
| CSI 1000 constituents | 10-40 | 30M-300M | 50K-500K CNY | Adequate |
| Small cap / micro | 20-100+ | <50M | <100K CNY | Sparse, noisy |

### Intraday Liquidity Patterns
- **U-shape:** Volume highest at open and close, lowest mid-afternoon.
- **AM > PM:** Morning session typically has 55-60% of daily volume.
- **Post-lunch spike:** First 5-10 minutes of PM session often sees volume spike.
- **Closing auction (SZSE):** Can be 5-15% of daily volume for index constituents.

### Liquidity Events
- **Index rebalance:** CSI 300/500/1000 rebalance quarterly (June, December semi-annually for CSI 300, quarterly for others). Massive volume in affected stocks, especially closing auction.
- **IPO lock-up expiry:** Large share unlock events create known sell pressure. Liquidity often increases before unlock date.
- **MSCI/FTSE inclusion changes:** Affects northbound flow for included/excluded stocks.

## Regulatory Environment

### Key Regulatory Factors for Research
- **Unusual trading alerts (异常交易):** Exchanges monitor for unusual price/volume patterns. Stocks may be flagged, leading to forced disclosure or trading halts.
- **Trading halts (停牌):** Stocks can be halted for corporate events, regulatory review, or extreme moves. Must handle in backtest (positions frozen during halt).
- **Short selling restrictions (融券):** Very limited stock availability for short selling. List changes frequently. Cannot assume short availability.
- **Order-to-trade ratio monitoring:** Exchanges monitor high O/T ratios (spoofing indicator). Affects legitimate quant strategies that place and cancel many orders.
- **Stamp duty changes:** Government uses stamp duty as market tool. Changed from 0.1% to 0.05% in Aug 2023. Can change again.
- **T+0 speculation:** Recurring market speculation about transitioning from T+1 to T+0. If implemented, would fundamentally change intraday dynamics. Monitor regulatory signals.
- **Registration-based IPO system:** ChiNext/STAR use registration-based system (vs approval-based for Main Board). More new listings, different first-day dynamics.
