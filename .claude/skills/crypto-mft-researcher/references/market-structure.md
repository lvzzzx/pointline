# Crypto Market Structure Reference

## Table of Contents
1. [Exchange Taxonomy](#exchange-taxonomy)
2. [Instrument Types](#instrument-types)
3. [Funding Rate Mechanics](#funding-rate-mechanics)
4. [Fee Structures](#fee-structures)
5. [Trading Sessions & Timing](#trading-sessions--timing)
6. [Liquidity Landscape](#liquidity-landscape)
7. [Crypto-Specific Risk Factors](#crypto-specific-risk-factors)
8. [Options Market Structure](#options-market-structure)

---

## Exchange Taxonomy

### Tier-1 CEX (Primary Data Sources)
| Exchange | Strengths | Perps | Spot | Options | Funding Freq |
|---|---|---|---|---|---|
| Binance | Deepest liquidity, most pairs | Yes | Yes | Yes (limited) | 8h |
| OKX | Strong derivatives, good API | Yes | Yes | Yes | 8h |
| Bybit | Derivatives focus, clean data | Yes | Yes | No | 8h |
| Deribit | Options king, BTC/ETH focus | Yes | Limited | Yes (primary) | 8h |
| Bitget | Growing derivatives | Yes | Yes | No | 8h |

### Tier-2 CEX
| Exchange | Notes |
|---|---|
| Gate.io | Wide altcoin coverage, lower liquidity |
| MEXC | Early altcoin listings, suspect volume |
| HTX (Huobi) | Declining, Asia-focused |
| KuCoin | Good altcoin coverage |
| Kraken | Regulated, fiat on-ramp, limited MFT utility |
| Coinbase | Regulated, institutional flow, limited derivatives |

### DEX (On-Chain)
| Protocol | Chain | Type | Notes |
|---|---|---|---|
| Uniswap v3 | Ethereum/Arb/Base | Spot AMM | Concentrated liquidity, tick data available |
| dYdX v4 | Cosmos (own chain) | Perps CLOB | Order book on-chain, good data |
| Hyperliquid | Own L1 | Perps CLOB | Fast settlement, growing liquidity |
| GMX | Arbitrum | Perps oracle-based | Oracle pricing, unique micro |
| Aevo | OP Stack | Options + Perps | On-chain options, growing |

## Instrument Types

### Spot
- Direct asset exchange (BTC/USDT, ETH/BTC).
- Settlement: immediate (T+0). No expiry. No funding.
- Quote currencies: USDT (dominant), USDC, BTC, ETH, FDUSD.
- **MFT relevance:** Basis leg for arb, microstructure features, lead-lag with perps.

### Linear Perpetual Swaps
- Margined and settled in USDT/USDC. PnL linear in price.
- No expiry. Funding mechanism anchors to spot.
- **Dominant instrument for crypto MFT.** Deepest liquidity, tightest spreads.
- Position size in contracts = units of base asset (1 contract = 1 BTC for BTC-USDT perp on most exchanges).
- Max leverage: typically 50-125x (exchange/pair dependent). Effective leverage for MFT: 1-10x.

### Inverse Perpetual Swaps
- Margined in base asset (BTC). PnL non-linear (`PnL = contracts * (1/entry - 1/exit)`).
- Convexity: long position gains accelerate as price rises, losses accelerate as price falls.
- **MFT relevance:** Different margining creates different liquidation dynamics. Lead-lag vs linear perps.

### Dated Futures (Quarterly)
- Fixed expiry (quarterly: March, June, September, December). Settlement at expiry.
- Basis to spot = implied interest rate + convenience yield.
- **MFT relevance:** Calendar spread trades, basis term structure features.

### Options (European-style)
- Deribit dominant (>90% volume). BTC and ETH primary. SOL growing.
- European exercise, cash-settled at expiry index price.
- Strikes: fine grid near ATM, wider OTM. Expiries: daily, weekly, monthly, quarterly.
- **MFT relevance:** IV surface features, GEX/vanna exposure estimation, put-call ratio.

## Funding Rate Mechanics

### Standard 8-Hour Funding (Binance, OKX, Bybit, Deribit)
- Settlement times: 00:00, 08:00, 16:00 UTC.
- **Calculation:** `funding_rate = clamp(premium_index + clamp(interest_rate - premium_index, -0.05%, 0.05%), -cap, cap)`.
- **Premium index:** `(TWAP(perp_mid) - TWAP(spot_index)) / TWAP(spot_index)` over funding period.
- **Interest rate:** Typically 0.01% per 8h (USDT borrow - USD borrow).
- **Payment:** `position_value * funding_rate`. Longs pay shorts if positive, shorts pay longs if negative.

### Predictive Funding Features
- **Current predicted rate:** Real-time TWAP of premium index. Available via API.
- **Funding rate velocity:** `d(predicted_rate)/dt`. Captures pre-settlement positioning.
- **Cross-exchange funding divergence:** Different exchanges can have different rates. Arb signal.

### Funding Rate Trading Patterns
- **Pre-settlement drift:** Prices tend to move toward funding neutralization ~30min before settlement.
- **Post-settlement reversal:** After funding payment, pressure that caused extreme rates may unwind.
- **Extreme funding as mean-reversion signal:** Funding > 0.1% or < -0.1% tends to mean-revert.

## Fee Structures

### Typical Fee Tiers (as of 2024-2025)

**Binance (Futures):**
| VIP Level | Maker | Taker | 30d Volume |
|---|---|---|---|
| Regular | 0.020% | 0.040% | < $15M |
| VIP 1 | 0.016% | 0.040% | $15M-$100M |
| VIP 3 | 0.012% | 0.032% | $500M-$1B |
| VIP 6 | 0.004% | 0.024% | $5B-$10B |

**Bybit (Futures):**
| VIP Level | Maker | Taker |
|---|---|---|
| Regular | 0.020% | 0.055% |
| VIP 1 | 0.018% | 0.040% |
| VIP 3 | 0.010% | 0.032% |

**Deribit (Options):**
- Maker: 0.02% of underlying, Taker: 0.03%.
- Cap: 12.5% of option price (prevents high fees on cheap options).

### Fee Considerations for MFT Backtest
- Always model at your actual VIP tier, not best-case.
- Maker vs taker matters enormously. MFT strategies should target >70% maker fills.
- BNB/platform token discounts (Binance: 10% discount using BNB).
- Some pairs have promotional 0% maker fees. Do not assume this persists.

## Trading Sessions & Timing

### Session Overlaps (Approximate, 24/7 Market)
| Session | UTC Hours | Characteristics |
|---|---|---|
| Asia | 00:00-08:00 | Moderate volume, CN/KR/JP traders, altcoin activity |
| Europe | 08:00-14:00 | Rising volume, institutional flow, BTC/ETH focus |
| US | 14:00-21:00 | Peak volume, CME correlation, highest volatility |
| US Evening | 21:00-00:00 | Declining volume, wider spreads |

### Key Timing Events
| Event | UTC Time | Impact |
|---|---|---|
| Funding settlement | 00:00, 08:00, 16:00 | Pre-/post-settlement price pressure |
| CME open | 14:30 (winter), 13:30 (summer) | BTC gap fill, vol spike |
| CME close | 21:00 (winter), 20:00 (summer) | Position squaring |
| Options expiry (Deribit) | 08:00 UTC Friday (weekly), last Friday of month/quarter | Max pain magnet effect |
| US CPI/FOMC | Varies | Major vol events, affects crypto via risk sentiment |

### Weekend Effects
- Volume drops 30-50% on weekends.
- Spreads widen. Slippage increases.
- **MFT impact:** Signals may degrade on weekends. Consider weekend flag feature or exclude.

## Liquidity Landscape

### BTC/ETH Liquidity Profile (Tier-1 Exchange)
| Metric | BTC-USDT Perp | ETH-USDT Perp |
|---|---|---|
| Typical spread | 0.5-1 bps | 1-2 bps |
| Top-of-book depth | $1-5M | $0.5-2M |
| 10-level depth | $5-20M | $2-10M |
| Daily volume | $10-30B | $5-15B |

### Altcoin Liquidity (Varies Widely)
- **Large cap (SOL, XRP):** 2-5 bps spread, $100K-$1M top depth.
- **Mid cap (ARB, OP):** 5-15 bps spread, $50K-$500K top depth.
- **Small cap:** 15-100+ bps spread, $10K-$100K top depth. Often illiquid.

### Liquidity Patterns
- **Intraday:** Tightest during US+EU overlap. Widest in Asian early morning.
- **Volatility impact:** Spreads widen 2-10x during high-vol events (liquidation cascades, black swans).
- **Cross-exchange:** Arbitrageurs keep prices in line across exchanges. Lead-lag is 50-500ms.

## Crypto-Specific Risk Factors

### Liquidation Cascades
- Cascading liquidations when leveraged positions are forcefully closed.
- Mechanism: price move → liquidations → market orders → more price move → more liquidations.
- **Detectable:** Open interest dropping fast + aggressive directional volume + funding rate extreme.
- **MFT relevance:** Liquidation cascades create predictable short-term patterns (overshoot then reversal).

### Exchange Risk
- Exchange downtime (API outages, maintenance windows).
- Withdrawal suspensions, trading halts.
- **Mitigation:** Monitor exchange status APIs. Multi-exchange execution.

### Regulatory Events
- SEC/CFTC enforcement actions → immediate price impact on affected tokens.
- Country bans/regulations → exchange flow changes.
- **Feature:** Regulatory event calendar as conditional feature.

### Token-Specific Events
- Protocol upgrades (Ethereum merge, Bitcoin halving).
- Airdrops (snapshot dates create pre-event dynamics).
- Unlock schedules (vesting cliff → sell pressure).
- **Feature:** Event calendar features for known upcoming events.

## Options Market Structure

### Deribit Specifics
- European-style, cash-settled.
- Settlement index: volume-weighted average from multiple exchanges.
- **Mark price:** Based on Deribit's IV surface model, not last trade. Important: mark ≠ last.
- **Block trades:** Large trades executed OTC, reported on exchange. Create information asymmetry.

### IV Surface Dynamics
- **Smile shape:** Crypto smiles are right-skewed (call skew) in bull markets, left-skewed in bear markets.
- **Term structure:** Typically upward sloping (higher IV for longer expiry). Inverts during events.
- **Sticky strike vs sticky delta:** Crypto tends toward sticky delta. IV moves with spot.

### Options Flow as Alpha
- **Unusual options activity:** Large trades in OTM options signal informed speculation.
- **Dealer hedging flow:** Market makers delta-hedge, creating predictable spot flow.
- **Gamma squeeze:** When dealers short gamma near a strike with large OI, hedging amplifies moves.
