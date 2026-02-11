# Chinese Stock L3 MFT Feature Engineering Guide

**Persona:** Quant Researcher (MFT)
**Data Source:** SZSE/SSE Level 3 (l3_orders, l3_ticks)
**Scope:** Opening/Closing Call Auctions + Continuous Trading
**Version:** 1.0

---

## Executive Summary

This guide provides a comprehensive feature engineering framework for mid-frequency trading (MFT) strategies on Chinese A-shares using Level 3 market data. The unique call auction mechanism of Chinese markets (09:15-09:25 opening, 14:57-15:00 closing) creates persistent alpha sources that are inaccessible with standard L1/L2 data feeds.

**Key Differentiators:**
- Call auction imbalance features with cancellable/non-cancellable phase separation
- L3-exclusive cancellation pattern analysis
- Queue position and fill rate estimation
- Session-aware temporal encoding
- Asymmetric cost modeling (stamp duty on sells only)

---

## 1. Market Structure Foundation

### 1.1 Trading Session Architecture

| Session | Time (CST) | UTC Range | Key Characteristics |
|---------|------------|-----------|---------------------|
| Pre-open Call Auction | 09:15-09:25 | 01:15-01:25 | 09:15-09:20: cancellable orders; 09:20-09:25: non-cancellable |
| Opening | 09:25 | 01:25 | Single-price match, highest daily volatility |
| Morning Continuous | 09:30-11:30 | 01:30-03:30 | Standard continuous double auction |
| Lunch Break | 11:30-13:00 | 03:30-05:00 | No trading activity |
| Afternoon Continuous | 13:00-14:57 | 05:00-06:57 | Standard continuous double auction |
| Closing Call Auction | 14:57-15:00 | 06:57-07:00 | Non-cancellable, MOC flows |

**Critical PIT Constraint:** All timestamps in Pointline are stored in UTC. A trade at "09:30:00 CST" appears as "01:30:00" in `ts_exch_us`.

### 1.2 Chinese Market Specifics

| Aspect | US Markets | Chinese A-Shares |
|--------|------------|------------------|
| Opening Mechanism | Immediate continuous | 10-minute call auction |
| Closing Mechanism | Continuous to close | 3-minute call auction |
| Price Limits | None (mostly) | ±10% (±20% STAR Market) |
| Settlement | T+2 | T+1 (no same-day selling) |
| Transaction Costs | Commission only | Commission + 0.1% stamp duty (sell only) |

**Why These Matter:**
- Call auctions concentrate price discovery into discrete events
- Price limits create predictable liquidity dynamics near bounds
- T+1 creates inventory effects and overnight risk asymmetry
- Stamp duty asymmetry makes long-short strategies directionally biased in cost

### 1.3 Channel Architecture & Deterministic Sequencing

L3 data from SZSE/SSE is organized into **independent channels**. Each channel carries a distinct instrument class and maintains its own sequence numbering.

| Exchange | Channel Scope | Notes |
|----------|--------------|-------|
| SZSE | Stocks | Main board equities |
| SZSE | Convertible Bonds | Separate channel from stocks |
| SZSE | Funds (ETFs) | Separate channel from stocks and bonds |
| SSE | Convertible Bonds | SSE's own channel |

#### `appl_seq_num` Properties

| Property | Behavior |
|----------|----------|
| Scope | Per-channel (not per-symbol, not global) |
| Starting value | 1 |
| Uniqueness | Unique within a channel for a given trading day |
| Contiguity | Continuous within a channel — gaps indicate message loss |
| Daily reset | Yes — resets to 1 at the start of each trading day |
| Cross-channel | NOT unique across channels |

#### Shared Sequence Space

Orders (`l3_orders`) and ticks (`l3_ticks`) share the same `(channel_no, appl_seq_num)` sequence within a channel. A tick's `bid_appl_seq_num` / `offer_appl_seq_num` reference the `appl_seq_num` of the originating orders.

This means within a single channel, the combined stream of orders and ticks forms a **total order** over all events.

#### Deterministic Replay Keys

Since `appl_seq_num` resets to 1 each trading day, multi-day replay must include `date` in the sort key. `ts_local_us` is an absolute UTC epoch and naturally orders across days.

| Scenario | Sort Key | Rationale |
|----------|----------|-----------|
| Intra-channel, single day | `(channel_no, appl_seq_num)` | Exchange-native total order within one day |
| Intra-channel, multi-day | `(date, channel_no, appl_seq_num)` | `appl_seq_num` resets daily; `date` disambiguates |
| Cross-channel merge | `(ts_local_us, file_id, file_line_number)` | No cross-channel sequence; fall back to arrival time |
| Cross-table merge (orders + ticks) | `(ts_local_us, file_id, file_line_number)` | Orders and ticks are separate feeds |

**Critical:** `appl_seq_num` alone is meaningless without `channel_no` and `date`. Two channels can both have `appl_seq_num=1` at the same time, and the same channel reuses `appl_seq_num=1` on different days.

---

## 2. Call Auction Mechanics

### 2.1 What is a Call Auction?

A call auction is a **batch matching process** (not continuous):

```
Timeline: 09:15 ---- 09:20 ---- 09:25 ---- 09:30
          [Cancellable] [Non-Cancellable] [Open]
               ↓              ↓             ↓
          Orders can    Orders locked    Single price
          be cancelled  no changes       determined
                                         at 09:25
```

**Matching Algorithm:**
1. Collect all orders during the auction period
2. Publish "indicative price" periodically (market transparency)
3. At auction end: find price that maximizes matched volume
4. All matched trades execute at this single price

### 2.2 Information Asymmetry Window

The 10-minute opening auction creates an **information revelation process**:

| Phase | Time | Information Content |
|-------|------|---------------------|
| Early | 09:15-09:18 | Initial positioning, testing depth |
| Commitment | 09:18-09:20 | Final cancellation window |
| Lock-in | 09:20-09:25 | True imbalance revealed, no escape |
| Discovery | 09:25 | Single price determined |
| Transition | 09:25-09:30 | 5-minute gap for quote adjustment |

**Alpha Principle:** The imbalance at 09:20 (when cancellations stop) predicts the 09:25 open price. The 09:25-09:30 gap reveals market maker confidence.

---

## 3. Feature Families

### 3.1 Call Auction Imbalance Features

#### 3.1.1 Bid/Ask Pressure Ratio

**Intuition:** Heavy buying pressure in auction predicts positive opening gaps.

**Formula:**
```python
call_bid_pressure_ratio = cum_bid_qty / (cum_bid_qty + cum_ask_qty)
call_ask_pressure_ratio = cum_ask_qty / (cum_bid_qty + cum_ask_qty)
```

**Interpretation:**
- Ratio > 0.6: Strong buying pressure → expect positive gap
- Ratio < 0.4: Strong selling pressure → expect negative gap
- Ratio ≈ 0.5: Balanced, less directional predictability

**Implementation Notes:**
- Use `l3_orders` with `order_type` filter
- Calculate cumulatively from 09:15-09:25
- Snap at 09:20 (pre-commitment) and 09:25 (final)

---

#### 3.1.2 Cancellation Pattern Features

**Intuition:** Cancellation behavior reveals trader conviction and information quality.

**Formula:**
```python
# Only valid for 09:15-09:20 (cancellable phase)
cancellable_orders = orders.filter(ts_exch_us < 09:20_utc)
call_cancel_ratio = cancelled_qty / submitted_qty

# Acceleration pattern
cancel_09_15_09_16 = cancels in first minute
cancel_09_19_09_20 = cancels in last minute
call_cancel_acceleration = cancel_09_19_09_20 / cancel_09_15_09_16
```

**Interpretation:**
- High cancel ratio (>30%): "Fishing" behavior, information discovery
- Low cancel ratio (<10%): Strong conviction, committed orders
- High acceleration: Urgency or last-minute information arrival

**L3 Advantage:** Requires individual order lifecycle tracking (order_id → cancel event). Impossible with L2.

---

#### 3.1.3 Price Discovery Dynamics

**Intuition:** The indicative price evolution reveals price discovery quality.

**Formula:**
```python
call_price_drift = auction_price - first_indicative_price
call_price_convergence = std(indicative_prices_over_time)
call_price_velocity = (indicative_price_t - indicative_price_t-1) / delta_t
```

**Interpretation:**
- High drift + low volatility: Strong directional conviction
- High drift + high volatility: Noisy price discovery
- Accelerating velocity: Urgency building
- Decelerating velocity: Consensus forming

---

#### 3.1.4 Final Auction Imbalance

**Intuition:** The net matched volume imbalance at auction close.

**Formula:**
```python
call_final_imbalance = matched_bid_vol - matched_ask_vol
call_final_imbalance_ratio = (matched_bid_vol - matched_ask_vol) / total_matched_vol
call_final_notional = auction_price * auction_volume
```

**Interpretation:**
- Extreme imbalance (>20% ratio): Supply/demand shock, expect gap
- Balanced (<5% ratio): Efficient price discovery
- High notional + imbalance: Institutional footprint

---

### 3.2 Opening Gap & Transition Features

#### 3.2.1 Open Gap Metrics

**Intuition:** The 5-minute gap between auction end (09:25) and continuous start (09:30) creates information assimilation effects.

**Formula:**
```python
open_gap_bps = (price_at_09_30 - auction_price) / auction_price * 10000
open_gap_direction = sign(open_gap_bps)
open_gap_magnitude = abs(open_gap_bps)
```

**Gap Classification:**
- Micro gap (< 10 bps): Neutral, follow auction direction
- Small gap (10-50 bps): Mean reversion likely
- Large gap (> 50 bps): Momentum continuation or sharp reversal
- Limit gap (> 1000 bps): Price limit hit, special handling required

---

#### 3.2.2 Transition Liquidity Features

**Intuition:** How quickly the order book normalizes after the auction gap.

**Formula:**
```python
open_immediate_spread = spread_at_09_30_00
open_depth_recovery_5s = top5_depth_at_09_30_05 / top5_depth_at_09_30_00
open_quote_stability = 1 / (num_quote_changes_09_30_00_to_09_30_30)
```

**Interpretation:**
- Fast recovery: Market maker confidence, liquid instrument
- Slow recovery: Uncertainty, wider spreads persist
- Unstable quotes: Toxic flow detection, adverse selection risk

---

#### 3.2.3 Auction Information Decay

**Intuition:** How auction volume/profile relates to early continuous trading.

**Formula:**
```python
auction_volume_decay = volume_09_30_09_35 / auction_volume
auction_price_retest_distance = min(abs(high_09_30_09_35 - auction_price),
                                     abs(low_09_30_09_35 - auction_price))
auction_vwap_divergence = (vwap_09_30_09_35 - auction_price) / auction_price
```

**Interpretation:**
- Low decay (< 20%): Auction was catalyst, persistent interest
- High decay (> 50%): One-time event, no follow-through
- Small retest distance: Auction price = fair value, consensus
- Large divergence: Auction mispriced, continuous correction

---

### 3.3 Closing Call Auction Features (14:57-15:00)

#### 3.3.1 Closing Imbalance Buildup

**Intuition:** The 3-minute closing auction concentrates MOC (market-on-close) flows.

**Formula:**
```python
close_imbalance_slope = slope(net_bid_pressure, 14:57-15:00)
close_imbalance_acceleration = second_derivative(net_bid_pressure)
close_volume_concentration = auction_volume / total_afternoon_volume
```

**Interpretation:**
- Steep positive slope: Aggressive buying into close
- Last-second surge: ETF tracking flow, urgent repositioning
- High concentration: Institutional-driven, predictable next-day gap

---

#### 3.3.2 MOC Signature Detection

**Intuition:** Identify market-on-close order patterns.

**Formula:**
```python
close_large_order_ratio = count(orders > 5% of avg_size) / total_orders
close_market_order_imbalance = market_orders_bid - market_orders_ask
close_price_stability = std(indicative_prices_during_auction)
```

**Interpretation:**
- High large order ratio: Institutional MOC participation
- Stable price with high volume: Absorptive auction, balanced
- Unstable price: Imbalanced flow, expect overnight gap

---

### 3.4 Order Flow Toxicity Features (Continuous Trading)

#### 3.4.1 Submission-Cancellation Ratio

**Intuition:** "Fleeting" orders indicate informed trading and liquidity toxicity.

**Formula:**
```python
# Rolling window (30 seconds)
submission_cancellation_ratio = cancelled_orders / submitted_orders
order_lifetime_mean = mean(time_between_submit_and_cancel)
order_lifetime_skew = skew(order_lifetimes)
```

**Toxicity Scale:**
- Benign (< 10%): Stable liquidity provision
- Elevated (10-30%): Moderate toxicity, caution warranted
- Toxic (> 30%): Informed flow likely, expect adverse selection
- Extreme (> 50%): "Quote stuffing" or aggressive HFT

**L3 Requirement:** Must track individual order_ids from submission to cancellation.

---

#### 3.4.2 Order Book Churn

**Intuition:** Rapid order book changes indicate uncertainty or manipulation.

**Formula:**
```python
order_book_churn = sum(abs(qty_change_at_each_level)) / avg_total_depth
order_replacement_rate = count(order_updates) / count(trades)
depth_volatility = std(total_depth) / mean(total_depth)
```

**Interpretation:**
- High churn: Liquidity instability, wider spreads likely
- High replacement: Algorithmic repositioning, non-toxic
- Depth volatility: Confidence oscillation, regime change

---

#### 3.4.3 Large Trade Imbalance

**Intuition:** Institutional trades (large size) carry more information than retail flow.

**Formula:**
```python
# Percentile-based threshold
large_trade_threshold = percentile_95(trade_notional, 1_day)

large_trade_imbalance = (
    sum(notional where notional > threshold and side = BUY) -
    sum(notional where notional > threshold and side = SELL)
)

large_trade_participation = large_trade_volume / total_volume
```

**Interpretation:**
- Positive imbalance with high participation: Institutional accumulation
- Negative imbalance with high participation: Distribution
- Low participation: Retail-driven, less informative

---

#### 3.4.4 Fill Rate Proxy (L3 Exclusive)

**Intuition:** Estimate queue position and time-to-fill for passive orders.

**Formula:**
```python
# Match orders with fills using order_id
for each filled_trade:
    order = lookup_order(trade.order_id)
    time_to_fill = trade.ts_exch_us - order.ts_exch_us

fill_rate_proxy = mean(time_to_fill)
fill_rate_percentile_95 = percentile_95(time_to_fill)
queue_depth_estimate = time_to_fill * arrival_rate
```

**Interpretation:**
- Short fills (< 100ms): Aggressive orders, crossing spread
- Medium fills (100ms-1s): Top-of-book queue position
- Long fills (> 1s): Deep in queue, passive providing
- Increasing fill times: Queue lengthening, urgency rising

---

### 3.5 Session-Aware Temporal Features

#### 3.5.1 Intraday Seasonality Encoding

Chinese stocks exhibit distinct intraday patterns:

```
Volume/          ___
Volatility      /   \        ___
Profile        /     \      /   \
              /       \____/     \___

           09:30   11:30  13:00  14:57  15:00
           [High]  [Low]  [Rise] [High] [Close]
```

**Formula:**
```python
# Time since key events
minutes_since_open = (ts - 09_30) / 60_000_000
minutes_to_close_call = (14_57 - ts) / 60_000_000
minutes_since_lunch = (ts - 13_00) / 60_000_000

# Cyclical encoding (for ML)
sin_time = sin(2 * pi * minutes_since_open / 240)  # 4-hour cycle
cos_time = cos(2 * pi * minutes_since_open / 240)

# Session phase classification
session_phase = case(
    when=09:30-10:00 then "opening_burst",
    when=10:00-11:20 then "mid_morning",
    when=11:20-11:30 then "lunch_approach",
    when=13:00-13:30 then "afternoon_open",
    when=13:30-14:40 then "mid_afternoon",
    when=14:40-15:00 then "close_buildup"
)
```

**Why This Matters:**
- A volume spike at 09:35 is normal (expected)
- A volume spike at 11:25 is unusual (more informative)
- Context determines signal significance

---

#### 3.5.2 Lunch Effect Features

**Intuition:** The lunch break creates information digestion and momentum persistence.

**Formula:**
```python
lunch_volume_fade = volume_11:25_11:30 / avg_volume_10:00_11:00
afternoon_momentum = return_13:00_13:05
afternoon_volume_surge = volume_13:00_13:05 / volume_11:25_11:30
```

**Interpretation:**
- Strong fade + strong afternoon open: Information confirmed
- Weak fade + weak afternoon: Lack of conviction
- Momentum continuation afternoon: Overnight news validated
- Momentum reversal afternoon: Overreaction correction

---

### 3.6 Cross-Sectional Factors

#### 3.6.1 Auction Imbalance Factor

**Construction:**
1. Calculate `call_final_imbalance_ratio` for universe of stocks
2. Rank within sector and overall market
3. Normalize to z-scores

**Trading Logic:**
- Long top decile (most positive imbalance)
- Short bottom decile (most negative imbalance)
- Hold: 30 minutes after open
- Close: Before lunch (11:30)

**Why Cross-Sectional:**
- Removes market beta exposure
- Captures relative supply/demand dynamics
- More stable than time-series signals

---

#### 3.6.2 Auction Contribution to Volatility

**Formula:**
```python
auction_vol = realized_vol(09:15-09:25) + realized_vol(14:57-15:00)
total_vol = realized_vol(09:15-15:00)
auction_contrib_vol = auction_vol / total_vol
```

**Interpretation:**
- High (> 30%): Auction-driven, news-sensitive, institutional
- Low (< 10%): Continuous-driven, retail, liquidity trading
- Regime classification: Different models for different auction profiles

---

### 3.7 Close-to-Open Auction Persistence (Overnight Alpha)

This feature family captures how **today's closing call auction** (14:57-15:00, Day T) relates to **tomorrow's opening call auction** (09:15-09:25, Day T+1). The overnight gap between these two auctions creates unique alpha opportunities through information persistence, sentiment carryover, and institutional repositioning.

#### Why Close-to-Open is Different in China

| Aspect | US Markets | Chinese A-Shares |
|--------|------------|------------------|
| Overnight trading | Pre-market/after-hours available | No overnight trading (T+1) |
| Information assimilation | Continuous | Discrete (two call auctions) |
| Price discovery | Distributed | Concentrated at auction boundaries |
| Institutional flows | Throughout day | Concentrated at open/close |

**Key Insight:** In Chinese markets, **all overnight information** must be expressed through:
1. The closing auction (Day T, 14:57-15:00)
2. The opening auction (Day T+1, 09:15-09:25)

There is no continuous overnight price formation. This creates a "pressure accumulation" dynamic.

---

#### 3.7.1 Close-to-Open Price Gap (Overnight Gap)

**Intuition:** The price jump from today's close to tomorrow's open reveals overnight sentiment and information arrival.

**Formula:**
```python
# Day T close auction price
close_auction_price_t = auction_price(day=T, session="close")

# Day T+1 open auction price
open_auction_price_t1 = auction_price(day=T+1, session="open")

overnight_gap_bps = (open_auction_price_t1 - close_auction_price_t) / close_auction_price_t * 10000
overnight_gap_direction = sign(overnight_gap_bps)
```

**Gap Classification:**
| Gap Size | Interpretation | Strategy Bias |
|----------|----------------|---------------|
| < 10 bps | Informationless overnight | Fade any pre-market signals |
| 10-50 bps | Moderate news/sentiment | Follow gap direction if confirmed by imbalance |
| 50-100 bps | Significant information | Momentum continuation likely |
| > 100 bps | Price limit proximity | Watch for limit-up/limit-down scenarios |
| > 1000 bps | Limit hit | Special handling required |

**Alpha Sources:**
1. **Gap Reversal:** Large gaps (> 50 bps) often partially revert in first 30 minutes
2. **Gap Continuation:** Gaps with confirming imbalance tend to continue
3. **Limit Pressure:** Gaps approaching ±10% predict intraday volatility

---

#### 3.7.2 Imbalance Persistence (Close → Open)

**Intuition:** Does today's closing auction imbalance predict tomorrow's opening auction imbalance? This reveals "stuck" inventory and overnight sentiment carryover.

**Formula:**
```python
# Day T closing imbalance
close_imbalance_t = bid_pressure_close_t - ask_pressure_close_t
close_imbalance_ratio_t = close_imbalance_t / total_close_volume_t

# Day T+1 opening imbalance
open_imbalance_t1 = bid_pressure_open_t1 - ask_pressure_open_t1
open_imbalance_ratio_t1 = open_imbalance_t1 / total_open_volume_t1

# Persistence metrics
imbalance_persistence = correlation(close_imbalance_ratio_t, open_imbalance_ratio_t1, window=20_days)
imbalance_carryover = open_imbalance_t1 / close_imbalance_t  # Magnitude ratio
imbalance_direction_consistency = sign(close_imbalance_t) == sign(open_imbalance_t1)
```

**Interpretation:**
- **High persistence (> 0.4 correlation):** Imbalances are structural (index rebalancing, long-term positioning)
- **Low persistence (< 0.1 correlation):** Imbalances are noise/day-specific
- **Strong carryover (> 1.5x):** Overnight information amplified the imbalance
- **Reversal carryover (< 0.5x or negative):** Day T imbalance was exhausted, Day T+1 is counter-positioning

**T+1 Constraint Effect:**
Because Chinese markets are T+1 (cannot sell same day), a strong buying imbalance on Day T creates:
1. Inventory accumulation for buyers
2. Potential selling pressure on Day T+1 (to exit positions)
3. This creates **predictable reversal patterns** in close-to-open imbalance

---

#### 3.7.3 Auction Volume Relationship

**Intuition:** The ratio of closing auction volume to next-day opening auction volume reveals urgency and information intensity.

**Formula:**
```python
close_auction_volume_t = volume(day=T, session="close_auction")
open_auction_volume_t1 = volume(day=T+1, session="open_auction")

volume_ratio_close_to_open = open_auction_volume_t1 / close_auction_volume_t
volume_zscore_vs_history = (volume_ratio_close_to_open - mean_ratio_20d) / std_ratio_20d

# Intraday vs overnight volume distribution
continuous_volume_t = volume(day=T, session="continuous")
overnight_volume_proxy = open_auction_volume_t1
volume_regime_shift = overnight_volume_proxy / continuous_volume_t
```

**Interpretation:**
- **Volume ratio > 2.0:** Opening urgency exceeds closing urgency → expect volatility expansion
- **Volume ratio < 0.5:** Quiet close followed by active open → overnight news event
- **High z-score:** Unusual volume pattern → regime change likely
- **Regime shift > 0.3:** Auction-driven day (institutional) vs continuous-driven (retail)

---

#### 3.7.4 Auction Momentum Features

**Intuition:** Sequential auction dynamics create momentum signals when imbalances persist across the overnight boundary.

**Formula:**
```python
# Auction momentum (directional persistence)
auction_momentum_raw = sign(close_imbalance_t) * sign(open_imbalance_t1)

# Weighted by magnitude
auction_momentum_weighted = (close_imbalance_ratio_t * open_imbalance_ratio_t1) / (std_close * std_open)

# Three-auction sequence (previous open → close → next open)
prev_open_imbalance = open_imbalance_t
close_imb = close_imbalance_t
next_open_imb = open_imbalance_t1

auction_sequence_score = sign(prev_open_imbalance) + sign(close_imb) + sign(next_open_imb)
# Range: -3 (strong sell sequence) to +3 (strong buy sequence)
```

**Momentum Patterns:**
| Pattern | Sequence | Interpretation |
|---------|----------|----------------|
| Strong Continuation | Buy → Buy → Buy | Aggressive accumulation, trend day likely |
| Exhaustion | Buy → Buy → Sell | Close buyers stuck, T+1 sellers emerge |
| Reversal Setup | Buy → Sell → Buy | Day T reversal, overnight confidence restored |
| Confirmed Reversal | Sell → Sell → Buy | Capitulation on Day T, recovery on T+1 |

---

#### 3.7.5 Overnight Sentiment Indicators

**Intuition:** Composite metrics that capture overnight sentiment using both price and volume information from the two auctions.

**Formula:**
```python
# Composite overnight sentiment score
overnight_sentiment = (
    0.4 * normalize(overnight_gap_bps) +
    0.3 * normalize(open_imbalance_ratio_t1) +
    0.2 * normalize(volume_ratio_close_to_open) +
    0.1 * normalize(auction_momentum_weighted)
)

# Sentiment surprise (deviation from expected)
expected_open_imbalance = close_imbalance_t * imbalance_persistence_coefficient
sentiment_surprise = open_imbalance_t1 - expected_open_imbalance

# Gap confirmation score
gap_confirmation = overnight_gap_bps * open_imbalance_ratio_t1
# Positive = gap direction confirmed by imbalance
# Negative = gap direction contradicted by imbalance (reversal likely)
```

**Interpretation:**
- **Sentiment > 2.0:** Strong bullish overnight consensus
- **Sentiment < -2.0:** Strong bearish overnight consensus
- **High surprise:** Unexpected overnight development
- **Negative gap confirmation:** False gap, fade immediately

---

#### 3.7.6 Price Discovery Efficiency

**Intuition:** Measures how efficiently information from the closing auction is incorporated into the next day's opening auction.

**Formula:**
```python
# Price discovery error (if close auction was "efficient")
expected_open_price = close_auction_price_t + overnight_expected_return
price_discovery_error = abs(open_auction_price_t1 - expected_open_price) / close_auction_price_t

# Information asymmetry proxy
close_indicative_volatility = std(indicative_prices_close_t)
open_indicative_volatility = std(indicative_prices_open_t1)
information_asymmetry = open_indicative_volatility / close_indicative_volatility

# Auction efficiency score
auction_efficiency = 1 / (1 + price_discovery_error * information_asymmetry)
```

**Interpretation:**
- **Low efficiency (< 0.3):** High uncertainty, wide spreads expected at T+1 open
- **High efficiency (> 0.7):** Consensus view, expect gap fill if large
- **High asymmetry:** Overnight news arrival, directional opportunity

---

#### 3.7.7 T+1 Inventory Pressure Proxy

**Intuition:** Estimates forced selling pressure on Day T+1 due to T+1 settlement rules and Day T auction accumulation.

**Formula:**
```python
# Estimate stuck buyers from Day T close auction
stuck_buyer_volume = max(0, close_imbalance_t)  # Excess bid volume

# Estimate their urgency to sell on T+1 (can't sell same day)
inventory_pressure_proxy = stuck_buyer_volume / open_auction_volume_t1

# Price distance to exit (how much up move needed for profit)
close_price = close_auction_price_t
high_price_day_t = high(day=T)
inventory_breakeven_gap = (high_price_day_t - close_price) / close_price

# Combined pressure score
exit_pressure_score = inventory_pressure_proxy / (1 + inventory_breakeven_gap)
```

**Interpretation:**
- **High pressure (> 0.5):** Day T buyers underwater, T+1 selling likely
- **Breakeven gap > 100 bps:** Trapped longs, resistance at Day T high
- **Low pressure (< 0.2):** Day T buyers profitable, can hold (no forced selling)

---

## 4. Implementation Templates

### 4.1 Session Boundary Definitions

```python
# UTC microseconds since midnight
SESSION_BOUNDS = {
    # Opening Auction
    "open_call_start": 1_15_00_000_000,          # 09:15 CST
    "open_call_uncancel_end": 1_20_00_000_000,   # 09:20 cutoff
    "open_call_end": 1_25_00_000_000,            # 09:25 opening

    # Continuous Trading
    "morning_start": 1_30_00_000_000,            # 09:30
    "morning_end": 3_30_00_000_000,              # 11:30
    "afternoon_start": 5_00_00_000_000,          # 13:00
    "close_call_start": 6_57_00_000_000,         # 14:57
    "close_call_end": 7_00_00_000_000,           # 15:00
}

def is_auction_period(ts_exch_us: int) -> bool:
    """Check if timestamp falls within auction periods."""
    time_of_day = ts_exch_us % 86_400_000_000  # Microseconds per day
    return (
        (SESSION_BOUNDS["open_call_start"] <= time_of_day <= SESSION_BOUNDS["open_call_end"]) or
        (SESSION_BOUNDS["close_call_start"] <= time_of_day <= SESSION_BOUNDS["close_call_end"])
    )
```

---

### 4.2 Call Auction Feature Extraction

```python
import polars as pl
from pointline.research import query

def extract_opening_auction_features(
    symbol_id: str,
    date: str,
) -> dict:
    """
    Extract PIT-safe opening auction features.

    Feature Time: 09:25 (auction close)
    Valid Lookback: 09:15-09:25
    Valid Prediction: 09:30 onwards
    """
    # Load L3 data
    orders = query.l3_orders("szse", symbol_id, date, date, decoded=True)
    ticks = query.l3_ticks("szse", symbol_id, date, date, decoded=True)

    # Sort for deterministic PIT ordering (cross-table merge uses arrival time)
    # For intra-channel replay, use: .sort(["channel_no", "appl_seq_num"])
    orders = orders.sort(["ts_local_us", "file_id", "file_line_number"])
    ticks = ticks.sort(["ts_local_us", "file_id", "file_line_number"])

    # === AUCTION PERIOD FILTERING ===
    def time_of_day(ts):
        return ts % 86_400_000_000

    open_call_orders = orders.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["open_call_start"]) &
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["open_call_end"])
    )

    # === PHASE SEPARATION ===
    # Cancellable phase (09:15-09:20)
    cancellable = open_call_orders.filter(
        pl.col("ts_exch_us").mod(86_400_000_000) < SESSION_BOUNDS["open_call_uncancel_end"]
    )

    # Non-cancellable phase (09:20-09:25)
    locked = open_call_orders.filter(
        pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["open_call_uncancel_end"]
    )

    # === FEATURE CALCULATION ===

    # 1. Cancellation Pattern
    cancel_features = cancellable.group_by("order_type").agg(
        pl.col("qty").sum().alias("total_qty")
    )

    # Pivot to get cancel ratio
    cancel_summary = cancel_features.pivot(
        values="total_qty",
        index="order_type",
        columns="order_type"
    )

    # order_type: 0=limit, 1=market, 2=cancel
    call_cancel_ratio = (
        pl.when(pl.col("2").is_not_null())
        .then(pl.col("2") / (pl.col("0").fill_null(0) + pl.col("1").fill_null(0) + pl.col("2")))
        .otherwise(0.0)
    )

    # 2. Bid/Ask Pressure
    bid_pressure = locked.filter(pl.col("side") == 0).select(pl.col("qty").sum()).item() or 0
    ask_pressure = locked.filter(pl.col("side") == 1).select(pl.col("qty").sum()).item() or 0

    call_bid_pressure_ratio = bid_pressure / (bid_pressure + ask_pressure) if (bid_pressure + ask_pressure) > 0 else 0.5

    # 3. Auction Volume
    auction_trades = ticks.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["open_call_end"]) &
        (pl.col("tick_type") == 0)  # Fills only
    )

    auction_volume = auction_trades.select(pl.col("qty").sum()).item() or 0
    auction_notional = auction_trades.select((pl.col("price") * pl.col("qty")).sum()).item() or 0

    return {
        "call_cancel_ratio": call_cancel_ratio,
        "call_bid_pressure_ratio": call_bid_pressure_ratio,
        "call_auction_volume": auction_volume,
        "call_auction_notional": auction_notional,
        "call_final_imbalance": bid_pressure - ask_pressure,
    }
```

---

### 4.3 Bar Aggregation with Session Awareness

```python
def create_session_aware_bars(
    symbol_id: str,
    date: str,
    bar_interval: str = "1m",
) -> pl.DataFrame:
    """
    Create OHLCV bars with Chinese session metadata.
    """
    ticks = query.l3_ticks("szse", symbol_id, date, date, decoded=True)
    ticks = ticks.filter(pl.col("tick_type") == 0)  # Fills only

    # Add session phase column
    def get_session_phase(ts):
        tod = ts % 86_400_000_000
        return (
            pl.when((tod >= SESSION_BOUNDS["open_call_start"]) & (tod <= SESSION_BOUNDS["open_call_end"]))
            .then(pl.lit("opening_auction"))
            .when((tod >= SESSION_BOUNDS["morning_start"]) & (tod <= 2_00_00_000_000))
            .then(pl.lit("opening_burst"))
            .when((tod > 2_00_00_000_000) & (tod <= SESSION_BOUNDS["morning_end"] - 10_00_000_000))
            .then(pl.lit("mid_morning"))
            .when((tod > SESSION_BOUNDS["morning_end"] - 10_00_000_000) & (tod <= SESSION_BOUNDS["morning_end"]))
            .then(pl.lit("lunch_approach"))
            .when((tod >= SESSION_BOUNDS["afternoon_start"]) & (tod <= 5_30_00_000_000))
            .then(pl.lit("afternoon_open"))
            .when((tod > 5_30_00_000_000) & (tod <= SESSION_BOUNDS["close_call_start"]))
            .then(pl.lit("mid_afternoon"))
            .when((tod >= SESSION_BOUNDS["close_call_start"]) & (tod <= SESSION_BOUNDS["close_call_end"]))
            .then(pl.lit("closing_auction"))
            .otherwise(pl.lit("unknown"))
        )

    ticks = ticks.with_columns([
        get_session_phase(pl.col("ts_exch_us")).alias("session_phase"),
    ])

    # Aggregate to bars
    bars = ticks.group_by_dynamic(
        "ts_local_us",
        every=bar_interval,
        period=bar_interval,
        closed="left",
        label="left",
    ).agg([
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("qty").sum().alias("volume"),
        (pl.col("price") * pl.col("qty")).sum().alias("notional"),
        pl.col("session_phase").last().alias("session_phase"),
        pl.len().alias("trade_count"),
    ])

    # Add time features
    bars = bars.with_columns([
        ((pl.col("ts_local_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us"))
         .dt.hour().alias("hour")),
        ((pl.col("ts_local_us") / 1_000_000).cast(pl.Int64).cast(pl.Datetime("us"))
         .dt.minute().alias("minute")),
    ])

    return bars
```

---

### 4.4 Close-to-Open Auction Feature Extraction

```python
def extract_close_to_open_features(
    symbol_id: str,
    date_t: str,  # Format: "2024-09-30"
) -> dict:
    """
    Extract features comparing Day T closing auction with Day T+1 opening auction.

    Feature Time: Day T+1 09:25 (after opening auction completes)
    Lookback Windows:
        - Day T close: 14:57-15:00
        - Day T+1 open: 09:15-09:25
    """
    from datetime import datetime, timedelta

    # Calculate T+1 date
    date_t1 = (datetime.strptime(date_t, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    # Load closing auction data (Day T)
    orders_t = query.l3_orders("szse", symbol_id, date_t, date_t, decoded=True)
    ticks_t = query.l3_ticks("szse", symbol_id, date_t, date_t, decoded=True)

    # Load opening auction data (Day T+1)
    orders_t1 = query.l3_orders("szse", symbol_id, date_t1, date_t1, decoded=True)
    ticks_t1 = query.l3_ticks("szse", symbol_id, date_t1, date_t1, decoded=True)

    # Filter closing auction (Day T)
    close_orders = orders_t.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["close_call_start"]) &
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["close_call_end"])
    ).sort(["ts_local_us", "file_id", "file_line_number"])

    close_ticks = ticks_t.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["close_call_start"]) &
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["close_call_end"]) &
        (pl.col("tick_type") == 0)  # Fills only
    ).sort(["ts_local_us", "file_id", "file_line_number"])

    # Filter opening auction (Day T+1)
    open_orders = orders_t1.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["open_call_start"]) &
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["open_call_end"])
    ).sort(["ts_local_us", "file_id", "file_line_number"])

    open_ticks = ticks_t1.filter(
        (pl.col("ts_exch_us").mod(86_400_000_000) >= SESSION_BOUNDS["open_call_start"]) &
        (pl.col("ts_exch_us").mod(86_400_000_000) <= SESSION_BOUNDS["open_call_end"]) &
        (pl.col("tick_type") == 0)
    ).sort(["ts_local_us", "file_id", "file_line_number"])

    # === AUCTION PRICE EXTRACTION ===
    close_auction_price = close_ticks.select(pl.col("price").last()).item()
    open_auction_price = open_ticks.select(pl.col("price").last()).item()

    # === OVERNIGHT GAP CALCULATION ===
    overnight_gap_bps = (open_auction_price - close_auction_price) / close_auction_price * 10000

    # === IMBALANCE CALCULATIONS ===
    # Closing imbalance (Day T)
    close_bid_pressure = close_orders.filter(pl.col("side") == 0).select(pl.col("qty").sum()).item() or 0
    close_ask_pressure = close_orders.filter(pl.col("side") == 1).select(pl.col("qty").sum()).item() or 0
    close_imbalance = close_bid_pressure - close_ask_pressure
    close_volume = close_ticks.select(pl.col("qty").sum()).item() or 0
    close_imbalance_ratio = close_imbalance / close_volume if close_volume > 0 else 0

    # Opening imbalance (Day T+1)
    open_bid_pressure = open_orders.filter(pl.col("side") == 0).select(pl.col("qty").sum()).item() or 0
    open_ask_pressure = open_orders.filter(pl.col("side") == 1).select(pl.col("qty").sum()).item() or 0
    open_imbalance = open_bid_pressure - open_ask_pressure
    open_volume = open_ticks.select(pl.col("qty").sum()).item() or 0
    open_imbalance_ratio = open_imbalance / open_volume if open_volume > 0 else 0

    # === PERSISTENCE METRICS ===
    imbalance_carryover = open_imbalance / close_imbalance if close_imbalance != 0 else 0
    imbalance_direction_consistent = (close_imbalance > 0) == (open_imbalance > 0)

    # === VOLUME RELATIONSHIP ===
    volume_ratio_close_to_open = open_volume / close_volume if close_volume > 0 else 0

    # === MOMENTUM SCORE ===
    auction_momentum_raw = (1 if close_imbalance > 0 else -1) * (1 if open_imbalance > 0 else -1)

    # === GAP CONFIRMATION ===
    gap_confirmation = overnight_gap_bps * open_imbalance_ratio

    # === T+1 INVENTORY PRESSURE ===
    stuck_buyer_volume = max(0, close_imbalance)
    inventory_pressure_proxy = stuck_buyer_volume / open_volume if open_volume > 0 else 0

    return {
        # Price gap
        "close_auction_price": close_auction_price,
        "open_auction_price_t1": open_auction_price,
        "overnight_gap_bps": overnight_gap_bps,

        # Imbalances
        "close_imbalance": close_imbalance,
        "close_imbalance_ratio": close_imbalance_ratio,
        "open_imbalance_t1": open_imbalance,
        "open_imbalance_ratio_t1": open_imbalance_ratio,

        # Persistence
        "imbalance_carryover": imbalance_carryover,
        "imbalance_direction_consistent": imbalance_direction_consistent,

        # Volume
        "close_auction_volume": close_volume,
        "open_auction_volume_t1": open_volume,
        "volume_ratio_close_to_open": volume_ratio_close_to_open,

        # Momentum & Sentiment
        "auction_momentum_raw": auction_momentum_raw,
        "gap_confirmation": gap_confirmation,

        # Inventory pressure
        "inventory_pressure_proxy": inventory_pressure_proxy,
    }


def calculate_imbalance_persistence(
    symbol_id: str,
    start_date: str,
    end_date: str,
) -> float:
    """
    Calculate rolling correlation of imbalance persistence over multiple days.
    """
    from datetime import datetime, timedelta

    close_imbalances = []
    open_imbalances = []

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current < end:
        date_str = current.strftime("%Y-%m-%d")
        next_date_str = (current + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            features = extract_close_to_open_features(symbol_id, date_str)
            close_imbalances.append(features["close_imbalance_ratio"])
            open_imbalances.append(features["open_imbalance_ratio_t1"])
        except Exception:
            # Skip dates with missing data
            pass

        current += timedelta(days=1)

    # Calculate correlation
    if len(close_imbalances) > 10:
        corr = pl.DataFrame({
            "close": close_imbalances,
            "open": open_imbalances,
        }).select(pl.corr("close", "open")).item()
        return corr
    else:
        return 0.0
```

---

## 5. PIT Safety & Validation

### 5.1 Critical Leakage Traps

| Trap | Wrong Approach | Correct Approach |
|------|---------------|------------------|
| **Post-auction return** | Include 09:30-09:35 return as feature for 09:25 prediction | Only use 09:15-09:25 data |
| **Future cancellations** | Use 09:15-09:22 cancels to predict 09:20 state | Truncate at prediction time |
| **Indicative price peeking** | Use final indicative price instead of sequence | Use time-indexed indicative prices |
| **Closing auction spillover** | Use 15:00 data for 14:57-14:58 features | Strict boundary enforcement |
| **Close-to-open lookahead** | Use T+1 opening auction to predict T close | Overnight features only valid after 09:25 on T+1 |
| **Persistence calculation leak** | Include current day when calculating historical persistence correlation | Use rolling window excluding current observation |
| **T+1 gap peeking** | Use intraday T+1 prices to validate overnight gap prediction | Gap prediction must use only T close + T+1 auction open |

### 5.2 Determinism Checklist

- [ ] Intra-channel single-day: sort by `(channel_no, appl_seq_num)`
- [ ] Intra-channel multi-day: sort by `(date, channel_no, appl_seq_num)` — `appl_seq_num` resets daily
- [ ] Cross-channel / cross-table merge: sort by `(ts_local_us, file_id, file_line_number)`
- [ ] Primary timeline: `ts_local_us` (not `ts_exch_us` for cross-venue)
- [ ] As-of joins: `strategy="backward"` only
- [ ] Forward transforms: Labels only, never features
- [ ] Bar boundaries: Fixed `closed` and `label` parameters
- [ ] Reproducibility: Same inputs → identical outputs

### 5.3 Validation Tests

```python
def test_pit_safety():
    """Verify no lookahead in feature generation."""
    symbol = "000001"
    date = "2024-09-30"

    # Generate features at 09:25
    features_0925 = extract_features(symbol, date, as_of_time="09:25:00")

    # Should NOT change when we add future data
    features_0925_with_future = extract_features(
        symbol, date,
        as_of_time="09:25:00",
        include_data_until="09:30:00"  # This should be ignored
    )

    assert features_0925 == features_0925_with_future, "PIT violation detected!"

def test_auction_boundary():
    """Verify auction period isolation."""
    symbol = "000001"
    date = "2024-09-30"

    # Opening auction features
    auction_features = extract_opening_auction_features(symbol, date)

    # Verify no continuous trading data included
    assert auction_features["max_timestamp"] <= SESSION_BOUNDS["open_call_end"]

def test_close_to_open_pit():
    """Verify close-to-open features don't leak future information."""
    symbol = "000001"
    date_t = "2024-09-30"

    # Extract features using only Day T close and Day T+1 open
    features = extract_close_to_open_features(symbol, date_t)

    # Feature timestamp should be Day T+1 09:25 (after opening auction)
    assert features["feature_timestamp"] >= datetime(2024, 10, 1, 1, 25, 0)

    # Gap calculation should NOT use any T+1 continuous trading data
    # (only opening auction 09:15-09:25)
    assert "09:30" not in features["data_sources_used"]

    # Persistence calculation should use historical window excluding current day
    persistence = calculate_imbalance_persistence(symbol, "2024-09-01", "2024-09-30")
    # Should not include Sept 30 data in its own persistence calculation

def test_overnight_gap_label_separation():
    """Ensure gap prediction features are separated from gap outcome labels."""
    symbol = "000001"
    date_t = "2024-09-30"

    # Features (available at 09:25 on T+1)
    features = extract_close_to_open_features(symbol, date_t)
    overnight_gap_bps = features["overnight_gap_bps"]  # This is a feature (known at 09:25)

    # Label (forward-looking, only known after 09:30)
    post_gap_return_30min = calculate_return("09:30", "10:00")  # This is a label

    # Never use post_gap_return as a feature for overnight_gap prediction
    assert "post_gap_return" not in features.keys()
```

---

## 6. Cost-Aware Evaluation

### 6.1 Chinese Trading Cost Structure

| Component | Rate | Side | Notes |
|-----------|------|------|-------|
| Commission | 0.025% | Both | Negotiable, often lower for institutions |
| Stamp Duty | 0.10% | Sell only | Government tax, non-negotiable |
| Transfer Fee | 0.002% | Both | Exchange fee |
| **Total Long** | ~0.027% | Buy |  |
| **Total Short** | ~0.127% | Sell | **4.7x higher cost!** |

### 6.2 Asymmetric Strategy Implications

```python
def calculate_break_even_threshold(direction: str, holding_period: str) -> float:
    """
    Calculate minimum alpha required to break even.
    """
    costs = {
        "long": 0.00027,   # 2.7 bps
        "short": 0.00127,  # 12.7 bps
    }

    # Round-trip cost
    round_trip_cost = costs["long"] + costs["short"]  # 15.4 bps

    # Break-even alpha per trade
    if direction == "long_only":
        return costs["long"]  # 2.7 bps
    elif direction == "short_only":
        return costs["short"]  # 12.7 bps
    elif direction == "long_short":
        return round_trip_cost / 2  # 7.7 bps average

# Example: Auction imbalance factor
# Requires 7.7 bps average alpha per trade to break even
# With 2 trades/day (open/close), need 15.4 bps daily alpha
```

### 6.3 Slippage Estimation

```python
def estimate_slippage_l3(
    symbol_id: str,
    timestamp_us: int,
    target_qty: float,
    side: str,  # "buy" or "sell"
    depth_levels: int = 10,
) -> dict:
    """
    Estimate execution slippage using L3 order book.
    """
    # Load book snapshot at timestamp
    book = load_book_snapshot(symbol_id, timestamp_us)

    remaining = target_qty
    total_cost = 0.0
    levels_consumed = 0

    for level in range(depth_levels):
        if side == "buy":
            price = book[f"ask_price_{level}"]
            available = book[f"ask_qty_{level}"]
        else:
            price = book[f"bid_price_{level}"]
            available = book[f"bid_qty_{level}"]

        take = min(remaining, available)
        total_cost += take * price
        remaining -= take
        levels_consumed += 1

        if remaining <= 0:
            break

    avg_price = total_cost / target_qty if target_qty > 0 else 0
    mid_price = (book["bid_price_0"] + book["ask_price_0"]) / 2

    if side == "buy":
        slippage_bps = (avg_price - mid_price) / mid_price * 10000
    else:
        slippage_bps = (mid_price - avg_price) / mid_price * 10000

    return {
        "slippage_bps": slippage_bps,
        "avg_fill_price": avg_price,
        "levels_consumed": levels_consumed,
        "fill_complete": remaining <= 0,
        "remaining_qty": remaining,
    }
```

---

## 7. Experiment Roadmap

### Phase 1: Univariate Feature Analysis (Weeks 1-2)

**Objective:** Establish baseline predictive power for each feature.

| Feature | Hypothesis | Test |
|---------|------------|------|
| `call_bid_pressure_ratio` | > 0.6 predicts positive 5-min return | Rank IC, quintile returns |
| `call_cancel_ratio` | High cancels predict volatility | Realized vol correlation |
| `open_gap_bps` | Large gaps mean-revert | Next 30-min return vs gap size |
| `close_imbalance_slope` | Positive slope predicts overnight gap | Next-day open return |
| **Close-to-Open Features** |||
| `overnight_gap_bps` | Gaps > 50bps mean-revert | First 30-min return vs gap size |
| `imbalance_carryover` | Carryover > 1.5x predicts continuation | Return 09:25-09:35 vs carryover |
| `gap_confirmation` | Positive confirmation predicts momentum | Return 09:30-10:00 vs confirmation score |
| `inventory_pressure_proxy` | High pressure predicts selling | Return 09:30-09:35 vs pressure |
| `volume_ratio_close_to_open` | Ratio > 2 predicts volatility expansion | Realized vol ratio vs volume ratio |

**Deliverables:**
- Feature distribution analysis
- Rank IC time series
- Regime-conditional performance (high/low vol)

### Phase 2: Multi-Factor Integration (Weeks 3-4)

**Objective:** Build composite signals and test orthogonality.

**Tasks:**
1. Combine auction features (pressure + cancellation)
2. Combine continuous features (flow + toxicity)
3. Cross-sectional ranking within sectors
4. Orthogonalize against standard factors (size, value, momentum)

**Deliverables:**
- Composite signal construction
- Variance inflation factor (VIF) analysis
- Cross-sectional rank IC

### Phase 3: Cost-Aware Backtest (Weeks 5-6)

**Objective:** Assess real-world profitability.

**Assumptions:**
- Commission: 0.025%
- Stamp duty: 0.1% (sell only)
- Slippage: L3-estimated or 5 bps baseline
- Delay: 1-second execution lag

**Metrics:**
- Gross Sharpe ratio
- Net Sharpe ratio (after costs)
- Break-even capacity (AUM where alpha decays to costs)
- Max drawdown and recovery

### Phase 4: Regime Analysis (Weeks 7-8)

**Objective:** Identify when features work/fail.

**Regime Definitions:**
- Volatility: High/low VIX equivalent
- Trend: Up/down/flat market
- Auction intensity: High/low auction volume contribution
- Sector rotation: Cyclical vs defensive leadership
- **Close-to-Open Specific:**
  - **Gap Regime:** Large gap (> 50bps) vs small gap (< 20bps) days
  - **Imbalance Persistence Regime:** High persistence (> 0.4) vs low persistence (< 0.1) periods
  - **T+1 Pressure Regime:** High inventory pressure (> 0.5) vs low pressure days
  - **Overnight News Regime:** Volume ratio > 2.0 (news event) vs normal nights

**Close-to-Open Regime Patterns:**
| Regime | Condition | Expected Feature Performance |
|--------|-----------|------------------------------|
| **Gap Reversal** | Large gap + negative confirmation | Fade gap direction |
| **Gap Continuation** | Large gap + positive confirmation | Follow gap direction |
| **T+1 Flush** | High close imbalance + high inventory pressure | Sell-off at T+1 open |
| **Overnight Momentum** | Consistent imbalance (Buy→Buy→Buy) | Trend day likely |
| **News Gap** | High volume ratio + gap > 50bps | Momentum, don't fade |

**Deliverables:**
- Regime-conditional Sharpe ratios
- Feature switching rules
- Dynamic allocation framework

---

## 8. References

### 8.1 Related Documentation

- `docs/guides/feature-pipeline-modes.md` - PIT-safe pipeline templates
- `skills/pointline-research/references/schemas.md` - L3 table schemas
- `skills/pointline-research/references/exchange_quirks.md` - SZSE/SSE specifics
- `skills/pointline-research/references/analysis_patterns.md` - General patterns

### 8.2 Academic References

1. **Call Auction Theory:** Pagano & Schwartz (2003), "A Closing Call's Impact on Market Quality"
2. **Opening Price Discovery:** Cao, Ghysels & Hatheway (2000), "Price Discovery without Trading"
3. **Order Flow Toxicity:** Easley, López de Prado & O'Hara (2012), "Flow Toxicity and Volatility"
4. **Chinese Market Microstructure:** Chen & Rui (2003), "Are China'S Bull Markets Normal?"

### 8.3 Internal Tools

```python
# Pointline API imports
from pointline.research import query
from pointline.research import features
from pointline.dim_symbol import read_dim_symbol_table
```

---

## 9. Appendix: Quick Reference

### 9.1 Feature Summary Table

| Feature | Category | L3 Required | Prediction Horizon | Priority |
|---------|----------|-------------|-------------------|----------|
| `call_bid_pressure_ratio` | Auction Imbalance | No | 09:25-09:35 | High |
| `call_cancel_ratio` | Auction Pattern | Yes | 09:25-09:35 | High |
| `call_final_imbalance` | Auction Imbalance | No | 09:25-09:35 | High |
| `open_gap_bps` | Gap Analysis | No | 09:30-09:35 | Medium |
| `auction_volume_decay` | Transition | No | 09:30-09:35 | Medium |
| `close_imbalance_slope` | Auction Imbalance | No | Overnight | High |
| `submission_cancellation_ratio` | Flow Toxicity | Yes | 1-5 minutes | High |
| `large_trade_imbalance` | Flow Toxicity | Partial | 1-5 minutes | Medium |
| `fill_rate_proxy` | Queue Position | Yes | 1-5 minutes | Medium |
| `session_phase` | Temporal | No | Context | Low |
| **Close-to-Open Features** ||||
| `overnight_gap_bps` | Overnight Gap | No | 09:30-10:00 | High |
| `imbalance_persistence` | Persistence | No | 09:25-09:35 | High |
| `imbalance_carryover` | Persistence | No | 09:25-09:35 | Medium |
| `volume_ratio_close_to_open` | Volume Analysis | No | Intraday | Medium |
| `auction_momentum_raw` | Momentum | No | 09:30-10:00 | High |
| `auction_sequence_score` | Momentum | No | Full Day | Medium |
| `gap_confirmation` | Sentiment | No | 09:30-09:35 | High |
| `overnight_sentiment` | Sentiment | No | 09:30-10:00 | High |
| `inventory_pressure_proxy` | T+1 Constraint | No | 09:30-10:00 | Medium |
| `price_discovery_error` | Efficiency | No | 09:30-09:35 | Low |

### 9.2 UTC Time Conversion Reference

| CST Event | UTC Time | `ts_exch_us` offset (microseconds) |
|-----------|----------|-----------------------------------|
| 09:15:00 | 01:15:00 | 4,500,000,000 |
| 09:20:00 | 01:20:00 | 4,800,000,000 |
| 09:25:00 | 01:25:00 | 5,100,000,000 |
| 09:30:00 | 01:30:00 | 5,400,000,000 |
| 11:30:00 | 03:30:00 | 12,600,000,000 |
| 13:00:00 | 05:00:00 | 18,000,000,000 |
| 14:57:00 | 06:57:00 | 25,020,000,000 |
| 15:00:00 | 07:00:00 | 25,200,000,000 |

---

**Document Version:** 1.0
**Last Updated:** 2024
**Owner:** Quant Research Team
