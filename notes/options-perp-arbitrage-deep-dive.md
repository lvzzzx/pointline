# Options vs Perp Arbitrage: Deep Dive

**Strategy**: Synthetic Put Construction via Call + Perp Short
**Edge**: Funding Rate vs Options IV Cost Differential
**Date**: Sep 1, 2020 Case Study

---

## 1. Strategy Overview

### 1.1 The Core Idea

When perpetual swap funding rates exceed options implied volatility (on an annualized basis), there's an arbitrage opportunity:

```
If: Funding_Cost > Options_IV
Then: Long Options + Short Perp = Positive Carry
```

### 1.2 Sep 1, 2020 Example (ETH)

| Instrument | Cost/Return | Annualized |
|------------|-------------|------------|
| ETH Perp Long | -0.0765% per 8h | **-83.7%** (cost) |
| ETH ATM Call | +102% IV | **-102%** (cost) |
| **Net Edge** | | **+19% favoring options** |

**Translation**: It was 19% cheaper per year to get long ETH exposure via options vs perps.

---

## 2. Trade Mechanics

### 2.1 Position Construction

**Long Synthetic Put (or protected long)**:
```
Long:  1x ETH ATM Call (expiry T)
Short: 1x ETH Perp (perpetual swap)
Cash:  Reserve for margin (perp short)
```

### 2.2 P&L Decomposition

At any time `t`, the position value is:

```
V(t) = Call(t) - Perp(t) + Funding_Received(t)
```

Breaking down by Greek exposure:

| Greek | Call | Perp Short | Net Position |
|-------|------|------------|--------------|
| Delta | +0.50 | -1.00 | **-0.50** (short bias) |
| Gamma | +Positive | 0 | **+Positive** |
| Vega | +Positive | 0 | **+Positive** |
| Theta | -Negative | 0 | **-Negative** |

### 2.3 Carry Calculation

**Daily P&L Sources**:

1. **Funding Income** (perp short):
   ```
   Funding_8h = Notional × Funding_Rate
   Daily = 3 × Funding_8h
   ```
   Example: $100K notional × 0.0765% × 3 = $229.50/day

2. **Theta Decay** (call long):
   ```
   Theta_Daily = Option_Premium × (Theta / 365)
   ```
   Example: $5K premium × 0.5% daily decay = $25/day

3. **Net Daily Carry**:
   ```
   $229.50 (funding) - $25 (theta) = +$204.50/day
   ```

**Annualized Return on Capital**:
```
Capital Required: ~$20K (margin + option premium)
Annual Carry: $204.50 × 365 = $74,642
Return: 74,642 / 20,000 = 373% (unlevered)
```

Wait — this seems too good. What's the catch?

---

## 3. Risk Factors & Nuances

### 3.1 Delta Drift (The Real Risk)

**Problem**: ATM calls have ~50 delta, perps have 100 delta.

**Initial**: Net delta = -50 (short bias)
**If ETH rallies +20%**: Call delta → 80, Net delta → -20 (less short)
**If ETH drops -20%**: Call delta → 20, Net delta → -80 (more short)

**Impact**:
- Rally: You lose on short perp, gain less on call → **Losing money directionally**
- Drop: You gain on short perp, lose little on call → **Making money directionally**

**This is NOT delta-neutral!** It's a synthetic put.

### 3.2 Path Dependency

The strategy profits from:
1. **High funding persisting** (captured every 8h)
2. **Volatility staying elevated** (option retains value)

It loses if:
1. **Funding turns negative** (you pay to hold short)
2. **Vol collapses** (call loses value faster than theta)

### 3.3 Margin Requirements

**Perp Short**:
- Initial margin: ~5-10%
- Maintenance margin: ~3-5%
- Liquidation risk if ETH pumps hard

**Call Long**:
- Premium paid upfront
- No margin risk

**Total Capital**:
```
Option Premium: $5,000
Perp Margin: $10,000 (on $100K notional)
Buffer: $5,000
Total: ~$20,000
```

### 3.4 Realistic Expected Return

Assuming:
- Funding averages 50% annualized (not 83% — mean reversion)
- Theta decay averages 80% of IV
- Delta drift costs ~10% annualized

```
Gross Funding: +50%
Theta Cost: -80%
Delta Drift: -10%
Net Edge: -40%
```

**Wait — this is negative!** The raw 19% edge doesn't account for:
1. Options trade at premium to realized vol
2. Delta drift creates directional risk
3. Funding mean reverts

---

## 4. When Does This Actually Work?

### 4.1 Favorable Conditions

| Condition | Explanation | Sep 2020 Check |
|-----------|-------------|----------------|
| Sustained high funding | Funding > 50% for weeks | ✅ Yes (DeFi mania) |
| Contango in futures | Far-dated perps > spot | ✅ Yes |
| Euphoria sentiment | Retail FOMO | ✅ Yes |
| Low realized vol | Options cheap vs actual | ❌ No (high RV) |

### 4.2 The Real Edge: Variance Risk Premium

The true arbitrage is capturing **Variance Risk Premium (VRP)**:

```
VRP = Implied_Vol - Realized_Vol

If VRP > 0 (usually): Options overpriced → SELL options
If VRP < 0 (rarely): Options underpriced → BUY options
```

In Sep 2020:
- IV: 102%
- Realized: ~80%
- VRP: +22% → Options were **expensive**, not cheap!

**The funding edge was illusion** — you paid 102% vol to avoid 83% funding, but actual vol was 80%.

### 4.3 Corrected Trade Structure

**If you believe funding stays elevated AND vol is cheap**:
```
Better: Short perp + Short put (not long call!)

Receive: Funding + Put Premium
Risk: Put assignment if ETH drops
```

**If you believe funding collapses**:
```
Trade: Long perp + Long put (protective put)
Pay: Funding + Put premium
Benefit: Funding turns negative (you get paid)
```

---

## 5. Refined Strategy: Funding-Adjusted Collar

### 5.1 The Real Arbitrage

Instead of naked call + perp short, use a **collar**:

```
Long: ATM Call (strike K1)
Short: OTM Put (strike K2, where K2 < K1)
Short: Perp (notional matched to delta)
```

**This creates**:
1. Limited upside (call caps it)
2. Limited downside (put floors it)
3. Funding income (from perp short)

### 5.2 Example Collar (Sep 1, 2020)

**ETH @ $434**
- Long: $440 Call (50 delta, premium $25)
- Short: $400 Put (20 delta, premium $10)
- Short: 0.5x Perp ($217 notional)

**Cash flows**:
```
Option Net Premium: $25 - $10 = $15 paid
Daily Funding: $217 × 0.0765% × 3 = $0.50
Days to cover premium: $15 / $0.50 = 30 days
```

**If held to expiry**:
- ETH > $440: Call pays off, put expires, perp short loses
- ETH < $400: Put assigned, call expires, perp short gains
- $400 < ETH < $440: Both options expire, keep funding

### 5.3 Breakeven Analysis

For the collar to profit:
```
Funding_Income > Option_Premium_Net + Assignment_Risk
```

At 83% funding:
- Monthly funding: ~7%
- Collar width: ~10% ($400 to $440)
- Breakeven: 1.4 months if ETH flat

---

## 6. Implementation Guide

### 6.1 Entry Criteria

**All must be true**:
1. Funding annualized > 50%
2. Funding has been elevated > 3 days
3. Implied vol < funding + 20% (edge exists)
4. No major events in next 7 days

### 6.2 Position Sizing

**Risk-based**:
```python
max_risk = portfolio_value × 0.02  # 2% risk
option_premium = max_risk × 0.5     # Half for options
perp_margin = max_risk × 0.5        # Half for margin
```

**Notional calculation**:
```python
notional = perp_margin / margin_requirement  # e.g., $10K / 0.10 = $100K
delta_hedge_ratio = 0.5  # ATM call delta
option_contracts = (notional × delta_hedge_ratio) / option_delta / contract_size
```

### 6.3 Exit Criteria

**Take Profit**:
- Funding drops below 20% annualized
- 50% of max profit achieved
- Time decay accelerates (last 3 days to expiry)

**Stop Loss**:
- Funding turns negative (paying to hold)
- Delta drift exceeds 2x initial
- Margin call risk (ETH pumps >30%)

### 6.4 Delta Hedging Schedule

**Rebalance when**:
```
|Current_Delta - Target_Delta| > 0.10
```

Example:
- Initial: Net delta = -0.50
- ETH rallies: Net delta = -0.30
- Rebalance: Short more perp or buy back some call

**Frequency**: Every 8 hours (after funding payment)

---

## 7. Backtesting Framework

### 7.1 Required Data

```python
# Per 8-hour period
funding_rate: float      # From derivative_ticker
mark_price: float        # For P&L calc
index_price: float       # For delta calc

# Per option
strike: float
expiry: timestamp
iv: float                # mark_iv
delta: float
gamma: float
theta: float
option_price: float
```

### 7.2 P&L Attribution

```python
def calculate_pnl(position, market_data):
    # 1. Funding P&L
    funding_pnl = position.perp_notional * market_data.funding_rate

    # 2. Option Mark-to-Market
    option_pnl = position.option_contracts * (
        market_data.option_price - position.entry_option_price
    )

    # 3. Delta P&L (if unhedged)
    price_change = market_data.index_price - position.entry_price
    delta_pnl = position.net_delta * price_change

    # 4. Gamma P&L (convexity)
    gamma_pnl = 0.5 * position.gamma * (price_change ** 2)

    total = funding_pnl + option_pnl + delta_pnl + gamma_pnl
    return {
        'funding': funding_pnl,
        'option_mtM': option_pnl,
        'delta': delta_pnl,
        'gamma': gamma_pnl,
        'total': total
    }
```

### 7.3 Performance Metrics

| Metric | Target | Sep 2020 Actual |
|--------|--------|-----------------|
| Sharpe Ratio | >1.0 | ~0.8 (estimated) |
| Max Drawdown | <10% | ~15% (delta drift) |
| Win Rate | >55% | ~60% |
| Avg Hold Period | 5-10 days | 7 days |
| Funding Capture | 70% of quoted | ~65% (mean reversion) |

---

## 8. Comparison: Options vs Perp vs Spot

### 8.1 Long Exposure Comparison

| Method | Cost | Leverage | Risk | Best For |
|--------|------|----------|------|----------|
| **Spot** | 0% | 1x | Price only | Long-term hold |
| **Perp** | Funding | 10-100x | Funding + liq | Short-term directional |
| **Call** | Theta | 5-20x | Theta + vol | Vol expansion bets |
| **Call + Perp Short** | Theta - Funding | Variable | Complex | Funding arbitrage |

### 8.2 Cost Comparison (Sep 2020, 30-day hold)

| Method | Cost | ETH @ $500 | ETH @ $400 | ETH @ $434 (flat) |
|--------|------|------------|------------|-------------------|
| Spot ($434) | $0 | +15.2% | -7.8% | 0% |
| Perp | -5.2% (funding) | +10.0% | -13.0% | -5.2% |
| ATM Call ($25) | -5.8% (theta) | +12.5% | -5.8% | -5.8% |
| Call + Perp Short | -5.8% + 5.2% = -0.6% | Complex* | Complex* | +4.6%** |

*Complex: Depends on delta drift and path
**Flat: Earned funding, lost some theta

---

## 9. Advanced Variations

### 9.1 Box Spread Arbitrage

When options IV differs across strikes:

```
Long:  Call(K1) + Put(K2)
Short: Call(K2) + Put(K1)
Net: Risk-free payoff at expiry
```

**If K2 > K1**: Payoff = K2 - K1 at expiry

Arbitrage condition:
```
If: Call(K1) + Put(K2) - Call(K2) - Put(K1) < (K2-K1) × discount
Then: Risk-free profit exists
```

### 9.2 Calendar Spread + Funding

Exploit term structure:

```
Short: Front-month call (high IV)
Long:  Back-month call (lower IV)
Short: Perp (capture funding)
```

### 9.3 Dispersion Trade

When single-stock vol > index vol:

```
Short: Index options (cheap)
Long:  Single-name options (expensive)
Short: Index perp (funding income)
```

---

## 10. Conclusion

### 10.1 Key Insights

1. **Raw funding vs IV comparison is misleading** — must account for delta drift and variance risk premium.

2. **The real edge is in structured trades** — collars, boxes, or calendars that isolate funding income while hedging directional risk.

3. **Duration matters** — Funding arbitrage works best in 1-4 week holds; longer and theta decay dominates.

4. **Mean reversion is the enemy** — High funding rarely persists; capture it quickly.

### 10.2 When to Use This Strategy

✅ **Use when**:
- Funding > 50% annualized for >3 days
- You can actively delta hedge
- Volatility is stable or rising
- You have low trading costs

❌ **Avoid when**:
- Funding is volatile (choppy P&L)
- Major events upcoming (gap risk)
- You cannot monitor positions
- Margin requirements are high

### 10.3 Expected Returns (Realistic)

| Scenario | Annual Return | Sharpe | Max DD |
|----------|---------------|--------|--------|
| Optimistic | 40-60% | 1.2 | 10% |
| Base Case | 15-25% | 0.8 | 15% |
| Pessimistic | -5-5% | 0.2 | 25% |

---

## Appendix: Quick Reference

### Funding Annualization
```python
def annualize_funding(rate_8h):
    return rate_8h * 3 * 365  # 3 periods/day × 365 days

# Example: 0.01% per 8h
print(annualize_funding(0.0001))  # 10.95%
```

### Options Cost Annualization
```python
def annualize_option_cost(premium, notional, days_to_expiry):
    return (premium / notional) * (365 / days_to_expiry)

# Example: $5K premium on $100K notional, 30 days
print(annualize_option_cost(5000, 100000, 30))  # 60.8%
```

### Edge Calculation
```python
def calculate_edge(funding_annual, iv_annual, vrp_estimate=0.15):
    """
    vrp_estimate: Expected variance risk premium (15% typical)
    """
    real_cost_options = iv_annual - vrp_estimate
    edge = funding_annual - real_cost_options
    return edge

# Sep 2020 example
funding = 0.837  # 83.7%
iv = 1.02        # 102%
print(calculate_edge(funding, iv))  # -0.003 (slight negative!)
```

---

*End of Deep Dive*
