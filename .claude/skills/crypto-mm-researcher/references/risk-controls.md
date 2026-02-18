# Risk Controls for Crypto MM

## Non-Negotiable Checklist (v1)

Before live deployment (or serious paper trading), all items below must be explicitly implemented and monitored:

1. **Hard inventory limits** (soft/hard/emergency bands with automatic de-risk actions)
2. **Adverse-selection guard** (post-fill markout thresholds trigger widening/pause)
3. **Volatility/spread regime switch** (auto-widen or pause in stressed regimes)
4. **Liquidity/depth floor** (reduce size or disable quoting when depth collapses)
5. **Data freshness + feed integrity checks** (staleness/sequence-gap fail-safe)
6. **Execution health checks** (rejects, cancel latency, throttle/API incidents)
7. **Cost floor enforcement** (quote only when expected edge exceeds all-in cost buffer)
8. **PnL drawdown circuit breaker** (intraday and rolling loss limits with escalation)
9. **Event-risk mode** (funding windows, macro events, liquidation spike behavior)
10. **Kill-switch + recovery protocol** (immediate stop/cancel/flatten and controlled restart)

If any one of these is missing, the MM stack should be treated as non-production-safe.

## 1) Risk Layers

1. **Pre-trade controls**: spread guards, volatility guards, notional caps
2. **In-trade controls**: inventory soft/hard limits, side throttles
3. **System controls**: data freshness checks, venue health checks
4. **Emergency controls**: kill switch and flatten protocol

## 2) Inventory Controls

- Soft limit: increase skew and widen risk side.
- Hard limit: disable one side; allow only reducing trades.
- Emergency limit: force taker hedge/flatten (policy-defined).

Suggested logic:
- `|inv| < soft`: normal two-sided
- `soft <= |inv| < hard`: asymmetric quoting
- `|inv| >= hard`: single-sided reduce-only + optional taker hedge

## 3) Market State Controls

Trigger widening or pause when:
- short-horizon realized vol exceeds threshold
- spread dislocates beyond normal percentile
- liquidation intensity spikes
- order book depth collapses

## 4) Data/Infra Controls

- Max staleness threshold for order book/trade streams
- Sequence gap detection in incremental feeds
- Throttle/API error monitoring
- Auto-disable quoting on stale or inconsistent data

## 5) Cost Controls

- Minimum expected edge check before quoting
- Disable quoting when expected spread capture < estimated all-in cost
- Separate maker and taker cost budgets

## 6) Kill-Switch Conditions

Any of the below can trigger hard stop:
- rapid drawdown beyond intraday threshold
- repeated inventory hard-limit breaches in short window
- persistent data staleness / sequence gaps
- venue incident (maintenance/outage/abnormal rejects)

## 7) Recovery Protocol

- Freeze new quote placement
- Cancel live passive orders
- Reduce inventory toward neutral via controlled execution
- Re-enable only after health checks pass and cooldown expires

## 8) Stress Test Scenarios

- 2x fees / 2x slippage
- 50% depth reduction
- sudden spread widening regime
- liquidation cascade directional move
- exchange API degradation window

A policy is not deployment-ready unless it remains controlled under these stresses.