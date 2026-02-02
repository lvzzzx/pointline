# Profile: Execution & TCA

**Focus:** Transaction Cost Analysis (TCA), slippage measurement, market impact, and fill probability.
**Triggers:** "slippage", "impact", "VWAP", "fill rate", "liquidity", "cost", "benchmark".

## Priority Data Sources
- **`trades`**: Global market executions (the benchmark).
- **`quotes`**: Best Bid/Ask (BBO) at the time of order submission.
- **`my_orders` / `my_trades`**: (If available) Private fill data.

## Analysis Patterns
- **Slippage vs. Benchmark:** (Execution Price - Mid Price @ Arrival).
- **Market Impact:** Price drift 1s, 5s, 60s after the trade.
- **Liquidity Capture:** Percentage of passive fills vs. aggressive takes.

## Critical Checks
- [ ] **Timestamp Alignment:** Precise alignment of "Decision Time" vs "Market State".
- [ ] **Spread Capture:** Did we capture the spread, or did we pay it?
- [ ] **Market Conditions:** Was volatility (RV) high during the execution window?
