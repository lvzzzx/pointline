# Profile: MFT & Alpha Research

**Focus:** Mid-frequency strategies, statistical arbitrage, factor modeling, and portfolio rebalancing.
**Triggers:** "rebalance", "funding rate", "correlation", "RSI", "moving average", "4h candles", "portfolio".

## Priority Data Sources
- **`kline_1h` / `kline_1m`**: Core for signal generation and backtesting.
- **`derivative_ticker`**: Funding rates, Open Interest (OI), and Index Prices.
- **`dim_asset_stats`**: 24h volume, volatility metrics for universe selection.

## Analysis Patterns
- **Factor Construction:** Cross-sectional ranking of assets (e.g., "Long top 10% by Momentum").
- **Funding Arb:** capturing yield differentials between spot and perps.
- **Basket Correlation:** Lead-lag relationships between sectors or tokens.

## Critical Checks
- [ ] **Survivorship Bias:** Are you including delisted assets in historical baskets?
- [ ] **Lookahead Bias (Resampling):** Ensure aggregations (e.g., 5m -> 1h) do not leak future close prices.
- [ ] **Funding Timing:** Remember `derivative_ticker` funding is often *predicted* for the *next* interval.
