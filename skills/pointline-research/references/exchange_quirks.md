# Exchange Quirks & Market Microstructure Notes

This document contains "tribal knowledge" about specific exchanges and asset classes. Use this context to interpret data correctly and avoid common pitfalls.

## Binance Futures (USDT-M)

- **Funding Rates:** Paid every 8 hours (00:00, 08:00, 16:00 UTC).
  - *Data Nuance:* The `derivative_ticker` table captures the *predicted* funding rate for the next interval, not the historical payment.
- **Liquidation Orders:** Often tagged with specific `TimeInForce` types (e.g., `IOC`) or can be inferred from `force_order` streams (if available). In standard trade streams, they appear as aggressive market orders.
- **Aggregated Trades:** Binance sends `aggTrades` (aggregated) and `trade` (individual). Pointline prioritizes `trade` for full granularity, but be aware that multiple trades with the exact same `ts_exch_us` and `price` might be part of a single matching engine event.

## SZSE (Shenzhen Stock Exchange)

- **Timestamps:**
  - Raw data is in Beijing Time (UTC+8).
  - Pointline stores all timestamps in **UTC**.
  - *Pitfall:* A trade at "09:30:00" Beijing time will appear as "01:30:00" in `ts_exch_us`.
- **Auction Sessions:**
  - **Call Auction:** 09:15 - 09:25. Orders can be cancelled 09:15-09:20, but *not* 09:20-09:25.
  - **Continuous Auction:** 09:30 - 11:30, 13:00 - 14:57.
  - **Closing Call:** 14:57 - 15:00.
  - *Data Nuance:* `szse_l3_orders` during auction periods may have different `type` codes or behaviors.
- **Tick Size:** Varies by asset type (A-shares vs B-shares vs Funds). Do not assume a constant tick size across the exchange.
- **Order IDs:** Are *not* guaranteed to be globally unique or monotonic across days. Only unique within a channel/symbol for a single trading day.

## SSE (Shanghai Stock Exchange)

- **Order Book Logic:** Unlike SZSE, SSE L3 data often comes as "snapshots" or specific order updates that differ slightly in format. Pointline normalizes this, but gaps in `seq_id` can occur if the upstream feed drops packets (UDP multicast).
- **Turnover:** Reported turnover is often cumulative. Pointline computes incremental turnover in `trades` table, but verify against `stock_basic` totals if strict precision is needed.

## Crypto Derivatives (General)

- **Contract Expiry:**
  - `BTC-29MAR24` means expiring on March 29, 2024.
  - *Pitfall:* Approaching expiry, liquidity dries up and spreads widen. Exclude expiring contracts from general "market health" analyses unless specifically studying expiration effects.
- **Inverse vs. Linear:**
  - **Linear (USDT-M):** PnL in USDT. Quantity in BTC.
  - **Inverse (COIN-M):** PnL in Coin (BTC). Quantity in Contracts (e.g., 1 contract = $100).
  - *Analysis Nuance:* When calculating "Notional Volume" for Inverse contracts, you MUST multiply `price * qty * contract_size`. For Linear, it's just `price * qty`.

## Date Handling

- **Business Days:** For Stock markets (SZSE/SSE), weekends and holidays (CN) mean *no data*.
  - *Self-Correction:* If `data_coverage` returns False for a weekday, check if it was a Chinese public holiday (e.g., Golden Week, Lunar New Year).
- **24/7 Markets:** Crypto never closes. "Daily" candles usually close at 00:00 UTC, but some legacy systems might use 00:00 UTC+8 (Hong Kong) or UTC-5 (New York). Pointline enforces **UTC** for all `kline` aggregation.
