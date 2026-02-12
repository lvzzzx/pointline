# Data Source: Tardis.dev CSV Datasets

This document describes the **downloadable CSV datasets** provided by Tardis.dev, covering cryptocurrency market data from 26+ exchanges.

> **Official Documentation:** https://docs.tardis.dev/downloadable-csv-files
>
> This document is based on the official Tardis.dev documentation. For the most up-to-date information, please refer to the official source.

---

## Document Reference

| Resource | URL |
| :--- | :--- |
| **CSV Format Details** | https://docs.tardis.dev/downloadable-csv-files#csv-format-details |
| **Data Types** | https://docs.tardis.dev/downloadable-csv-files#data-types |
| **Datasets API** | https://docs.tardis.dev/api/datasets |
| **Main Website** | https://tardis.dev |

---

## 1. Source Overview

| Attribute | Value |
| :--- | :--- |
| **Vendor** | Tardis.dev (https://tardis.dev) |
| **Access Method** | Datasets API (HTTP download) or Python/Node.js SDK |
| **Markets** | Cryptocurrency spot, futures, options (26+ exchanges) |
| **Data Types** | L2 order book, trades, quotes, options, liquidations, funding |
| **Format** | Gzip-compressed CSV (`.csv.gz`) |
| **Delimiter** | Comma (`,`) |
| **Timestamp Format** | Microseconds since epoch (UTC) |
| **Update Schedule** | Daily around 06:00 UTC (next-day availability) |

---

## 2. Access Methods

### 2.1 Python SDK

```python
# pip install tardis-dev
from tardis_dev import datasets

datasets.download(
    exchange="deribit",
    data_types=[
        "incremental_book_L2",
        "trades",
        "quotes",
        "derivative_ticker",
        "book_snapshot_25",
        "liquidations"
    ],
    from_date="2019-11-01",
    to_date="2019-11-02",
    symbols=["BTC-PERPETUAL", "ETH-PERPETUAL"],
    api_key="YOUR_API_KEY"  # Optional for monthly samples
)
```

### 2.2 HTTP Direct Download

```bash
# URL pattern
https://datasets.tardis.dev/v1/<exchange>/<data_type>/<YYYY>/<MM>/<DD>/<symbol>.csv.gz

# Example
curl -o deribit_trades_2019-11-01_BTC-PERPETUAL.csv.gz \
  https://datasets.tardis.dev/v1/deribit/trades/2019/11/01/BTC-PERPETUAL.csv.gz
```

### 2.3 Free Sample Data

Historical datasets for the **first day of each month** are available without API key:
- Access: `https://datasets.tardis.dev/v1/...` (no authentication required)
- Full historical data requires API key

---

## 3. File Naming Convention

```
<exchange>_<data_type>_<YYYY-MM-DD>_<symbol>.csv.gz
```

| Component | Example | Description |
| :--- | :--- | :--- |
| `<exchange>` | `deribit`, `binance-futures` | Exchange name |
| `<data_type>` | `trades`, `incremental_book_L2` | Data content type |
| `<YYYY-MM-DD>` | `2019-11-01` | Trade date (UTC) |
| `<symbol>` | `BTC-PERPETUAL` | Instrument symbol (uppercase, exchange-native) |

**Example:** `deribit_trades_2019-11-01_BTC-PERPETUAL.csv.gz`

---

## 4. Common Fields

All data types share these common fields:

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name (lowercase) |
| `symbol` | String | Instrument symbol (uppercase, exchange-native) |
| `timestamp` | Integer | Exchange timestamp in **microseconds since epoch** (UTC). If exchange does not provide, `local_timestamp` is used as fallback |
| `local_timestamp` | Integer | Message arrival timestamp in **microseconds since epoch** (UTC) |

---

## 5. Data Types

### 5.1 incremental_book_L2

**Reference:** https://docs.tardis.dev/downloadable-csv-files#incremental_book_l2

**Description:** Tick-level incremental order book L2 updates collected from exchanges' real-time WebSocket order book L2 data feeds. Data depth matches the underlying exchange feed.

**Use Case:** Full order book reconstruction.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `is_snapshot` | Boolean | `true` = part of initial order book snapshot; `false` = incremental update |
| `side` | String | `bid` (buy orders) or `ask` (sell orders) |
| `price` | Float | Price level being updated |
| `amount` | Float | Updated price level amount (**absolute value, not delta**). Amount of `0` indicates price level removal |

**Order Book Reconstruction Notes:**
- Multiple levels may be updated in a single message (group by `local_timestamp` to identify)
- When `is_snapshot` transitions from `false` to `true`, discard existing order book state
- Amount is absolute, not delta

---

### 5.2 book_snapshot_25

**Reference:** https://docs.tardis.dev/downloadable-csv-files#book_snapshot_25

**Description:** Tick-level order book snapshots reconstructed from exchanges' real-time WebSocket order book L2 data feeds. Each row represents **top 25 levels** from each side of the limit order book recorded every time any of the tracked bids/asks top 25 levels have changed.

**Use Case:** Research requiring order book state without reconstruction.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `asks[0].price` ... `asks[24].price` | Float | Ask prices in **ascending order** (index 0 = best ask) |
| `asks[0].amount` ... `asks[24].amount` | Float | Ask amounts corresponding to prices |
| `bids[0].price` ... `bids[24].price` | Float | Bid prices in **descending order** (index 0 = best bid) |
| `bids[0].amount` ... `bids[24].amount` | Float | Bid amounts corresponding to prices |

**Notes:**
- Empty values if insufficient price levels available
- Levels beyond exchange-provided depth are empty

---

### 5.3 book_snapshot_5

**Reference:** https://docs.tardis.dev/downloadable-csv-files#book_snapshot_5

**Description:** Tick-level order book snapshots reconstructed from exchanges' real-time WebSocket order book L2 data feeds. Each row represents **top 5 levels** from each side of the limit order book recorded every time any of the tracked bids/asks top 5 levels have changed.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `asks[0].price` ... `asks[4].price` | Float | Ask prices in **ascending order** (index 0 = best ask) |
| `asks[0].amount` ... `asks[4].amount` | Float | Ask amounts |
| `bids[0].price` ... `bids[4].price` | Float | Bid prices in **descending order** (index 0 = best bid) |
| `bids[0].amount` ... `bids[4].amount` | Float | Bid amounts |

---

### 5.4 trades

**Reference:** https://docs.tardis.dev/downloadable-csv-files#trades

**Description:** Individual trades data collected from exchanges' real-time WebSocket trades data feeds.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `id` | String | Trade ID (exchange-provided; may be numeric, GUID, or empty) |
| `side` | String | Liquidity taker side: `buy` (taker bought), `sell` (taker sold), `unknown` (not provided) |
| `price` | Float | Trade price |
| `amount` | Float | Trade amount (quantity) |

**Side Interpretation:**
- `buy`: Aggressive buyer (market order or limit hitting ask)
- `sell`: Aggressive seller (market order or limit hitting bid)
- `unknown`: Exchange did not provide trade direction

---

### 5.5 quotes

**Reference:** https://docs.tardis.dev/downloadable-csv-files#quotes

**Description:** Top-of-book (best bid/ask) quotes. Typically sourced from exchange `bookTicker` or similar top-of-book feeds.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `bid_price` | Float | Best bid price |
| `bid_amount` | Float | Best bid amount |
| `ask_price` | Float | Best ask price |
| `ask_amount` | Float | Best ask amount |

---

### 5.6 derivative_ticker

**Reference:** https://docs.tardis.dev/downloadable-csv-files#derivative_ticker

**Description:** Derivative market data including mark price, index price, funding rate, open interest, and predicted funding. Available for futures and perpetual contracts.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `mark_price` | Float | Mark price (used for margin calculations) |
| `index_price` | Float | Underlying index price |
| `last_price` | Float | Last traded price |
| `funding_rate` | Float | Current funding rate |
| `predicted_funding_rate` | Float | Predicted next funding rate (if provided by exchange) |
| `funding_timestamp` | Integer | Next funding event timestamp (µs) |
| `open_interest` | Float | Current open interest (contracts or base asset units) |

**Notes:**
- Field availability varies by exchange
- `open_interest` units vary by exchange (contract-based or notional)
- `predicted_funding_rate` may be null if exchange does not provide

---

### 5.7 liquidations

**Reference:** https://docs.tardis.dev/downloadable-csv-files#liquidations

**Description:** Liquidation events from exchanges' real-time liquidation feeds.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `id` | String | Liquidation ID (exchange-provided, may be empty) |
| `side` | String | `buy` (short liquidation) or `sell` (long liquidation) |
| `price` | Float | Liquidation price |
| `amount` | Float | Liquidated amount |

**Side Interpretation:**
- `buy`: Short position liquidated (forced buy to cover)
- `sell`: Long position liquidated (forced sell)

---

### 5.8 options_chain

**Reference:** https://docs.tardis.dev/downloadable-csv-files#options_chain

**Description:** Tick-level options summary info (strike prices, expiration dates, open interest, implied volatility, greeks, etc.) for all active options instruments collected from exchanges' real-time WebSocket options tickers data feeds. Available for Deribit (sourced from ticker channel) and OKEx Options (sourced from option/summary and index/ticker channels).

**Symbol Convention:** For `options_chain` data type, use `OPTIONS` as symbol (one file per day containing all options).

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Option instrument symbol |
| `timestamp` | Integer | Ticker timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `type` | String | Option type: `call` or `put` |
| `strike` | Float | Strike price |
| `expiration` | Integer | Option expiration timestamp (µs since epoch) |
| `open_interest` | Float | Current open interest (empty if not provided) |
| `last_price` | Float | Last trade price (empty if no trades) |
| `bid_price` | Float | Best bid price (empty if no bids) |
| `bid_amount` | Float | Best bid amount (empty if no bids) |
| `bid_iv` | Float | Implied volatility for best bid (empty if no bids) |
| `ask_price` | Float | Best ask price (empty if no asks) |
| `ask_amount` | Float | Best ask amount (empty if no asks) |
| `ask_iv` | Float | Implied volatility for best ask (empty if no asks) |
| `mark_price` | Float | Mark price (empty if not provided) |
| `mark_iv` | Float | Implied volatility for mark price (empty if not provided) |
| `underlying_index` | String | Underlying index symbol |
| `underlying_price` | Float | Underlying asset price |
| `delta` | Float | Delta greek |
| `gamma` | Float | Gamma greek |
| `vega` | Float | Vega greek |
| `theta` | Float | Theta greek |
| `rho` | Float | Rho greek |

**Notes:**
- Greeks and IV fields availability varies by exchange
- Empty values common for illiquid options

---

## 6. Exchange Coverage

### Supported Exchanges (26+)

| Exchange | Spot | Futures | Options |
| :--- | :--- | :--- | :--- |
| Binance | ✅ | ✅ (USD-M, COIN-M) | ❌ |
| Binance US | ✅ | ❌ | ❌ |
| BitMEX | ❌ | ✅ | ❌ |
| Bitfinex | ✅ | ✅ | ❌ |
| bitFlyer | ✅ | ✅ | ❌ |
| Bitstamp | ✅ | ❌ | ❌ |
| Bittrex | ✅ | ❌ | ❌ |
| Bybit | ✅ | ✅ | ❌ |
| Cex.io | ✅ | ❌ | ❌ |
| Coinbase | ✅ | ❌ | ❌ |
| Coinbase Pro | ✅ | ❌ | ❌ |
| Crypto.com | ✅ | ❌ | ❌ |
| Deribit | ❌ | ✅ | ✅ |
| FTX (historical) | ✅ | ✅ | ❌ |
| Gate.io | ✅ | ✅ | ❌ |
| Gemini | ✅ | ❌ | ❌ |
| HitBTC | ✅ | ❌ | ❌ |
| Huobi | ✅ | ✅ | ❌ |
| Kraken | ✅ | ✅ | ❌ |
| KuCoin | ✅ | ❌ | ❌ |
| OKEx | ✅ | ✅ | ✅ |
| Poloniex | ✅ | ❌ | ❌ |
| ... | ... | ... | ... |

**Note:** See Tardis.dev FAQ for maximum order book depth per exchange.

---

## 7. Timestamp Semantics

### Field Definitions

| Field | Source | Reliability |
| :--- | :--- | :--- |
| `timestamp` | Exchange-provided | Preferred for event time |
| `local_timestamp` | Tardis collection server | Fallback; includes network latency |

### Timezone

All timestamps are **UTC microseconds since Unix epoch** (January 1, 1970).

### Conversion Examples

```python
import datetime

# Microseconds to datetime
ts_us = 1572566400000000  # Example
dt = datetime.datetime.fromtimestamp(ts_us / 1_000_000, tz=datetime.timezone.utc)
# Result: 2019-11-01 00:00:00 UTC
```

---

## 8. Data Availability and Scheduling

### Update Schedule

| Data Type | Availability |
| :--- | :--- |
| Ongoing data | Next day around **06:00 UTC** |
| Historical samples | First day of each month (free, no API key) |
| Full historical | API key required |

### Latency Considerations

- `local_timestamp` includes network transit time from exchange to Tardis collectors
- Typical latency: < 100ms for major exchanges
- Cross-Atlantic/Asian routing may add 100-300ms

---

## 9. Order Book Reconstruction Guide

### From incremental_book_L2

```python
import pandas as pd

# Load data
df = pd.read_csv('incremental_book_L2.csv.gz')

# Initialize order book
order_book = {'bids': {}, 'asks': {}}  # price -> amount

# Process updates
for _, row in df.iterrows():
    side = row['side']  # 'bid' or 'ask'
    price = row['price']
    amount = row['amount']

    if row['is_snapshot']:
        # Reset book on snapshot
        order_book[side + 's'] = {}

    if amount == 0:
        # Remove level
        order_book[side + 's'].pop(price, None)
    else:
        # Update level (absolute amount)
        order_book[side + 's'][price] = amount
```

### Grouping Multi-Level Updates

Exchange messages often contain multiple level updates:

```python
# Group by local_timestamp to identify atomic updates
grouped = df.groupby('local_timestamp')

for ts, group in grouped:
    # All rows in 'group' arrived in same message
    apply_updates_atomic(order_book, group)
```

---

## 10. Data Quality Notes

### Known Limitations

1. **Trade IDs:** Format varies by exchange (numeric, UUID, or missing)
2. **Open Interest:** Units vary (contract count vs. base asset notional)
3. **Options Greeks:** Not all exchanges provide; may be calculated or missing
4. **Funding Rates:** Prediction availability varies by exchange
5. **Snapshot Boundaries:** First message of day may be partial snapshot

### Exchange-Specific Quirks

| Exchange | Quirk |
| :--- | :--- |
| Binance | `book_snapshot_25` limited to exchange-provided depth |
| BitMEX | Trade `id` may be empty for old data |
| Deribit | Options data most comprehensive |
| OKEx | Options Greeks require recent subscriptions |

---

## Appendix: Sample Data URLs

### Free Samples (First of Month)

```
https://datasets.tardis.dev/v1/deribit/trades/2020/03/01/BTC-PERPETUAL.csv.gz
https://datasets.tardis.dev/v1/bitmex/trades/2020/03/01/XBTUSD.csv.gz
https://datasets.tardis.dev/v1/binance-futures/trades/2020/03/01/BTCUSDT.csv.gz
```

### Authenticated Access

Full historical data requires API key appended as query parameter:

```
https://datasets.tardis.dev/v1/deribit/trades/2023/01/15/BTC-PERPETUAL.csv.gz?apiKey=YOUR_KEY
```
