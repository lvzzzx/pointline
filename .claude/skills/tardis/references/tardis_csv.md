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

---

## 4. Grouped Symbols

**Reference:** https://docs.tardis.dev/downloadable-csv-files#grouped-symbols

Grouped symbols aggregate data across all instruments of a given market type into a single file per day. Use a grouped symbol to download everything for that market category at once.

### Available Grouped Symbols

| Grouped Symbol | Description |
| :--- | :--- |
| `SPOT` | All spot market instruments |
| `FUTURES` | All futures contracts |
| `PERPETUALS` | All perpetual swap contracts |
| `OPTIONS` | All options instruments |

### Data Type Compatibility

| Data Type | Supported Grouped Symbols |
| :--- | :--- |
| `trades` | `SPOT`, `FUTURES`, `OPTIONS`, `PERPETUALS` |
| `incremental_book_L2` | `FUTURES` |
| `derivative_ticker` | `FUTURES`, `PERPETUALS` |
| `options_chain` | `OPTIONS` |
| `quotes` | `OPTIONS` |
| `liquidations` | `FUTURES`, `PERPETUALS`, `OPTIONS` |

### Download Examples

```
https://datasets.tardis.dev/v1/binance/trades/2020/09/01/SPOT.csv.gz
https://datasets.tardis.dev/v1/deribit/options_chain/2020/09/01/OPTIONS.csv.gz
https://datasets.tardis.dev/v1/deribit/liquidations/2021/09/01/PERPETUALS.csv.gz
```

### Notes

- The `symbol` field in the downloaded CSV still contains the individual instrument symbol per row, not the grouped symbol name.
- Symbol must be uppercase in the URL. Replace `/` and `:` characters with `-` for URL safety.
- Available grouped symbols for each exchange are listed in the `/exchanges/:exchange` API endpoint: `https://api.tardis.dev/v1/exchanges`.

---

## 5. Common Fields

All data types share these common fields:

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name (lowercase) |
| `symbol` | String | Instrument symbol (uppercase, exchange-native) |
| `timestamp` | Integer | Exchange timestamp in **microseconds since epoch** (UTC). If exchange does not provide, `local_timestamp` is used as fallback |
| `local_timestamp` | Integer | Message arrival timestamp in **microseconds since epoch** (UTC) |

---

## 6. Data Types

### 6.1 incremental_book_L2

**Reference:** https://docs.tardis.dev/downloadable-csv-files#incremental_book_l2

**Description:** Tick-level incremental order book L2 updates collected from exchanges' real-time WebSocket order book L2 data feeds. Data depth matches the underlying exchange feed.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `is_snapshot` | Boolean | `true` = initial order book snapshot; `false` = incremental update |
| `side` | String | `bid` or `ask` |
| `price` | Float | Price level being updated |
| `amount` | Float | Updated amount (**absolute, not delta**). `0` = level removal |

**Reconstruction Notes:**
- Group by `local_timestamp` to identify atomic multi-level updates
- When `is_snapshot` transitions from `false` to `true`, discard existing book state
- Amount is absolute, not delta

---

### 6.2 book_snapshot_25

**Reference:** https://docs.tardis.dev/downloadable-csv-files#book_snapshot_25

**Description:** Top 25 levels per side, recorded every time any tracked level changes.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `asks[0].price` ... `asks[24].price` | Float | Ask prices in **ascending** order (index 0 = best ask) |
| `asks[0].amount` ... `asks[24].amount` | Float | Ask amounts |
| `bids[0].price` ... `bids[24].price` | Float | Bid prices in **descending** order (index 0 = best bid) |
| `bids[0].amount` ... `bids[24].amount` | Float | Bid amounts |

Empty values if insufficient price levels available.

---

### 6.3 book_snapshot_5

**Reference:** https://docs.tardis.dev/downloadable-csv-files#book_snapshot_5

**Description:** Top 5 levels per side, recorded every time any tracked level changes.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `asks[0].price` ... `asks[4].price` | Float | Ask prices in **ascending** order (index 0 = best ask) |
| `asks[0].amount` ... `asks[4].amount` | Float | Ask amounts |
| `bids[0].price` ... `bids[4].price` | Float | Bid prices in **descending** order (index 0 = best bid) |
| `bids[0].amount` ... `bids[4].amount` | Float | Bid amounts |

---

### 6.4 trades

**Reference:** https://docs.tardis.dev/downloadable-csv-files#trades

**Description:** Individual trades from exchanges' real-time WebSocket trade feeds.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `id` | String | Trade ID (exchange-provided; may be numeric, GUID, or empty) |
| `side` | String | Taker side: `buy`, `sell`, or `unknown` |
| `price` | Float | Trade price |
| `amount` | Float | Trade amount (quantity) |

**Side Interpretation:**
- `buy`: Aggressive buyer (market order or limit hitting ask)
- `sell`: Aggressive seller (market order or limit hitting bid)
- `unknown`: Exchange did not provide trade direction

---

### 6.5 quotes

**Reference:** https://docs.tardis.dev/downloadable-csv-files#quotes

**Description:** Top-of-book (best bid/ask) quotes from exchange `bookTicker` or similar feeds.

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

### 6.6 derivative_ticker

**Reference:** https://docs.tardis.dev/downloadable-csv-files#derivative_ticker

**Description:** Derivative market data: mark price, index price, funding rate, open interest. Available for futures and perpetual contracts.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `mark_price` | Float | Mark price (margin calculations) |
| `index_price` | Float | Underlying index price |
| `last_price` | Float | Last traded price |
| `funding_rate` | Float | Current funding rate |
| `predicted_funding_rate` | Float | Predicted next funding rate (may be null) |
| `funding_timestamp` | Integer | Next funding event timestamp (µs) |
| `open_interest` | Float | Open interest (contracts or base asset units — varies by exchange) |

---

### 6.7 liquidations

**Reference:** https://docs.tardis.dev/downloadable-csv-files#liquidations

**Description:** Liquidation events from exchanges' real-time liquidation feeds.

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Instrument symbol |
| `timestamp` | Integer | Exchange timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `id` | String | Liquidation ID (may be empty) |
| `side` | String | `buy` (short liquidation) or `sell` (long liquidation) |
| `price` | Float | Liquidation price |
| `amount` | Float | Liquidated amount |

---

### 6.8 options_chain

**Reference:** https://docs.tardis.dev/downloadable-csv-files#options_chain

**Description:** Tick-level options summary: strike prices, expiration dates, open interest, implied volatility, greeks. Available for Deribit and OKEx. Use `OPTIONS` as the symbol (one file per day containing all options).

**Schema**

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | String | Exchange name |
| `symbol` | String | Option instrument symbol |
| `timestamp` | Integer | Ticker timestamp (µs) |
| `local_timestamp` | Integer | Arrival timestamp (µs) |
| `type` | String | `call` or `put` |
| `strike` | Float | Strike price |
| `expiration` | Integer | Expiration timestamp (µs since epoch) |
| `open_interest` | Float | Open interest (empty if not provided) |
| `last_price` | Float | Last trade price (empty if no trades) |
| `bid_price` | Float | Best bid price |
| `bid_amount` | Float | Best bid amount |
| `bid_iv` | Float | Implied volatility for best bid |
| `ask_price` | Float | Best ask price |
| `ask_amount` | Float | Best ask amount |
| `ask_iv` | Float | Implied volatility for best ask |
| `mark_price` | Float | Mark price |
| `mark_iv` | Float | Implied volatility for mark price |
| `underlying_index` | String | Underlying index symbol |
| `underlying_price` | Float | Underlying asset price |
| `delta` | Float | Delta |
| `gamma` | Float | Gamma |
| `vega` | Float | Vega |
| `theta` | Float | Theta |
| `rho` | Float | Rho |

Empty values are common for illiquid options or exchanges that don't provide greeks.

---

## 7. Exchange Coverage

| Exchange | Spot | Futures | Options |
| :--- | :--- | :--- | :--- |
| Binance | Yes | Yes (USD-M, COIN-M) | No |
| Binance US | Yes | No | No |
| BitMEX | No | Yes | No |
| Bitfinex | Yes | Yes | No |
| bitFlyer | Yes | Yes | No |
| Bitstamp | Yes | No | No |
| Bybit | Yes | Yes | No |
| Coinbase | Yes | No | No |
| Deribit | No | Yes | Yes |
| Gate.io | Yes | Yes | No |
| Huobi | Yes | Yes | No |
| Kraken | Yes | Yes | No |
| OKEx | Yes | Yes | Yes |
| ... | ... | ... | ... |

Full exchange list at https://api.tardis.dev/v1/exchanges.

---

## 8. Timestamp Semantics

| Field | Source | Reliability |
| :--- | :--- | :--- |
| `timestamp` | Exchange-provided | Preferred for event time |
| `local_timestamp` | Tardis collection server | Fallback; includes network latency |

All timestamps are **UTC microseconds since Unix epoch** (January 1, 1970).

### Conversion

```python
import datetime

ts_us = 1572566400000000
dt = datetime.datetime.fromtimestamp(ts_us / 1_000_000, tz=datetime.timezone.utc)
# 2019-11-01 00:00:00+00:00
```

---

## 9. Data Availability

| Data | Availability |
| :--- | :--- |
| Ongoing data | Next day around **06:00 UTC** |
| Historical samples | First day of each month (free, no API key) |
| Full historical | API key required |

### Latency

- `local_timestamp` includes network transit from exchange to Tardis collectors
- Typical: <100ms for major exchanges
- Cross-region routing may add 100-300ms

---

## 10. Order Book Reconstruction

### From incremental_book_L2

```python
import pandas as pd

df = pd.read_csv('incremental_book_L2.csv.gz')
order_book = {'bids': {}, 'asks': {}}

for _, row in df.iterrows():
    side = row['side'] + 's'  # 'bids' or 'asks'

    if row['is_snapshot']:
        order_book[side] = {}

    if row['amount'] == 0:
        order_book[side].pop(row['price'], None)
    else:
        order_book[side][row['price']] = row['amount']
```

### Atomic Updates

```python
# Group by local_timestamp to identify multi-level atomic updates
for ts, group in df.groupby('local_timestamp'):
    for _, row in group.iterrows():
        apply_update(order_book, row)
```

---

## 11. Data Quality Notes

| Issue | Details |
| :--- | :--- |
| **Trade IDs** | Format varies by exchange (numeric, UUID, or missing) |
| **Open Interest** | Units vary (contract count vs. base asset notional) |
| **Options Greeks** | Not all exchanges provide; may be empty |
| **Funding Rates** | `predicted_funding_rate` may be null |
| **Day Boundaries** | First message of day may be partial snapshot |

### Exchange-Specific Quirks

| Exchange | Quirk |
| :--- | :--- |
| Binance | `book_snapshot_25` limited to exchange-provided depth |
| BitMEX | Trade `id` may be empty for old data |
| Deribit | Most comprehensive options data |
| OKEx | Options Greeks require recent subscriptions |
