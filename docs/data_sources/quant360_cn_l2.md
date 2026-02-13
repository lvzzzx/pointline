# Data Source: Quant360 Chinese Stock Level 2/3 Data

This document describes the raw data format for **Shenzhen Stock Exchange (SZSE)** and **Shanghai Stock Exchange (SSE)** Level 2/3 data provided by **Quant360**.

---

## 1. Source Overview

| Attribute | Value |
| :--- | :--- |
| **Vendor** | Data.Quant360.com |
| **Exchanges** | Shenzhen Stock Exchange (SZSE), Shanghai Stock Exchange (SSE) |
| **Data Types** | Order Stream, Tick/Trade Stream, L2 Snapshots |
| **Format** | 7-Zip Archives (`.7z`) containing individual CSV files per symbol |
| **Compression** | LZMA2, solid archive |
| **Encoding** | CSV with header row, UTF-8 |

---

## 2. File Naming Conventions

### 2.1 Pattern

```
<type>_<market>_<exchange>_<YYYYMMDD>.7z
```

### 2.2 Components

| Component | Values | Description |
| :--- | :--- | :--- |
| `<type>` | `order_new`, `tick_new`, `L2_new`, `L1_new` | Data content type |
| `<market>` | `STK`, `ConFI` | Market segment: STK=Stocks, ConFI=Convertible Bonds |
| `<exchange>` | `SZ`, `SH` | Exchange: SZ=SZSE, SH=SSE |
| `<YYYYMMDD>` | e.g., `20240102` | Trading date |

### 2.3 Type Definitions

| Type | Exchange | Content |
| :--- | :--- | :--- |
| `order_new` | Both | Order flow events (new orders, cancellations) |
| `tick_new` | Both | Trade executions (transactions) |
| `L2_new` | SZSE only | Level 2 order book snapshots (10 levels) |

### 2.4 Internal Structure

Each `.7z` archive contains a directory named `<type>_<market>_<exchange>_<YYYYMMDD>/` with individual CSV files:

- **File per symbol:** `<Symbol>.csv` (e.g., `600000.csv`, `000001.csv`)
- **Naming:** Symbol code without exchange suffix (SSE: 600000-699999, SZSE: 000001-099999)

---

## 3. SSE Order Stream

**File Pattern:** `order_new_STK_SH_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 3.1 Schema

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `SecurityID` | String | Stock symbol code (e.g., `600000`) |
| `TransactTime` | Long | Timestamp in format `YYYYMMDDHHMMSSmmm` (milliseconds) |
| `OrderNo` | Integer | Unique order identifier (Order ID) |
| `Price` | Float | Order limit price (CNY) |
| `Balance` | Integer | Order quantity in shares (remaining quantity) |
| `OrderBSFlag` | String | Order side: `B`=Buy, `S`=Sell |
| `OrdType` | String | Order type: `A`=Add (new order), `D`=Delete (cancel) |
| `OrderIndex` | Integer | Sequential order index within the day |
| `ChannelNo` | Integer | Exchange channel ID |
| `BizIndex` | Integer | Global business sequence number across all symbols |

### 3.2 Semantics

- **`OrdType='A'`**: New order entering the matching engine
- **`OrdType='D'`**: Order cancellation (order removed from book)
- **`OrderNo`**: Persistent identifier linking orders to trades
- **`Balance`**: Shows remaining quantity at time of event

---

## 4. SSE Tick/Trade Stream

**File Pattern:** `tick_new_STK_SH_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 4.1 Schema

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `SecurityID` | String | Stock symbol code (e.g., `600000`) |
| `TradeTime` | Long | Timestamp in format `YYYYMMDDHHMMSSmmm` (milliseconds) |
| `TradePrice` | Float | Execution price (CNY) |
| `TradeQty` | Integer | Trade quantity in shares |
| `TradeAmount` | Float | Trade notional value (CNY), calculated as `TradePrice × TradeQty` |
| `BuyNo` | Integer | Buy order ID (aggressor or passive, see `TradeBSFlag`) |
| `SellNo` | Integer | Sell order ID (counterparty) |
| `TradeIndex` | Integer | Sequential trade index within the day |
| `ChannelNo` | Integer | Exchange channel ID |
| `TradeBSFlag` | String | Trade direction: `B`=Buy-initiated, `S`=Sell-initiated, `N`=Unknown/Neutral |
| `BizIndex` | Integer | Global business sequence number across all symbols |

### 4.2 Semantics

- **`TradeBSFlag`**: Indicates which side initiated the trade
  - `B`: Buyer was aggressive (hit the ask)
  - `S`: Seller was aggressive (hit the bid)
  - `N`: Cannot determine aggressor (e.g., auction trades)
- **`BuyNo`/`SellNo`**: Reference `OrderNo` from Order Stream, linking trades to resting orders

---

## 5. SZSE Order Stream

**File Pattern:** `order_new_STK_SZ_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 5.1 Schema

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ApplSeqNum` | Integer | Application Sequence Number: Global unique sequence number (Order ID) |
| `Side` | Integer | Order side: `1`=Buy, `2`=Sell |
| `OrdType` | Integer | Order type: `1`=Market, `2`=Limit |
| `Price` | Float | Limit price of the order (CNY) |
| `OrderQty` | Integer | Volume of the order in shares |
| `TransactTime` | Long | Timestamp in format `YYYYMMDDHHMMSSmmm` |
| `ChannelNo` | Integer | Exchange channel ID |
| `ExpirationDays` | Integer | Order expiration days (optional, usually 0) |
| `ExpirationType` | Integer | Expiration type (optional) |
| `Contactor` | String | Contact information (optional) |
| `ConfirmID` | String | Confirmation ID (optional) |

### 5.2 Semantics

- **`OrdType=1` (Market)**: Request to trade immediately at best available price
- **`OrdType=2` (Limit)**: Request to trade at specified `Price` or better
- **`ApplSeqNum`**: Primary key for order identification and trade linkage

---

## 6. SZSE Tick/Trade Stream

**File Pattern:** `tick_new_STK_SZ_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 6.1 Schema

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ApplSeqNum` | Integer | Global sequence number of this event |
| `BidApplSeqNum` | Integer | Bid Order ID: References `ApplSeqNum` from Order Stream |
| `OfferApplSeqNum` | Integer | Ask Order ID: References `ApplSeqNum` from Order Stream |
| `Price` | Float | Transaction price (CNY), or `0.000` for cancellations |
| `Qty` | Integer | Volume traded or cancelled |
| `ExecType` | String | Event type: `F`=Fill (trade), `4`=Cancel (withdrawal) |
| `TransactTime` | Long | Timestamp in format `YYYYMMDDHHMMSSmmm` |
| `ChannelNo` | Integer | Exchange channel ID |

### 6.2 Semantics

- **`ExecType='F'` (Fill)**: Trade execution
  - `Price`: Execution price
  - `BidApplSeqNum` and `OfferApplSeqNum`: Link to matched orders
  - `Qty`: Shares traded
- **`ExecType='4'` (Cancel)**: Order cancellation
  - `Price`: `0.000`
  - One of `BidApplSeqNum` or `OfferApplSeqNum` identifies cancelled order
  - `Qty`: Shares cancelled

---

## 7. SZSE Level 2 Snapshots

**File Pattern:** `L2_new_STK_SZ_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 7.1 Schema

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `SendingTime` | Long | Gateway sending timestamp (`YYYYMMDDHHMMSSmmm`) |
| `MsgSeqNum` | Integer | Message sequence number |
| `ImageStatus` | String | Image status indicator |
| `QuotTime` | Long | Quote timestamp (`YYYYMMDDHHMMSSmmm`) |
| `PreClosePx` | Float | Previous closing price (CNY) |
| `OpenPx` | Float | Opening price (CNY) |
| `HighPx` | Float | Highest price of the day (CNY) |
| `LowPx` | Float | Lowest price of the day (CNY) |
| `LastPx` | Float | Last traded price (CNY) |
| `ClosePx` | Integer | Closing price (0 if not closed) |
| `Volume` | Integer | Total traded volume (shares) |
| `Amount` | Float | Total traded notional (CNY) |
| `AveragePx` | Float | Volume-weighted average price (VWAP) |
| `BidPrice` | Array[Float] | Bid prices for 10 levels (JSON array format) |
| `BidOrderQty` | Array[Integer] | Total bid quantity at each of 10 levels |
| `BidNumOrders` | Array[Integer] | Number of bid orders at each of 10 levels |
| `BidOrders` | Array[Integer] | First 50 individual bid order quantities |
| `OfferPrice` | Array[Float] | Ask prices for 10 levels (JSON array format) |
| `OfferOrderQty` | Array[Integer] | Total ask quantity at each of 10 levels |
| `OfferNumOrders` | Array[Integer] | Number of ask orders at each of 10 levels |
| `OfferOrders` | Array[Integer] | First 50 individual ask order quantities |
| `NumTrades` | Integer | Number of trades in the day |
| `TotalBidQty` | Integer | Total bid quantity across all levels |
| `WeightedAvgBidPx` | Float | Volume-weighted average bid price |
| `TotalOfferQty` | Integer | Total ask quantity across all levels |
| `WeightedAvgOfferPx` | Float | Volume-weighted average ask price |
| `Change1` | Float | Price change metric 1 |
| `Change2` | Float | Price change metric 2 |
| `TotalLongPosition` | Integer | Total long position (for derivatives) |
| `PeRatio1` | Float | Price-to-earnings ratio 1 |
| `PeRatio2` | Float | Price-to-earnings ratio 2 |
| `UpperLimitPx` | Float | Upper limit price (CNY) |
| `LowerLimitPx` | Float | Lower limit price (CNY) |
| `WeightedAvgPxChg` | Float | Change in VWAP |
| `PreWeightedAvgPx` | Float | Previous VWAP |
| `TradingPhaseCode` | String | Trading phase: `S0`=Pre-market, `T0`=Trading, etc. |
| `NoOrdersB1` | Integer | Number of orders at best bid |
| `NoOrdersS1` | Integer | Number of orders at best ask |

### 7.2 Array Format

Array fields are stored as JSON-style bracket notation:

```
BidPrice:     "[11.630,11.620,11.610,11.600,11.590,11.580,11.570,11.560,11.550,11.540]"
BidOrderQty:  "[254100,476700,492500,1323400,283700,332700,243200,624700,484400,187400]"
BidNumOrders: "[74,94,118,367,85,146,76,168,185,50]"
BidOrders:    "[300,1000,300,2000,500,500,6900,1700,30000,...]" (up to 50 elements)
```

### 7.3 Snapshot Timing

- **Frequency:** Approximately 30-second intervals during trading hours
- **Coverage:** Full market depth (10 levels) with order queue details

---

## 8. Exchange-Specific Differences

| Feature | SSE (SH) | SZSE (SZ) |
| :--- | :--- | :--- |
| **Symbol Location** | `SecurityID` (in CSV) | Filename only (not in CSV) |
| **Order ID Field** | `OrderNo` | `ApplSeqNum` |
| **Side Encoding** | `B`/`S` (strings) | `1`/`2` (integers) |
| **Order Type** | `A`/`D` (Add/Delete) | `1`/`2` (Market/Limit) + separate cancel stream |
| **Cancel Representation** | Separate `OrdType='D'` rows | `ExecType='4'` in Tick stream |
| **Trade Linkage** | `BuyNo` + `SellNo` | `BidApplSeqNum` + `OfferApplSeqNum` |
| **Trade Direction** | `TradeBSFlag` (B/S/N) | Implicit from order linkage |
| **L2 Snapshots** | Not available in Bronze | Available (`L2_new_*`) |
| **Timestamp Format** | Milliseconds (`YYYYMMDDHHMMSSmmm`) | Milliseconds (`YYYYMMDDHHMMSSmmm`) |
| **Timezone** | China Standard Time (CST, UTC+8) | China Standard Time (CST, UTC+8) |

---

## 9. Aggressor Side (Trade Direction) Determination

Both SSE and SZSE Level-2 data provide mechanisms to determine which side initiated (was the aggressor in) a trade.

### 9.1 Core Rule (Both Exchanges)

The aggressor side can be determined by comparing the order reference numbers of the two matched orders:

```
IF BuyOrderRef > SellOrderRef → BUYER is aggressor (主动买 / 外盘)
IF SellOrderRef > BuyOrderRef → SELLER is aggressor (主动卖 / 内盘)
```

**Logic**: Higher sequence number indicates the later-arriving order, which is the taker/aggressor that crossed the spread to hit a resting order.

### 9.2 Exchange-Specific Implementation

#### SSE (Shanghai)

| Field | Description | Canonical Name |
| :--- | :--- | :--- |
| `BuyNo` | Buy order ID | `bid_order_ref` |
| `SellNo` | Sell order ID | `ask_order_ref` |
| `TradeBSFlag` | Native aggressor flag (B=Buy, S=Sell, N=Unknown) | `aggressor_side` (derived) |

**Note**: SSE provides `TradeBSFlag` directly in the raw data, but this can be cross-validated with the `BuyNo` vs `SellNo` comparison.

#### SZSE (Shenzhen)

| Field | Description | Canonical Name |
| :--- | :--- | :--- |
| `BidApplSeqNum` | Bid order ID (from Order Stream `ApplSeqNum`) | `bid_order_ref` |
| `OfferApplSeqNum` | Ask order ID (from Order Stream `ApplSeqNum`) | `ask_order_ref` |

**Note**: SZSE does **not** provide a native aggressor flag. The aggressor side **must** be inferred by comparing `BidApplSeqNum` vs `OfferApplSeqNum`.

### 9.3 SQL Implementation

To add aggressor side to your analysis:

```sql
SELECT
    symbol,
    ts_event_us,
    price / 10000.0 AS price_yuan,
    qty / 100.0 AS qty_shares,
    CASE
        WHEN bid_order_ref > ask_order_ref THEN 'BUY'
        WHEN ask_order_ref > bid_order_ref THEN 'SELL'
        ELSE 'UNKNOWN'
    END AS aggressor_side
FROM cn_tick_events
WHERE event_kind = 'TRADE'
```

### 9.4 Important Notes

| Consideration | Details |
| :--- | :--- |
| **Cancel Events** | `event_kind='CANCEL'` has `price=0`, `qty=0`; aggressor side is not meaningful for cancellations |
| **Auction Trades** | Opening/closing auction trades may not have clear aggressor; marked as `UNKNOWN` |
| **Sequence Gaps** | If one order reference is NULL (data quality issue), result is `UNKNOWN` |
| **Validation** | SSE's `TradeBSFlag` can validate the inferred aggressor from order refs |

### 9.5 References

- **知乎技术分析**: [怎样判断深交所Level2逐笔数据的买卖方向？](https://www.zhihu.com/question/52064487)
- **DolphinDB Orderbook Engine**: Industry-standard implementation using order reference comparison
- **上交所数据重建规则**: "若主动买【买方订单号>卖方订单号】，将无法在逐笔委托中查找到买方的原始委托"

---

## 10. Timestamp Reference

### 10.1 Timezone

All timestamps and file dates use **China Standard Time (CST, UTC+8)**.

**Evidence:**
- Order files start at `09:15:00.000` (pre-market open in China)
- Tick files start at `09:25:00.000` (opening auction in China)
- Files end at `15:00:00.000` (market close in China)
- File date `<YYYYMMDD>` corresponds to the trading date in China timezone

### 10.2 Format

Timestamps use the format: `YYYYMMDDHHMMSSmmm`
- 4 digits: Year
- 2 digits: Month
- 2 digits: Day
- 2 digits: Hour (24-hour format)
- 2 digits: Minute
- 2 digits: Second
- 3 digits: Millisecond

Example: `20240102093000123` = 2024-01-02 09:30:00.123

### 10.3 Market Hours (China Standard Time)

| Phase | Time | Description |
| :--- | :--- | :--- |
| Pre-market (Order) | 09:15:00 - 09:25:00 | Call auction, orders accepted |
| Opening Auction | 09:25:00 - 09:30:00 | Opening price determination |
| Morning Session | 09:30:00 - 11:30:00 | Continuous trading |
| Afternoon Session | 13:00:00 - 15:00:00 | Continuous trading |
| Closing Auction | 14:57:00 - 15:00:00 | Closing price determination (SSE stocks) |

---

## 11. Data Volume Reference

Based on sample file analysis:

| Metric | SSE 600000 (SPD Bank) | Typical Range |
| :--- | :--- | :--- |
| **Order Records/Day** | ~25,000 | 10,000 - 100,000+ |
| **Trade Records/Day** | ~17,000 | 5,000 - 50,000+ |
| **Symbols/Archive** | ~2,299 | Full market coverage |
| **Compression Ratio** | ~20:1 | Varies by volatility |

---

## Appendix: Symbol Code Ranges

| Exchange | Code Range | Description |
| :--- | :--- | :--- |
| **SSE** | 600000 - 699999 | A-shares (main board) |
| **SSE** | 680000 - 689999 | STAR Market (科创板) |
| **SSE** | 510000 - 519999 | ETFs |
| **SZSE** | 000001 - 009999 | Main Board (主板) |
| **SZSE** | 300000 - 309999 | ChiNext (创业板) |
| **SZSE** | 159000 - 159999 | ETFs |

---

## 12. v2 Integration Plan (Clean Cut)

The active clean-cut implementation plan for integrating this data source into the new v2 core is:

- `docs/internal/execplan-v2-quant360-cn-l2-integration.md`

Scope notes for that plan:

- No backward compatibility path.
- No CLI refactor/rewrite in this phase.
- Quant360 ingestion and canonical schemas are implemented in v2 core modules only.

## 13. v2 Upstream Adapter Contract

For v2, archive handling is explicitly separated from ingestion core:

- Upstream adapter package: `pointline/v2/vendors/quant360/upstream/`
- Ingestion core package: `pointline/v2/ingestion/`

Contract boundary:

- Upstream adapter input: raw Quant360 `.7z` archives.
- Upstream adapter output: extracted per-symbol `.csv.gz` files in deterministic Bronze layout.
- Ingestion core input: extracted files only (no `.7z` parsing in core).

Current extracted layout contract:

```
exchange=<exchange>/type=<stream_type>/date=<YYYY-MM-DD>/symbol=<symbol>/<symbol>.csv.gz
```

Examples:

- `exchange=szse/type=order_new/date=2024-01-02/symbol=000001/000001.csv.gz`
- `exchange=sse/type=tick_new/date=2024-01-02/symbol=600000/600000.csv.gz`
- `exchange=szse/type=L2_new/date=2024-01-02/symbol=000001/000001.csv.gz`

## 14. v2 Parser Intermediate Contract (Strict)

For v2 ingestion, parser output is a strict internal contract consumed by canonicalization.
Canonicalization does not apply alias fallback for required parser fields.

### 14.1 `order_new` parser output (required columns)

- `symbol`, `exchange`, `ts_event_us`
- `appl_seq_num`, `channel_no`
- `side_raw`, `ord_type_raw`, `order_action_raw`
- `price_raw`, `qty_raw`
- `biz_index_raw`, `order_index_raw`

### 14.2 `tick_new` parser output (required columns)

- `symbol`, `exchange`, `ts_event_us`
- `appl_seq_num`, `channel_no`
- `bid_appl_seq_num`, `offer_appl_seq_num`
- `exec_type_raw`, `trade_bs_flag_raw`
- `price_raw`, `qty_raw`
- `biz_index_raw`, `trade_index_raw`

### 14.3 `L2_new` parser output (required columns)

- `symbol`, `exchange`, `ts_event_us`
- `ts_local_us`, `msg_seq_num`
- `image_status`, `trading_phase_code_raw`
- `bid_price_levels`, `bid_qty_levels`, `ask_price_levels`, `ask_qty_levels`

### 14.4 Contract behavior

- Missing required parser columns fail fast with explicit missing-column errors.
- SSE/SZSE raw schema differences are handled in parser modules only.
- Canonical table definitions remain in `pointline/schemas/events_cn.py`.
