# Data Source: Quant360 Chinese Stock Level 2/3 Data

This document describes the raw data format for **Shenzhen Stock Exchange (SZSE)** and **Shanghai Stock Exchange (SSE)** Level 2/3 data provided by **Quant360**.

> **Vendor Portal:** https://data.quant360.com
>
> This document is based on the Quant360 data delivery format. For the most up-to-date information, please refer to the vendor directly.

---

## Document Reference

| Resource | Description |
| :--- | :--- |
| **Vendor Portal** | https://data.quant360.com |
| **SSE L2 Specification** | Shanghai Stock Exchange Level 2 market data interface specification |
| **SZSE L2 Specification** | Shenzhen Stock Exchange Binary market data interface specification |

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
| **Timezone** | China Standard Time (CST, UTC+8) |

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
| `<YYYYMMDD>` | e.g., `20240102` | Trading date (CST) |

### 2.3 Type Availability

| Type | SSE (SH) | SZSE (SZ) | Content |
| :--- | :--- | :--- | :--- |
| `order_new` | Yes | Yes | Order flow events (new orders, cancellations) |
| `tick_new` | Yes | Yes | Trade executions (transactions) |
| `L2_new` | No | Yes | Level 2 order book snapshots (10 levels) |

### 2.4 Internal Structure

Each `.7z` archive contains a directory named `<type>_<market>_<exchange>_<YYYYMMDD>/` with individual CSV files:

- **File per symbol:** `<Symbol>.csv` (e.g., `600000.csv`, `000001.csv`)
- **Naming:** Symbol code without exchange suffix

---

## 3. SSE Order Stream

**File Pattern:** `order_new_STK_SH_<YYYYMMDD>.7z`

### 3.1 Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| `SecurityID` | String | Stock symbol code (e.g., `600000`) |
| `TransactTime` | Long | Timestamp: `YYYYMMDDHHMMSSmmm` |
| `OrderNo` | Integer | Unique order identifier |
| `Price` | Float | Order limit price (CNY) |
| `Balance` | Integer | Order quantity in shares (remaining) |
| `OrderBSFlag` | String | Side: `B`=Buy, `S`=Sell |
| `OrdType` | String | Type: `A`=Add (new order), `D`=Delete (cancel) |
| `OrderIndex` | Integer | Sequential order index within the day |
| `ChannelNo` | Integer | Exchange channel ID |
| `BizIndex` | Integer | Global business sequence number across all symbols |

### 3.2 Semantics

- **`OrdType='A'`**: New order entering the matching engine
- **`OrdType='D'`**: Order cancellation (order removed from book)
- **`OrderNo`**: Persistent identifier linking orders to trades
- **`Balance`**: Remaining quantity at time of event

---

## 4. SSE Tick/Trade Stream

**File Pattern:** `tick_new_STK_SH_<YYYYMMDD>.7z`

### 4.1 Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| `SecurityID` | String | Stock symbol code (e.g., `600000`) |
| `TradeTime` | Long | Timestamp: `YYYYMMDDHHMMSSmmm` |
| `TradePrice` | Float | Execution price (CNY) |
| `TradeQty` | Integer | Trade quantity in shares |
| `TradeAmount` | Float | Trade notional value (CNY) = `TradePrice * TradeQty` |
| `BuyNo` | Integer | Buy order ID (references `OrderNo` from Order Stream) |
| `SellNo` | Integer | Sell order ID (references `OrderNo` from Order Stream) |
| `TradeIndex` | Integer | Sequential trade index within the day |
| `ChannelNo` | Integer | Exchange channel ID |
| `TradeBSFlag` | String | Aggressor: `B`=Buy-initiated, `S`=Sell-initiated, `N`=Unknown |
| `BizIndex` | Integer | Global business sequence number across all symbols |

### 4.2 Semantics

- **`TradeBSFlag`**: Indicates which side initiated the trade
  - `B`: Buyer was aggressive (hit the ask / 主动买)
  - `S`: Seller was aggressive (hit the bid / 主动卖)
  - `N`: Cannot determine aggressor (e.g., auction trades)
- **`BuyNo`/`SellNo`**: Reference `OrderNo` from Order Stream, linking trades to resting orders

---

## 5. SZSE Order Stream

**File Pattern:** `order_new_STK_SZ_<YYYYMMDD>.7z`

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

### 5.1 Schema

| Column | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `ApplSeqNum` | Integer | Yes | Global unique sequence number (Order ID) |
| `Side` | Integer | Yes | Side: `1`=Buy, `2`=Sell |
| `OrdType` | Integer | Yes | Type: `1`=Market, `2`=Limit |
| `Price` | Float | Yes | Limit price (CNY) |
| `OrderQty` | Integer | Yes | Volume in shares |
| `TransactTime` | Long | Yes | Timestamp: `YYYYMMDDHHMMSSmmm` |
| `ChannelNo` | Integer | Yes | Exchange channel ID |
| `ExpirationDays` | Integer | No | Order expiration days (usually 0) |
| `ExpirationType` | Integer | No | Expiration type |
| `Contactor` | String | No | Contact information |
| `ConfirmID` | String | No | Confirmation ID |

### 5.2 Semantics

- **`OrdType=1` (Market)**: Trade immediately at best available price
- **`OrdType=2` (Limit)**: Trade at specified `Price` or better
- **`ApplSeqNum`**: Primary key for order identification and trade linkage

---

## 6. SZSE Tick/Trade Stream

**File Pattern:** `tick_new_STK_SZ_<YYYYMMDD>.7z`

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

### 6.1 Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| `ApplSeqNum` | Integer | Global sequence number of this event |
| `BidApplSeqNum` | Integer | Bid Order ID (references `ApplSeqNum` from Order Stream) |
| `OfferApplSeqNum` | Integer | Ask Order ID (references `ApplSeqNum` from Order Stream) |
| `Price` | Float | Execution price (CNY), or `0.000` for cancellations |
| `Qty` | Integer | Volume traded or cancelled (shares) |
| `ExecType` | String | Event type: `F`=Fill (trade), `4`=Cancel (withdrawal) |
| `TransactTime` | Long | Timestamp: `YYYYMMDDHHMMSSmmm` |
| `ChannelNo` | Integer | Exchange channel ID |

### 6.2 Semantics

- **`ExecType='F'` (Fill)**: Trade execution
  - `Price`: Execution price (CNY)
  - `BidApplSeqNum` and `OfferApplSeqNum`: Link to matched orders
  - `Qty`: Shares traded
- **`ExecType='4'` (Cancel)**: Order cancellation
  - `Price`: `0.000`
  - One of `BidApplSeqNum` or `OfferApplSeqNum` identifies cancelled order
  - `Qty`: Shares cancelled

---

## 7. SZSE Level 2 Snapshots

**File Pattern:** `L2_new_STK_SZ_<YYYYMMDD>.7z`

**Note:** Symbol identifier is **not included in CSV columns**; it is derived from the filename (`<Symbol>.csv`).

### 7.1 Schema

| Column | Type | Description |
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
| `BidPrice` | Array[Float] | Bid prices for 10 levels |
| `BidOrderQty` | Array[Integer] | Total bid quantity at each of 10 levels |
| `BidNumOrders` | Array[Integer] | Number of bid orders at each of 10 levels |
| `BidOrders` | Array[Integer] | First 50 individual bid order quantities |
| `OfferPrice` | Array[Float] | Ask prices for 10 levels |
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
| `TradingPhaseCode` | String | Trading phase (see Section 10.3) |
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

## 8. Exchange Comparison

| Feature | SSE (SH) | SZSE (SZ) |
| :--- | :--- | :--- |
| **Symbol Location** | `SecurityID` column | Filename only (not in CSV) |
| **Order ID Field** | `OrderNo` | `ApplSeqNum` |
| **Side Encoding** | `B`/`S` (strings) | `1`/`2` (integers) |
| **Order Type** | `A`/`D` (Add/Delete) | `1`/`2` (Market/Limit) + separate cancel |
| **Cancel Representation** | `OrdType='D'` in order stream | `ExecType='4'` in tick stream |
| **Trade Linkage** | `BuyNo` + `SellNo` | `BidApplSeqNum` + `OfferApplSeqNum` |
| **Trade Direction** | Explicit `TradeBSFlag` (B/S/N) | Inferred from order ref comparison |
| **L2 Snapshots** | Not available | Available (`L2_new_*`) |
| **Closing Auction** | No explicit phase | Dedicated 14:57-15:00 phase |
| **Timestamp Format** | `YYYYMMDDHHMMSSmmm` (ms) | `YYYYMMDDHHMMSSmmm` (ms) |
| **Timezone** | CST (UTC+8) | CST (UTC+8) |

---

## 9. Aggressor Side Determination

### 9.1 Core Rule

The aggressor (taker) is the later-arriving order. Compare order reference numbers:

```
BuyOrderRef > SellOrderRef → BUYER is aggressor (主动买 / 外盘)
SellOrderRef > BuyOrderRef → SELLER is aggressor (主动卖 / 内盘)
```

Higher sequence number = later-arriving order = taker that crossed the spread.

### 9.2 SSE Implementation

| Field | Usage |
| :--- | :--- |
| `BuyNo` | Buy order ID |
| `SellNo` | Sell order ID |
| `TradeBSFlag` | Native aggressor flag (`B`/`S`/`N`) — provided directly |

SSE provides `TradeBSFlag` in raw data. Can be cross-validated with `BuyNo` vs `SellNo` comparison.

### 9.3 SZSE Implementation

| Field | Usage |
| :--- | :--- |
| `BidApplSeqNum` | Bid order ID (from Order Stream) |
| `OfferApplSeqNum` | Ask order ID (from Order Stream) |

SZSE does **not** provide a native aggressor flag. Must be inferred from `BidApplSeqNum` vs `OfferApplSeqNum`.

### 9.4 Edge Cases

| Case | Behavior |
| :--- | :--- |
| Auction trades | No clear aggressor; `TradeBSFlag='N'` (SSE) or equal refs |
| Cancel events | `Price=0`, `Qty=cancelled shares`; aggressor not meaningful |
| NULL order ref | Data quality issue; aggressor is indeterminate |

### 9.5 References

- [怎样判断深交所Level2逐笔数据的买卖方向？](https://www.zhihu.com/question/52064487) (Zhihu)
- DolphinDB Orderbook Engine — industry-standard order ref comparison
- 上交所数据重建规则: "若主动买【买方订单号>卖方订单号】，将无法在逐笔委托中查找到买方的原始委托"

---

## 10. Timestamp Reference

### 10.1 Timezone

All timestamps and file dates use **China Standard Time (CST, UTC+8)**.

### 10.2 Format

```
YYYYMMDDHHMMSSmmm
```

| Component | Digits | Description |
| :--- | :--- | :--- |
| `YYYY` | 4 | Year |
| `MM` | 2 | Month |
| `DD` | 2 | Day |
| `HH` | 2 | Hour (24-hour) |
| `MM` | 2 | Minute |
| `SS` | 2 | Second |
| `mmm` | 3 | Millisecond |

Example: `20240102093000123` = 2024-01-02 09:30:00.123 CST

### 10.3 Market Hours (CST)

| Phase | Time | Description |
| :--- | :--- | :--- |
| Pre-Open Call Auction | 09:15:00 - 09:25:00 | Orders accepted, no matching |
| Opening Auction | 09:25:00 - 09:30:00 | Opening price determination |
| Morning Session | 09:30:00 - 11:30:00 | Continuous trading |
| Lunch Break | 11:30:00 - 13:00:00 | No trading |
| Afternoon Session | 13:00:00 - 14:57:00 | Continuous trading |
| Closing Auction (SZSE) | 14:57:00 - 15:00:00 | Closing price determination |
| After-Hours (STAR/ChiNext) | 15:05:00 - 15:30:00 | Limited post-market trading |

### 10.4 Trading Phase Codes (L2 Snapshots)

| Code | Phase |
| :--- | :--- |
| `S0` | Pre-market / Pre-open |
| `T0` | Continuous trading |
| `B0` | Break / Lunch |
| `E0` | End of day / Closed |

---

## 11. Data Volume Reference

| Metric | SSE 600000 (SPD Bank) | Typical Range |
| :--- | :--- | :--- |
| **Order Records/Day** | ~25,000 | 10,000 - 100,000+ |
| **Trade Records/Day** | ~17,000 | 5,000 - 50,000+ |
| **Symbols/Archive** | ~2,299 | Full market coverage |
| **Compression Ratio** | ~20:1 | Varies by volatility |

---

## Appendix A: Symbol Code Ranges

| Exchange | Code Range | Market Segment |
| :--- | :--- | :--- |
| **SSE** | 600000 - 699999 | A-shares Main Board (主板) |
| **SSE** | 680000 - 689999 | STAR Market (科创板) |
| **SSE** | 510000 - 519999 | ETFs |
| **SZSE** | 000001 - 009999 | Main Board (主板) |
| **SZSE** | 300000 - 309999 | ChiNext (创业板) |
| **SZSE** | 159000 - 159999 | ETFs |

---

## Appendix B: Price Limits (涨跌停)

| Board | Daily Limit | Notes |
| :--- | :--- | :--- |
| Main Board (主板) | ±10% | Based on previous close (`PreClosePx`) |
| ChiNext (创业板) | ±20% | Since 2020 registration reform |
| STAR Market (科创板) | ±20% | Since inception |
| New Listings (ChiNext/STAR) | No limit | First 5 trading days |

Upper/lower limit prices are available in L2 snapshots as `UpperLimitPx` / `LowerLimitPx`.

---

## Appendix C: Market Microstructure

| Parameter | Value |
| :--- | :--- |
| **Tick Size** | 0.01 CNY per share (all boards) |
| **Lot Size (Main/ChiNext)** | 100 shares |
| **Lot Size (STAR Market)** | 200 shares |
| **T+1 Settlement** | Shares bought today cannot be sold until next trading day |
| **Short Selling** | Restricted to designated securities via margin trading (融券) |
