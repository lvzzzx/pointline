# Data Source: Quant360 SZSE Level 2 Data

This document describes the raw data format for **Shenzhen Stock Exchange (SZSE) Level 2** data provided by **Quant360**. It covers both the **Order Stream** (New Orders) and the **Tick Stream** (Transactions & Cancellations), which together allow for full Order Book reconstruction.

## 1. Source Overview

- **Vendor:** Data.Quant360.com
- **Exchange:** Shenzhen Stock Exchange (SZSE)
- **Data Types:**
    1.  **Order Stream:** Level 2 Orders (New Order Placement)
    2.  **Tick Stream:** Level 2 Tick-by-Tick Transactions (Trades & Order Cancellations)
- **Format:** 7-Zip Archives (`.7z`) containing individual CSV files per symbol.

---

## 2. Order Data Stream

**File Pattern:** `order_new_STK_SZ_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 2.1 Schema
Each row represents a **New Order** entering the matching engine.

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ApplSeqNum` | Integer | **Application Sequence Number**: Global unique sequence number (Order ID). Key for reconstruction. |
| `Side` | Integer | **Side**: `1`=Buy, `2`=Sell |
| `OrdType` | Integer | **Order Type**: `1`=Market, `2`=Limit |
| `Price` | Float | Limit price of the order. |
| `OrderQty` | Integer | Volume of the order. |
| `TransactTime` | Long | Timestamp (`YYYYMMDDHHMMSSmmm`). |
| `ChannelNo` | Integer | Exchange channel ID. |
| *Other* | - | `ExpirationDays`, `ExpirationType`, `Contactor`, `ConfirmID` (Usually unused/0). |

### 2.2 Semantics
- **`OrdType=1` (Market):** Request to trade immediately at best price.
- **`OrdType=2` (Limit):** Request to trade at `Price` or better.

---

## 3. Tick Data Stream

**File Pattern:** `tick_new_STK_SZ_<YYYYMMDD>.7z` containing `<Symbol>.csv`

### 3.1 Schema
Each row represents an **Event** (Match or Cancel) occurring in the matching engine.

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `ApplSeqNum` | Integer | Global sequence number of this *event*. |
| `BidApplSeqNum` | Integer | **Bid Order ID**: References `ApplSeqNum` from the Order Stream. |
| `OfferApplSeqNum` | Integer | **Ask Order ID**: References `ApplSeqNum` from the Order Stream. |
| `Price` | Float | Transaction Price (0.000 for Cancellations). |
| `Qty` | Integer | Volume traded or cancelled. |
| `ExecType` | String | **Type**: `F`=Fill, `4`=Cancel |
| `TransactTime` | Long | Timestamp (`YYYYMMDDHHMMSSmmm`). |
| `ChannelNo` | Integer | Exchange channel ID. |

### 3.2 Semantics
- **`ExecType=F` (Fill):** A trade occurred.
    - `Price` is the execution price.
    - `BidApplSeqNum` and `OfferApplSeqNum` link to the specific orders matched.
- **`ExecType=4` (Cancel):** An order was withdrawn.
    - `Qty` is the amount cancelled.
    - One of `Bid/OfferApplSeqNum` will be non-zero to identify the cancelled order.

---

## 4. L3 Order Book Reconstruction

By processing both streams, you can reconstruct the **Level 3 (Order-by-Order) Book**.

### 4.1 Linkage Logic
The streams are linked via the Order ID:
- **Order Stream:** `ApplSeqNum` is the **Order ID**.
- **Tick Stream:** `BidApplSeqNum` and `OfferApplSeqNum` reference that **Order ID**.

### 4.2 Algorithm

1.  **Initialize:** Empty Bids and Asks maps.
2.  **Process Events** (Sort both streams by `TransactTime`, then `ApplSeqNum`):
    *   **IF Source = Order Stream:**
        *   **Action:** INSERT into Book.
        *   **Key:** `ApplSeqNum`
        *   **Value:** `{Price, Qty, Side}`.
    *   **IF Source = Tick Stream (Type = '4' Cancel):**
        *   **Action:** DECREMENT Qty.
        *   **Target:** Look up order via `BidApplSeqNum` (if Side=Buy) or `OfferApplSeqNum` (if Side=Sell).
        *   **Result:** Reduce Order's Qty by `Tick.Qty`. If Qty becomes 0, REMOVE from book.
    *   **IF Source = Tick Stream (Type = 'F' Fill):**
        *   **Action:** DECREMENT Qty.
        *   **Target:** Look up *both* orders (`BidApplSeqNum` and `OfferApplSeqNum`).
        *   **Result:** Reduce Qty of both orders by `Tick.Qty`. Remove if 0. (Market orders might not exist in book if executed immediately, handle gracefully).

### 4.3 Market vs. Limit Execution
- **Limit Orders (`OrdType=2`):** Sit in the book until matched (`ExecType=F`) or cancelled (`ExecType=4`).
- **Market Orders (`OrdType=1`):** Usually execute immediately. They generate `ExecType=F` events instantly. They might not strictly "rest" in the L3 book depending on matching engine latency, but they *drive* the `ExecType=F` events.
