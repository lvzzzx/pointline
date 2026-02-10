# SZSE Level 3 Data: Timestamp Semantics and Sequence Validation

**Created:** 2026-02-06
**Status:** ✅ Implemented | **Module:** `pointline.io.vendors.quant360` (SZSE L3 parser)
**Context:** Quant360 SZSE L3 data lacks arrival timestamps; channel-based ordering guarantees. Implemented with exchange time as `ts_local_us` fallback.

---

## Executive Summary

**Key Findings:**
1. ✅ **No arrival time provided** → Use exchange time as `ts_local_us` fallback
2. ✅ **Channel = security type grouping** → Each channel is independent ordered stream
3. ✅ **ApplSeqNum is contiguous** → Expected increment of 1 per message within channel
4. ✅ **Orders and ticks share sequence space** → Unified numbering within channel
5. ⚠️ **Timestamps may invert across channels** → Sort per-channel for guaranteed ordering

**Impact:**
- Must validate sequence continuity per (channel_no, date)
- Must validate timestamp monotonicity per (channel_no, date)
- Cross-table validation required (orders and ticks share ApplSeqNum)

---

## Problem Statement

Unlike crypto market data (e.g., Tardis) which provides both exchange time and arrival time, Quant360 SZSE Level 3 data **only provides exchange timestamps** (`TransactTime`).

This creates two challenges:
1. **No true arrival time** - Cannot know actual data capture time
2. **Point-in-time correctness** - Architecture requires `ts_local_us` for deterministic replay

---

## Solution: Exchange Time as Arrival Time Fallback

### Design Decision

**Use `TransactTime` (exchange time) as `ts_local_us` fallback** when arrival time is unavailable.

**Rationale:**
- Exchange time is the best available proxy
- SZSE/SSE exchange clocks are highly accurate (sub-millisecond sync, exchange-managed)
- Sequence numbers (`appl_seq_num`) per channel provide ordering guarantees
- Alternative (null `ts_local_us`) breaks existing replay architecture

**Trade-offs:**
- ✅ Enables deterministic replay with existing architecture
- ✅ Exchange time is accurate and reliable
- ⚠️ Not true arrival time (no network latency captured)
- ⚠️ Cannot distinguish exchange vs arrival time in queries

---

## Channel Semantics (Critical Understanding)

### What is `channel_no`?

From [SZSE STEP specification](http://www.szse.cn/marketServices/technicalservice/interface/P020211013598707245917.pdf) and [DolphinDB documentation](https://docs.dolphindb.cn/zh/tutorials/orderBookSnapshotEngine.html):

**Channel = Security Type Partition Key**

Each channel groups securities by type:
- Channel 1: SZSE stocks (Main Board, SME Board, ChiNext)
- Channel 2: SZSE convertible bonds
- Channel 3: SZSE funds (ETFs, LOFs)
- Channel 4: SSE securities (if cross-listed data)
- ... (additional channels for other security types)

**Channel Characteristics:**
- **Independent sequence space**: Each channel has its own `appl_seq_num` starting from 1
- **Independent heartbeats**: Each channel sends heartbeat messages when idle
- **Parallel processing**: Channels enable concurrent processing of different security types
- **Multiple symbols per channel**: One channel contains many symbols of the same type

### Example Channel Structure

```
Channel 1 (SZSE Stocks):
  ApplSeqNum=1: Order symbol=000001 (Ping An Bank)
  ApplSeqNum=2: Tick symbol=000002 (Vanke A)
  ApplSeqNum=3: Order symbol=000001 (Ping An Bank)
  ApplSeqNum=4: Order symbol=300750 (CATL)
  ...

Channel 2 (SZSE Convertible Bonds):
  ApplSeqNum=1: Order symbol=123001 (some convertible bond)
  ApplSeqNum=2: Tick symbol=123001
  ...
```

---

## Sequence Number Semantics (ApplSeqNum)

### Unified Sequence Space

From [SZSE STEP Market Data Feed specification](http://www.szsi.cn/cpfw/overseas/market/technical/202108/P020210805527697225323.pdf):

**Key Property:** Within the same channel, **orders (逐笔委托) and ticks (逐笔成交) share the same `appl_seq_num` sequence**.

**Message Interleaving:**
```
Channel 1, Date=2024-09-30:
  ApplSeqNum=1000: Order (UA201) - buy 100 shares @ 50.00
  ApplSeqNum=1001: Order (UA201) - sell 200 shares @ 50.01
  ApplSeqNum=1002: Tick (UA202) - trade 50 shares @ 50.00
  ApplSeqNum=1003: Heartbeat (placeholder)
  ApplSeqNum=1004: Order (UA201) - buy 150 shares @ 49.99
  ApplSeqNum=1005: Tick (UA202) - trade 100 shares @ 50.00
```

### Continuity Guarantee

From official documentation:

> "MDGW (Market Data Gateway) will replace unsupported messages with a placeholder message, and send the placeholder message to VSS to ensure the continuity of ApplSeqNum."

**Implication:**
- Sequences are **expected to be contiguous** (increment by 1)
- Gaps indicate message loss or data corruption
- Under normal network conditions: `ApplSeqNum[i] = ApplSeqNum[i-1] + 1`

---

## Timestamp Ordering Characteristics

### Within-Channel Monotonicity

**Expected:** Timestamps should increase (or stay equal) with `appl_seq_num` within a channel.

```python
# Within Channel 1:
ApplSeqNum=1000, ts_local_us=1696070400000000  # 2024-09-30 09:30:00.000
ApplSeqNum=1001, ts_local_us=1696070400050000  # 2024-09-30 09:30:00.050
ApplSeqNum=1002, ts_local_us=1696070400050000  # 2024-09-30 09:30:00.050 (same ms)
ApplSeqNum=1003, ts_local_us=1696070400100000  # 2024-09-30 09:30:00.100
```

**Violations indicate:**
- Clock skew within channel (rare)
- Data corruption
- Out-of-order delivery (should not happen)

### Cross-Channel Non-Monotonicity

From [中文文档](https://zhuanlan.zhihu.com/p/649040063):

> "When processing multiple channels in the same engine, after sorting by ApplSeqNum in ascending order, **the tick time field may no longer be in ascending order**."

**Example:**
```python
# Cross-channel sorting by ApplSeqNum can break timestamp order:
Channel 1, ApplSeqNum=1000, ts_local_us=1696070400100000  # 09:30:00.100
Channel 2, ApplSeqNum=1001, ts_local_us=1696070400050000  # 09:30:00.050 (earlier!)
Channel 1, ApplSeqNum=1002, ts_local_us=1696070400150000  # 09:30:00.150
```

**Root Cause:**
- Different channels may have slight processing delays
- Channel-specific buffering in MDGW
- Network timing variations between channels

**Implication:** Cannot rely on ApplSeqNum for global chronological ordering across channels.

---

## Schema Documentation Update

### Updated Schema Comments

```python
# pointline/tables/szse_l3_orders.py
SZSE_L3_ORDERS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,

    # CRITICAL: ts_local_us is FALLBACK for SZSE data
    # - No arrival time provided by Quant360
    # - Uses exchange time (TransactTime) converted from CST to UTC
    # - Monotonic within (channel_no, date), may invert across channels
    # - Deterministic ordering: use (channel_no, appl_seq_num)
    "ts_local_us": pl.Int64,

    # Order sequence number - contiguous within (channel_no, date)
    # - Shared sequence space with ticks (orders and ticks interleaved)
    # - Expected: ApplSeqNum[i] = ApplSeqNum[i-1] + 1
    # - Gaps indicate message loss
    "appl_seq_num": pl.Int64,

    "side": pl.UInt8,  # 0=buy, 1=sell
    "ord_type": pl.UInt8,  # 0=market, 1=limit
    "px_int": pl.Int64,
    "order_qty_int": pl.Int64,

    # Channel number - security type grouping (independent ordered stream)
    # - Each channel has its own appl_seq_num sequence starting from 1
    # - Examples: Channel 1 = SZSE stocks, Channel 2 = SZSE bonds, etc.
    # - Multiple symbols per channel
    "channel_no": pl.Int32,

    "file_id": pl.Int32,
    "file_line_number": pl.Int32,
}
```

Similarly for `szse_l3_ticks.py`.

---

## Validation Implementation

### Rule 1: Sequence Continuity (Per Channel, Per Date)

```python
def validate_sequence_continuity(df: pl.DataFrame) -> pl.DataFrame:
    """Validate ApplSeqNum is contiguous (increments by 1) per (channel_no, date).

    SZSE guarantee: Under normal operation, sequences are contiguous.
    Gaps indicate message loss or data corruption.

    Args:
        df: DataFrame with channel_no, date, appl_seq_num columns

    Returns:
        DataFrame with violations (empty if valid)

    Raises:
        ValueError: If critical sequence violations detected
    """
    if df.is_empty():
        return df

    # Sort by (channel_no, date, appl_seq_num)
    sorted_df = df.sort(["channel_no", "date", "appl_seq_num"])

    # Compute previous sequence and gap
    with_prev = sorted_df.with_columns([
        pl.col("appl_seq_num")
          .shift(1)
          .over(["channel_no", "date"])
          .alias("prev_seq"),
    ]).with_columns([
        (pl.col("appl_seq_num") - pl.col("prev_seq")).alias("gap"),
    ])

    # Expected gap = 1 (contiguous), anything else is anomalous
    violations = with_prev.filter(
        pl.col("gap").is_not_null() &
        (pl.col("gap") != 1)
    )

    if not violations.is_empty():
        # Summarize violations by channel/date
        gap_stats = violations.group_by(["channel_no", "date"]).agg([
            pl.col("gap").count().alias("violation_count"),
            pl.col("gap").min().alias("min_gap"),
            pl.col("gap").max().alias("max_gap"),
            pl.col("appl_seq_num").min().alias("first_violation_seq"),
        ])

        logger.error(
            f"Sequence continuity violations: {len(violations)} non-contiguous sequences"
        )
        logger.error(f"Gap statistics by channel:\n{gap_stats}")

        # Critical if many violations or large gaps
        max_gap = gap_stats["max_gap"].max()
        total_violations = len(violations)

        if max_gap > 100 or total_violations > 10:
            raise ValueError(
                f"CRITICAL: Severe sequence discontinuity detected. "
                f"Max gap: {max_gap}, Total violations: {total_violations}. "
                f"Data may be corrupted or incomplete."
            )

    return violations


def validate_sequence_monotonicity(df: pl.DataFrame) -> pl.DataFrame:
    """Validate ApplSeqNum never decreases within (channel_no, date).

    This catches out-of-order delivery or duplicate sequence numbers.
    Less strict than continuity check (allows gaps, but no reversals).

    Args:
        df: DataFrame with channel_no, date, appl_seq_num columns

    Returns:
        DataFrame with violations (empty if valid)
    """
    if df.is_empty():
        return df

    sorted_df = df.sort(["channel_no", "date", "appl_seq_num"])

    with_prev = sorted_df.with_columns([
        pl.col("appl_seq_num")
          .shift(1)
          .over(["channel_no", "date"])
          .alias("prev_seq"),
    ])

    # Find reversals: current seq <= previous seq
    violations = with_prev.filter(
        pl.col("prev_seq").is_not_null() &
        (pl.col("appl_seq_num") <= pl.col("prev_seq"))
    )

    if not violations.is_empty():
        logger.error(
            f"Sequence monotonicity violations: {len(violations)} rows with "
            f"appl_seq_num <= previous within same (channel, date). "
            f"This indicates out-of-order data or duplicate sequences."
        )
        # This is always critical
        raise ValueError(
            f"CRITICAL: Non-monotonic sequences detected. "
            f"Data is corrupted or delivered out of order."
        )

    return violations
```

### Rule 2: Timestamp Monotonicity (Per Channel, Per Date)

```python
def validate_timestamp_monotonicity(df: pl.DataFrame) -> pl.DataFrame:
    """Validate ts_local_us increases (or stays equal) with appl_seq_num.

    Within each (channel_no, date), timestamps should not decrease.
    Equal timestamps are OK (multiple events at same millisecond).

    Args:
        df: DataFrame with channel_no, date, appl_seq_num, ts_local_us columns

    Returns:
        DataFrame with violations (empty if valid)
    """
    if df.is_empty():
        return df

    # Sort by sequence to check timestamp consistency
    sorted_df = df.sort(["channel_no", "date", "appl_seq_num"])

    with_prev = sorted_df.with_columns([
        pl.col("ts_local_us")
          .shift(1)
          .over(["channel_no", "date"])
          .alias("prev_ts"),
    ])

    # Find timestamp reversals (current < previous)
    violations = with_prev.filter(
        pl.col("prev_ts").is_not_null() &
        (pl.col("ts_local_us") < pl.col("prev_ts"))
    )

    if not violations.is_empty():
        # Compute reversal magnitudes
        violation_stats = violations.with_columns([
            ((pl.col("prev_ts") - pl.col("ts_local_us")) / 1_000_000.0)
              .alias("reversal_seconds")
        ]).group_by(["channel_no", "date"]).agg([
            pl.col("reversal_seconds").count().alias("reversal_count"),
            pl.col("reversal_seconds").max().alias("max_reversal_sec"),
            pl.col("appl_seq_num").min().alias("first_reversal_seq"),
        ])

        logger.error(
            f"Timestamp monotonicity violations: {len(violations)} timestamp reversals"
        )
        logger.error(f"Reversal statistics:\n{violation_stats}")

        # Critical if large reversals
        max_reversal = violation_stats["max_reversal_sec"].max()
        if max_reversal > 1.0:  # > 1 second reversal
            raise ValueError(
                f"CRITICAL: Large timestamp reversals detected (max: {max_reversal:.3f}s). "
                f"Clock skew or data corruption."
            )

    return violations
```

### Rule 3: Cross-Table Sequence Validation

```python
def validate_cross_table_sequences(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
) -> pl.DataFrame:
    """Validate orders and ticks have non-overlapping ApplSeqNum within (channel, date).

    Since they share the same sequence space, no ApplSeqNum should appear in both tables
    for the same (channel_no, date).

    Args:
        orders: szse_l3_orders DataFrame
        ticks: szse_l3_ticks DataFrame

    Returns:
        DataFrame with duplicate ApplSeqNum entries (empty if valid)
    """
    # Find overlapping sequences
    duplicates = (
        orders.select(["channel_no", "date", "appl_seq_num"])
        .join(
            ticks.select(["channel_no", "date", "appl_seq_num"]),
            on=["channel_no", "date", "appl_seq_num"],
            how="inner"
        )
    )

    if not duplicates.is_empty():
        dup_stats = duplicates.group_by(["channel_no", "date"]).agg([
            pl.col("appl_seq_num").count().alias("collision_count")
        ])

        logger.error(
            f"Cross-table sequence collisions: {len(duplicates)} ApplSeqNum values "
            f"appear in both orders and ticks tables"
        )
        logger.error(f"Collision statistics:\n{dup_stats}")

        raise ValueError(
            f"CRITICAL: {len(duplicates)} ApplSeqNum collisions between orders and ticks. "
            f"Data corruption or ingestion error."
        )

    return duplicates


def validate_cross_table_continuity(
    orders: pl.DataFrame,
    ticks: pl.DataFrame,
) -> pl.DataFrame:
    """Validate combined orders+ticks have contiguous ApplSeqNum per (channel, date).

    Since they share the same sequence space, when merged and sorted by ApplSeqNum,
    the combined sequence should be contiguous.

    Args:
        orders: szse_l3_orders DataFrame
        ticks: szse_l3_ticks DataFrame

    Returns:
        DataFrame with gap statistics per (channel_no, date)
    """
    # Combine both tables
    combined = pl.concat([
        orders.select(["channel_no", "date", "appl_seq_num", "ts_local_us"])
          .with_columns(pl.lit("order").alias("type")),
        ticks.select(["channel_no", "date", "appl_seq_num", "ts_local_us"])
          .with_columns(pl.lit("tick").alias("type")),
    ]).sort(["channel_no", "date", "appl_seq_num"])

    # Check continuity on combined data
    violations = validate_sequence_continuity(combined)

    if not violations.is_empty():
        logger.warning(
            f"Combined orders+ticks sequence gaps: {len(violations)} discontinuities. "
            f"This is expected if one table is missing data."
        )

    return violations
```

---

## Integration into Validation Pipeline

Update `pointline/tables/szse_l3_orders.py`:

```python
def validate_szse_l3_orders(df: pl.DataFrame) -> pl.DataFrame:
    """Apply quality checks to SZSE L3 order data.

    Validates:
    - Non-negative px_int and order_qty_int
    - Valid timestamp ranges
    - Valid side codes (0-1)
    - Valid order type codes (0-1)
    - Non-null required fields
    - exchange_id matches normalized exchange
    - **NEW: Sequence continuity per (channel_no, date)** ⭐
    - **NEW: Sequence monotonicity per (channel_no, date)** ⭐
    - **NEW: Timestamp monotonicity per (channel_no, date)** ⭐

    Returns filtered DataFrame (invalid rows removed) or raises on critical errors.
    """
    if df.is_empty():
        return df

    # ... existing validation code ...

    # NEW: Sequence validation (critical - must pass)
    try:
        sequence_violations = validate_sequence_monotonicity(df)
        # If no exception, check was passed
    except ValueError as e:
        # Re-raise with context
        raise ValueError(f"SZSE L3 orders failed sequence monotonicity: {e}") from e

    try:
        continuity_violations = validate_sequence_continuity(df)
        # Warning logged inside function, don't fail unless severe
    except ValueError as e:
        raise ValueError(f"SZSE L3 orders failed sequence continuity: {e}") from e

    # NEW: Timestamp validation (critical)
    try:
        timestamp_violations = validate_timestamp_monotonicity(df)
    except ValueError as e:
        raise ValueError(f"SZSE L3 orders failed timestamp monotonicity: {e}") from e

    return valid.select(df.columns)
```

Similarly for `pointline/tables/szse_l3_ticks.py`.

Add cross-table validation to ingestion service:

```python
# pointline/services/szse_l3_ingestion_service.py (hypothetical)
def validate_cross_table_consistency(self, exchange: str, date: str):
    """Run cross-table validation after both orders and ticks are ingested."""
    orders = self.load_orders(exchange, date)
    ticks = self.load_ticks(exchange, date)

    # Check for sequence collisions
    validate_cross_table_sequences(orders, ticks)

    # Check combined continuity (warning only)
    validate_cross_table_continuity(orders, ticks)
```

---

## Deterministic Ordering for Replay

### Ordering Strategies

```python
from pointline.research import query

# Load SZSE L3 orders
orders = query.l3_orders("szse", "000001", "2024-09-30", "2024-10-01", decoded=True)

# Strategy 1: Per-Channel Replay (RECOMMENDED) ✅
# - Guarantees: Monotonic timestamps, contiguous sequences
# - Use case: Analyzing single channel (e.g., stocks only)
orders_per_channel = orders.sort(["channel_no", "appl_seq_num"])

# Strategy 2: Cross-Channel Chronological (BEST-EFFORT) ⚠️
# - Guarantees: Approximate chronological order
# - Trade-off: May have small timestamp inversions between channels
# - Use case: Global market replay across all security types
orders_chronological = orders.sort(["ts_local_us", "channel_no", "appl_seq_num"])

# Strategy 3: Pure Timestamp Sort (NOT RECOMMENDED) ❌
# - Problem: Ignores sequence ordering within channels
# - Risk: May violate causality for same-channel events
orders_timestamp_only = orders.sort(["ts_local_us"])  # Don't use!
```

### Ordering Decision Matrix

| Use Case | Recommended Ordering | Guarantees |
|----------|---------------------|------------|
| **Single symbol analysis** | `["channel_no", "appl_seq_num"]` | Perfect order within channel |
| **Single channel (stocks only)** | `["channel_no", "appl_seq_num"]` | Perfect order, all stocks |
| **Cross-channel chronological** | `["ts_local_us", "channel_no", "appl_seq_num"]` | Approximate global time order |
| **Multi-symbol, same channel** | `["channel_no", "symbol_id", "appl_seq_num"]` | Per-symbol order within channel |
| **Full market replay** | `["ts_local_us", "channel_no", "appl_seq_num"]` | Best-effort chronological |

---

## CLAUDE.md Updates

Add to **Timeline Semantics** section:

```markdown
## Timeline Semantics

**Default replay timeline:** `ts_local_us` (arrival time), **not** `ts_exch_us` (exchange time)

### EXCEPTION: SZSE/SSE Level 3 Data (Quant360 Vendor)

For `szse_l3_orders` and `szse_l3_ticks` tables:

**Timestamp Semantics:**
- **No arrival timestamps provided by vendor**
- **Fallback:** `ts_local_us` = exchange time (`TransactTime`, converted CST → UTC)
- **Implication:** PIT correctness depends on exchange clock accuracy (high for SZSE/SSE)

**Channel Semantics:**
- `channel_no` = security type grouping (stocks, bonds, funds, etc.)
- Each channel is an independent ordered stream
- Multiple symbols per channel

**Sequence Guarantees:**
- `appl_seq_num` is contiguous within (channel_no, date)
- Orders and ticks share the same sequence space (interleaved)
- Expected: `ApplSeqNum[i] = ApplSeqNum[i-1] + 1`
- Gaps indicate message loss

**Ordering Guarantees:**
- Within channel: Timestamps monotonic with `appl_seq_num`
- Across channels: Timestamps may have slight inversions
- **Recommended ordering:** `(channel_no, appl_seq_num)` per channel
- **Cross-channel ordering:** `(ts_local_us, channel_no, appl_seq_num)` for chronological

**Validation Enforced:**
- Sequence continuity per (channel_no, date)
- Sequence monotonicity per (channel_no, date)
- Timestamp monotonicity per (channel_no, date)
- Cross-table sequence collision detection

**Rationale:**
- Exchange time is the best available proxy when arrival time is missing
- SZSE/SSE exchange clocks are highly accurate (sub-millisecond sync)
- Sequence numbers provide deterministic ordering within channels
- Channel independence enables parallel processing
```

---

## Comparison: Crypto vs SZSE Data

| Aspect | Crypto (Tardis) | SZSE (Quant360) |
|--------|-----------------|------------------|
| **Arrival time** | ✅ Provided (`ts_local`) | ❌ Not provided |
| **Exchange time** | ✅ Provided (`ts_exch`) | ✅ Provided (`TransactTime`) |
| **ts_local_us semantics** | True arrival time | **Fallback: exchange time** |
| **Ordering guarantee** | `ts_local_us` (single stream) | `(channel_no, appl_seq_num)` (multi-stream) |
| **Sequence numbers** | ❌ None | ✅ `appl_seq_num` per channel |
| **Channel concept** | ❌ None | ✅ Security type partitions |
| **Sequence continuity** | N/A | ✅ Contiguous (gap = 1) |
| **Cross-stream ordering** | N/A | ⚠️ Timestamps may invert |
| **Unified sequence space** | N/A | ✅ Orders + ticks interleaved |
| **Monotonicity validation** | Timestamp only | **Timestamp + sequence per channel** |
| **PIT correctness** | Guaranteed (arrival) | High confidence (exchange clock) |

---

## Implementation Checklist

### Phase 1: Documentation (Immediate) ✅
- [x] Create this design document
- [ ] Update `szse_l3_orders.py` schema comments
- [ ] Update `szse_l3_ticks.py` schema comments
- [ ] Update CLAUDE.md "Timeline Semantics" section
- [ ] Document ordering strategies in research API

### Phase 2: Validation Functions (High Priority) 🔥
- [ ] Implement `validate_sequence_continuity()` in `tables/szse_l3_orders.py`
- [ ] Implement `validate_sequence_monotonicity()` in `tables/szse_l3_orders.py`
- [ ] Implement `validate_timestamp_monotonicity()` in `tables/szse_l3_orders.py`
- [ ] Copy validation functions to `tables/szse_l3_ticks.py`
- [ ] Implement `validate_cross_table_sequences()` in new `tables/szse_l3_validation.py`
- [ ] Implement `validate_cross_table_continuity()` in `tables/szse_l3_validation.py`
- [ ] Integrate into `validate_szse_l3_orders()`
- [ ] Integrate into `validate_szse_l3_ticks()`
- [ ] Add cross-table validation to ingestion service

### Phase 3: Unit Tests (High Priority) 🔥
- [ ] Test `validate_sequence_continuity()` with contiguous data
- [ ] Test `validate_sequence_continuity()` with gaps (expected failure)
- [ ] Test `validate_sequence_monotonicity()` with reversals (expected failure)
- [ ] Test `validate_timestamp_monotonicity()` with inversions (expected failure)
- [ ] Test `validate_cross_table_sequences()` with collisions (expected failure)
- [ ] Test cross-channel timestamp inversions (expected behavior)
- [ ] Create test fixtures with realistic channel/sequence patterns

### Phase 4: Real Data Testing (Critical) ⚠️
- [ ] Test with real Quant360 SZSE data files
- [ ] Verify no sequence violations in historical data
- [ ] Measure typical gap sizes (should be 0)
- [ ] Measure cross-channel timestamp inversions (magnitude)
- [ ] Document observed channel numbers and their meanings
- [ ] Validate assumptions about sequence continuity

### Phase 5: Research API Helpers (Future)
- [ ] Add `ensure_channel_order(df)` helper function
- [ ] Add `ensure_chronological_order(df)` helper function
- [ ] Warn if user queries SZSE data without proper sorting
- [ ] Example notebooks showing proper ordering strategies
- [ ] Performance comparison: per-channel vs chronological replay

---

## Open Questions (Resolved ✅)

1. ✅ **What is the semantic meaning of `channel_no`?**
   - **Answer:** Security type partition (stocks, bonds, funds, etc.)

2. ✅ **Are sequence numbers guaranteed contiguous?**
   - **Answer:** Yes, expected to be contiguous (increment by 1) within (channel_no, date)

3. ✅ **Do orders and ticks share the same sequence space?**
   - **Answer:** Yes, unified sequence space per channel (interleaved)

4. ✅ **Cross-channel timestamp ordering?**
   - **Answer:** Timestamps may invert across channels (channel-specific buffering)

5. ✅ **Typical gap sizes?**
   - **Answer:** Expected 0 gaps under normal operation (contiguous)

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| **Sequence gaps in production data** | Strict validation fails ingestion; alert operator for re-fetch |
| **Timestamp inversions across channels** | Document expected behavior; provide per-channel ordering option |
| **Cross-table sequence collisions** | Cross-table validation in ingestion pipeline; fail early |
| **Exchange clock skew** | Low risk (exchange-managed clocks); validate timestamp monotonicity |
| **Channel semantics change** | Monitor SZSE spec updates; add channel mapping configuration |

---

## References

### Official Documentation
- [SZSE STEP Market Data Feed Specification (Ver1.17)](http://www.szsi.cn/cpfw/overseas/market/technical/202108/P020210805527697225323.pdf)
- [SZSE STEP Interface Specification (Ver1.11)](http://www.szse.cn/marketServices/technicalservice/interface/P020211013598707245917.pdf)
- [SZSE Binary Trading Data Interface Specification (Ver1.24)](http://www.szse.cn/marketServices/technicalservice/interface/P020220107557856971513.pdf)

### Implementation Guides
- [DolphinDB Orderbook Engine Tutorial](https://docs.dolphindb.cn/zh/tutorials/orderBookSnapshotEngine.html)
- [Level 2 逐笔数据重建 Orderbook 原理 (Chinese)](https://zhuanlan.zhihu.com/p/649040063)
- [处理 Level-2 行情数据实例 (DolphinDB)](https://docs.dolphindb.cn/zh/tutorials/l2_stk_data_proc_2.html)

---

## Summary

**Key Takeaways:**

1. ✅ **Exchange time as fallback** - Acceptable and documented
2. ✅ **Channel = security type** - Independent ordered streams
3. ✅ **Sequence continuity** - Expected contiguous (gap = 1)
4. ✅ **Unified sequence space** - Orders and ticks interleaved
5. ✅ **Per-channel ordering** - Guaranteed deterministic replay
6. ⚠️ **Cross-channel inversions** - Timestamps may not be globally monotonic

**Critical Implementation:**
- Validate sequence continuity per (channel_no, date)
- Validate timestamp monotonicity per (channel_no, date)
- Cross-table validation for sequence collisions
- Document ordering strategies for researchers

**Next Steps:**
1. Implement validation functions (Phase 2)
2. Test with real Quant360 data (Phase 4)
3. Update schema comments and documentation (Phase 1)
4. Add cross-table validation to ingestion pipeline
