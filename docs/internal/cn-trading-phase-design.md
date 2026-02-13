# Chinese A-Share Trading Phase Design Options

## Context

Chinese A-share markets (SSE, SZSE) have unique trading phase structures that are critical for research analysis:

| Phase | Time (CST, UTC+8) | Exchange | Description |
|-------|--------------------|----------|-------------|
| Pre-opening | 09:15-09:25 | Both | Opening auction (集合竞价), 9:20-9:25 no cancel |
| Morning | 09:30-11:30 | Both | Continuous auction (连续竞价) |
| Noon Break | 11:30-13:00 | Both | Market closed |
| Afternoon | 13:00-14:57 | Both | Continuous auction |
| Closing | 14:57-15:00 | SZSE only | Closing auction (收盘集合竞价) |
| After-hours | 15:05-15:30 | STAR/ChiNext only | Fixed-price trading (盘后固定价格) |

These phases affect price formation, matching rules, liquidity patterns, and volatility characteristics. Research workflows often need explicit phase slicing (for example, comparing opening auction vs continuous trading impact).

---

## Current Decision (v2)

We selected **Option 1 (query-time classification)** for v2, implemented as **explicit research primitives**.

Contract:
- `load_events(...)` remains a pure canonical event loader.
- CN phase logic is applied explicitly with primitives in `pointline/v2/research/cn_trading_phases.py`.
- No schema changes in canonical event tables.
- No implicit phase filtering inside `load_events`.

This aligns with v2 principles: clean core contracts, no hidden enrichment, explicit user intent.

---

## Option 1: Query-Time Classification (Implemented)

### Module Surface

```python
# pointline/v2/research/cn_trading_phases.py

class TradingPhase(str, Enum):
    CLOSED = "CLOSED"
    PRE_OPEN = "PRE_OPEN"
    MORNING = "MORNING"
    NOON_BREAK = "NOON_BREAK"
    AFTERNOON = "AFTERNOON"
    CLOSING = "CLOSING"
    AFTER_HOURS = "AFTER_HOURS"


def classify_phase(*, ts_event_us: int, exchange: str, market_type: str | None = None) -> TradingPhase: ...

def add_phase_column(
    df: pl.DataFrame,
    *,
    exchange: str,
    ts_col: str = "ts_event_us",
    market_type: str | None = None,
    out_col: str = "trading_phase",
) -> pl.DataFrame: ...


def filter_by_phase(
    df: pl.DataFrame,
    *,
    exchange: str,
    phases: list[TradingPhase | str],
    ts_col: str = "ts_event_us",
    market_type: str | None = None,
    phase_col: str = "trading_phase",
    keep_phase_col: bool = False,
) -> pl.DataFrame: ...
```

### Explicit Composition with Research API

```python
from pathlib import Path

from pointline.v2.research import TradingPhase, filter_by_phase, load_events

# 1) Load canonical events (no hidden phase filtering)
trades = load_events(
    silver_root=Path("/data/silver"),
    table="trades",
    exchange="szse",
    symbol="300001",
    start="2024-01-15T09:00:00+08:00",
    end="2024-01-15T15:30:00+08:00",
)

# 2) Apply explicit phase slicing
opening = filter_by_phase(
    trades,
    exchange="szse",
    phases=[TradingPhase.PRE_OPEN],
)

continuous = filter_by_phase(
    trades,
    exchange="szse",
    phases=[TradingPhase.MORNING, TradingPhase.AFTERNOON],
)

after_hours = filter_by_phase(
    trades,
    exchange="szse",
    phases=[TradingPhase.AFTER_HOURS],
    market_type="growth_board",  # ChiNext gating
)
```

### Performance Notes

The v2 implementation uses **vectorized Polars expressions** for phase derivation, not per-row Python UDF in query execution. This keeps Option 1 reasonably efficient while preserving schema stability.

### Pros

| Advantage | Description |
|-----------|-------------|
| No schema change | Event table schemas stay canonical and market-neutral |
| No storage overhead | Zero additional bytes stored per row |
| Rule flexibility | Phase rules can be updated without re-ingestion |
| Explicit UX | Users can see and control where phase filtering happens |
| v2-aligned | Preserves single-responsibility `load_events(...)` |

### Cons

| Disadvantage | Description |
|--------------|-------------|
| Runtime compute | Phase classification still happens at query time |
| No partition pruning by phase | Storage layout cannot skip files by `trading_phase` |
| Repeated work | Repeated phase slicing recomputes classification |

---

## Option 2: Ingestion-Time Storage (Deferred)

Add `trading_phase` as a persisted event column during ingestion.

### Pros

| Advantage | Description |
|-----------|-------------|
| Query performance | Simple persisted predicate |
| Partition pruning | Possible if partitioned by phase |
| External visibility | Phase is directly visible in exported tables |

### Cons

| Disadvantage | Description |
|--------------|-------------|
| Schema impact | Canonical event schema becomes market-specific |
| Rule change cost | Rule updates require re-processing data |
| Ingestion overhead | Extra transform work during ingestion |

---

## Comparison Matrix

| Criteria | Option 1 (Query-Time, Explicit Primitives) | Option 2 (Ingestion-Time Storage) |
|----------|---------------------------------------------|-----------------------------------|
| Storage cost | Zero | ~1 byte/row (if Int8) |
| Query latency | Higher than persisted predicate | Lower |
| Ingestion speed | Faster | Slower |
| Schema impact | None | New column required |
| Rule flexibility | High | Low |
| Partition pruning | No | Yes (if partitioned) |
| v2 philosophy fit | Strong | Weaker |

---

## Migration Trigger to Option 2

Revisit Option 2 only if all are true:
- Profiling shows phase classification is a material bottleneck.
- Phase filtering appears in a majority of CN research workloads.
- Phase rules are stable enough to accept re-ingestion cost.

Until then, keep Option 1 as the default architecture.

---

## Validation Status

Implemented and covered by tests:
- `tests/v2/research/test_cn_trading_phases.py`
- `tests/v2/research/test_discovery_query.py`

Current contract is tested as:
- `load_events(...)` returns canonical events only.
- `filter_by_phase(...)` is called explicitly for phase slicing.

---

Document version: 2026-02-13
Decision status: Option 1 selected and implemented in v2
