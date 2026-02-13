# Chinese A-Share Trading Phase Design Options

## Context

Chinese A-share markets (SSE, SZSE) have unique trading phase structures that are **critical for research analysis**:

| Phase | Time | Exchange | Description |
|-------|------|----------|-------------|
| **Pre-opening** | 09:15-09:25 | Both | Opening auction (集合竞价), 9:20-9:25 no cancel |
| **Morning** | 09:30-11:30 | Both | Continuous auction (连续竞价) |
| **Noon Break** | 11:30-13:00 | Both | Market closed |
| **Afternoon** | 13:00-14:57 | Both | Continuous auction |
| **Closing** | 14:57-15:00 | SZSE only | Closing auction (收盘集合竞价) |
| **After-hours** | 15:05-15:30 | STAR/ChiNext only | Fixed-price trading (盘后固定价格) |

These phases affect:
- Price formation mechanisms
- Order matching rules
- Volume/liquidity patterns
- Volatility characteristics

Research workflows often need to **filter or compare across phases** (e.g., "opening auction vs continuous" price impact).

---

## Option 1: Query-Time Classification (Runtime)

Add a utility module that classifies timestamps into trading phases at query time.

### Implementation

```python
# pointline/v2/research/cn_trading_phases.py

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from datetime import datetime
from typing import Literal

import polars as pl


class TradingPhase(Enum):
    PRE_OPEN = auto()      # 09:15-09:25 (opening auction)
    MORNING = auto()       # 09:30-11:30 (continuous)
    NOON_BREAK = auto()    # 11:30-13:00 (closed)
    AFTERNOON = auto()     # 13:00-14:57 (continuous)
    CLOSING = auto()       # 14:57-15:00 (closing auction, SZSE)
    AFTER_HOURS = auto()   # 15:05-15:30 (STAR/ChiNext only)
    CLOSED = auto()        # All other times


@dataclass(frozen=True)
class PhaseConfig:
    """Exchange-specific phase rules."""
    has_closing_auction: bool
    has_after_hours: bool


# Exchange-specific configurations
CN_EXCHANGE_CONFIG: dict[str, PhaseConfig] = {
    "sse": PhaseConfig(has_closing_auction=False, has_after_hours=True),  # STAR Market
    "szse": PhaseConfig(has_closing_auction=True, has_after_hours=True),  # ChiNext
}


def classify_phase(
    ts_event_us: int,
    exchange: str,
    market_type: str | None = None,
) -> TradingPhase:
    """Classify timestamp into trading phase."""
    from pointline.v2.ingestion.timezone import exchange_tz

    tz = exchange_tz(exchange)
    dt = datetime.fromtimestamp(ts_event_us / 1_000_000, tz)
    time_val = dt.hour * 100 + dt.minute  # HHMM format

    config = CN_EXCHANGE_CONFIG.get(exchange.lower(), CN_EXCHANGE_CONFIG["szse"])

    if 915 <= time_val < 925:
        return TradingPhase.PRE_OPEN
    if 925 <= time_val < 930:
        return TradingPhase.CLOSED
    if 930 <= time_val < 1130:
        return TradingPhase.MORNING
    if 1130 <= time_val < 1300:
        return TradingPhase.NOON_BREAK
    if 1300 <= time_val < 1457:
        return TradingPhase.AFTERNOON
    if 1457 <= time_val < 1500:
        return TradingPhase.CLOSING if config.has_closing_auction else TradingPhase.CLOSED
    if 1505 <= time_val < 1530:
        if config.has_after_hours and market_type in ("star_board", "growth_board"):
            return TradingPhase.AFTER_HOURS
        return TradingPhase.CLOSED

    return TradingPhase.CLOSED


def add_phase_column(df: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """Add trading_phase column via map_elements."""
    def _classify(ts: int) -> str:
        return classify_phase(ts, exchange).name

    return df.with_columns(
        pl.col("ts_event_us")
        .map_elements(_classify, return_dtype=pl.Utf8)
        .alias("trading_phase")
    )


def filter_by_phase(
    df: pl.DataFrame,
    exchange: str,
    phases: list[TradingPhase],
) -> pl.DataFrame:
    """Filter DataFrame to include only specified trading phases."""
    df_with_phase = add_phase_column(df, exchange)
    phase_names = [p.name for p in phases]
    return df_with_phase.filter(pl.col("trading_phase").is_in(phase_names))
```

### Integration with Research API

```python
# In pointline/v2/research/query.py

def load_events(
    *,
    silver_root: Path,
    table: str,
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    columns: list[str] | None = None,
    include_lineage: bool = False,
    symbol_meta_columns: list[str] | None = None,
    # New parameter
    trading_phases: list[TradingPhase] | None = None,
) -> pl.DataFrame:
    """Load events with optional trading phase filtering."""
    # ... existing loading logic ...

    frame = lf.select(scan_cols).collect()

    # Apply phase filtering if requested
    if trading_phases is not None:
        from pointline.v2.research.cn_trading_phases import add_phase_column
        frame = add_phase_column(frame, exchange_norm)
        phase_names = [p.name for p in trading_phases]
        frame = frame.filter(pl.col("trading_phase").is_in(phase_names))

    # ... rest of logic ...
```

### Usage Example

```python
from pointline.v2.research import load_events
from pointline.v2.research.cn_trading_phases import TradingPhase

# Opening auction analysis
opening_trades = load_events(
    silver_root=Path("/data/silver"),
    table="trades",
    exchange="szse",
    symbol="000001",
    start="2024-01-15T09:15:00",
    end="2024-01-15T09:25:00",
    trading_phases=[TradingPhase.PRE_OPEN],
)

# Continuous trading only (exclude auctions)
continuous_trades = load_events(
    silver_root=Path("/data/silver"),
    table="trades",
    exchange="szse",
    symbol="000001",
    start="2024-01-15T09:30:00",
    end="2024-01-15T15:00:00",
    trading_phases=[TradingPhase.MORNING, TradingPhase.AFTERNOON],
)
```

### Pros

| Advantage | Description |
|-----------|-------------|
| **No schema change** | Events table schema remains unchanged |
| **No storage overhead** | Zero additional bytes stored per row |
| **Rule flexibility** | Phase rules can be updated without re-ingestion |
| **Backward compatible** | Existing queries continue to work unchanged |

### Cons

| Disadvantage | Description |
|--------------|-------------|
| **Query-time overhead** | UDF per row adds latency (estimated ~10-20% for large queries) |
| **Cannot partition by phase** | Cannot prune files by trading phase at scan time |
| **Repeated computation** | Same timestamp classified repeatedly across queries |

---

## Option 2: Ingestion-Time Storage (Schema Extension)

Add `trading_phase` as an integer column during ingestion, storing the phase classification permanently.

### Schema Change

```python
# pointline/schemas/events_cn.py

from pointline.schemas.types import ColumnSpec, TableSpec
import polars as pl


CN_TRADES = TableSpec(
    name="cn_trades",
    kind="event",
    column_specs=(
        # ... existing columns ...
        ColumnSpec("exchange", pl.Utf8),
        ColumnSpec("symbol", pl.Utf8),
        ColumnSpec("ts_event_us", pl.Int64),
        ColumnSpec("price", pl.Int64),
        ColumnSpec("qty", pl.Int64),
        # New column
        ColumnSpec("trading_phase", pl.Int8, nullable=True),
        # ...
    ),
    partition_by=("exchange", "trading_date"),
    business_keys=("exchange", "symbol", "ts_event_us"),
    tie_break_keys=("exchange", "symbol", "ts_event_us"),
    schema_version="v2",
)
```

### Ingestion Pipeline Update

```python
# In pointline/v2/vendors/quant360/canonicalize.py or pipeline.py

from pointline.v2.research.cn_trading_phases import classify_phase, TradingPhase


def canonicalize_trades(
    raw_df: pl.DataFrame,
    exchange: str,
    market_type: str | None = None,
) -> pl.DataFrame:
    """Convert raw trades to canonical v2 format with phase classification."""
    # ... existing canonicalization ...

    # Add trading phase at ingestion time
    df = df.with_columns(
        pl.col("ts_event_us")
        .map_elements(
            lambda ts: classify_phase(ts, exchange, market_type).value,
            return_dtype=pl.Int8,
        )
        .alias("trading_phase")
    )

    return df
```

### Query Optimization

With phase stored, queries become simple filter predicates:

```python
# Direct filter without UDF
lf = pl.scan_delta(path).filter(
    (pl.col("exchange") == "szse")
    & (pl.col("trading_phase") == TradingPhase.PRE_OPEN.value)  # Fast predicate pushdown
)
```

### Optional: Partition by Phase

For large datasets, partition layout can include phase:

```
silver/cn_trades/
  exchange=szse/
    trading_date=20240115/
      trading_phase=1/     # PRE_OPEN
        part-00001.parquet
      trading_phase=2/     # MORNING
        part-00001.parquet
```

This enables **partition pruning** — only read relevant phase files.

### Pros

| Advantage | Description |
|-----------|-------------|
| **Query performance** | Simple integer comparison, no UDF overhead |
| **Partition pruning** | Can skip entire partitions when filtering by phase |
| **Explicit in data** | Phase is visible in raw data exports / external tools |
| **Reproducible** | Same classification applied consistently at ingestion |

### Cons

| Disadvantage | Description |
|--------------|-------------|
| **Schema change required** | All event tables need new column |
| **Storage overhead** | +1 byte per row (Int8) — negligible for typical datasets |
| **Ingestion overhead** | ~5-10% slower ingestion due to classification UDF |
| **Rule changes need re-ingestion** | If exchange changes phase rules, data must be re-processed |
| **Not applicable to all markets** | Crypto/FX don't have these phases — column would be null |

---

## Comparison Matrix

| Criteria | Option 1 (Query-Time) | Option 2 (Ingestion-Time) |
|----------|----------------------|---------------------------|
| **Storage cost** | Zero | ~1 byte/row |
| **Query latency** | Higher (UDF) | Lower (predicate) |
| **Ingestion speed** | Faster | Slower (~5-10%) |
| **Schema impact** | None | New column required |
| **Rule flexibility** | High | Low (requires re-ingest) |
| **Partition pruning** | No | Yes (if partitioned) |
| **Cross-tool visibility** | No | Yes |
| **Implementation effort** | Low | Medium (schema + pipeline) |

---

## Recommendations

### Choose Option 1 (Query-Time) If:

- We want to **avoid schema changes** in the short term
- Phase rules are **still evolving** or exchange-specific exceptions are common
- Query latency increase is **acceptable** for research workloads
- We prioritize **ingestion throughput** over query speed

### Choose Option 2 (Ingestion-Time) If:

- Phase classification is **fundamental** to downstream analysis (very likely for China A-share research)
- We need **sub-second query latency** on large datasets
- Phase-based **partition pruning** would significantly reduce I/O
- Phase rules are **stable** and unlikely to change

### Hybrid Recommendation

**Start with Option 1**, validate usage patterns and performance. **Migrate to Option 2** if:
- Query profiling shows phase classification is a bottleneck
- Researchers consistently filter by phase (evidence from query logs)
- Dataset size grows to 100M+ rows per symbol

This approach allows rapid iteration without schema migration risk, while keeping the door open for optimization.

---

## Decision Checklist

- [ ] Review query latency requirements with research team
- [ ] Benchmark Option 1 on representative dataset (e.g., 1 month of SZSE L2)
- [ ] Confirm phase rule stability with market data vendor
- [ ] Evaluate partition pruning benefits for typical query patterns
- [ ] Decide before production rollout (schema changes are harder post-launch)

---

## Appendix: Trading Phase Enum Values (Int8)

```python
class TradingPhase(Enum):
    PRE_OPEN = 1      # 09:15-09:25
    MORNING = 2       # 09:30-11:30
    NOON_BREAK = 3    # 11:30-13:00
    AFTERNOON = 4     # 13:00-14:57
    CLOSING = 5       # 14:57-15:00 (SZSE)
    AFTER_HOURS = 6   # 15:05-15:30 (STAR/ChiNext)
    CLOSED = 0        # All other times
```

---

*Document version: 2024-02-13*
*Decision pending: See checklist above*
