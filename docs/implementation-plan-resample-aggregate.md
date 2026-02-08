# Implementation Plan: Resample-Aggregate Design with Spine Integration

**Status:** Proposed
**Date:** 2026-02-07
**Target Completion:** 7 weeks
**Backward Compatibility:** Not required (clean slate)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Phase 0: Spine Protocol & Foundation](#phase-0-spine-protocol--foundation-week-1)
3. [Phase 1: Registry System](#phase-1-registry-system-week-1-2)
4. [Phase 2: Bucket Assignment & Aggregation](#phase-2-bucket-assignment--aggregation-week-2-3)
5. [Phase 3: Custom Aggregations](#phase-3-custom-aggregations-week-3-4)
6. [Phase 4: Pipeline Orchestration](#phase-4-pipeline-orchestration-with-spines-week-4-5)
7. [Phase 5: Validation & Observability](#phase-5-validation--observability-week-5-6)
8. [Phase 6: Integration & Documentation](#phase-6-integration--documentation-week-6-7)
9. [Testing Strategy](#testing-strategy)
10. [Rollout Checklist](#rollout-checklist)
11. [Success Criteria](#success-criteria)

---

## Architecture Overview

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 0: Spine Building (Bucket Assignment Strategy)       │
├─────────────────────────────────────────────────────────────┤
│ SpineBuilder Protocol:                                      │
│ ├─ ClockSpineBuilder (time intervals)                       │
│ ├─ VolumeSpineBuilder (volume thresholds)                   │
│ ├─ DollarSpineBuilder (notional thresholds)                 │
│ └─ TradeSpineBuilder (trade count)                          │
│                                                              │
│ Output: LazyFrame[ts_local_us, exchange_id, symbol_id]      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     v
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Bucket Assignment                                  │
├─────────────────────────────────────────────────────────────┤
│ Assign data to spine buckets via as-of join                │
│ Strategy: backward (PIT correctness)                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     v
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Aggregation (Registry-Driven)                     │
├─────────────────────────────────────────────────────────────┤
│ Apply registered aggregations by mode                       │
│ ├─ Pattern A: aggregate_then_feature                        │
│ └─ Pattern B: feature_then_aggregate                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     v
┌─────────────────────────────────────────────────────────────┐
│ Phase 3: Feature Computation                               │
├─────────────────────────────────────────────────────────────┤
│ Mode-specific feature computation on aggregated bars        │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Spine as Foundation**: Spines define bucket boundaries for all resampling
2. **Contract-First**: Explicit config schema with validation before execution
3. **PIT Correctness**: Backward-only joins, bar timestamp = spine boundary
4. **Registry-Based**: Custom aggregations registered with semantic policies
5. **Three Pipeline Modes**: event_joined, tick_then_bar, bar_then_feature

### Core Concepts

**Spine**: Defines bucket boundaries (time points) for resampling
- Clock spines: Regular intervals (1m, 5m, 1h)
- Event-driven spines: Irregular intervals (volume, dollar, trade thresholds)

**Bucket Assignment**: As-of join to assign data points to spine buckets
- Strategy: `backward` (PIT correctness)
- Result: Each data point gets `bucket_ts` from nearest prior spine point

**Aggregation Patterns**:
- **Pattern A** (aggregate_then_feature): Aggregate raw values, then compute features
  - Example: `mean(price)` → convert to bps
- **Pattern B** (feature_then_aggregate): Compute features on ticks, then aggregate
  - Example: Compute `spread_bps` on each tick → `mean(spread_bps)`, `std(spread_bps)`

**Pipeline Modes**:
1. **event_joined**: Build event timeline via backward as-of joins (HFT)
2. **tick_then_bar**: Compute microfeatures at tick level, then aggregate (microstructure)
3. **bar_then_feature**: Aggregate to bars first, then features (production MFT)

---

## Phase 0: Spine Protocol & Foundation (Week 1)

### Goal
Define spine protocol and preserve existing spine builders

### Deliverables

#### 1. Spine Protocol Definition

**File**: `pointline/research/spine/protocol.py`

```python
from typing import Protocol
import polars as pl

class SpineBuilder(Protocol):
    """Protocol for all spine builders.

    A spine defines bucket boundaries for resampling data.
    Implementations can be clock-based (regular intervals) or
    event-driven (volume/dollar/trade thresholds).
    """

    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        *,
        exchange_id: int | None = None,
    ) -> pl.LazyFrame:
        """Build spine with bucket boundaries.

        Returns LazyFrame with required columns:
        - ts_local_us: Bucket boundary timestamp (spine point)
        - exchange_id: Exchange identifier
        - symbol_id: Symbol identifier

        Timestamp semantics (from bar-aggregation.md):
        - Bar timestamp = spine boundary = end of bar window
        - Bar window = [T_prev, T_current) (half-open interval)
        - PIT guarantee: all data in bar has ts_local_us < T_current
        """
        ...

    @property
    def spine_type(self) -> str:
        """Spine type identifier (e.g., 'clock', 'volume', 'dollar')."""
        ...

    def describe(self) -> dict:
        """Return human-readable configuration."""
        ...
```

#### 2. Spine Configuration Classes

**File**: `pointline/research/spine/config.py`

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class SpineConfig:
    """Base configuration for all spines."""
    symbol_id: int
    start_ts_us: int
    end_ts_us: int
    exchange_id: int | None = None

@dataclass(frozen=True)
class ClockSpineConfig(SpineConfig):
    """Clock-based spine configuration."""
    every: str  # "1m", "5m", "1h", "1d"
    offset: str | None = None  # Offset from epoch (e.g., "30s")

@dataclass(frozen=True)
class VolumeSpineConfig(SpineConfig):
    """Volume bar spine configuration."""
    volume_threshold: float  # BTC, ETH, contracts, etc.

@dataclass(frozen=True)
class DollarSpineConfig(SpineConfig):
    """Dollar bar spine configuration."""
    dollar_threshold: float  # USD notional

@dataclass(frozen=True)
class TradeSpineConfig(SpineConfig):
    """Trade bar spine configuration."""
    trade_count: int  # Number of trades per bar
```

#### 3. ClockSpineBuilder (NEW)

**File**: `pointline/research/spine/builders/clock.py`

```python
class ClockSpineBuilder:
    """Regular time interval spine."""

    def __init__(self, every: str, offset: str | None = None):
        self.every = every
        self.offset = offset
        self.every_us = self._parse_duration(every)
        self.offset_us = self._parse_duration(offset) if offset else 0

    @property
    def spine_type(self) -> str:
        return "clock"

    def describe(self) -> dict:
        return {
            "type": "clock",
            "every": self.every,
            "offset": self.offset,
        }

    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        *,
        exchange_id: int | None = None,
    ) -> pl.LazyFrame:
        """Generate regular time intervals."""

        # Align start to grid with offset
        grid_start = self._align_to_grid(start_ts_us, self.every_us, self.offset_us)

        # Generate timestamps (bar boundaries = interval ends)
        timestamps = list(range(grid_start, end_ts_us + self.every_us, self.every_us))

        # Resolve exchange_id if needed
        if exchange_id is None:
            from pointline import registry
            symbol_info = registry.resolve_symbol(symbol_id)
            exchange_id = symbol_info["exchange_id"]

        return pl.LazyFrame({
            "ts_local_us": timestamps,
            "exchange_id": [exchange_id] * len(timestamps),
            "symbol_id": [symbol_id] * len(timestamps),
        })

    def _align_to_grid(self, ts: int, every_us: int, offset_us: int) -> int:
        """Align timestamp to grid."""
        return ((ts - offset_us) // every_us) * every_us + offset_us + every_us

    def _parse_duration(self, duration: str) -> int:
        """Parse duration string to microseconds.

        Examples:
            "1s" -> 1_000_000
            "1m" -> 60_000_000
            "5m" -> 300_000_000
            "1h" -> 3_600_000_000
            "1d" -> 86_400_000_000
        """
        import re

        if duration is None:
            return 0

        match = re.match(r'^(\d+)([smhd])$', duration)
        if not match:
            raise ValueError(f"Invalid duration: {duration}")

        value = int(match.group(1))
        unit = match.group(2)

        multipliers = {
            's': 1_000_000,
            'm': 60_000_000,
            'h': 3_600_000_000,
            'd': 86_400_000_000,
        }

        return value * multipliers[unit]
```

#### 4. VolumeSpineBuilder (MIGRATED)

**File**: `pointline/research/spine/builders/volume.py`

```python
class VolumeSpineBuilder:
    """Volume bar spine.

    MIGRATED: From existing pointline spine system.
    Now implements SpineBuilder protocol.
    """

    def __init__(self, volume_threshold: float):
        self.volume_threshold = volume_threshold

    @property
    def spine_type(self) -> str:
        return "volume"

    def describe(self) -> dict:
        return {
            "type": "volume",
            "threshold": self.volume_threshold,
        }

    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        *,
        exchange_id: int | None = None,
    ) -> pl.LazyFrame:
        """Build spine from cumulative volume threshold crossings.

        Spine timestamps = when cumulative volume crosses thresholds.
        These are the bar boundary timestamps (interval ends).
        """
        from pointline import research

        # Load trades
        trades = research.load_trades(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
        )

        # Decode quantities
        from pointline.tables.trades import decode_fixed_point
        from pointline.dim_symbol import read_dim_symbol_table
        dim_symbol = read_dim_symbol_table()
        trades = decode_fixed_point(trades, dim_symbol)

        # Compute cumulative volume and detect threshold crossings
        spine = (
            trades
            .sort(["exchange_id", "symbol_id", "ts_local_us"])
            .with_columns([
                # Cumulative volume
                pl.col("qty").abs().cum_sum()
                .over(["exchange_id", "symbol_id"])
                .alias("cum_volume"),

                # Bucket ID (which threshold crossing)
                (pl.col("qty").abs().cum_sum().over(["exchange_id", "symbol_id"])
                 / self.volume_threshold)
                .floor().cast(pl.Int64).alias("bucket_id"),
            ])
            .group_by(["exchange_id", "symbol_id", "bucket_id"])
            .agg([
                # Bar timestamp = last trade timestamp that crossed threshold
                pl.col("ts_local_us").max().alias("ts_local_us"),
            ])
            .sort(["exchange_id", "symbol_id", "ts_local_us"])
            .select(["ts_local_us", "exchange_id", "symbol_id"])
        )

        return spine
```

#### 5. DollarSpineBuilder (MIGRATED)

**File**: `pointline/research/spine/builders/dollar.py`

```python
class DollarSpineBuilder:
    """Dollar bar spine.

    MIGRATED: From existing system.
    Similar to VolumeSpineBuilder but uses notional (price * qty).
    """

    def __init__(self, dollar_threshold: float):
        self.dollar_threshold = dollar_threshold

    @property
    def spine_type(self) -> str:
        return "dollar"

    def describe(self) -> dict:
        return {
            "type": "dollar",
            "threshold": self.dollar_threshold,
        }

    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        *,
        exchange_id: int | None = None,
    ) -> pl.LazyFrame:
        """Build spine from cumulative dollar volume threshold crossings."""
        from pointline import research
        from pointline.tables.trades import decode_fixed_point
        from pointline.dim_symbol import read_dim_symbol_table

        # Load and decode trades
        trades = research.load_trades(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
        )
        dim_symbol = read_dim_symbol_table()
        trades = decode_fixed_point(trades, dim_symbol)

        # Compute notional and cumulative dollar volume
        spine = (
            trades
            .with_columns([
                (pl.col("px") * pl.col("qty").abs()).alias("notional"),
            ])
            .with_columns([
                pl.col("notional").cum_sum()
                .over(["exchange_id", "symbol_id"])
                .alias("cum_notional"),

                (pl.col("notional").cum_sum().over(["exchange_id", "symbol_id"])
                 / self.dollar_threshold)
                .floor().cast(pl.Int64).alias("bucket_id"),
            ])
            .group_by(["exchange_id", "symbol_id", "bucket_id"])
            .agg([
                pl.col("ts_local_us").max().alias("ts_local_us"),
            ])
            .sort(["exchange_id", "symbol_id", "ts_local_us"])
            .select(["ts_local_us", "exchange_id", "symbol_id"])
        )

        return spine
```

#### 6. TradeSpineBuilder (MIGRATED)

**File**: `pointline/research/spine/builders/trade.py`

```python
class TradeSpineBuilder:
    """Trade bar spine (every N trades).

    MIGRATED: From existing system.
    """

    def __init__(self, trade_count: int):
        self.trade_count = trade_count

    @property
    def spine_type(self) -> str:
        return "trade"

    def describe(self) -> dict:
        return {
            "type": "trade",
            "count": self.trade_count,
        }

    def build_spine(
        self,
        symbol_id: int,
        start_ts_us: int,
        end_ts_us: int,
        *,
        exchange_id: int | None = None,
    ) -> pl.LazyFrame:
        """Build spine from every Nth trade."""
        from pointline import research

        trades = research.load_trades(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
        )

        # Every Nth trade becomes a spine point
        spine = (
            trades
            .sort(["exchange_id", "symbol_id", "ts_local_us"])
            .with_columns([
                pl.col("ts_local_us").cum_count()
                .over(["exchange_id", "symbol_id"])
                .alias("trade_num"),
            ])
            .filter(pl.col("trade_num") % self.trade_count == 0)
            .select(["ts_local_us", "exchange_id", "symbol_id"])
        )

        return spine
```

### Testing Requirements

**File**: `tests/research/spine/test_spine_builders.py`

```python
import pytest
import polars as pl
from pointline.research.spine.builders import (
    ClockSpineBuilder,
    VolumeSpineBuilder,
    DollarSpineBuilder,
    TradeSpineBuilder,
)

def test_clock_spine_protocol_compliance():
    """Verify ClockSpineBuilder implements protocol."""
    builder = ClockSpineBuilder(every="1m")

    assert hasattr(builder, 'build_spine')
    assert hasattr(builder, 'spine_type')
    assert hasattr(builder, 'describe')
    assert builder.spine_type == "clock"

def test_clock_spine_grid_alignment():
    """Verify clock spine aligns to grid correctly."""
    builder = ClockSpineBuilder(every="1m")

    # Build spine
    spine = builder.build_spine(
        symbol_id=12345,
        start_ts_us=0,
        end_ts_us=300_000_000,  # 5 minutes
        exchange_id=1,
    ).collect()

    # Should have 5 boundaries (1m, 2m, 3m, 4m, 5m)
    assert len(spine) == 5

    # Timestamps should be at 1-minute intervals
    timestamps = spine["ts_local_us"].to_list()
    assert timestamps == [60_000_000, 120_000_000, 180_000_000, 240_000_000, 300_000_000]

def test_clock_spine_offset():
    """Verify clock spine offset works correctly."""
    builder = ClockSpineBuilder(every="1m", offset="30s")

    spine = builder.build_spine(
        symbol_id=12345,
        start_ts_us=0,
        end_ts_us=300_000_000,
        exchange_id=1,
    ).collect()

    # First boundary should be at 30s + 1m = 90s
    assert spine["ts_local_us"][0] == 90_000_000

def test_volume_spine_threshold_crossings():
    """Verify volume spine detects threshold crossings."""
    # TODO: Implement with synthetic trade data
    pass

def test_dollar_spine_notional():
    """Verify dollar spine uses notional correctly."""
    # TODO: Implement with synthetic trade data
    pass

def test_trade_spine_count():
    """Verify trade spine counts trades correctly."""
    # TODO: Implement with synthetic trade data
    pass

def test_spine_timestamp_semantics():
    """Verify spine timestamps = bar boundaries (interval ends)."""
    # TODO: Verify all builders produce timestamps at interval END
    pass
```

### Files Created

```
pointline/research/spine/
├── __init__.py
├── protocol.py              # SpineBuilder protocol
├── config.py                # Spine configuration classes
└── builders/
    ├── __init__.py
    ├── clock.py             # ClockSpineBuilder (NEW)
    ├── volume.py            # VolumeSpineBuilder (MIGRATED)
    ├── dollar.py            # DollarSpineBuilder (MIGRATED)
    └── trade.py             # TradeSpineBuilder (MIGRATED)

tests/research/spine/
├── __init__.py
└── test_spine_builders.py   # Protocol and correctness tests
```

### Acceptance Criteria

- [ ] SpineBuilder protocol defined with required methods
- [ ] All config classes frozen and immutable
- [ ] ClockSpineBuilder passes grid alignment tests
- [ ] All spine builders implement protocol correctly
- [ ] Timestamp semantics validated (bar timestamp = interval end)
- [ ] Duration parsing handles s/m/h/d units
- [ ] All spine builders return correct schema (ts_local_us, exchange_id, symbol_id)

---

## Phase 1: Registry System (Week 1-2)

### Goal
Build typed aggregation registry with semantic policies

### Deliverables

#### 1. Registry Core

**File**: `pointline/research/resample/registry.py`

```python
from dataclasses import dataclass
from typing import Callable, Literal
import polars as pl

@dataclass(frozen=True)
class AggregationMetadata:
    """Registry entry metadata."""
    name: str
    stage: Literal["feature_then_aggregate", "aggregate_then_feature", "hybrid"]
    semantic_type: str  # "price", "size", "notional", "event_id", "state_variable"
    mode_allowlist: list[str]  # ["HFT", "MFT", "LFT"]
    required_columns: list[str]
    pit_policy: dict[str, str]
    determinism: dict[str, list[str]]
    impl: Callable

class AggregationRegistry:
    """Global registry for aggregations."""

    _registry: dict[str, AggregationMetadata] = {}
    _profiles: dict[str, set[str]] = {
        "hft_default": {"sum", "mean", "last", "count", "microprice_close", "ofi_cont"},
        "mft_default": {"sum", "mean", "std", "last", "count", "spread_distribution"},
        "lft_default": {"sum", "mean", "last", "count"},
    }

    @classmethod
    def register(
        cls,
        name: str,
        *,
        stage: str,
        semantic_type: str,
        mode_allowlist: list[str],
        required_columns: list[str],
        pit_policy: dict | None = None,
    ):
        """Decorator to register aggregations."""
        def decorator(func: Callable):
            metadata = AggregationMetadata(
                name=name,
                stage=stage,
                semantic_type=semantic_type,
                mode_allowlist=mode_allowlist,
                required_columns=required_columns,
                pit_policy=pit_policy or {"feature_direction": "backward_only"},
                determinism={"required_sort": ["exchange_id", "symbol_id", "ts_local_us"]},
                impl=func,
            )
            cls._registry[name] = metadata
            return func
        return decorator

    @classmethod
    def get(cls, name: str) -> AggregationMetadata:
        """Retrieve aggregation by name."""
        if name not in cls._registry:
            raise ValueError(f"Aggregation {name} not registered")
        return cls._registry[name]

    @classmethod
    def validate_for_mode(cls, name: str, mode: str) -> None:
        """Validate aggregation is allowed for mode."""
        meta = cls.get(name)
        if mode not in meta.mode_allowlist:
            raise ValueError(f"{name} not allowed in {mode} mode")

    @classmethod
    def get_profile(cls, profile: str) -> set[str]:
        """Get aggregation set for profile."""
        if profile not in cls._profiles:
            raise ValueError(f"Profile {profile} not found")
        return cls._profiles[profile]

    @classmethod
    def list_aggregations(cls) -> list[str]:
        """List all registered aggregations."""
        return list(cls._registry.keys())

# Semantic type policies
SEMANTIC_POLICIES = {
    "price": {
        "allowed_aggs": ["last", "mean", "min", "max"],
        "forbidden_aggs": ["sum"],
    },
    "size": {
        "allowed_aggs": ["sum", "mean", "std", "max"],
        "forbidden_aggs": [],
    },
    "notional": {
        "allowed_aggs": ["sum", "mean", "std", "max"],
        "forbidden_aggs": [],
    },
    "event_id": {
        "allowed_aggs": ["count", "nunique", "last"],
        "forbidden_aggs": ["sum", "mean"],
    },
    "state_variable": {
        "allowed_aggs": ["last", "mean", "diff"],
        "forbidden_aggs": ["sum"],
    },
}
```

#### 2. Built-in Aggregations

**File**: `pointline/research/resample/builtins.py`

```python
from pointline.research.resample.registry import AggregationRegistry
import polars as pl

# Numeric aggregations
@AggregationRegistry.register(
    name="sum",
    stage="aggregate_then_feature",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_sum(source_col: str) -> pl.Expr:
    return pl.col(source_col).sum()

@AggregationRegistry.register(
    name="mean",
    stage="aggregate_then_feature",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_mean(source_col: str) -> pl.Expr:
    return pl.col(source_col).mean()

@AggregationRegistry.register(
    name="std",
    stage="aggregate_then_feature",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_std(source_col: str) -> pl.Expr:
    return pl.col(source_col).std()

@AggregationRegistry.register(
    name="min",
    stage="aggregate_then_feature",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_min(source_col: str) -> pl.Expr:
    return pl.col(source_col).min()

@AggregationRegistry.register(
    name="max",
    stage="aggregate_then_feature",
    semantic_type="size",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_max(source_col: str) -> pl.Expr:
    return pl.col(source_col).max()

@AggregationRegistry.register(
    name="last",
    stage="aggregate_then_feature",
    semantic_type="state_variable",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_last(source_col: str) -> pl.Expr:
    return pl.col(source_col).last()

@AggregationRegistry.register(
    name="first",
    stage="aggregate_then_feature",
    semantic_type="state_variable",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_first(source_col: str) -> pl.Expr:
    return pl.col(source_col).first()

@AggregationRegistry.register(
    name="count",
    stage="aggregate_then_feature",
    semantic_type="event_id",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_count(source_col: str) -> pl.Expr:
    return pl.col(source_col).count()

@AggregationRegistry.register(
    name="nunique",
    stage="aggregate_then_feature",
    semantic_type="event_id",
    mode_allowlist=["HFT", "MFT", "LFT"],
    required_columns=[],
)
def agg_nunique(source_col: str) -> pl.Expr:
    return pl.col(source_col).n_unique()
```

### Testing Requirements

**File**: `tests/research/resample/test_registry.py`

```python
import pytest
from pointline.research.resample.registry import AggregationRegistry, SEMANTIC_POLICIES

def test_registry_registration():
    """Test aggregation registration."""
    assert "sum" in AggregationRegistry._registry
    assert "mean" in AggregationRegistry._registry

def test_registry_get():
    """Test retrieving aggregation metadata."""
    meta = AggregationRegistry.get("sum")
    assert meta.name == "sum"
    assert meta.stage == "aggregate_then_feature"
    assert meta.semantic_type == "size"

def test_registry_mode_validation():
    """Test mode allowlist validation."""
    # sum is allowed in all modes
    AggregationRegistry.validate_for_mode("sum", "HFT")
    AggregationRegistry.validate_for_mode("sum", "MFT")

def test_registry_profile():
    """Test profile loading."""
    hft_aggs = AggregationRegistry.get_profile("hft_default")
    assert "sum" in hft_aggs
    assert "mean" in hft_aggs

def test_semantic_policy_enforcement():
    """Test semantic type policy enforcement."""
    price_policy = SEMANTIC_POLICIES["price"]

    # sum not allowed for price
    assert "sum" in price_policy["forbidden_aggs"]

    # mean allowed for price
    assert "mean" in price_policy["allowed_aggs"]
```

### Files Created

```
pointline/research/resample/
├── __init__.py
├── registry.py              # AggregationRegistry core
└── builtins.py              # Built-in aggregations

tests/research/resample/
├── __init__.py
└── test_registry.py         # Registry tests
```

### Acceptance Criteria

- [ ] Registry can register and retrieve aggregations
- [ ] Mode validation enforces allowlists
- [ ] Semantic policies defined for all types
- [ ] All built-in aggregations registered
- [ ] Profiles loaded correctly
- [ ] Registry tests passing

---

## Phase 2: Bucket Assignment & Aggregation (Week 2-3)

### Goal
Integrate spines with aggregation framework

### Deliverables

#### 1. Bucket Assignment

**File**: `pointline/research/resample/bucket_assignment.py`

```python
import polars as pl

def assign_to_buckets(
    data: pl.LazyFrame,
    spine: pl.LazyFrame,
    *,
    deterministic: bool = True,
) -> pl.LazyFrame:
    """Assign each data point to its bucket via as-of join.

    This is the KEY integration between spines and aggregation.

    Uses bar-aggregation.md timestamp semantics:
    - Each tick assigned to most recent spine boundary
    - Bar at timestamp T contains all ticks with ts < T
    - PIT correctness guaranteed by backward join

    Args:
        data: Raw data (trades, quotes, book, etc.)
        spine: Spine with bucket boundaries
        deterministic: Enforce deterministic sort order

    Returns:
        Data with spine timestamps joined (bucket assignment)
    """

    # Step 1: Validate columns
    _validate_bucket_assignment(data, spine)

    # Step 2: Enforce deterministic ordering
    if deterministic:
        data = _enforce_deterministic_sort(data)
        spine = _enforce_deterministic_sort(spine)

    # Step 3: As-of join (backward strategy for PIT correctness)
    bucketed = data.join_asof(
        spine.select(["ts_local_us", "exchange_id", "symbol_id"]).rename({
            "ts_local_us": "bucket_ts"
        }),
        left_on="ts_local_us",
        right_on="bucket_ts",
        by=["exchange_id", "symbol_id"],
        strategy="backward",
    )

    return bucketed

def _validate_bucket_assignment(data: pl.LazyFrame, spine: pl.LazyFrame) -> None:
    """Validate required columns for bucket assignment."""
    required = {"ts_local_us", "exchange_id", "symbol_id"}

    data_cols = set(data.columns)
    spine_cols = set(spine.columns)

    if not required.issubset(data_cols):
        raise ValueError(f"Data missing required columns: {required - data_cols}")

    if not required.issubset(spine_cols):
        raise ValueError(f"Spine missing required columns: {required - spine_cols}")

def _enforce_deterministic_sort(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Enforce deterministic sort order for PIT correctness."""
    sort_cols = ["exchange_id", "symbol_id", "ts_local_us"]

    # Add tie-breakers if available
    if "file_id" in lf.columns:
        sort_cols.append("file_id")
    if "file_line_number" in lf.columns:
        sort_cols.append("file_line_number")

    return lf.sort(sort_cols)
```

#### 2. Aggregation Configuration

**File**: `pointline/research/resample/config.py`

```python
from dataclasses import dataclass, field
from typing import Literal

@dataclass(frozen=True)
class AggregationSpec:
    """Single aggregation specification."""
    name: str  # Output column name
    source_column: str
    agg: str  # "sum", "mean", "last", or custom aggregation name
    semantic_type: str | None = None  # For validation

@dataclass(frozen=True)
class AggregateConfig:
    """Contract for aggregation operations."""
    by: list[str]
    aggregations: list[AggregationSpec]
    mode: Literal["event_joined", "tick_then_bar", "bar_then_feature"]
    registry_profile: str = "default"
```

#### 3. Aggregation Implementation

**File**: `pointline/research/resample/aggregate.py`

```python
import polars as pl
from pointline.research.resample.config import AggregateConfig, AggregationSpec
from pointline.research.resample.registry import AggregationRegistry, SEMANTIC_POLICIES

def aggregate(
    bucketed_data: pl.LazyFrame,
    config: AggregateConfig,
    *,
    spine: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Apply aggregations to bucketed data.

    Args:
        bucketed_data: Data with bucket_ts column from assign_to_buckets()
        config: Aggregation configuration with registry validation
        spine: Optional spine for left join preservation

    Returns:
        Aggregated bars (one row per bucket)
    """

    # Step 1: Pre-execution validation
    _validate_aggregate_config(bucketed_data, config)

    # Step 2: Separate aggregations by stage
    stage_1_aggs = []  # aggregate_then_feature (Pattern A)
    stage_2_aggs = []  # feature_then_aggregate (Pattern B)

    for spec in config.aggregations:
        if spec.agg in AggregationRegistry._registry:
            meta = AggregationRegistry.get(spec.agg)
            AggregationRegistry.validate_for_mode(spec.agg, config.mode)

            if meta.stage == "aggregate_then_feature":
                stage_1_aggs.append((spec, meta))
            else:
                stage_2_aggs.append((spec, meta))
        else:
            # Built-in Polars aggregation (Pattern A by default)
            stage_1_aggs.append((spec, None))

    # Step 3: Apply Pattern A (aggregate raw values)
    agg_exprs = []
    for spec, meta in stage_1_aggs:
        if meta:
            expr = meta.impl(spec.source_column)
        else:
            expr = _build_builtin_agg_expr(spec)
        agg_exprs.append(expr.alias(spec.name))

    result = (
        bucketed_data
        .group_by(config.by)
        .agg(agg_exprs)
    )

    # Step 4: Apply Pattern B (compute features then aggregate)
    if stage_2_aggs:
        # Compute features on original data
        feature_data = bucketed_data
        for spec, meta in stage_2_aggs:
            feature_data = meta.impl(feature_data, spec)

        # Aggregate computed features (mean, std, min, max, etc.)
        feature_agg_exprs = []
        for spec, meta in stage_2_aggs:
            feature_col = f"_{spec.name}_feature"

            # Standard distribution statistics for Pattern B
            feature_agg_exprs.extend([
                pl.col(feature_col).mean().alias(f"{spec.name}_mean"),
                pl.col(feature_col).std().alias(f"{spec.name}_std"),
                pl.col(feature_col).min().alias(f"{spec.name}_min"),
                pl.col(feature_col).max().alias(f"{spec.name}_max"),
            ])

        feature_result = (
            feature_data
            .group_by(config.by)
            .agg(feature_agg_exprs)
        )

        # Join Pattern A and Pattern B results
        result = result.join(
            feature_result,
            on=config.by,
            how="left",
        )

    # Step 5: Left join with spine to preserve all spine points
    if spine is not None:
        result = spine.join(
            result,
            left_on=["exchange_id", "symbol_id", "ts_local_us"],
            right_on=config.by,
            how="left",
        )

    return result

def _validate_aggregate_config(bucketed_data: pl.LazyFrame, config: AggregateConfig) -> None:
    """Pre-execution validation."""

    # Check bucket_ts or grouping columns exist
    for col in config.by:
        if col not in bucketed_data.columns:
            raise ValueError(f"Grouping column {col} not found in data")

    # Validate each aggregation
    for spec in config.aggregations:
        # Check semantic type compatibility
        if spec.semantic_type:
            _validate_semantic_type(spec.agg, spec.semantic_type)

        # Check required columns for custom aggregations
        if spec.agg in AggregationRegistry._registry:
            meta = AggregationRegistry.get(spec.agg)
            for col in meta.required_columns:
                if col not in bucketed_data.columns:
                    raise ValueError(
                        f"Required column {col} not found for {spec.agg}"
                    )

        # Check source column exists
        if spec.source_column and spec.source_column not in bucketed_data.columns:
            raise ValueError(f"Source column {spec.source_column} not found")

def _validate_semantic_type(agg: str, semantic_type: str) -> None:
    """Validate aggregation allowed for semantic type."""
    policy = SEMANTIC_POLICIES.get(semantic_type, {})

    if agg in policy.get("forbidden_aggs", []):
        raise ValueError(
            f"Aggregation {agg} not allowed for semantic type {semantic_type}"
        )

    allowed = policy.get("allowed_aggs", [])
    if allowed and agg not in allowed:
        raise ValueError(
            f"Aggregation {agg} not in allowlist for {semantic_type}"
        )

def _build_builtin_agg_expr(spec: AggregationSpec) -> pl.Expr:
    """Build Polars expression for built-in aggregations."""
    col = pl.col(spec.source_column)

    match spec.agg:
        case "sum": return col.sum()
        case "mean": return col.mean()
        case "std": return col.std()
        case "min": return col.min()
        case "max": return col.max()
        case "last": return col.last()
        case "first": return col.first()
        case "count": return col.count()
        case "nunique": return col.n_unique()
        case _: raise ValueError(f"Unknown aggregation: {spec.agg}")
```

### Testing Requirements

**File**: `tests/research/resample/test_bucket_assignment.py`

```python
import pytest
import polars as pl
from pointline.research.resample.bucket_assignment import assign_to_buckets
from pointline.research.spine.builders import ClockSpineBuilder

def test_bucket_assignment_backward_join():
    """Test bucket assignment uses backward join."""
    # Create spine (1-minute boundaries)
    spine = pl.LazyFrame({
        "ts_local_us": [60_000_000, 120_000_000, 180_000_000],
        "exchange_id": [1, 1, 1],
        "symbol_id": [12345, 12345, 12345],
    })

    # Create data
    data = pl.LazyFrame({
        "ts_local_us": [50_000_000, 110_000_000, 170_000_000],
        "exchange_id": [1, 1, 1],
        "symbol_id": [12345, 12345, 12345],
        "value": [100, 200, 300],
    })

    # Assign to buckets
    bucketed = assign_to_buckets(data, spine).collect()

    # First data point (50s) should be in first bucket (60s)
    assert bucketed["bucket_ts"][0] == 60_000_000
    # Second data point (110s) should be in second bucket (120s)
    assert bucketed["bucket_ts"][1] == 120_000_000
    # Third data point (170s) should be in third bucket (180s)
    assert bucketed["bucket_ts"][2] == 180_000_000

def test_bucket_assignment_pit_correctness():
    """Test no future data in buckets."""
    # TODO: Verify PIT invariant
    pass

def test_bucket_assignment_deterministic_sort():
    """Test deterministic sort enforcement."""
    # TODO: Test with file_id and file_line_number
    pass
```

**File**: `tests/research/resample/test_aggregate.py`

```python
import pytest
import polars as pl
from pointline.research.resample import aggregate, AggregateConfig, AggregationSpec

def test_aggregate_pattern_a():
    """Test Pattern A: aggregate raw values."""
    # Bucketed data
    data = pl.LazyFrame({
        "exchange_id": [1, 1, 1, 1],
        "symbol_id": [12345, 12345, 12345, 12345],
        "bucket_ts": [60_000_000, 60_000_000, 120_000_000, 120_000_000],
        "qty": [10, 20, 15, 25],
    })

    config = AggregateConfig(
        by=["exchange_id", "symbol_id", "bucket_ts"],
        aggregations=[
            AggregationSpec(name="volume", source_column="qty", agg="sum"),
        ],
        mode="bar_then_feature",
    )

    result = aggregate(data, config).collect()

    # First bucket: 10 + 20 = 30
    assert result.filter(pl.col("bucket_ts") == 60_000_000)["volume"][0] == 30
    # Second bucket: 15 + 25 = 40
    assert result.filter(pl.col("bucket_ts") == 120_000_000)["volume"][0] == 40

def test_aggregate_pattern_b():
    """Test Pattern B: compute then aggregate."""
    # TODO: Test with custom aggregation that computes features first
    pass

def test_aggregate_semantic_validation():
    """Test semantic type policy enforcement."""
    data = pl.LazyFrame({
        "exchange_id": [1],
        "symbol_id": [12345],
        "bucket_ts": [60_000_000],
        "price": [50000],
    })

    # sum not allowed for price semantic type
    config = AggregateConfig(
        by=["exchange_id", "symbol_id", "bucket_ts"],
        aggregations=[
            AggregationSpec(
                name="price_sum",
                source_column="price",
                agg="sum",
                semantic_type="price",
            ),
        ],
        mode="bar_then_feature",
    )

    with pytest.raises(ValueError, match="not allowed for semantic type"):
        aggregate(data, config)

def test_aggregate_spine_preservation():
    """Test spine preservation (left join maintains all spine points)."""
    # TODO: Test that all spine points preserved even if no data in bucket
    pass
```

### Files Created

```
pointline/research/resample/
├── bucket_assignment.py     # Bucket assignment logic
├── config.py                # Configuration classes
└── aggregate.py             # Aggregation implementation

tests/research/resample/
├── test_bucket_assignment.py
└── test_aggregate.py
```

### Acceptance Criteria

- [ ] Bucket assignment uses backward as-of join
- [ ] PIT correctness validated (no future data)
- [ ] Deterministic sort enforced
- [ ] Pattern A aggregations working
- [ ] Pattern B aggregations working
- [ ] Semantic type validation enforced
- [ ] Spine preservation working (left join)
- [ ] Missing column validation working

---

## Phase 3: Custom Aggregations (Week 3-4)

### Goal
Implement starter set of custom aggregations

### Deliverables

#### 1. Microstructure Aggregations

**File**: `pointline/research/resample/aggregations/microstructure.py`

```python
from pointline.research.resample.registry import AggregationRegistry
from pointline.research.resample.config import AggregationSpec
import polars as pl

@AggregationRegistry.register(
    name="microprice_close",
    stage="feature_then_aggregate",
    semantic_type="book_top",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["bids_px_int", "asks_px_int", "bids_sz_int", "asks_sz_int"],
)
def microprice_close(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Compute microprice at tick level, take last per bucket.

    Microprice = (bid_px * ask_sz + ask_px * bid_sz) / (bid_sz + ask_sz)
    """
    return lf.with_columns([
        (
            (pl.col("bids_px_int") * pl.col("asks_sz_int")
             + pl.col("asks_px_int") * pl.col("bids_sz_int"))
            / (pl.col("bids_sz_int") + pl.col("asks_sz_int"))
        ).alias("_microprice_feature")
    ])

@AggregationRegistry.register(
    name="spread_distribution",
    stage="feature_then_aggregate",
    semantic_type="quote_top",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["ask_px_int", "bid_px_int"],
)
def spread_distribution(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Compute spread at tick level for distribution stats.

    Returns mean, std, min, max of spread in bps.
    Pattern B from bar-aggregation.md.
    """
    return lf.with_columns([
        (
            (pl.col("ask_px_int") - pl.col("bid_px_int"))
            / pl.col("bid_px_int") * 10000
        ).alias("_spread_bps_feature")
    ])

@AggregationRegistry.register(
    name="ofi_cont",
    stage="feature_then_aggregate",
    semantic_type="book_depth",
    mode_allowlist=["HFT"],
    required_columns=["bids_sz_int", "asks_sz_int"],
)
def ofi_cont(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Order flow imbalance (OFI) at each tick.

    OFI = ΔBid_volume - ΔAsk_volume
    """
    return lf.with_columns([
        (
            pl.col("bids_sz_int").diff().fill_null(0)
            - pl.col("asks_sz_int").diff().fill_null(0)
        ).alias("_ofi_feature")
    ])
```

#### 2. Trade Flow Aggregations

**File**: `pointline/research/resample/aggregations/trade_flow.py`

```python
from pointline.research.resample.registry import AggregationRegistry
from pointline.research.resample.config import AggregationSpec
import polars as pl

@AggregationRegistry.register(
    name="signed_trade_imbalance",
    stage="aggregate_then_feature",
    semantic_type="trade_flow",
    mode_allowlist=["HFT", "MFT"],
    required_columns=["qty_int", "side"],
)
def signed_trade_imbalance(source_col: str) -> pl.Expr:
    """Signed trade volume imbalance.

    Pattern A: Aggregate signed volumes, then compute imbalance.
    """
    buy_vol = (
        pl.when(pl.col("side") == 0)
        .then(pl.col(source_col))
        .otherwise(0)
        .sum()
    )
    sell_vol = (
        pl.when(pl.col("side") == 1)
        .then(pl.col(source_col))
        .otherwise(0)
        .sum()
    )
    return (buy_vol - sell_vol) / (buy_vol + sell_vol)
```

#### 3. Liquidation Aggregations

**File**: `pointline/research/resample/aggregations/liquidations.py`

```python
from pointline.research.resample.registry import AggregationRegistry
from pointline.research.resample.config import AggregationSpec
import polars as pl

@AggregationRegistry.register(
    name="liq_qty_sum",
    stage="aggregate_then_feature",
    semantic_type="liquidation",
    mode_allowlist=["MFT"],
    required_columns=["qty_int", "is_liquidation"],
)
def liq_qty_sum(source_col: str) -> pl.Expr:
    """Sum liquidation quantity."""
    return (
        pl.when(pl.col("is_liquidation") == 1)
        .then(pl.col(source_col))
        .otherwise(0)
        .sum()
    )

@AggregationRegistry.register(
    name="liq_count",
    stage="aggregate_then_feature",
    semantic_type="liquidation",
    mode_allowlist=["MFT"],
    required_columns=["is_liquidation"],
)
def liq_count(source_col: str) -> pl.Expr:
    """Count liquidation events."""
    return pl.col("is_liquidation").sum()

@AggregationRegistry.register(
    name="liq_oi_pressure",
    stage="feature_then_aggregate",
    semantic_type="liquidation",
    mode_allowlist=["MFT"],
    required_columns=["qty_int", "side", "is_liquidation", "open_interest_int"],
)
def liq_oi_pressure(lf: pl.LazyFrame, spec: AggregationSpec) -> pl.LazyFrame:
    """Liquidation pressure relative to OI.

    Pressure = signed_liq_qty / open_interest
    """
    return lf.with_columns([
        (
            pl.when(pl.col("is_liquidation") == 1)
            .then(
                pl.when(pl.col("side") == 0)
                .then(pl.col("qty_int"))
                .otherwise(-pl.col("qty_int"))
            )
            .otherwise(0)
            / pl.col("open_interest_int")
        ).alias("_liq_oi_pressure_feature")
    ])
```

### Testing Requirements

**File**: `tests/research/resample/aggregations/test_custom_aggregations.py`

```python
import pytest
import polars as pl
from pointline.research.resample.registry import AggregationRegistry

def test_microprice_close_registration():
    """Test microprice_close is registered."""
    assert "microprice_close" in AggregationRegistry._registry
    meta = AggregationRegistry.get("microprice_close")
    assert meta.stage == "feature_then_aggregate"

def test_spread_distribution_computation():
    """Test spread distribution computes correctly."""
    # Create synthetic data
    data = pl.LazyFrame({
        "bucket_ts": [60_000_000] * 3,
        "bid_px_int": [50000, 50010, 49990],
        "ask_px_int": [50005, 50015, 49995],
    })

    # Apply spread_distribution
    from pointline.research.resample.aggregations.microstructure import spread_distribution
    from pointline.research.resample.config import AggregationSpec

    spec = AggregationSpec(name="spread", source_column="bid_px_int", agg="spread_distribution")
    result = spread_distribution(data, spec).collect()

    # Verify feature column created
    assert "_spread_bps_feature" in result.columns

    # Manual calculation: (50005-50000)/50000 * 10000 = 1 bps
    expected_spread_0 = (50005 - 50000) / 50000 * 10000
    assert abs(result["_spread_bps_feature"][0] - expected_spread_0) < 0.01

def test_ofi_cont_diff_calculation():
    """Test OFI computes differences correctly."""
    # TODO: Test with sequential book snapshots
    pass

def test_signed_trade_imbalance():
    """Test signed trade imbalance."""
    # TODO: Test with buy/sell trades
    pass

def test_liq_qty_sum():
    """Test liquidation quantity sum."""
    # TODO: Test with liquidation flags
    pass

def test_liq_oi_pressure():
    """Test liquidation OI pressure."""
    # TODO: Test with OI data
    pass
```

### Files Created

```
pointline/research/resample/aggregations/
├── __init__.py
├── microstructure.py        # microprice_close, spread_distribution, ofi_cont
├── trade_flow.py            # signed_trade_imbalance
└── liquidations.py          # liq_qty_sum, liq_count, liq_oi_pressure

tests/research/resample/aggregations/
├── __init__.py
└── test_custom_aggregations.py
```

### Acceptance Criteria

- [ ] All custom aggregations registered
- [ ] microprice_close computes correctly
- [ ] spread_distribution produces distribution stats
- [ ] ofi_cont uses diff() correctly
- [ ] signed_trade_imbalance separates buy/sell
- [ ] liq_qty_sum filters liquidations
- [ ] liq_oi_pressure normalizes by OI
- [ ] All tests passing

---

## Phase 4: Pipeline Orchestration with Spines (Week 4-5)

### Goal
High-level pipeline API integrating spines

### Deliverables

#### 1. Pipeline Configuration

**File**: `pointline/research/resample/pipeline.py`

```python
from dataclasses import dataclass
from typing import Literal
import polars as pl

from pointline.research.spine.protocol import SpineBuilder
from pointline.research.spine.config import SpineConfig
from pointline.research.resample.config import AggregateConfig
from pointline.research.resample.bucket_assignment import assign_to_buckets
from pointline.research.resample.aggregate import aggregate

@dataclass(frozen=True)
class PipelineConfig:
    """Complete pipeline configuration with spine integration."""
    mode: Literal["event_joined", "tick_then_bar", "bar_then_feature"]

    # Spine defines bucket assignment strategy
    spine_builder: SpineBuilder
    spine_config: SpineConfig

    # Data sources
    sources: dict[str, pl.LazyFrame | str]

    # Aggregation config (optional - depends on mode)
    aggregate: AggregateConfig | None = None

    # Join policy for event_joined mode
    join_policy: dict | None = None

    # Registry profile
    registry_profile: str = "default"

def pipeline(
    config: PipelineConfig,
    *,
    deterministic: bool = True,
) -> pl.LazyFrame:
    """Execute research pipeline with spine-based bucketing.

    Three modes:
    1. event_joined: Build event timeline via backward as-of joins
    2. tick_then_bar: Compute microfeatures at tick level, then aggregate
    3. bar_then_feature: Aggregate to bars first, then compute features

    All modes use spine for bucket assignment.
    """

    # Step 1: Build spine (defines bucket boundaries)
    spine = config.spine_builder.build_spine(
        symbol_id=config.spine_config.symbol_id,
        start_ts_us=config.spine_config.start_ts_us,
        end_ts_us=config.spine_config.end_ts_us,
        exchange_id=config.spine_config.exchange_id,
    )

    # Step 2: Execute mode-specific pipeline
    match config.mode:
        case "event_joined":
            return _pipeline_event_joined(spine, config, deterministic)
        case "tick_then_bar":
            return _pipeline_tick_then_bar(spine, config, deterministic)
        case "bar_then_feature":
            return _pipeline_bar_then_feature(spine, config, deterministic)
        case _:
            raise ValueError(f"Unknown mode: {config.mode}")

def _pipeline_event_joined(
    spine: pl.LazyFrame,
    config: PipelineConfig,
    deterministic: bool,
) -> pl.LazyFrame:
    """Mode 1: Event-level features via backward joins.

    Workflow:
    1. Use spine as primary event timeline
    2. Backward as-of join other streams to spine
    3. Compute features on aligned events
    4. Each spine point becomes a feature row

    Use case: HFT research, tick-level precision
    """

    # Spine becomes the primary timeline
    result = spine

    # Join all sources to spine via backward as-of join
    for name, source in config.sources.items():
        source_lf = _load_source(source)

        result = _backward_asof_join(
            result,
            source_lf,
            config.join_policy or {},
            deterministic,
        )

    return result

def _pipeline_tick_then_bar(
    spine: pl.LazyFrame,
    config: PipelineConfig,
    deterministic: bool,
) -> pl.LazyFrame:
    """Mode 2: Microfeatures first, then aggregate.

    Workflow:
    1. Assign ticks to spine buckets
    2. Compute features at tick level (Pattern B)
    3. Aggregate features to bars

    Use case: Microstructure research (spread distribution, OFI, etc.)
    """

    # Load primary source (ticks/events)
    ticks = _load_source(config.sources["primary"])

    # Assign ticks to spine buckets
    bucketed = assign_to_buckets(ticks, spine, deterministic=deterministic)

    # Aggregate (will apply feature_then_aggregate stage)
    if config.aggregate:
        bars = aggregate(bucketed, config.aggregate, spine=spine)
    else:
        # No aggregation - just return bucketed data
        bars = bucketed

    return bars

def _pipeline_bar_then_feature(
    spine: pl.LazyFrame,
    config: PipelineConfig,
    deterministic: bool,
) -> pl.LazyFrame:
    """Mode 3: Aggregate to bars first, then features.

    Workflow:
    1. Assign each source stream to spine buckets
    2. Aggregate each stream to bars (Pattern A)
    3. Join bar streams (already aligned by spine)
    4. Compute features on bar data

    Use case: Production research, scalable MFT workflows
    Most common mode.
    """

    # Aggregate each source to bars
    bar_streams = {}
    for name, source in config.sources.items():
        source_lf = _load_source(source)

        # Assign to buckets
        bucketed = assign_to_buckets(source_lf, spine, deterministic=deterministic)

        # Aggregate
        if config.aggregate:
            bars = aggregate(bucketed, config.aggregate, spine=spine)
        else:
            bars = bucketed

        bar_streams[name] = bars

    # Join bar streams (all aligned to same spine)
    result = bar_streams["primary"]
    for name, bars in bar_streams.items():
        if name == "primary":
            continue

        # Use ts_local_us if spine was joined, otherwise bucket_ts
        join_on = ["exchange_id", "symbol_id", "ts_local_us"]
        if "bucket_ts" in result.columns and "bucket_ts" in bars.columns:
            join_on = ["exchange_id", "symbol_id", "bucket_ts"]

        result = result.join(bars, on=join_on, how="left")

    return result

def _backward_asof_join(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    join_policy: dict,
    deterministic: bool,
) -> pl.LazyFrame:
    """PIT-correct backward as-of join."""

    time_col = join_policy.get("time_col", "ts_local_us")
    by = join_policy.get("by", ["exchange_id", "symbol_id"])

    # Enforce deterministic ordering
    if deterministic:
        from pointline.research.resample.bucket_assignment import _enforce_deterministic_sort
        left = _enforce_deterministic_sort(left)
        right = _enforce_deterministic_sort(right)

    return left.join_asof(
        right,
        on=time_col,
        by=by,
        strategy="backward",
    )

def _load_source(source: pl.LazyFrame | str) -> pl.LazyFrame:
    """Load data source (LazyFrame or path/query string)."""
    if isinstance(source, pl.LazyFrame):
        return source

    # TODO: Support loading from paths, Delta tables, query API, etc.
    raise NotImplementedError(f"Loading from {type(source)} not yet supported")
```

#### 2. Convenience Functions

**File**: `pointline/research/resample/convenience.py`

```python
import polars as pl
from pointline.research.resample.pipeline import pipeline, PipelineConfig
from pointline.research.resample.config import AggregateConfig, AggregationSpec
from pointline.research.spine.builders import (
    ClockSpineBuilder,
    VolumeSpineBuilder,
    DollarSpineBuilder,
    TradeSpineBuilder,
)
from pointline.research.spine.config import (
    ClockSpineConfig,
    VolumeSpineConfig,
    DollarSpineConfig,
    TradeSpineConfig,
)

def resample_to_clock_bars(
    data: pl.LazyFrame,
    symbol_id: int,
    start: str | int,
    end: str | int,
    every: str,
    aggregations: list[dict],
    mode: str = "bar_then_feature",
) -> pl.LazyFrame:
    """Convenience: Resample to clock bars.

    Example:
        bars = research.resample_to_clock_bars(
            trades,
            symbol_id=12345,
            start="2024-05-01",
            end="2024-05-02",
            every="1m",
            aggregations=[
                {"name": "volume", "source_column": "qty_int", "agg": "sum"},
                {"name": "vwap", "source_column": "px_int", "agg": "mean"},
            ],
        )
    """
    from pointline.utils.time import parse_timestamp

    start_ts_us = parse_timestamp(start) if isinstance(start, str) else start
    end_ts_us = parse_timestamp(end) if isinstance(end, str) else end

    config = PipelineConfig(
        mode=mode,
        spine_builder=ClockSpineBuilder(every=every),
        spine_config=ClockSpineConfig(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
        ),
        sources={"primary": data},
        aggregate=AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[AggregationSpec(**agg) for agg in aggregations],
            mode=mode,
        ),
    )

    return pipeline(config)

def resample_to_volume_bars(
    data: pl.LazyFrame,
    symbol_id: int,
    start: str | int,
    end: str | int,
    volume_threshold: float,
    aggregations: list[dict],
    mode: str = "bar_then_feature",
) -> pl.LazyFrame:
    """Convenience: Resample to volume bars.

    Example:
        bars = research.resample_to_volume_bars(
            trades,
            symbol_id=12345,
            start="2024-05-01",
            end="2024-05-02",
            volume_threshold=1000.0,  # 1000 BTC per bar
            aggregations=[...],
        )
    """
    from pointline.utils.time import parse_timestamp

    start_ts_us = parse_timestamp(start) if isinstance(start, str) else start
    end_ts_us = parse_timestamp(end) if isinstance(end, str) else end

    config = PipelineConfig(
        mode=mode,
        spine_builder=VolumeSpineBuilder(volume_threshold=volume_threshold),
        spine_config=VolumeSpineConfig(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            volume_threshold=volume_threshold,
        ),
        sources={"primary": data},
        aggregate=AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[AggregationSpec(**agg) for agg in aggregations],
            mode=mode,
        ),
    )

    return pipeline(config)

def resample_to_dollar_bars(
    data: pl.LazyFrame,
    symbol_id: int,
    start: str | int,
    end: str | int,
    dollar_threshold: float,
    aggregations: list[dict],
    mode: str = "bar_then_feature",
) -> pl.LazyFrame:
    """Convenience: Resample to dollar bars."""
    from pointline.utils.time import parse_timestamp

    start_ts_us = parse_timestamp(start) if isinstance(start, str) else start
    end_ts_us = parse_timestamp(end) if isinstance(end, str) else end

    config = PipelineConfig(
        mode=mode,
        spine_builder=DollarSpineBuilder(dollar_threshold=dollar_threshold),
        spine_config=DollarSpineConfig(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            dollar_threshold=dollar_threshold,
        ),
        sources={"primary": data},
        aggregate=AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[AggregationSpec(**agg) for agg in aggregations],
            mode=mode,
        ),
    )

    return pipeline(config)

def resample_to_trade_bars(
    data: pl.LazyFrame,
    symbol_id: int,
    start: str | int,
    end: str | int,
    trade_count: int,
    aggregations: list[dict],
    mode: str = "bar_then_feature",
) -> pl.LazyFrame:
    """Convenience: Resample to trade bars."""
    from pointline.utils.time import parse_timestamp

    start_ts_us = parse_timestamp(start) if isinstance(start, str) else start
    end_ts_us = parse_timestamp(end) if isinstance(end, str) else end

    config = PipelineConfig(
        mode=mode,
        spine_builder=TradeSpineBuilder(trade_count=trade_count),
        spine_config=TradeSpineConfig(
            symbol_id=symbol_id,
            start_ts_us=start_ts_us,
            end_ts_us=end_ts_us,
            trade_count=trade_count,
        ),
        sources={"primary": data},
        aggregate=AggregateConfig(
            by=["exchange_id", "symbol_id", "bucket_ts"],
            aggregations=[AggregationSpec(**agg) for agg in aggregations],
            mode=mode,
        ),
    )

    return pipeline(config)
```

### Testing Requirements

**File**: `tests/research/resample/test_pipeline.py`

```python
import pytest
import polars as pl
from pointline.research.resample import pipeline, PipelineConfig
from pointline.research.spine.builders import ClockSpineBuilder, VolumeSpineBuilder

def test_pipeline_bar_then_feature_clock():
    """Test bar_then_feature mode with clock spine."""
    # TODO: End-to-end test
    pass

def test_pipeline_tick_then_bar_volume():
    """Test tick_then_bar mode with volume spine."""
    # TODO: End-to-end test
    pass

def test_pipeline_event_joined():
    """Test event_joined mode."""
    # TODO: End-to-end test
    pass

def test_pipeline_mode_switching():
    """Test same data/spine with different modes."""
    # TODO: Verify different modes produce different results appropriately
    pass

def test_pipeline_determinism():
    """Test same config produces identical output."""
    # TODO: Run twice, compare outputs
    pass
```

### Files Created

```
pointline/research/resample/
├── pipeline.py              # Pipeline orchestration
└── convenience.py           # Convenience functions

tests/research/resample/
└── test_pipeline.py         # Pipeline tests
```

### Acceptance Criteria

- [ ] All three modes implemented
- [ ] Pipeline works with all spine types
- [ ] Convenience functions working
- [ ] Mode switching tested
- [ ] Determinism validated
- [ ] Multi-source joins working

---

## Phase 5: Validation & Observability (Week 5-6)

### Goal
Runtime diagnostics and artifact persistence

### Deliverables

#### 1. Validation Framework

**File**: `pointline/research/resample/validation.py`

```python
from dataclasses import dataclass
import polars as pl
from pointline.research.spine.protocol import SpineBuilder
from pointline.research.resample.pipeline import PipelineConfig

@dataclass
class ValidationReport:
    """Runtime validation report."""
    bucket_completeness: float | None  # % of expected buckets present (clock only)
    null_inflation: dict[str, float]  # % nulls per column after joins
    lag_distribution: dict[str, dict]  # p50, p95, p99 join lags
    row_loss: float  # % rows lost in joins
    warnings: list[str]
    errors: list[str]
    spine_coverage: dict  # Spine-specific metrics

def validate_pipeline(
    lf: pl.LazyFrame,
    config: PipelineConfig,
) -> ValidationReport:
    """Runtime validation checks."""

    warnings = []
    errors = []

    # Check 1: Bucket completeness (clock spines only)
    if config.spine_builder.spine_type == "clock":
        completeness = _check_bucket_completeness_clock(lf, config.spine_builder)
        if completeness < 0.95:
            warnings.append(f"Low bucket completeness: {completeness:.2%}")
    else:
        completeness = None

    # Check 2: Null inflation after joins
    null_rates = _check_null_inflation(lf)
    for col, rate in null_rates.items():
        if rate > 0.1:
            warnings.append(f"High null rate in {col}: {rate:.2%}")

    # Check 3: Spine coverage
    spine_coverage = validate_spine_coverage(
        config.spine_builder.build_spine(
            config.spine_config.symbol_id,
            config.spine_config.start_ts_us,
            config.spine_config.end_ts_us,
        ),
        lf,
        config.spine_builder,
    )

    return ValidationReport(
        bucket_completeness=completeness,
        null_inflation=null_rates,
        lag_distribution={},  # TODO: Implement
        row_loss=0.0,  # TODO: Implement
        warnings=warnings,
        errors=errors,
        spine_coverage=spine_coverage,
    )

def validate_spine_coverage(
    spine: pl.LazyFrame,
    data: pl.LazyFrame,
    spine_builder: SpineBuilder,
) -> dict:
    """Validate spine covers data properly."""

    spine_df = spine.collect()
    data_df = data.collect()

    spine_start = spine_df["ts_local_us"].min()
    spine_end = spine_df["ts_local_us"].max()
    data_start = data_df["ts_local_us"].min()
    data_end = data_df["ts_local_us"].max()

    warnings = []

    if data_start < spine_start:
        warnings.append(f"Data starts before spine: {data_start} < {spine_start}")

    if data_end > spine_end:
        warnings.append(f"Data ends after spine: {data_end} > {spine_end}")

    # Threshold accuracy for event-driven spines
    if spine_builder.spine_type in ["volume", "dollar", "trade"]:
        threshold_accuracy = _check_threshold_accuracy(
            spine_df, data_df, spine_builder
        )
    else:
        threshold_accuracy = None

    return {
        "coverage_ok": len(warnings) == 0,
        "warnings": warnings,
        "threshold_accuracy": threshold_accuracy,
    }

def _check_bucket_completeness_clock(
    lf: pl.LazyFrame,
    spine_builder: SpineBuilder,
) -> float:
    """Check % of expected buckets present."""
    df = lf.select(["bucket_ts"]).unique().collect()

    # Calculate expected buckets
    min_ts = df["bucket_ts"].min()
    max_ts = df["bucket_ts"].max()
    every_us = spine_builder.every_us
    expected = (max_ts - min_ts) // every_us + 1
    actual = len(df)

    return actual / expected if expected > 0 else 1.0

def _check_null_inflation(lf: pl.LazyFrame) -> dict[str, float]:
    """Calculate null rate per column."""
    df = lf.collect()
    return {
        col: df[col].null_count() / len(df)
        for col in df.columns
    }

def _check_threshold_accuracy(
    spine: pl.DataFrame,
    data: pl.DataFrame,
    spine_builder: SpineBuilder,
) -> dict:
    """Check if event-driven spine thresholds are accurate."""

    # Assign data to spine buckets
    bucketed = data.join_asof(
        spine.select(["ts_local_us", "exchange_id", "symbol_id"]).rename({
            "ts_local_us": "bucket_ts"
        }),
        left_on="ts_local_us",
        right_on="bucket_ts",
        by=["exchange_id", "symbol_id"],
        strategy="backward",
    )

    # Compute actual values per bucket
    if spine_builder.spine_type == "volume":
        actual = (
            bucketed
            .group_by("bucket_ts")
            .agg(pl.col("qty_int").sum().alias("actual_volume"))
        )
        threshold = spine_builder.volume_threshold
        actual_col = "actual_volume"

    elif spine_builder.spine_type == "dollar":
        actual = (
            bucketed
            .with_columns([
                (pl.col("px_int") * pl.col("qty_int")).alias("notional")
            ])
            .group_by("bucket_ts")
            .agg(pl.col("notional").sum().alias("actual_notional"))
        )
        threshold = spine_builder.dollar_threshold
        actual_col = "actual_notional"

    elif spine_builder.spine_type == "trade":
        actual = (
            bucketed
            .group_by("bucket_ts")
            .agg(pl.count().alias("actual_trades"))
        )
        threshold = spine_builder.trade_count
        actual_col = "actual_trades"
    else:
        return {}

    # Compute accuracy statistics
    mean_actual = actual[actual_col].mean()
    std_actual = actual[actual_col].std()

    return {
        "threshold": threshold,
        "mean_actual": mean_actual,
        "std_actual": std_actual,
        "accuracy": mean_actual / threshold if threshold > 0 else None,
    }
```

#### 2. Artifact Persistence

**File**: `pointline/research/resample/artifacts.py`

```python
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Literal
import json
from pointline.research.resample.validation import ValidationReport

@dataclass
class RunArtifact:
    """Persisted run metadata with spine info."""
    run_id: str
    timestamp: int
    config_hash: str
    mode: str

    # Spine metadata
    spine_type: str
    spine_config: dict
    spine_coverage: dict

    # Registry metadata
    registry_profile: str
    registry_versions: dict[str, str]

    # Validation
    input_coverage: dict[str, dict]
    validation_report: ValidationReport

    # Output
    output_schema: dict
    status: Literal["success", "failed", "partial"]

def persist_artifact(
    artifact: RunArtifact,
    output_path: Path,
) -> None:
    """Persist run artifact to disk."""
    artifact_path = (
        output_path / "artifacts" / "resample_aggregate" / "runs" / artifact.run_id
    )
    artifact_path.mkdir(parents=True, exist_ok=True)

    # Write metadata
    with open(artifact_path / "metadata.json", "w") as f:
        json.dump(asdict(artifact), f, indent=2, default=str)

    # Write validation report separately
    with open(artifact_path / "validation.json", "w") as f:
        json.dump(asdict(artifact.validation_report), f, indent=2)

    # Write config
    with open(artifact_path / "config.json", "w") as f:
        json.dump(artifact.spine_config, f, indent=2)

def load_artifact(artifact_path: Path) -> RunArtifact:
    """Load run artifact from disk."""
    with open(artifact_path / "metadata.json") as f:
        data = json.load(f)

    # Reconstruct ValidationReport
    validation_data = data.pop("validation_report")
    validation_report = ValidationReport(**validation_data)

    return RunArtifact(**data, validation_report=validation_report)
```

### Testing Requirements

**File**: `tests/research/resample/test_validation.py`

```python
import pytest
from pointline.research.resample.validation import (
    validate_pipeline,
    validate_spine_coverage,
)

def test_validation_bucket_completeness():
    """Test bucket completeness check."""
    # TODO: Test with missing buckets
    pass

def test_validation_null_inflation():
    """Test null rate calculation."""
    # TODO: Test with joins that produce nulls
    pass

def test_validation_spine_coverage():
    """Test spine coverage validation."""
    # TODO: Test with data before/after spine
    pass

def test_validation_threshold_accuracy():
    """Test threshold accuracy for event-driven spines."""
    # TODO: Test with volume/dollar/trade bars
    pass
```

### Files Created

```
pointline/research/resample/
├── validation.py            # Validation framework
└── artifacts.py             # Artifact persistence

tests/research/resample/
└── test_validation.py       # Validation tests
```

### Acceptance Criteria

- [ ] Validation report generated successfully
- [ ] Bucket completeness checked for clock spines
- [ ] Null inflation detected
- [ ] Spine coverage validated
- [ ] Threshold accuracy checked for event-driven spines
- [ ] Artifacts persisted and loaded correctly

---

## Phase 6: Integration & Documentation (Week 6-7)

### Goal
Wire into existing Pointline APIs and document

### Deliverables

#### 1. API Integration

**File**: `pointline/research/__init__.py` (updated)

```python
# Existing APIs (unchanged)
from pointline.research.query import (
    trades,
    quotes,
    book_snapshot_25,
)

# New spine-based APIs
from pointline.research.spine.builders import (
    ClockSpineBuilder,
    VolumeSpineBuilder,
    DollarSpineBuilder,
    TradeSpineBuilder,
)

from pointline.research.spine.config import (
    ClockSpineConfig,
    VolumeSpineConfig,
    DollarSpineConfig,
    TradeSpineConfig,
)

from pointline.research.resample import (
    pipeline,
    aggregate,
    assign_to_buckets,
    PipelineConfig,
    AggregateConfig,
    AggregationSpec,
    AggregationRegistry,
)

# Convenience functions
from pointline.research.resample.convenience import (
    resample_to_clock_bars,
    resample_to_volume_bars,
    resample_to_dollar_bars,
    resample_to_trade_bars,
)

__all__ = [
    # Query API
    "trades",
    "quotes",
    "book_snapshot_25",
    # Spine builders
    "ClockSpineBuilder",
    "VolumeSpineBuilder",
    "DollarSpineBuilder",
    "TradeSpineBuilder",
    # Spine configs
    "ClockSpineConfig",
    "VolumeSpineConfig",
    "DollarSpineConfig",
    "TradeSpineConfig",
    # Pipeline
    "pipeline",
    "PipelineConfig",
    # Aggregation
    "aggregate",
    "AggregateConfig",
    "AggregationSpec",
    "AggregationRegistry",
    # Bucket assignment
    "assign_to_buckets",
    # Convenience
    "resample_to_clock_bars",
    "resample_to_volume_bars",
    "resample_to_dollar_bars",
    "resample_to_trade_bars",
]
```

#### 2. Documentation

**File**: `docs/guides/resample-aggregate-guide.md`

See full documentation content in Phase 6 section of the original plan (too long to repeat here, but includes):
- Quick Start examples
- All three pipeline modes
- Custom aggregations
- PIT correctness guarantees
- Performance benchmarks
- Complete API reference

#### 3. Examples

**File**: `research/03_experiments/exp_2026-02-07_volume_bars_example/README.md`

```markdown
# Volume Bars VPIN Example

## Objective

Demonstrate volume bar construction and VPIN calculation using new resample-aggregate framework.

## Method

1. Load BTCUSDT trades for 1 day
2. Construct volume bars (1000 BTC threshold)
3. Compute VPIN over 50-bar rolling window
4. Compare with clock bars

## Results

Volume bars produced more uniform information content than clock bars (as expected).

## Code

See `notebook.ipynb` for complete implementation.
```

### Testing Requirements

**File**: `tests/research/test_integration.py`

```python
import pytest
from pointline import research

def test_e2e_clock_bars_btcusdt():
    """End-to-end test with real BTCUSDT data."""
    # Load 1 day of data
    trades = research.query.trades(
        "binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02"
    )

    # Resample to 1-minute bars
    bars = research.resample_to_clock_bars(
        trades,
        symbol_id=12345,
        start="2024-05-01",
        end="2024-05-02",
        every="1m",
        aggregations=[
            {"name": "volume", "source_column": "qty_int", "agg": "sum"},
            {"name": "vwap", "source_column": "px_int", "agg": "mean"},
        ],
    )

    df = bars.collect()

    # Assertions
    assert len(df) == 1440  # 1 day = 1440 minutes
    assert "volume" in df.columns
    assert "vwap" in df.columns
    assert df["volume"].sum() > 0

def test_e2e_volume_bars_btcusdt():
    """End-to-end test with volume bars."""
    # TODO: Test volume bars
    pass

def test_determinism_clock_bars():
    """Test deterministic outputs."""
    # TODO: Run twice, compare
    pass

def test_pit_correctness():
    """Test PIT correctness with synthetic data."""
    # TODO: Verify no future data
    pass
```

### Files Created

```
docs/guides/
└── resample-aggregate-guide.md

research/03_experiments/
└── exp_2026-02-07_volume_bars_example/
    ├── README.md
    └── notebook.ipynb

tests/research/
└── test_integration.py
```

### Acceptance Criteria

- [ ] All APIs exported from `pointline.research`
- [ ] Documentation complete and examples work
- [ ] Integration tests passing with real data
- [ ] Example notebooks working
- [ ] API ergonomics validated

---

## Testing Strategy

### Unit Tests (Per Phase)

**Coverage Target:** 80%+

Each phase includes unit tests for:
- Core functionality
- Error handling
- Edge cases
- Validation logic

### Integration Tests (Phase 6)

**Real Data Tests:**
```python
def test_e2e_btcusdt_1day_clock():
    """1 day BTCUSDT, 1-minute clock bars."""
    # Uses real Pointline data
    # Validates full pipeline

def test_e2e_btcusdt_1day_volume():
    """1 day BTCUSDT, 1000 BTC volume bars."""
    # Tests event-driven spine

def test_e2e_multi_source():
    """Trades + quotes joined to bars."""
    # Tests multi-source pipeline
```

### Property-Based Tests (Phase 6)

**Hypothesis Tests:**
```python
from hypothesis import given, strategies as st

@given(
    every=st.sampled_from(["1m", "5m", "1h"]),
    mode=st.sampled_from(["bar_then_feature", "tick_then_bar"]),
)
def test_no_future_dependency(every, mode):
    """Property: Features never depend on future data."""
    # Generate synthetic data
    # Run pipeline
    # Verify PIT correctness

@given(
    volume_threshold=st.floats(min_value=100, max_value=10000),
)
def test_volume_bar_threshold_accuracy(volume_threshold):
    """Property: Volume per bar ≈ threshold."""
    # Generate synthetic trades
    # Build volume spine
    # Verify actual volume ≈ threshold
```

### Performance Tests (Phase 6)

**Benchmark Suite:**
```python
def benchmark_clock_vs_tick():
    """Compare clock bars vs tick-level performance."""
    # 1 day BTCUSDT
    # Measure: clock bars (1m), tick-level features
    # Assert: clock bars >= 5x faster

def benchmark_volume_vs_clock():
    """Compare volume bars vs clock bars performance."""
    # Should be similar after aggregation
```

---

## Rollout Checklist

### Phase 0 (Week 1)
- [ ] SpineBuilder protocol defined
- [ ] ClockSpineBuilder implemented
- [ ] VolumeSpineBuilder migrated
- [ ] DollarSpineBuilder migrated
- [ ] TradeSpineBuilder migrated
- [ ] Spine config classes defined
- [ ] All spine builders pass protocol tests
- [ ] Timestamp semantics validated

### Phase 1 (Week 1-2)
- [ ] Registry system implemented
- [ ] Semantic policies defined
- [ ] Built-in aggregations registered
- [ ] Registry tests passing
- [ ] Mode validation working
- [ ] Profiles loaded correctly

### Phase 2 (Week 2-3)
- [ ] Bucket assignment function implemented
- [ ] Aggregate function with spine integration
- [ ] Pattern A and B separation working
- [ ] Spine preservation (left join) working
- [ ] PIT correctness validated
- [ ] Semantic type validation enforced

### Phase 3 (Week 3-4)
- [ ] Custom aggregations implemented:
  - [ ] microprice_close
  - [ ] spread_distribution
  - [ ] ofi_cont
  - [ ] signed_trade_imbalance
  - [ ] liq_qty_sum
  - [ ] liq_count
  - [ ] liq_oi_pressure
- [ ] All custom aggs tested
- [ ] All custom aggs registered

### Phase 4 (Week 4-5)
- [ ] Pipeline orchestration implemented
- [ ] All three modes working with all spine types
- [ ] Mode switching tests passing
- [ ] Convenience functions implemented
- [ ] Multi-source joins working

### Phase 5 (Week 5-6)
- [ ] Spine coverage validation
- [ ] Threshold accuracy validation
- [ ] Artifact persistence working
- [ ] Observability hooks added
- [ ] Validation tests passing

### Phase 6 (Week 6-7)
- [ ] Integration with existing APIs
- [ ] End-to-end tests passing:
  - [ ] Clock bars + bar_then_feature
  - [ ] Volume bars + bar_then_feature
  - [ ] Dollar bars + tick_then_bar
  - [ ] Trade bars + event_joined
- [ ] Documentation complete
- [ ] Examples verified
- [ ] Performance benchmarks run
- [ ] Ready for production use

---

## Success Criteria

### 1. Correctness
- ✅ All spine types produce PIT-correct bars
- ✅ Threshold accuracy for event-driven spines (>95%)
- ✅ Deterministic outputs (same config → identical results)
- ✅ No future data in features (PIT validation passing)

### 2. Flexibility
- ✅ Clock and event-driven spines work with all modes
- ✅ Can mix spine types with different aggregation strategies
- ✅ Easy to add new spine types (protocol-based)
- ✅ Easy to add new aggregations (registry-based)

### 3. Performance
- ✅ Bar aggregation faster than tick-level features (>5x)
- ✅ Volume bars as fast as clock bars (similar after aggregation)
- ✅ Lazy evaluation preserved throughout pipeline

### 4. Usability
- ✅ Simple API for common cases (convenience functions)
- ✅ Full control via pipeline API when needed
- ✅ Clear error messages with actionable guidance
- ✅ Documentation examples work without modification

### 5. Extensibility
- ✅ SpineBuilder protocol for custom spines
- ✅ Registry for custom aggregations with semantic policies
- ✅ No core changes needed for extensions
- ✅ Plugin architecture demonstrated

### 6. Safety
- ✅ Pre-execution validation catches errors early
- ✅ Semantic type policies prevent nonsensical operations
- ✅ Required columns validated before execution
- ✅ PIT policy enforcement working

---

## Summary of Key Design Decisions

1. **Spine as Foundation**: Spines define bucket boundaries for all resampling (clock and event-driven)
2. **Contract-First**: Explicit config schema with pre-execution validation
3. **Registry Pattern**: Custom aggregations registered with semantic policies and mode allowlists
4. **Two Aggregation Patterns**: Pattern A (aggregate_then_feature) and Pattern B (feature_then_aggregate)
5. **Three Pipeline Modes**: event_joined (HFT), tick_then_bar (microstructure), bar_then_feature (production MFT)
6. **PIT Correctness**: Bar timestamp = spine boundary, backward-only joins, deterministic ordering
7. **Extensibility**: Protocol-based spines, registry-based aggregations, no core changes for extensions
8. **No Backward Compatibility**: Clean slate implementation without legacy constraints

---

## Next Steps After Completion

1. **Phase 7 (Future)**: LLM Agent Integration
   - Agent emits contract-compliant configs
   - Validation catches agent errors
   - Observability for agent-generated pipelines

2. **Phase 8 (Future)**: Performance Optimization
   - Parallel bucket processing
   - Incremental bar updates
   - Caching strategies

3. **Phase 9 (Future)**: Advanced Spines
   - Imbalance bars (order flow imbalance)
   - Information-theoretic bars (entropy)
   - Microstructure spines (quote depth changes)

---

**End of Implementation Plan**
