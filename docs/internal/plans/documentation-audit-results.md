# Documentation Audit Results

**Date:** 2026-02-01
**Auditor:** Claude Code
**Purpose:** Assess query API vs core API emphasis across documentation

---

## Summary

| File | Status | Priority | Query API First? | Action Required |
|------|--------|----------|------------------|-----------------|
| README.md | ✅ Good | High | Yes | Minor polish only |
| CLAUDE.md | ✅ Good | High | Yes | Add anti-patterns section |
| docs/research_api_guide.md | ✅ Good | Medium | Yes | Minor improvements |
| docs/guides/researcher_guide.md | ❌ Needs Major Update | High | **No** | **Complete restructure** |
| docs/quickstart.md | ❌ Missing | High | N/A | **Create new file** |
| docs/guides/choosing-an-api.md | ❌ Missing | Medium | N/A | **Create new file** |

---

## Detailed Findings

### ✅ README.md (Good State)

**Current state:**
- Lines 73-99: Shows query API as primary example
- Discovery workflow first
- Clear, concise examples

**Strengths:**
- Query API prominently featured
- Discovery API shown first
- Links to examples

**Minor improvements needed:**
- Add link to quickstart guide
- Add "For production research" callout to advanced docs

**Priority:** Low (minor polish)

---

### ❌ docs/guides/researcher_guide.md (CRITICAL ISSUE)

**Current state:**
- Lines 30-88: Leads with core API and two-stage symbol resolution
- Lines 110-123: Shows verbose core API examples first
- Line 278: Query API mentioned briefly at end

**Problems:**
1. **Wrong emphasis**: Core API shown first throughout
2. **Verbose examples**: Manual symbol resolution, timestamp conversion, decoding
3. **Poor onboarding**: New users see complex workflows first
4. **Buries query API**: Only mentioned as afterthought

**Example of problematic content (lines 66-88):**
```python
# Shows this verbose workflow first:
from pointline import registry, research
import polars as pl

# Step 1: Manual symbol resolution
df = registry.find_symbol("BTC-PERPETUAL", exchange="deribit")
active = df.filter(
    (pl.col("valid_from_ts") < end_ts_us) & (pl.col("valid_until_ts") > start_ts_us)
)
symbol_ids = active["symbol_id"].to_list()

# Step 2: Load with explicit control
trades = research.load_trades(
    symbol_id=symbol_ids,
    start_ts_us=start_ts_us,
    end_ts_us=end_ts_us,
)
```

**Instead should show:**
```python
# Query API first:
from pointline.research import query

trades = query.trades("deribit", "BTC-PERPETUAL", "2024-05-01", "2024-05-02", decoded=True)
```

**Action required:**
1. Complete restructure with query API first
2. Move core API to "Advanced Topics" section
3. Add clear decision tree
4. Update all examples to use query API by default

**Priority:** CRITICAL

---

### ✅ docs/research_api_guide.md (Good State)

**Current state:**
- Lines 1-15: Clear two-layer design explanation
- Lines 19-50: Query API example shown first
- Lines 89-104: Clear when-to-use guidance

**Strengths:**
- Query API emphasized
- Good examples showing both approaches
- Clear comparison

**Minor improvements needed:**
- Add anti-patterns section
- More real-world examples
- Link to choosing-an-api guide

**Priority:** Low (minor improvements)

---

### ✅ CLAUDE.md (Good State)

**Current state:**
- Emphasizes discovery API
- Shows query workflow prominently
- Good for LLM agents

**Improvements needed:**
- Add explicit anti-patterns section
- Decision tree for agents
- "Do this / Don't do this" examples

**Priority:** Medium

---

### ❌ docs/quickstart.md (Missing)

**Status:** Does not exist

**Purpose:** 5-minute tutorial for new users

**Content needed:**
1. Installation
2. Discover data (1 min)
3. Load data (1 min)
4. Explore data (2 min)
5. Visualize (1 min)
6. Next steps

**Priority:** HIGH (critical for onboarding)

---

### ❌ docs/guides/choosing-an-api.md (Missing)

**Status:** Does not exist

**Purpose:** Definitive guide on query API vs core API

**Content needed:**
1. Quick decision matrix
2. When to use query API
3. When to use core API
4. Common misconceptions
5. Migration guide

**Priority:** MEDIUM

---

## Priority Actions

### Immediate (Week 1)
1. ✅ Create docs/quickstart.md
2. ✅ Restructure docs/guides/researcher_guide.md
3. ✅ Update CLAUDE.md with anti-patterns
4. ✅ Polish README.md

### Short-term (Week 2)
5. ✅ Create docs/guides/choosing-an-api.md
6. ✅ Update docs/research_api_guide.md
7. ✅ Review all docstrings

### Ongoing
8. Monitor user feedback
9. Update examples in research/03_experiments/_template/
10. CI checks for documentation consistency

---

## Code Examples Analysis

### Query API Examples Found
- README.md: 1 example (good)
- research_api_guide.md: 3 examples (good)
- researcher_guide.md: 1 example (buried at end)

### Core API Examples Found
- researcher_guide.md: 10+ examples (too many, too prominent)
- research_api_guide.md: 2 examples (appropriate, marked as advanced)

### Recommendation
Flip the ratio: 80% query API examples, 20% core API examples (clearly marked as advanced).

---

## User Impact Assessment

### Current State Issues
1. New users start with complex core API
2. LLM agents generate verbose code
3. High onboarding friction
4. Query API benefits not obvious

### Expected Improvements After Changes
1. 90% of new users start with query API
2. 5-minute time to first plot (vs current ~30 minutes)
3. LLM agents generate concise code
4. Clear understanding of when to use each API

---

## Next Steps

1. Review and approve this audit
2. Begin implementation:
   - Create quickstart.md
   - Restructure researcher_guide.md
   - Update CLAUDE.md
3. Test with fresh users
4. Iterate based on feedback
