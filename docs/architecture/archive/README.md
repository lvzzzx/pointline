# Architecture Archive

This directory contains historical architectural documents that are preserved for reference but are not the current source of truth for system design.

## Contents

| Document | Date | Reason Archived |
|:---|:---|:---|
| `adr-research-framework-v2-clean-architecture.md` | 2026-02-10 | ADR documenting v2 architecture decisions. Implementation evolved from v1 design to v2 schemas. Decisions are encoded in the codebase. |
| `coingecko_api_validation.md` | 2026-02-10 | Historical API validation results. CoinGecko integration is complete; dim_asset_stats table is implemented. |
| `resample-aggregate-design.md` | 2026-02-10 | v1 design spec superseded by `bar-aggregation.md`. Initial design was iterated into the v2 resample module implementation. |

## When to Archive

Documents should be moved here when:
- ✅ Design decisions are encoded in implementation
- ✅ Specifications have been superseded by newer versions
- ✅ API validation/testing is complete
- ✅ ADRs serve historical context only

## Do Not Archive

- ❌ Current system architecture
- ❌ Active API specifications
- ❌ Implemented feature documentation
- ❌ Operational runbooks
