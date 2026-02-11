# Architecture Archive

This directory contains all architectural documentation except the core `design.md`.

## Why Archive Everything Else?

The `design.md` document provides the essential mental model for the data lake architecture. All other documents are preserved here for reference but are not required for daily development.

## Live Document

| Document | Location | Purpose |
|:---|:---|:---|
| **Local-Host Data Lake Design** | `docs/architecture/design.md` | Core architecture: Bronze/Silver/Gold, PIT semantics, storage defaults |

## Archived Documents

### Core Architecture (Historical Context)
| Document | Date | Notes |
|:---|:---|:---|
| `bar-aggregation.md` | 2026-02 | Bar aggregation implementation spec |
| `context-risk-layer-design.md` | 2026-02 | Context/risk layer with oi_capacity plugin |
| `north-star-research-architecture.md` | 2026-02 | Target architecture for research execution |
| `storage-io-design.md` | 2026-02 | Storage layer architecture (Delta + Polars) |

### Infrastructure & Planning
| Document | Date | Notes |
|:---|:---|:---|
| `infra-north-star.md` | 2026-02 | Infrastructure vision and principles |
| `infra-roadmap.md` | 2026-02 | Phased infrastructure roadmap |
| `performance-considerations.md` | 2026-02 | Performance tuning guide |

### Specifications
| Document | Date | Notes |
|:---|:---|:---|
| `dim-symbol-sync.md` | 2026-02 | CLI spec for symbol synchronization |
| `ingest-quarantine.md` | 2026-02 | File-level quarantine policy |
| `szse-timestamp-semantics.md` | 2026-02 | SZSE L3 timestamp handling |

### ADRs & Design History
| Document | Date | Notes |
|:---|:---|:---|
| `adr-research-framework-v2-clean-architecture.md` | 2026-02 | ADR with v1→v2 schema evolution note |
| `resample-aggregate-design.md` | 2026-02 | v1 design superseded by bar-aggregation |

### API Validation
| Document | Date | Notes |
|:---|:---|:---|
| `coingecko_api_validation.md` | 2026-02 | Historical API validation results |

### Future/Roadmap
| Document | Date | Notes |
|:---|:---|:---|
| `quant-agent-architecture.md` | 2026-02 | 🚧 PROPOSED — NOT IMPLEMENTED |

### Meta
| Document | Date | Notes |
|:---|:---|:---|
| `README.md` | 2026-02 | Original architecture folder index |

## When to Look Here

- 🔍 **Deep dive** into a specific component
- 📚 **Historical context** for design decisions
- 🔮 **Future roadmap** (quant-agent)
- 📊 **Performance tuning** specifics
- 🌏 **Vendor-specific** semantics (SZSE)

## Single Source of Truth

For everyday development, refer only to:
- **`docs/architecture/design.md`** — Core data lake architecture
- **`docs/guides/`** — How-to guides for specific tasks
- **`docs/reference/`** — API and schema reference
