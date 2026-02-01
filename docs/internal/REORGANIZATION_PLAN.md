# Documentation Reorganization Plan

## Current State Analysis

### Problems
1. **No clear entry point**: 6 top-level docs, unclear which to read first
2. **Mixed audiences**: User docs mixed with internal plans and architecture
3. **No navigation**: Users don't know the learning path
4. **Scattered reference material**: API docs split across multiple files

### Current Structure
```
docs/
├── quickstart.md (332 lines)              ← USER: Good!
├── research_api_guide.md (291 lines)      ← REFERENCE: Needs organization
├── schemas.md (730 lines)                 ← REFERENCE: Should be in reference/
├── guides/
│   ├── choosing-an-api.md (388 lines)     ← USER: Good!
│   ├── researcher_guide.md (567 lines)    ← USER: Good but long
│   └── dim_asset_stats_usage.md (192 lines) ← USER: Niche topic
├── architecture/                          ← INTERNAL/ADVANCED: OK as-is
├── plans/                                 ← INTERNAL: Should be hidden
├── implementation/                        ← INTERNAL: Should be hidden
├── development/                           ← DEVELOPER: OK
├── data_sources/                          ← REFERENCE: Should be consolidated
├── bronze-prehooks-implementation.md      ← INTERNAL: Should be in internal/
├── ci-cd.md                               ← DEVELOPER: Should be in development/
└── cli-migration-plan.md                  ← INTERNAL: Should be in internal/
```

---

## Proposed New Structure

### Design Principles
1. **Clear learning path**: README → Quickstart → Tutorial → Guides → Reference
2. **Audience separation**: User docs vs Developer docs vs Internal docs
3. **Progressive disclosure**: Start simple, reveal complexity as needed
4. **Scannable navigation**: Every directory has a README.md

### New Structure
```
docs/
│
├── README.md                              ← NEW: "Start here" navigation hub
│
├── quickstart.md                          ← KEEP: 5-minute intro (no changes)
├── tutorial.md                            ← NEW: 30-minute end-to-end workflow
├── troubleshooting.md                     ← NEW: Common errors and solutions
│
├── guides/                                ← USER GUIDES (how-to for specific tasks)
│   ├── README.md                          ← NEW: Navigation for guides
│   ├── researcher-guide.md                ← RENAME: researcher_guide.md → researcher-guide.md
│   ├── choosing-an-api.md                 ← KEEP: No changes
│   ├── data-ingestion.md                  ← NEW: How to get data into the lake
│   ├── production-workflows.md            ← NEW: Reproducible research patterns
│   └── dim-asset-stats-usage.md           ← RENAME: dim_asset_stats_usage.md
│
├── reference/                             ← NEW: Complete API/CLI/schema reference
│   ├── README.md                          ← NEW: Navigation for reference
│   ├── api-reference.md                   ← NEW: Consolidated API docs
│   ├── cli-reference.md                   ← NEW: All CLI commands with examples
│   ├── schemas.md                         ← MOVE: From docs/schemas.md
│   └── data-sources.md                    ← NEW: Consolidate data_sources/
│
├── architecture/                          ← KEEP AS-IS: Advanced/internal design docs
│   └── (no changes)
│
├── development/                           ← DEVELOPER DOCS (contributing, setup)
│   ├── README.md                          ← NEW: Developer guide navigation
│   ├── worktree-setup.md                  ← KEEP: Already here
│   ├── ci-cd.md                           ← MOVE: From docs/ci-cd.md
│   └── contributing.md                    ← NEW: How to contribute
│
└── internal/                              ← NEW: Internal implementation notes
    ├── README.md                          ← NEW: Explain this is internal
    ├── plans/                             ← MOVE: From docs/plans/
    ├── implementation/                    ← MOVE: From docs/implementation/
    ├── bronze-prehooks-implementation.md  ← MOVE: From docs/
    └── cli-migration-plan.md              ← MOVE: From docs/
```

---

## Migration Tasks

### Phase 1: Navigation & New Structure (High Priority)
- [ ] Create `docs/README.md` - Navigation hub with clear learning path
- [ ] Create `docs/guides/README.md` - Guide to guides
- [ ] Create `docs/reference/README.md` - Reference navigation
- [ ] Create `docs/development/README.md` - Developer onboarding
- [ ] Create `docs/internal/README.md` - Mark as internal

### Phase 2: New Essential Docs (High Priority)
- [ ] Create `docs/tutorial.md` - 30-minute end-to-end workflow
- [ ] Create `docs/troubleshooting.md` - Common errors and solutions
- [ ] Create `docs/reference/cli-reference.md` - Complete CLI reference
- [ ] Create `docs/guides/data-ingestion.md` - How to get data in

### Phase 3: Consolidation & Moves (Medium Priority)
- [ ] Move `docs/schemas.md` → `docs/reference/schemas.md`
- [ ] Rename `docs/guides/researcher_guide.md` → `researcher-guide.md`
- [ ] Rename `docs/guides/dim_asset_stats_usage.md` → `dim-asset-stats-usage.md`
- [ ] Move `docs/ci-cd.md` → `docs/development/ci-cd.md`
- [ ] Move `docs/plans/` → `docs/internal/plans/`
- [ ] Move `docs/implementation/` → `docs/internal/implementation/`
- [ ] Move `docs/bronze-prehooks-implementation.md` → `docs/internal/`
- [ ] Move `docs/cli-migration-plan.md` → `docs/internal/`

### Phase 4: Consolidate Reference Docs (Medium Priority)
- [ ] Create `docs/reference/api-reference.md` (combine and reorganize):
  - Content from `research_api_guide.md`
  - API sections from `researcher_guide.md`
  - Discovery API documentation
- [ ] Create `docs/reference/data-sources.md` (consolidate):
  - Content from `data_sources/` directory
  - Vendor-specific documentation

### Phase 5: Update Cross-References (Low Priority)
- [ ] Update all internal links to reflect new structure
- [ ] Update README.md to point to new docs/README.md
- [ ] Update CLAUDE.md to reference new structure

---

## New File Templates

### docs/README.md
```markdown
# Pointline Documentation

Welcome! This guide will help you find what you need.

## 🚀 Getting Started (New Users)

**Never used Pointline before?** Start here:

1. **[5-Minute Quickstart](quickstart.md)** - Get up and running
2. **[30-Minute Tutorial](tutorial.md)** - Complete end-to-end workflow
3. **[Troubleshooting](troubleshooting.md)** - Common errors and solutions

## 📖 User Guides

**Want to accomplish something specific?** See [guides/](guides/):

- [Researcher's Guide](guides/researcher-guide.md) - Comprehensive reference
- [Choosing an API](guides/choosing-an-api.md) - Query API vs Core API
- [Data Ingestion](guides/data-ingestion.md) - How to get data into the lake
- [Production Workflows](guides/production-workflows.md) - Reproducible research

## 📚 Reference Documentation

**Looking up specific details?** See [reference/](reference/):

- [API Reference](reference/api-reference.md) - Complete Python API
- [CLI Reference](reference/cli-reference.md) - All command-line tools
- [Schemas](reference/schemas.md) - Table structures and data types
- [Data Sources](reference/data-sources.md) - Vendor-specific details

## 🏗️ Advanced Topics

- [Architecture](architecture/design.md) - System design and principles
- [Performance](architecture/performance-considerations.md) - Optimization guide

## 👥 Contributing

- [Development Guide](development/README.md) - Setup for contributors
- [Worktree Setup](development/worktree-setup.md) - Git worktree workflow

---

**Can't find what you need?** Check the [troubleshooting guide](troubleshooting.md) or [open an issue](https://github.com/pointline/pointline/issues).
```

### docs/tutorial.md (Outline - to be written)
```markdown
# Tutorial: Your First Analysis (30 Minutes)

This tutorial walks you through a complete workflow from setup to results.

## What You'll Learn
- Set up your environment
- Download sample data
- Ingest data into the lake
- Discover available data
- Load and analyze market data
- Create visualizations

## Prerequisites
- Python 3.10+
- 30 minutes

## Step 1: Setup
...

## Step 2: Get Sample Data
...

## Step 3: Ingest Data
...

## Step 4: Discover & Explore
...

## Step 5: Analyze
...

## Next Steps
...
```

### docs/troubleshooting.md (Outline - to be written)
```markdown
# Troubleshooting Guide

Common errors and how to fix them.

## Table of Contents
- [Installation & Setup](#installation--setup)
- [Data Loading Errors](#data-loading-errors)
- [Symbol Resolution](#symbol-resolution)
- [Performance Issues](#performance-issues)

## Installation & Setup

### "Lake root not configured"
**Error:**
```
FileNotFoundError: Lake root not found: /Users/username/data/lake
```

**Solution:**
...

## Data Loading Errors

### "No data found for date range"
...

### "Symbol not found"
...
```

---

## Implementation Order

### Week 1: Foundation
1. Create all README.md files (navigation)
2. Create troubleshooting.md
3. Update main README.md to link to docs/README.md

### Week 2: Content
4. Create tutorial.md
5. Create reference/cli-reference.md
6. Create guides/data-ingestion.md

### Week 3: Reorganization
7. Move files to new locations
8. Update all cross-references
9. Test all links

---

## Success Metrics

After reorganization, a new user should be able to:
1. ✅ Find the "start here" page in < 10 seconds
2. ✅ Complete first analysis in < 10 minutes (with tutorial)
3. ✅ Solve common errors using troubleshooting guide
4. ✅ Look up any API function in < 30 seconds

---

## Questions for Discussion

1. Should we keep `research_api_guide.md` as-is or consolidate into `reference/api-reference.md`?
2. Should `internal/` be gitignored or kept in repo?
3. Should we create a `docs/faq.md` or fold FAQs into troubleshooting?
4. Should guides use kebab-case (my-guide.md) or snake_case (my_guide.md)?

---

## Approval

- [ ] Structure approved
- [ ] File naming convention decided
- [ ] Ready to start Phase 1
