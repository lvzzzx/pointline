# Internal Documentation

This directory contains **internal implementation notes, plans, and design documents** that are not relevant to end users or external contributors.

---

## ⚠️ Note for Users

**If you're a Pointline user or contributor, you probably don't need to read anything in this directory.**

For user-facing documentation, see:
- [Documentation Hub](../README.md)
- [Quickstart](../quickstart.md)
- [User Guides](../guides/)
- [Reference Documentation](../reference/)

---

## 📁 Contents

This directory contains:

### Implementation Plans
**Purpose:** Working documents for features during development

**Located in:** `plans/`

These are **historical artifacts** - they document the planning process but may be outdated once the feature is implemented. Refer to user-facing docs for current behavior.

---

### Implementation Notes
**Purpose:** Technical notes from development process

**Located in:** Root of `internal/` (or `archive/` if historical)

These capture implementation details, decisions made during development, and context that might not fit in code comments or user docs.

---

### Archive
**Purpose:** Historical documents from completed work

**Located in:** `archive/`

Documents that have been superseded or are historical only. Preserved for reference but not actively maintained. See [archive/README.md](archive/README.md) for contents.

Examples:
- `archive/bronze-prehooks-implementation.md` - Bronze layer preprocessing implementation (archived)
- `archive/cli-migration-plan.md` - CLI refactoring notes (archived)

---

## 🔍 When to Use This Directory

**Add documents here if:**
- ✅ It's an implementation plan (pre-development)
- ✅ It documents internal decisions not relevant to users
- ✅ It's a working document that will become outdated
- ✅ It contains experimental or deprecated approaches

**Don't add documents here if:**
- ❌ Users need this information → Put in `docs/guides/`
- ❌ It's API reference material → Put in `docs/reference/`
- ❌ It's architecture design → Put in `docs/architecture/`
- ❌ It's for contributors → Put in `docs/development/`

---

## 📚 Document Lifecycle

```
Idea → Plan → Implementation → User Docs (docs/)
                                        ↓
                                Archive (if historical)
                                        ↓
                                Historical artifact
                                (kept for reference)
```

**Example:**
1. Feature idea: "Data Discovery API"
2. Planning: `internal/plan_data_discovery_api.md` created (later archived)
3. Implementation: Code written, tests added
4. Documentation: `docs/guides/researcher-guide.md` updated with discovery API usage
5. Archive: Plan moved to `internal/archive/` for historical reference

---

## 🗂️ Directory Structure

```
internal/
├── README.md (this file)
├── plans/                              # Feature planning documents
│   ├── (archived: plan_data_discovery_api.md, plan_quant360_szse_l3.md, query-api-promotion-plan.md)
│   └── ...
├── implementation/                     # Implementation notes
│   └── (archived: timezone-partitioning.md)
├── archive/                            # Historical documents
│   ├── bronze-prehooks-implementation.md
│   ├── cli-migration-plan.md
│   └── ...
└── ...
```

---

## 💡 For Maintainers

**When to clean up internal docs:**
- Archive outdated plans after feature is complete
- Move evergreen content to appropriate public docs
- Remove plans that are no longer relevant

**Guidelines:**
- Keep plans for 6-12 months after implementation (for context)
- Mark obsolete docs with `[OBSOLETE]` prefix
- Link from plans to final implementation in public docs
