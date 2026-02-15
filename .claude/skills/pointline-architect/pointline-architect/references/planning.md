# Planning & Proposal Standards

## Table of Contents
- [ExecPlan Standard](#execplan-standard)
- [Research Proposal Standard](#research-proposal-standard)
- [Clarifying Question Protocol](#clarifying-question-protocol)

## ExecPlan Standard

Source: `.agent/PLANS.md`. Use for features and refactors.

### Non-Negotiable Requirements

1. **Self-contained**: Every ExecPlan fully understandable by a novice. No external context needed.
2. **Living document**: Maintain Progress, Surprises & Discoveries, Decision Log, Outcomes sections throughout execution.
3. **Produces working behavior**: Not just code changes — demonstrably working end-to-end.

### Required Sections

1. **Purpose / Big Picture** — User-facing behavior this plan delivers.
2. **Progress** — Checklist with timestamps. Updated as work proceeds.
3. **Surprises & Discoveries** — Unexpected behaviors, optimizations found during implementation.
4. **Decision Log** — All decisions with rationale and date. Immutable once written.
5. **Outcomes & Retrospective** — Summary, gaps, lessons learned. Written after completion.
6. **Context and Orientation** — File paths, module names, term definitions. Everything needed to start.
7. **Plan of Work** — Sequence of edits with exact file locations.
8. **Concrete Steps** — Exact commands, expected output.
9. **Validation and Acceptance** — Testing strategy and behavior verification.
10. **Idempotence and Recovery** — Safe retries, rollback procedures.

### Formatting Rules

- Single fenced code block labeled `md`
- Two newlines after every heading
- Prose-first narrative (avoid checklists in narrative sections)
- Plain language — no jargon without immediate definition

### Example Structure

```md
# ExecPlan: Add Options Chain Table

## Purpose

Support ingestion of Deribit options chain snapshots into a new `options_chain` Silver table,
enabling research on implied volatility surfaces.

## Progress

- [x] 2024-11-14: Define TableSpec for options_chain
- [x] 2024-11-14: Add parser in vendors/tardis/
- [ ] Add validation rules
- [ ] Integration test with sample data

## Context and Orientation

- New spec goes in `pointline/schemas/events.py`
- Parser follows existing Tardis patterns in `pointline/vendors/tardis/parsers.py`
- Sample Bronze file: `tests/fixtures/tardis/options_chain_sample.csv`

## Plan of Work

1. Define `OPTIONS_CHAIN` TableSpec with columns: ...
2. Register in `EVENT_SPECS` dict
3. Implement `parse_options_chain()` parser
...
```

## Research Proposal Standard

Source: `.agent/PROPOSALS.md`. Use for research hypotheses and experiments.

### Non-Negotiable Requirements

1. **Self-contained**: Understandable by newcomers.
2. **Testable hypothesis**: In plain language, falsifiable.
3. **Clarifying questions**: Specific, decision-oriented, with defaults.
4. **Concrete validation**: Metrics, baselines, pass/fail criteria.
5. **PIT constraints**: Data sources identified, lookahead prevention addressed.

### Required Sections

1. **Executive Summary** — Idea, expected edge, success measurement (3-5 sentences).
2. **Research Objective and Hypothesis** — Testable hypotheses, mechanism.
3. **Scope and Non-Goals** — What's in, what's explicitly out.
4. **Data and PIT Constraints** — Datasets, sampling horizon, lookahead prevention.
5. **Feature or Model Concept** — Technical approach.
6. **Experiment Design** — Train/val/test splits, baselines, ablations, controls.
7. **Evaluation Metrics and Acceptance Criteria** — Quantitative pass/fail.
8. **Risks and Mitigations** — What could go wrong, contingency plans.
9. **Implementation Readiness** — Expected code artifacts, dependencies.
10. **Clarifying Questions for Requester** — 1-5 questions.
11. **Decision Needed** — What approval is being sought.
12. **Decision Log** — Track revisions and decisions over time.

### Evaluation Metrics Template

For signal research:
- IC > 0.02 (meaningful), > 0.05 (strong), > 0.08 (check for bugs)
- ICIR > 0.5 (acceptable), > 1.0 (strong)
- OOS/IS Sharpe ratio > 0.5 (not overfit)
- Deflated Sharpe correcting for number of trials

For infrastructure:
- Ingestion throughput (rows/sec)
- Query latency (p50, p99)
- Storage efficiency (bytes/row)
- Correctness: zero quarantine on known-good data

## Clarifying Question Protocol

Apply to both ExecPlans and Proposals.

### Rules

- Ask only questions that change **direction, cost, timeline, or acceptance criteria**.
- 1-5 questions maximum.
- Each question has three parts:
  1. **Question**: Specific, answerable.
  2. **Why it matters**: What decision depends on the answer.
  3. **Default if no answer**: What you'll assume.

### Example

> **Q1: Should the options_chain table support both European and American exercise styles?**
>
> *Why it matters:* Affects schema design (exercise_type column) and validation rules.
>
> *Default:* Include `exercise_type` column, support both.
