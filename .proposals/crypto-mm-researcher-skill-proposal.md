# Crypto Market Making Research Skill (L2-First): Proposal

## Executive Summary

This proposal introduces a new skill: `crypto-mm-researcher`, focused on systematic crypto market making research rather than directional alpha prediction. The skill will provide practical guidance for inventory-aware quoting, adverse-selection control, fill modeling, and risk management under realistic crypto data constraints.

The central design choice is **L2-first realism**: most crypto venues provide incremental L2 order book updates, not true L3 queue-level data. Therefore, v1 will prioritize robust research workflows at **5s-60s quoting horizons** (default), with a clearly caveated advanced mode for 100ms-5s.

Success is measured by documentation quality, methodological correctness, and the ability to support reproducible, cost-aware MM research workflows.

## Research Objective and Hypothesis

Objective: define a production-relevant skill package for crypto MM research that is realistic under L2-only market data and suitable for medium-low latency decision loops.

Hypothesis H1: An L2-first MM research framework (inventory + toxicity + conservative fill assumptions) produces more robust and transferable results than queue-perfect assumptions that require unavailable L3 data.

Hypothesis H2: At 5s-60s horizons, inventory/risk controls and cost modeling explain more performance variance than microsecond-level queue assumptions.

Hypothesis H3: A structured MM skill (models + simulator assumptions + evaluation + risk controls) reduces research leakage and over-optimistic backtests compared with ad hoc MM experimentation.

## Scope and Non-Goals

In scope:

- New skill package `crypto-mm-researcher`.
- L2-first market making methodology for CEX perps.
- Inventory-aware quoting and spread/skew control.
- Fill modeling assumptions suitable for incremental L2 data.
- Cost-aware and adverse-selection-aware evaluation framework.
- Risk controls and stress testing playbook.

Non-goals:

- No claim of queue-exact L3 fidelity in v1.
- No exchange-specific execution engine implementation.
- No sub-millisecond infra optimization guidance as core scope.
- No fully automated strategy deployment tooling.

## Data and PIT Constraints

Primary data domains:

- Incremental L2 order book updates (price-level deltas).
- Trades (with aggressor side when available).
- Quotes / top-of-book snapshots.
- Derivatives context: funding, open interest, liquidations.
- Fee schedules and tick/lot metadata per exchange.

PIT constraints:

- All feature computation and quote decisions must be forward-only by event time.
- Cross-stream joins (book/trades/funding/OI) must use backward/as-of alignment with explicit staleness handling.
- No future normalization statistics.
- Deterministic replay assumptions for simulator outputs.

## Feature or Model Concept

Core MM research components:

1. Quoting model:
   - Reservation price and inventory skew (Avellaneda-Stoikov-inspired, crypto-adapted).
   - Dynamic spread control by volatility, spread regime, and toxicity.

2. Microstructure features (L2-first):
   - Top-k imbalance, OFI, microprice drift, spread dynamics, cancel intensity proxies.

3. Inventory policy:
   - Soft and hard inventory limits.
   - Single-side bias under inventory pressure.
   - Optional taker hedge triggers for breach states.

4. Fill and adverse-selection modeling:
   - Probabilistic maker fill model under L2-only constraints.
   - Conservative adverse-selection penalty assumptions.

## Experiment Design

Design principles:

- Start with a transparent baseline model and simple control rules.
- Use walk-forward evaluation with session/regime segmentation.
- Run ablations by removing one control block at a time.

Baseline stack:

- Baseline A: symmetric fixed-spread two-sided quoting with static inventory limits.
- Baseline B: inventory-skewed quoting with static risk filters.
- Candidate C: dynamic spread + toxicity filter + inventory state machine.

Ablations:

- Remove inventory skew.
- Remove toxicity gating.
- Remove dynamic spread widening.
- Replace probabilistic fill model with naive fill assumption (for bias quantification).

## Evaluation Metrics and Acceptance Criteria

Required metrics:

- Net PnL (after maker/taker fees, spread costs, slippage assumptions).
- Inventory-adjusted Sharpe.
- Adverse selection cost per fill.
- Fill rate, quote-to-fill ratio, cancel ratio.
- Inventory distribution (mean, variance, tail percentiles).
- Drawdown and drawdown duration.
- Session and volatility-regime stability.

Acceptance criteria for skill readiness:

- Documents provide explicit L2-only assumptions and limitations.
- Evaluation checklist includes cost and adverse-selection decomposition.
- Risk-control section includes hard-stop conditions and stress scenarios.
- Example workflows can be executed conceptually without requiring L3 data.

Failure conditions:

- Implicit L3 assumptions appear in core guidance.
- Evaluation ignores costs or adverse selection.
- No clear mitigation for inventory tail risk.

## Risks and Mitigations

Risk: over-optimistic fills due to missing L3 queue state.
Mitigation: conservative fill priors, sensitivity analysis, explicit caveats.

Risk: strategy overfit to one session/regime.
Mitigation: required session/regime breakdown and stability checks.

Risk: data quality/timestamp issues in high-frequency streams.
Mitigation: event-time hygiene, staleness flags, deterministic replay checks.

Risk: underestimating cost drag in noisy symbols.
Mitigation: conservative fee/slippage defaults and stress test at 2x costs.

## Implementation Readiness

If approved, create:

- `.claude/skills/crypto-mm-researcher/SKILL.md`
- `.claude/skills/crypto-mm-researcher/references/mm-models.md`
- `.claude/skills/crypto-mm-researcher/references/execution-simulation.md`
- `.claude/skills/crypto-mm-researcher/references/evaluation.md`
- `.claude/skills/crypto-mm-researcher/references/risk-controls.md`
- `.claude/skills/crypto-mm-researcher/references/market-structure-mm.md`

Expected content standards:

- Practical defaults for L2-only environments.
- Clear separation of v1 default (5s-60s) and advanced mode (100ms-5s).
- Reusable checklists and decision flow for model/horizon selection.

## Clarifying Questions for Requester

- Question: Should v1 scope be CEX perps only, or include spot MM from day one?
  Why it matters: Spot changes fee, basis, and hedging assumptions.
  Default if no answer: CEX perps only in v1.

- Question: Should the default strategy style be two-sided passive quoting with optional taker hedge, or inventory-first single-sided quoting?
  Why it matters: Changes simulator focus and primary KPIs.
  Default if no answer: Two-sided passive quoting core + optional taker hedge module.

- Question: Should advanced 100ms-5s mode be included in v1 docs or deferred to v2?
  Why it matters: Impacts documentation complexity and user expectations.
  Default if no answer: Include as caveated advanced appendix in v1.

## Decision Needed

Approve this proposal to draft and add the new `crypto-mm-researcher` skill with L2-first defaults and 5s-60s primary horizon.

## Decision Log

- Decision: Set default quoting horizon to 5s-60s (L2-first).
  Rationale: Most crypto feeds provide L2 incremental data, not L3 queue state; this improves realism and robustness.
  Date/Author: 2026-02-18 / Codex

- Decision: Position 100ms-5s as advanced mode only.
  Rationale: Requires stronger execution assumptions and stricter fill-model caveats.
  Date/Author: 2026-02-18 / Codex

## Handoff to ExecPlan

If approved for implementation:

- Create an ExecPlan referencing this proposal.
- Resolve clarifying questions explicitly.
- Implement skill files in milestones: core SKILL, model reference, simulator assumptions, evaluation checklist, risk controls.
- Validate cross-references and consistency with existing `crypto-mft-researcher` skill.