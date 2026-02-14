# Codex Research Proposal Template Guide

This document defines how to write research proposals in this repository. The default use case is research ideas (feature engineering, signal design, dataset studies, experiment programs), not implementation task breakdown.

Store proposal documents under `.proposals/`.
Canonical example: `.proposals/chinese-stock-l3-mft-features.md`.

## How to use Proposals and PROPOSALS.md

Use a proposal when we are exploring what to research, why it may work, and how we will validate it before coding a full pipeline. A proposal should make decisions explicit and expose uncertainty early.

If a proposal is accepted and moves to implementation, create an ExecPlan that follows `.agent/PLANS.md`.

## Non-negotiable requirements

* Every proposal must be self-contained and understandable by a newcomer.
* Every proposal must state the research hypothesis in plain language.
* Every proposal must include a **Clarifying Questions for Requester** section.
* Clarifying questions must be specific, decision-oriented, and minimal.
* Every proposal must include default assumptions if questions are unanswered.
* Every proposal must define concrete validation (metrics, baselines, and pass/fail criteria).
* Every proposal must identify data sources and PIT (point-in-time) constraints.

## Clarifying question protocol

Ask only questions that change research direction, cost, timeline, or acceptance criteria.

Use this pattern for each question:

* `Question`: What decision is needed.
* `Why it matters`: What changes based on the answer.
* `Default if no answer`: The assumption used to proceed.

Guidelines:

* Ask 1-5 questions.
* Prefer multiple-choice framing when possible.
* Do not ask for information that can be discovered from the codebase or proposal corpus.
* If risk is low, document assumptions and continue.

## Recommended research proposal structure

# `<Short, outcome-oriented title>`

## Executive Summary

Summarize the idea, expected edge, and how success will be measured.

## Research Objective and Hypothesis

State the objective and one or more testable hypotheses.

## Scope and Non-Goals

Define what is in scope and explicitly excluded.

## Data and PIT Constraints

List required datasets and table names, sampling horizon, and point-in-time constraints that prevent lookahead bias.

## Feature or Model Concept

Describe the candidate features/signals/models and the intuition behind them.

## Experiment Design

Describe splits, baselines, ablations, controls, and robustness checks.

## Evaluation Metrics and Acceptance Criteria

Define the exact metrics, target thresholds, and failure conditions.

## Risks and Mitigations

Cover data quality risks, overfitting risks, leakage risks, and operational constraints.

## Implementation Readiness

Describe what code artifacts are expected if approved (for example: research notebook/script, ETL additions, tests, docs).

## Clarifying Questions for Requester

Use this exact format:

* Question: …
  Why it matters: …
  Default if no answer: …

## Decision Needed

State the explicit approval needed to proceed and what the next artifact will be (usually an ExecPlan or experiment script).

## Decision Log

Track proposal revisions:

* Decision: …
  Rationale: …
  Date/Author: …

## Handoff to ExecPlan

If approved for build-out, create an ExecPlan that:

* references this proposal by path,
* resolves all clarifying question outcomes,
* converts accepted experiments into milestone-based implementation steps.

## Proposal quality bar

A proposal is ready when:

* the hypothesis is testable,
* data requirements and PIT constraints are explicit,
* success/failure criteria are measurable,
* unresolved decisions are minimal and clearly listed,
* the requester can approve or redirect with low ambiguity.
