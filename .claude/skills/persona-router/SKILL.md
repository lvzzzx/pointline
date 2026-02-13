---
name: persona-router
description: Use when a user wants to choose or switch personas for an LLM coding task, map work to Pointline roles, or generate copy-paste persona prompts (including HFT/MFT quant variants).
---

# Persona Router

## Overview
Route a task to the correct Pointline persona and return a copy-paste prompt template the user can run immediately.

## When To Use
- User asks which persona should handle a task.
- User asks how to switch personas between tasks.
- User asks for reusable persona prompts.
- User asks for HFT vs MFT persona variants.

## Workflow
1. If the user explicitly specifies a persona, honor it directly.
2. Otherwise, classify the task using the routing rules below.
3. Choose one primary persona. If mixed scope, use a phase split.
4. Return a ready-to-use prompt with `Persona`, `Task`, `Focus`, and `Deliver`.
5. Keep output concise and actionable.

## Routing Rules
- Storage/replay/ingestion semantics: `Data Infra Engineer`
- Validation/schema/contract semantics: `Data Quality Engineer`
- Research API/feature-pipeline behavior: `Research Engineer`
- Signal definition/evaluation and experiments: `Quant Researcher`
- Tooling/CI/dev workflow: `Platform/DevEx`

If Quant Researcher is selected:
- High-frequency microstructure/replay/latency focus -> `Quant Researcher (HFT)`
- Multi-stream seconds/minutes feature workflows -> `Quant Researcher (MFT)`

## Output Format
Always return:

```text
Persona: <role>
Task: <rewritten user task>
Context: <key files/tables/constraints>
Focus:
- <priority 1>
- <priority 2>
- <priority 3>
Deliver:
- <deliverable 1>
- <deliverable 2>
```

For mixed ownership tasks, return phase split:

```text
Phase 1 Persona: <role>
Phase 1 Task: <task>
Phase 1 Deliver: <output>

Phase 2 Persona: <role>
Phase 2 Task: <task>
Phase 2 Deliver: <output>
```

## Prompt Source
Prefer template language from `docs/persona-prompts.md` when available in the active repo.

For full variants/examples, load:
- `references/templates.md`
