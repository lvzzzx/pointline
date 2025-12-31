# Spec: Create Researcher's Guide

## Goal
Create a comprehensive documentation guide for Quantitative Researchers (both human and LLM agents) to effectively use the data lake.

## Requirements
1.  **Target Audience:** Quantitative Researchers and LLM Assistants.
2.  **Location:** `docs/guides/researcher_guide.md`.
3.  **Content Structure:**
    -   **Introduction:** Purpose and design philosophy (PIT, Speed).
    -   **Access Patterns:** Examples for DuckDB (SQL) and Polars (Python).
    -   **Core Concepts:** Explanation of PIT semantics, Symbol Resolution (SCD2), and Fixed-Point Math.
    -   **Common Workflows:** Code recipes for common tasks (L2 loading, Order Book reconstruction, As-Of Joins, VWAP).
    -   **Agent Interface:** Schema descriptions and query templates optimized for LLMs.
4.  **Consistency:** Must align with `docs/architecture/design.md` and `src/dim_symbol.py`.

## Implementation Details
-   Create directory `docs/guides`.
-   Draft `researcher_guide.md` with clear headers and code blocks.
-   Ensure code examples are syntactically correct and follow project conventions.
