# Spec: Refactor Documentation Structure

## Goal
Reorganize the project documentation to improve navigability and provide a clear entry point for new users and developers.

## Requirements
1.  **Centralized Docs:** Move all architectural documents to `docs/architecture/`.
2.  **Project Entry Point:** Create a high-quality root `README.md` that provides:
    -   Project Title & Description
    -   Quick Links (Architecture, Setup, Contributing)
    -   Basic "Getting Started" instructions
3.  **Configuration Consistency:** Ensure `pyproject.toml` points to the new location of the readme file if it was referencing the old one.

## Implementation Details
-   Move `design.md` -> `docs/architecture/design.md`.
-   Create `README.md` at the project root.
-   Update `pyproject.toml` `readme` field.
