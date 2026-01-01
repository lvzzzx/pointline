# Repository Guidelines

## Project Structure & Module Organization
- `pointline/` holds the Python package (ETL utilities and symbol management). Example: `pointline/dim_symbol.py`.
- `tests/` contains pytest tests, e.g. `tests/test_dim_symbol.py`.
- `docs/` stores architecture and guides, including `docs/architecture/design.md`.
- `conductor/` captures project workflow and product guidance (`conductor/workflow.md`, `conductor/product-guidelines.md`, `conductor/tech-stack.md`).

## Build, Test, and Development Commands
- `pip install -e .` installs the package in editable mode.
- `pip install -e .[dev]` adds dev dependencies (pytest).
- `pytest` runs the full test suite (uses `tests/`).
- `hatch build` builds a wheel using the configured build backend.

## Coding Style & Naming Conventions
- Python 3.10+.
- Indentation: 4 spaces; use type hints for public functions.
- Naming: `snake_case` for functions/variables, `PascalCase` for classes.
- Formatting: Ruff is configured with `line-length = 100` in `pyproject.toml`. Use it as the source of truth for line length.

## Testing Guidelines
- Framework: `pytest`.
- Convention: test files named `test_*.py` under `tests/`.
- Aim for >80% coverage as defined in `conductor/workflow.md`.
- When adding functionality, write tests first (TDD) and keep fixtures localized to the test module.

## Commit & Pull Request Guidelines
- Commit messages in history use short, imperative summaries with optional scopes, e.g. `docs: Add Researcher’s Guide` or `conductor(plan): ...`.
- Use a consistent prefix (`feat:`, `fix:`, `docs:`, `conductor(scope):`) and keep messages concise.
- PRs should include: a short summary, tests run (or reason not run), and references to any relevant docs or plan items.

## Architecture & Product Docs
- Read `docs/architecture/design.md` before changing schemas or ETL semantics.
- If you change the tech stack or tooling, update `conductor/tech-stack.md` first and note the rationale.
