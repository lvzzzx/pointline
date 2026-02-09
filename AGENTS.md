# Repository Guidelines

## Project Structure & Module Organization
- `pointline/` holds the Python package (CLI, ETL services, research API, table schemas, I/O vendors).
- `pointline/cli/` contains CLI parser and command implementations.
- `tests/` contains pytest suites (plus targeted parser tests under `tests/parsers/`).
- `docs/` stores user guides, architecture docs, and development workflows.
- `scripts/` contains operational helpers (schema migration, data reorganization, validation scripts).
- `.github/workflows/` defines CI and release automation.

## Build, Test, and Development Commands
- Preferred setup (uv workflow):
  - `uv sync --all-extras`
  - `source .venv/bin/activate`
  - `pre-commit install`
- Alternative editable install:
  - `uv pip install -e ".[dev]"`
- Core checks:
  - `pytest`
  - `pytest --cov=pointline --cov-report=term -v`
  - `ruff check .`
  - `ruff format .`
  - `mypy pointline --ignore-missing-imports`
  - `bandit -r pointline -ll`
- Packaging:
  - `python -m build`
- CLI help and sanity checks:
  - `pointline --help`
  - `pointline config show`
  - `pointline symbol search BTC --exchange binance-futures`

## Coding Style & Naming Conventions
- Python 3.10+.
- Indentation: 4 spaces; use type hints for public functions.
- Naming: `snake_case` for functions/variables, `PascalCase` for classes.
- Formatting: Ruff is configured with `line-length = 100` in `pyproject.toml`. Use it as the source of truth for line length.

## Testing Guidelines
- Framework: `pytest`.
- Convention: test files named `test_*.py` under `tests/`.
- Markers are defined in `pyproject.toml` (`slow`, `integration`); use marker filters for focused runs.
- Aim for >80% coverage for substantial feature work.
- When adding functionality, write tests first (TDD) and keep fixtures localized to the test module.
- Before pushing, run `pre-commit run --all-files`.

## Commit & Pull Request Guidelines
- Commit messages in history use short, imperative summaries with optional scopes, e.g. `docs: add SZSE Level 3 timestamp semantics` or `feat(skills): add persona-router`.
- Use a consistent prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `style:`) and keep messages concise.
- PRs should include: a short summary, tests run (or reason not run), and references to any relevant docs or plan items.

## Development Workflows
- Standard change workflow:
  1. Create/switch to a feature branch.
  2. Add or update tests first (TDD).
  3. Implement code changes.
  4. Run quality gates locally: `pytest`, `ruff check .`, `ruff format .`, `pre-commit run --all-files`.
  5. Commit with a scoped message and open a PR.
- Git worktree workflow:
  1. `git worktree add ../pointline-<feature> <branch>`
  2. In that worktree: `uv venv && source .venv/bin/activate`
  3. `uv pip install -e ".[dev]"`
  4. `pre-commit install` (required per worktree)
- Ingestion workflow (CLI):
  1. `pointline bronze discover --pending-only`
  2. `pointline bronze ingest --vendor tardis --data-type trades --validate`
  3. `pointline manifest show`
  4. `pointline validation stats`
  5. `pointline dq run --table all` and `pointline dq summary`

## Architecture & Documentation
- Read `docs/architecture/design.md` before changing schemas or ETL semantics.
- Use `docs/development/README.md` and `docs/development/worktree-setup.md` for contributor workflows.
- Use `docs/reference/cli-reference.md` and `docs/reference/api-reference.md` when changing interfaces.
- If you change tooling or workflows, update the relevant file under `docs/development/`.

# ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in `PLANS.md`) from design to implementation.
