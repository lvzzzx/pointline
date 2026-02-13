# Repository Guidelines

## Project Structure & Module Organization
- `pointline/`: main Python package (CLI, ETL services, research API, schemas, vendor I/O).
- `pointline/cli/`: CLI parser, command implementations, and command wiring.
- `tests/`: pytest suites, including parser-focused tests under `tests/parsers/`.
- `docs/`: architecture, references, and contributor workflow docs.
- `scripts/`: operational helpers (migrations, data reorg, validation tasks).
- `.github/workflows/`: CI and release automation.

## Build, Test, and Development Commands
- `uv sync --all-extras`: install project and optional dependency groups.
- `source .venv/bin/activate`: activate local virtual environment.
- `pre-commit install`: set up local hooks for lint/format/static checks.
- `pytest`: run all tests.
- `pytest --cov=pointline --cov-report=term -v`: run tests with coverage details.
- `ruff check .` / `ruff format .`: lint and format codebase.
- `mypy pointline --ignore-missing-imports`: run type checks.
- `bandit -r pointline -ll`: run security-focused static checks.
- `python -m build`: build distributable package artifacts.

## Coding Style & Naming Conventions
- Python 3.10+ with 4-space indentation.
- Use type hints for public functions and interfaces.
- Naming: `snake_case` for functions/variables, `PascalCase` for classes.
- Ruff is the style source of truth (`line-length = 100` in `pyproject.toml`).

## Testing Guidelines
- Framework: `pytest`; markers include `slow` and `integration`.
- Test files must be named `test_*.py` under `tests/`.
- Prefer TDD for new features: add/adjust tests first, then implement.
- Keep fixtures local to the test module unless broad reuse is needed.
- Target at least 80% coverage for substantial feature work.

## Commit & Pull Request Guidelines
- Use concise, imperative commit messages with prefixes like `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `style:`.
- Example: `docs: add SZSE Level 3 timestamp semantics`.
- PRs should include a short summary, tests run (or why skipped), and links to related docs/issues.

## Architecture & Workflow Notes
- Read `docs/architecture/design.md` before changing schema or ETL semantics.
- For larger features/refactors, create an ExecPlan per `PLANS.md`.
- Before pushing, run `pre-commit run --all-files`.
