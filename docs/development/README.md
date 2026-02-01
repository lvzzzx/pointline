# Development Guide

Welcome! This guide helps you set up a development environment and contribute to Pointline.

---

## 🚀 Quick Start for Contributors

### Prerequisites
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (required for dependency management)
- Git

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/pointline/pointline.git
cd pointline

# 2. Create virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies (editable mode)
uv pip install -e ".[dev]"

# 4. Install pre-commit hooks (REQUIRED)
pre-commit install

# 5. Verify setup
pytest
ruff check .
```

---

## 📁 Project Structure

```
pointline/
├── pointline/              # Main package
│   ├── research/           # Research API (query + core)
│   ├── services/           # ETL services
│   ├── tables/             # Table schemas and parsing
│   ├── io/                 # Data access layer
│   └── cli/                # Command-line interface
├── tests/                  # Test suite
├── examples/               # Usage examples
├── docs/                   # Documentation
└── research/               # Research experiments (optional)
```

---

## 🧪 Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_trades.py

# Run with verbose output
pytest -v

# Run specific test function
pytest tests/test_trades.py::test_parse_tardis_trades_csv

# Run with coverage
pytest --cov=pointline --cov-report=html
```

**Test requirements:**
- Minimum 80% code coverage
- Test both success and failure cases
- Use fixtures and mocks for external dependencies
- See `tests/test_trades.py` for example structure

---

## 🎨 Code Style

**Linting and Formatting:**
```bash
# Check code style
ruff check .

# Auto-fix issues
ruff check --fix .

# Format code
ruff format .

# Pre-commit hooks (runs automatically on commit)
pre-commit run --all-files
```

**Style guidelines:**
- Line length: 100 characters (configured in `pyproject.toml`)
- Use type hints throughout
- Follow PEP 8 conventions
- See [Code Standards](../CLAUDE.md#code-standards) for details

---

## 🔧 Development Workflows

### Git Worktrees

**For working on multiple branches simultaneously:**

See [Worktree Setup Guide](worktree-setup.md) for complete instructions.

**Quick reference:**
```bash
# Create worktree for feature branch
git worktree add ../pointline-feature feature-branch

# IMPORTANT: Install pre-commit hooks in each worktree
cd ../pointline-feature
pre-commit install

# Work in the worktree...

# Remove worktree when done
git worktree remove ../pointline-feature
```

---

### Making Changes

**Workflow:**
1. Create a feature branch
2. Write tests first (TDD)
3. Implement the feature
4. Ensure tests pass
5. Run linting and formatting
6. Commit with descriptive message
7. Submit pull request

**Commit message format:**
```
feat: add data discovery API for self-service exploration
fix: improve timestamp parsing and documentation clarity
docs: promote query API as default for exploration
```

---

## 📦 Dependencies

**Managed with uv:**
```bash
# Add a dependency
uv pip install package-name

# Update dependencies
uv pip compile pyproject.toml -o requirements.txt

# Sync environment
uv pip sync
```

**Why uv?**
- Fast, deterministic dependency resolution
- `uv.lock` ensures reproducible builds
- Compatible with pip and pip-tools

---

## 🔍 CI/CD

See [CI/CD Documentation](development/ci-cd.md) for complete pipeline details.

**Automated checks on pull requests:**
- ✅ Tests (pytest)
- ✅ Linting (ruff)
- ✅ Type checking (mypy, planned)
- ✅ Coverage report

---

## 📝 Documentation

**When to update docs:**
- New features → Update user guides
- API changes → Update [Research API Guide](../reference/api-reference.md)
- Schema changes → Update [Schemas](../reference/schemas.md)
- Bug fixes → Update [Troubleshooting](../troubleshooting.md) (coming soon)

**Documentation structure:**
- User-facing: `docs/`, `docs/guides/`
- Reference: `docs/reference/`
- Architecture: `docs/architecture/`
- Development: `docs/development/` (this directory)

---

## 🐛 Debugging

**Common development issues:**

### Pre-commit hooks failing
```bash
# Run hooks manually to see errors
pre-commit run --all-files

# Skip hooks for a specific commit (use sparingly)
git commit --no-verify
```

### Tests failing after checkout
```bash
# Ensure dependencies are up to date
uv pip install -e ".[dev]"

# Clear pytest cache
pytest --cache-clear
```

### Import errors
```bash
# Reinstall in editable mode
uv pip install -e ".[dev]"
```

---

## 🤝 Contributing Guidelines

### Before submitting a PR

- [ ] Tests pass: `pytest`
- [ ] Linting passes: `ruff check .`
- [ ] Code formatted: `ruff format .`
- [ ] Pre-commit hooks pass: `pre-commit run --all-files`
- [ ] Documentation updated (if applicable)
- [ ] CHANGELOG updated (if user-facing change)

### PR review process

1. Submit PR with clear description
2. Automated checks run (CI/CD)
3. Code review by maintainers
4. Address feedback
5. Merge when approved

---

## 📖 Additional Resources

- [Product Vision](../../conductor/product.md) - Goals and target audience
- [Architecture](../architecture/design.md) - System design
- [Researcher's Guide](../guides/researcher-guide.md) - User perspective

---

## 💡 Getting Help

**Stuck on something?**

1. Check existing [issues](https://github.com/pointline/pointline/issues)
2. Ask in discussions
3. Ping maintainers in Slack/Discord (if available)

---

## 🎯 Good First Issues

**New to the project?** Look for issues tagged:
- `good-first-issue`
- `documentation`
- `help-wanted`

**Ideas for contributions:**
- Add more examples
- Improve documentation
- Add tests for uncovered code
- Fix typos and formatting
