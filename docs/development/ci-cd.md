# CI/CD Workflow Guide

## Overview

This repository uses GitHub Actions for continuous integration and delivery. The workflow automatically runs tests, linters, and security checks on every pull request and push to main.

## Workflows

### 1. CI Workflow (`.github/workflows/ci.yml`)

Runs on every push and pull request to `main`.

**Jobs:**

- **Lint**: Runs Ruff formatter and linter
  - Checks code formatting (`ruff format --check`)
  - Checks code quality (`ruff check`)

- **Test**: Runs pytest on Python 3.10, 3.11, and 3.12
  - Executes all tests in `tests/`
  - Generates coverage report
  - Uploads coverage to Codecov (Python 3.12 only)

- **Type Check**: Runs mypy for static type checking
  - Currently set to `continue-on-error: true` (remove once type hints are complete)

- **Security**: Scans for security vulnerabilities
  - Bandit: Checks for common security issues
  - Safety: Checks dependencies for known vulnerabilities

- **Build**: Builds Python package
  - Only runs if lint and test pass
  - Uploads build artifacts

### 2. Release Workflow (`.github/workflows/release.yml`)

Runs when you push a git tag matching `v*.*.*` (e.g., `v0.1.0`).

**Actions:**
- Builds the Python package
- Generates changelog from git commits
- Creates GitHub Release with artifacts
- (Optional) Publishes to PyPI

### 3. PR Labeler (`.github/workflows/pr-labeler.yml`)

Automatically labels pull requests based on changed files:
- `area: ETL` - Changes to services or I/O
- `area: schema` - Changes to table schemas
- `area: research` - Changes to research code
- `area: CLI` - Changes to CLI
- `area: tests` - Changes to tests
- `dependencies` - Changes to pyproject.toml

## Local Development

### Install Dev Dependencies

```bash
pip install -e ".[dev]"
```

### Run Checks Locally (Before Pushing)

```bash
# Linting
ruff check .
ruff format .

# Type checking
mypy pointline --ignore-missing-imports

# Tests with coverage
pytest --cov=pointline --cov-report=term -v

# Security scan
bandit -r pointline -ll
```

### Pre-commit Hook (Recommended)

Create `.git/hooks/pre-commit`:

```bash
#!/bin/bash
set -e

echo "Running pre-commit checks..."

# Format check
ruff format --check . || {
    echo "❌ Format check failed. Run: ruff format ."
    exit 1
}

# Lint check
ruff check . || {
    echo "❌ Lint check failed. Fix issues or run: ruff check --fix ."
    exit 1
}

# Run tests
pytest || {
    echo "❌ Tests failed."
    exit 1
}

echo "✅ All checks passed!"
```

Make it executable:
```bash
chmod +x .git/hooks/pre-commit
```

## Workflow Integration with Branch Protection

Once branch protection is enabled on `main`, the CI workflow becomes a **required status check**:

1. Go to: https://github.com/lvzzzx/pointline/settings/branches
2. Edit the `main` branch protection rule
3. Enable: **Require status checks to pass before merging**
4. Select required checks:
   - ✅ `Lint (Ruff)`
   - ✅ `Test (Python 3.10)`
   - ✅ `Test (Python 3.11)`
   - ✅ `Test (Python 3.12)`
   - ✅ `Build Package`

This ensures no PR can be merged unless all CI checks pass.

## Making a Release

### Option 1: Git Tag (Recommended)

```bash
# Create and push a version tag
git tag v0.1.0
git push origin v0.1.0

# The release workflow will automatically:
# 1. Build the package
# 2. Create a GitHub release
# 3. Attach dist files
```

### Option 2: Manual Workflow Dispatch

1. Go to: https://github.com/lvzzzx/pointline/actions/workflows/release.yml
2. Click "Run workflow"
3. Enter version (e.g., `v0.1.0`)

## Publishing to PyPI (Optional)

To enable PyPI publishing:

1. Create PyPI API token: https://pypi.org/manage/account/token/
2. Add to GitHub Secrets: `PYPI_TOKEN`
3. Uncomment the `publish-pypi` job in `.github/workflows/release.yml`

## Monitoring

### Check CI Status

```bash
# View recent workflow runs
gh run list

# View specific run details
gh run view <run-id>

# Watch a running workflow
gh run watch
```

### Badges (Add to README.md)

```markdown
[![CI](https://github.com/lvzzzx/pointline/actions/workflows/ci.yml/badge.svg)](https://github.com/lvzzzx/pointline/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/lvzzzx/pointline/branch/main/graph/badge.svg)](https://codecov.io/gh/lvzzzx/pointline)
```

## Troubleshooting

### Workflow Fails on Dependencies

Update the cache key in workflows if `pyproject.toml` changes frequently.

### Codecov Upload Fails

1. Sign up at https://codecov.io
2. Connect your repository
3. Add `CODECOV_TOKEN` to GitHub Secrets

### Tests Pass Locally but Fail in CI

Common causes:
- Environment differences (Python version, OS)
- Missing test data files
- Hardcoded paths (use relative paths)
- Timezone issues (use UTC in tests)

### Security Scan False Positives

Update `.github/workflows/ci.yml` to ignore specific issues:

```yaml
- name: Run Bandit security scan
  run: bandit -r pointline -ll --skip B101,B601
```

## Best Practices

1. **Always run checks locally** before pushing
2. **Keep PRs small** for faster CI runs
3. **Add tests** for new features (aim for 80% coverage)
4. **Fix linting issues** immediately (don't accumulate tech debt)
5. **Review CI logs** when checks fail
6. **Update dependencies** regularly
7. **Version bumps** should update `pyproject.toml` version field

## Cost and Performance

- GitHub Actions provides **2,000 free minutes/month** for private repos
- Current CI runs take ~3-5 minutes per push
- Caching reduces dependency installation time by ~80%
- Parallel jobs (Python versions) run concurrently

## Next Steps

1. ✅ Set up branch protection with required status checks
2. ⬜ Set up Codecov integration (optional)
3. ⬜ Add integration tests for ETL pipelines
4. ⬜ Set up automatic dependency updates (Dependabot)
5. ⬜ Add performance benchmarking workflow
