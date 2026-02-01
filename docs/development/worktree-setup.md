# Git Worktree Setup Guide

This guide explains how to properly set up pre-commit hooks when creating or switching to a new git worktree.

## Why This Matters

Pre-commit hooks are installed in the shared `.git/hooks/` directory, but they reference the Python interpreter from a specific worktree's virtualenv. When you create or switch to a new worktree, you need to reinstall pre-commit to point to the correct Python interpreter.

## Quick Setup (New Worktree)

When creating or switching to a new worktree, run these commands:

```bash
# 1. Navigate to your worktree
cd /path/to/your/worktree

# 2. Create/activate virtualenv (using uv)
uv venv
source .venv/bin/activate

# 3. Install dependencies
uv pip install -e ".[dev]"

# 4. Install pre-commit hooks (IMPORTANT!)
pre-commit install

# 5. Verify it's working
pre-commit run --all-files
```

## Why Step 4 is Critical

The `pre-commit install` command creates a hook script that looks like this:

```bash
#!/usr/bin/env bash
INSTALL_PYTHON=/path/to/YOUR_WORKTREE/.venv/bin/python3
ARGS=(hook-impl --config=.pre-commit-config.yaml --hook-type=pre-commit)
# ...
```

**Without reinstalling**, the hook points to the PREVIOUS worktree's Python, which causes issues:
- If the old worktree is deleted, commits will fail
- Different worktrees may have different package versions
- The hook won't use your current virtualenv

## Verification

After running `pre-commit install`, verify it's pointing to the correct worktree:

```bash
# Check which Python the hook uses
grep "INSTALL_PYTHON" $(git rev-parse --git-path hooks/pre-commit)

# Should show YOUR worktree's path, e.g.:
# INSTALL_PYTHON=/Users/you/.supacode/repos/pointline/YOUR-WORKTREE/.venv/bin/python3
```

## What Gets Checked

The pre-commit hooks automatically run on every commit:

1. **Ruff Lint** - Auto-fixes Python code issues
2. **Ruff Format** - Auto-formats code (100 char line length)
3. **Trailing Whitespace** - Removes trailing spaces
4. **End of File Fixer** - Ensures newline at EOF
5. **YAML/TOML Check** - Validates config file syntax
6. **Large Files Check** - Prevents files >5MB from being committed
7. **Merge Conflict Check** - Catches `<<<<<<< HEAD` markers
8. **Mixed Line Endings** - Normalizes to LF (Unix-style)

## Manual Usage

```bash
# Run on all files (useful after pulling changes)
pre-commit run --all-files

# Run on currently staged files only
pre-commit run

# Skip hooks for one commit (use sparingly!)
git commit --no-verify -m "emergency fix"

# Update hook versions
pre-commit autoupdate
```

## Troubleshooting

### "pre-commit not found" error

**Problem**: Hook can't find pre-commit

**Solution**: Make sure you've installed dev dependencies and activated the virtualenv:
```bash
source .venv/bin/activate
uv pip install -e ".[dev]"
pre-commit install
```

### Hook uses wrong Python path

**Problem**: Hook references a different worktree's Python

**Solution**: Reinstall pre-commit in your current worktree:
```bash
pre-commit install
```

### Hooks fail on commit

**Problem**: Pre-commit hooks are failing and blocking your commit

**Solution**:
1. Most issues are auto-fixed - just review and re-commit:
   ```bash
   git add .  # Stage the auto-fixes
   git commit -m "your message"
   ```

2. For persistent issues, fix them manually:
   ```bash
   # See what's wrong
   pre-commit run --all-files

   # Fix the issues in your editor
   # Then commit again
   ```

3. Only as a last resort, skip hooks:
   ```bash
   git commit --no-verify -m "WIP: will fix later"
   ```

## Configuration

Pre-commit configuration is in `.pre-commit-config.yaml` at the repo root.

**Current hooks:**
- Ruff (v0.8.4) - Python linting & formatting
- pre-commit-hooks (v5.0.0) - General file checks

**Updating hook versions:**
```bash
pre-commit autoupdate
```

## Additional Resources

- [Pre-commit documentation](https://pre-commit.com)
- [Ruff documentation](https://docs.astral.sh/ruff/)
- [Project code standards](../../CLAUDE.md#code-standards)
