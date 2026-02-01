# Skills Integration Guide

This guide explains how the Pointline skills are integrated with the repository for version control and distribution.

## Directory Structure

```
skills/
├── README.md                          # Complete skills documentation
├── build.sh                           # Build script for packaging
├── .gitignore                         # Ignore .skill packages
└── pointline-research/                # Skill source (git-tracked)
    ├── SKILL.md                       # Main skill guide
    └── references/                    # Reference documentation
        ├── analysis_patterns.md       # Quant analysis patterns
        ├── best_practices.md          # Reproducibility principles
        └── schemas.md                 # Table schemas
```

## Quick Start

### For Users

**Install the skill:**
```bash
# Option 1: Build from source
cd skills
./build.sh
cp pointline-research.skill ~/.claude/skills/

# Option 2: Use symlink for live editing
ln -s "$(pwd)/pointline-research" ~/.claude/skills/pointline-research
```

**Use the skill:**
The skill auto-triggers when working with market data:
- "Load BTC trades on Binance Futures for May 1, 2024"
- "Check what symbols are available on Deribit"
- "Calculate VWAP for ETHUSDT trades"

### For Developers

**Edit skill source:**
```bash
# Edit main guide
vim skills/pointline-research/SKILL.md

# Edit reference files
vim skills/pointline-research/references/analysis_patterns.md
vim skills/pointline-research/references/best_practices.md
vim skills/pointline-research/references/schemas.md
```

**Build and test:**
```bash
# Build package
cd skills
./build.sh

# Install for testing
cp pointline-research.skill ~/.claude/skills/
```

**Commit changes:**
```bash
# Stage skill source (not .skill package)
git add skills/pointline-research/

# Commit
git commit -m "feat(skill): add new analysis pattern for order flow"
```

## What's Git-Tracked

✅ **Tracked:**
- `skills/README.md` - Documentation
- `skills/build.sh` - Build script
- `skills/.gitignore` - Ignore rules
- `skills/pointline-research/` - All source files

❌ **Ignored:**
- `skills/*.skill` - Binary packages (built on-demand)

## Evolution Workflow

### When API Changes

Example: Adding new `query.klines()` method

1. **Update SKILL.md:**
   ```bash
   vim skills/pointline-research/SKILL.md
   # Add documentation for new method
   ```

2. **Update schemas if needed:**
   ```bash
   vim skills/pointline-research/references/schemas.md
   # Add kline_1h schema details
   ```

3. **Test changes:**
   ```bash
   cd skills && ./build.sh
   cp pointline-research.skill ~/.claude/skills/
   # Test with Claude Code
   ```

4. **Commit:**
   ```bash
   git add skills/pointline-research/
   git commit -m "feat(skill): add klines query API documentation"
   ```

### When Adding Analysis Patterns

Example: Adding volume cluster analysis

1. **Update reference:**
   ```bash
   vim skills/pointline-research/references/analysis_patterns.md
   # Add new pattern with example code
   ```

2. **Rebuild and test:**
   ```bash
   cd skills && ./build.sh
   # Test pattern works correctly
   ```

3. **Commit:**
   ```bash
   git add skills/pointline-research/references/analysis_patterns.md
   git commit -m "feat(skill): add volume cluster analysis pattern"
   ```

## Distribution

### Internal Team

**Method 1: Source distribution** (recommended)
- Team clones repo
- Builds skill from source: `cd skills && ./build.sh`
- Everyone stays in sync with git

**Method 2: Package distribution**
- Build skill: `cd skills && ./build.sh`
- Share `pointline-research.skill` via Slack/email
- Team installs manually

### External Users

**Option 1: Open source**
- Include `skills/` in public repo
- Users build from source
- Document in main README

**Option 2: Pre-built packages**
- Build skill: `cd skills && ./build.sh`
- Attach to GitHub releases
- Users download and install

## Automation

### Auto-rebuild on Commit

Add to `.git/hooks/pre-commit`:
```bash
#!/bin/bash
if git diff --cached --name-only | grep -q "^skills/pointline-research/"; then
    echo "Rebuilding skill..."
    cd skills && ./build.sh && cd ..
fi
```

### CI/CD Integration

GitHub Actions workflow (`.github/workflows/build-skills.yml`):
```yaml
name: Build Skills
on:
  push:
    paths: ['skills/**']
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build skill
        run: cd skills && ./build.sh
      - uses: actions/upload-artifact@v3
        with:
          name: pointline-research-skill
          path: skills/pointline-research.skill
```

## Troubleshooting

**Build fails: "skill-creator not found"**
```bash
# Ensure skill-creator is installed
ls ~/.claude/skills/skill-creator/scripts/package_skill.py
```

**Changes not appearing in Claude**
```bash
# Restart Claude Code (skills load at startup)
# Or rebuild and reinstall:
cd skills && ./build.sh && cp pointline-research.skill ~/.claude/skills/
```

**Skill not triggering**
- Check `description` field in SKILL.md frontmatter
- Must include trigger keywords ("HFT", "crypto", "market data", etc.)

## See Also

- [skills/README.md](skills/README.md) - Detailed skills documentation
- [CLAUDE.md](CLAUDE.md) - Project instructions for Claude Code
- [docs/](docs/) - Additional documentation
