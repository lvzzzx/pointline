# Pointline Skills

This directory contains Claude Code skills for working with the Pointline data lake.

## Available Skills

### pointline-research

Guide for using Pointline for quantitative trading research. Helps LLM agents:
- Discover available data (exchanges, symbols, coverage)
- Load market data efficiently (trades, quotes, orderbook)
- Analyze with PIT correctness and reproducibility
- Apply common quant patterns (spreads, VWAP, order flow)

**Supports:** Crypto (26+ exchanges), Chinese stocks (SZSE/SSE Level 3)

## Development Workflow

### 1. Edit Skill Source

Edit files in `skills/pointline-research/`:
```
skills/
├── pointline-research/
│   ├── SKILL.md              # Main skill guide
│   └── references/
│       ├── analysis_patterns.md     # Quant analysis patterns
│       ├── best_practices.md        # Reproducibility principles
│       └── schemas.md               # Table schemas
├── build.sh                  # Build script
└── README.md                 # This file
```

### 2. Build Skill Package

```bash
# Build to current directory
cd skills
./build.sh

# Build to specific output directory
OUTPUT_DIR=../dist ./build.sh
```

**Output:** `pointline-research.skill` (distributable package)

### 3. Install for Local Development

**Option A: Symlink for live editing**
```bash
# Create symlink in Claude's skills directory
ln -s "$(pwd)/pointline-research" ~/.claude/skills/pointline-research

# Changes to source files are immediately available
```

**Option B: Install packaged skill**
```bash
# Build and install
./build.sh
cp pointline-research.skill ~/.claude/skills/
```

### 4. Test the Skill

Start a new Claude Code session and test:
```bash
# Skill should auto-trigger on research queries
# Example: "Load BTC trades on Binance Futures for May 1, 2024"
```

### 5. Commit Changes

```bash
# Stage skill source files
git add skills/pointline-research/

# Commit with descriptive message
git commit -m "feat(skill): update pointline-research with new analysis patterns"
```

## Distribution

### For Team Members

**Share the built package:**
```bash
# Build the skill
./build.sh

# Share the .skill file
# Team members can install by:
#   1. Copying pointline-research.skill to ~/.claude/skills/
#   2. Or using Claude Code's skill installation UI
```

### For External Users

**Option 1: Ship with repository**
- Include `skills/` directory in repo
- Users build from source: `cd skills && ./build.sh`

**Option 2: Pre-built releases**
- Build skill package: `./build.sh`
- Commit `pointline-research.skill` to repo (or attach to GitHub releases)
- Users download and install

**Option 3: Documentation only**
- Keep `skills/` directory for internal use
- Document installation in main README
- Users build from source if needed

## Keeping Skills Updated

### When Codebase Changes

Update skill when these change:
- **API changes:** Query/Core API signatures, new discovery methods
- **Schema changes:** New tables, columns, encoding formats
- **Best practices:** New reproducibility patterns, anti-patterns discovered
- **Analysis patterns:** New common workflows added

### Update Process

1. **Edit source files:**
   ```bash
   vim skills/pointline-research/SKILL.md
   vim skills/pointline-research/references/analysis_patterns.md
   ```

2. **Test locally** (if using symlink, changes are live)

3. **Rebuild package:**
   ```bash
   cd skills
   ./build.sh
   ```

4. **Commit changes:**
   ```bash
   git add skills/
   git commit -m "feat(skill): add new TWAP analysis pattern"
   ```

5. **Distribute updated skill** (share .skill file with team)

## Automation Ideas

### CI/CD Integration

Add to `.github/workflows/build-skills.yml`:
```yaml
name: Build Skills

on:
  push:
    paths:
      - 'skills/**'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build skill
        run: |
          cd skills
          ./build.sh
      - name: Upload artifact
        uses: actions/upload-artifact@v3
        with:
          name: pointline-research-skill
          path: skills/pointline-research.skill
```

### Pre-commit Hook

Add to `.git/hooks/pre-commit`:
```bash
#!/bin/bash
# Auto-rebuild skill on commit if source changed

if git diff --cached --name-only | grep -q "^skills/pointline-research/"; then
    echo "Skill source changed, rebuilding..."
    cd skills && ./build.sh && cd ..
    git add pointline-research.skill
fi
```

## Troubleshooting

### Build fails: "skill-creator not found"

Install skill-creator:
```bash
# skill-creator should be in ~/.claude/skills/skill-creator/
# If missing, download from Claude Code skill marketplace
```

### Changes not appearing in Claude Code

1. **Restart Claude Code session** (skills loaded at startup)
2. **Check skill is installed:**
   ```bash
   ls ~/.claude/skills/pointline-research/
   ```
3. **Verify SKILL.md frontmatter** (YAML must be valid)

### Skill not triggering

Check description field in `SKILL.md` frontmatter:
- Must include trigger keywords (e.g., "HFT", "crypto", "market data")
- Must describe when to use the skill

## Version Control Best Practices

### What to Track

✅ **DO track:**
- `skills/pointline-research/SKILL.md`
- `skills/pointline-research/references/*.md`
- `skills/build.sh`
- `skills/README.md`

❌ **DON'T track (add to .gitignore):**
- `*.skill` files (binary, unless distributing via repo)
- `__pycache__/`
- `.DS_Store`

### Gitignore Example

```gitignore
# Skills - track source, not packages
*.skill

# Unless you want to distribute pre-built packages:
# !skills/pointline-research.skill
```

### Branching Strategy

**Feature branches for skill updates:**
```bash
# Create branch for skill update
git checkout -b feat/skill-add-execution-analysis

# Make changes
vim skills/pointline-research/references/analysis_patterns.md

# Commit and push
git add skills/
git commit -m "feat(skill): add execution quality analysis patterns"
git push origin feat/skill-add-execution-analysis
```

**Review process:**
- Test skill in development
- Get feedback from team
- Merge to main
- Distribute updated .skill file

## Examples

### Adding a New Analysis Pattern

1. **Edit reference file:**
   ```bash
   vim skills/pointline-research/references/analysis_patterns.md
   ```

2. **Add pattern:**
   ```markdown
   ### Volume Cluster Analysis

   Identify high-volume price levels...

   \```python
   # Example code
   \```
   ```

3. **Rebuild and test:**
   ```bash
   cd skills && ./build.sh
   # Test in Claude Code session
   ```

4. **Commit:**
   ```bash
   git add skills/pointline-research/references/analysis_patterns.md
   git commit -m "feat(skill): add volume cluster analysis pattern"
   ```

### Updating for API Changes

When Pointline API changes (e.g., new `query.klines()` method):

1. **Update SKILL.md:**
   ```bash
   vim skills/pointline-research/SKILL.md
   ```

2. **Add new method documentation:**
   ```markdown
   ### kline_1h

   \```python
   klines = query.kline_1h("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)
   \```
   ```

3. **Update schemas reference:**
   ```bash
   vim skills/pointline-research/references/schemas.md
   ```

4. **Rebuild, test, commit:**
   ```bash
   cd skills && ./build.sh
   git add skills/
   git commit -m "feat(skill): add kline_1h query API documentation"
   ```

## Contributing

When contributing skill updates:
1. **Follow existing patterns** (see SKILL.md structure)
2. **Keep it concise** (progressive disclosure principle)
3. **Test thoroughly** (try skill with actual queries)
4. **Document changes** (update this README if workflow changes)

## Links

- [Skill Creator Guide](https://docs.anthropic.com/claude/docs/skills) (official docs)
- [Pointline Documentation](../docs/) (local repo docs)
- [CLAUDE.md](../CLAUDE.md) (project instructions)
