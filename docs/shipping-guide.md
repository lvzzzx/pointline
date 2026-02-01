# Shipping Guide: Pointline + Skills

Guide for distributing the Pointline project with Claude Code skills.

## Distribution Strategies

### Strategy 1: Open Source (Recommended)

**Best for:** Public research, community projects, open-source contributions

**Setup:**
```bash
# 1. Skills are in the repo
skills/
├── pointline-research/  # Source tracked in git
├── build.sh             # Build script
└── README.md            # Documentation

# 2. Update main README with installation
```

**README.md addition:**
```markdown
## Installation

### 1. Install Python Package
\```bash
git clone https://github.com/your-org/pointline.git
cd pointline
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
\```

### 2. Install Claude Code Skill (Optional)
For AI-assisted research with Claude Code:
\```bash
cd skills
./build.sh
cp pointline-research.skill ~/.claude/skills/
\```

Or use symlink for development:
\```bash
ln -s $(pwd)/skills/pointline-research ~/.claude/skills/pointline-research
\```

**What it does:** Enables Claude Code to assist with:
- Discovering available data
- Loading market data correctly
- Applying quant analysis patterns
- Ensuring PIT correctness and reproducibility
```

**Pros:**
- ✅ Users always get latest skill with code
- ✅ Single source of truth (git)
- ✅ Easy to contribute improvements
- ✅ No separate distribution needed

**Cons:**
- ❌ Users must build skill themselves
- ❌ Requires skill-creator installed

---

### Strategy 2: GitHub Releases with Pre-built Skills

**Best for:** Users who want easy installation without building

**Setup:**

1. **Create release workflow** (`.github/workflows/release.yml`):

```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  build-skill:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install skill-creator
        run: |
          # Download skill-creator (adjust path as needed)
          # Or include it in the repo
          pip install skill-creator  # If available on PyPI

      - name: Build skill
        run: |
          cd skills
          ./build.sh

      - name: Upload skill artifact
        uses: actions/upload-artifact@v3
        with:
          name: pointline-research-skill
          path: skills/pointline-research.skill

  release:
    needs: build-skill
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Download skill artifact
        uses: actions/download-artifact@v3
        with:
          name: pointline-research-skill

      - name: Create Release
        uses: softprops/action-gh-release@v1
        with:
          files: |
            pointline-research.skill
          body: |
            ## Installation

            **Python Package:**
            \```bash
            pip install pointline
            \```

            **Claude Code Skill:**
            1. Download `pointline-research.skill` from assets below
            2. Copy to `~/.claude/skills/`
            3. Restart Claude Code
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

2. **Tag a release:**
```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

**Pros:**
- ✅ Pre-built skill ready to download
- ✅ Easy for non-technical users
- ✅ Automated via CI/CD

**Cons:**
- ❌ Skill version tied to releases
- ❌ Users might use outdated skill between releases

---

### Strategy 3: Python Package with Bundled Skill

**Best for:** PyPI distribution, enterprise deployments

**Setup:**

1. **Include skill in package** (`setup.py` or `pyproject.toml`):

```python
# pyproject.toml
[project]
name = "pointline"
# ... other fields

[tool.setuptools.packages.find]
where = ["."]
include = ["pointline*", "skills*"]

[tool.setuptools.package-data]
"skills.pointline-research" = ["SKILL.md", "references/*.md"]
```

2. **Post-install helper** (`pointline/cli/install_skill.py`):

```python
"""Install Pointline Claude Code skill."""
import shutil
import subprocess
from pathlib import Path

def install_skill():
    """Install the pointline-research skill."""
    # Find skill source in package
    import pointline
    package_dir = Path(pointline.__file__).parent.parent
    skill_source = package_dir / "skills" / "pointline-research"

    if not skill_source.exists():
        print("❌ Skill source not found in package")
        return 1

    # Build skill
    build_script = package_dir / "skills" / "build.sh"
    if build_script.exists():
        print("📦 Building skill...")
        result = subprocess.run([str(build_script)], cwd=build_script.parent)
        if result.returncode != 0:
            print("❌ Failed to build skill")
            return 1

    # Install to Claude Code
    skill_package = package_dir / "skills" / "pointline-research.skill"
    claude_skills = Path.home() / ".claude" / "skills"
    claude_skills.mkdir(parents=True, exist_ok=True)

    dest = claude_skills / "pointline-research.skill"
    shutil.copy(skill_package, dest)

    print(f"✅ Skill installed to: {dest}")
    print("   Restart Claude Code to use the skill")
    return 0

if __name__ == "__main__":
    exit(install_skill())
```

3. **Add CLI command** (`pointline/cli/parser.py`):

```python
# Add subcommand
skill_parser = subparsers.add_parser(
    "install-skill",
    help="Install Claude Code skill for AI-assisted research"
)
skill_parser.set_defaults(func=lambda args: install_skill())
```

4. **Document in README:**

```markdown
## Installation

\```bash
pip install pointline

# Install Claude Code skill (optional)
pointline install-skill
\```
```

**Pros:**
- ✅ One command installation
- ✅ Skill version matches package version
- ✅ Professional user experience

**Cons:**
- ❌ Larger package size
- ❌ Requires skill-creator in environment

---

### Strategy 4: Docker with Pre-installed Skill

**Best for:** Containerized deployments, reproducible environments

**Setup:**

1. **Dockerfile:**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install Pointline
COPY . /app
RUN pip install -e ".[dev]"

# Build and install skill
RUN cd skills && \
    ./build.sh && \
    mkdir -p ~/.claude/skills && \
    cp pointline-research.skill ~/.claude/skills/

# Set up volume for Claude Code
VOLUME ["/root/.claude"]

CMD ["bash"]
```

2. **Docker Compose:**

```yaml
version: '3.8'
services:
  pointline:
    build: .
    volumes:
      - ./data:/data
      - claude-skills:/root/.claude/skills
    environment:
      - LAKE_ROOT=/data/lake

volumes:
  claude-skills:
```

3. **Usage:**

```bash
# Build and run
docker-compose up -d

# Claude Code can access skills via mounted volume
```

**Pros:**
- ✅ Everything pre-configured
- ✅ Reproducible environment
- ✅ Easy deployment

**Cons:**
- ❌ Larger image size
- ❌ Users need Docker knowledge

---

## Recommended Setup for Pointline

Based on your project type:

### If Open Source Research Project

**Recommended: Strategy 1 (Source) + Strategy 2 (Releases)**

**Why:**
- Researchers can build from source (always latest)
- Non-technical users can download pre-built
- CI/CD automates releases

**Implementation:**

1. **Keep current setup:**
   ```
   skills/
   ├── pointline-research/  # Git-tracked source
   ├── build.sh
   └── README.md
   ```

2. **Add GitHub Actions** for releases (see Strategy 2)

3. **Update main README:**
   ```markdown
   ## Installation

   ### For Researchers (Recommended)
   \```bash
   git clone https://github.com/your-org/pointline.git
   cd pointline

   # Install package
   uv venv && source .venv/bin/activate
   uv pip install -e ".[dev]"

   # Install skill (for Claude Code)
   cd skills && ./build.sh
   ln -s $(pwd)/pointline-research ~/.claude/skills/pointline-research
   \```

   ### For Quick Start
   1. Install: `pip install pointline`
   2. Download skill from [latest release](releases/latest)
   3. Copy to `~/.claude/skills/`
   ```

### If Private/Enterprise Project

**Recommended: Strategy 3 (Python Package) + Internal PyPI**

**Why:**
- Single `pip install` command
- Version control aligned
- Professional experience

**Implementation:**

1. Bundle skill in package
2. Add `pointline install-skill` command
3. Host on internal PyPI/Artifactory
4. Document in internal wiki

---

## Complete Shipping Checklist

### Pre-Release

- [ ] **Update skill for latest API**
  ```bash
  vim skills/pointline-research/SKILL.md
  vim skills/pointline-research/references/*.md
  ```

- [ ] **Test skill thoroughly**
  ```bash
  cd skills && ./build.sh
  cp pointline-research.skill ~/.claude/skills/
  # Test with Claude Code
  ```

- [ ] **Update documentation**
  - [ ] README.md with installation
  - [ ] CHANGELOG.md with skill changes
  - [ ] docs/ with examples

- [ ] **Verify git tracking**
  ```bash
  git status skills/
  # Should show source files, not .skill packages
  ```

- [ ] **Build verification**
  ```bash
  cd skills && ./build.sh
  # Should succeed without errors
  ```

### Release Process

- [ ] **Tag release**
  ```bash
  git tag -a v0.1.0 -m "Release v0.1.0: Initial release with Claude Code skill"
  git push origin v0.1.0
  ```

- [ ] **GitHub Actions builds skill** (if using Strategy 2)

- [ ] **Verify release assets**
  - [ ] Python package (PyPI or GitHub)
  - [ ] Skill package (GitHub releases)
  - [ ] Documentation

- [ ] **Announce**
  - [ ] Update README with latest version
  - [ ] Post to discussions/blog
  - [ ] Notify team/users

### Post-Release

- [ ] **Monitor feedback**
  - Issues with skill installation
  - Skill not triggering correctly
  - Missing documentation

- [ ] **Plan updates**
  - API changes → skill updates
  - New patterns → add to references
  - Bug fixes → patch release

---

## Example: Complete README Section

Add this to your `README.md`:

```markdown
## Installation

### Prerequisites
- Python 3.10+
- (Optional) [Claude Code](https://claude.ai/code) for AI-assisted research

### Basic Installation

\```bash
# Clone repository
git clone https://github.com/your-org/pointline.git
cd pointline

# Install with uv (recommended)
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"

# Verify installation
pointline --version
\```

### Claude Code Integration (Optional)

Pointline includes a Claude Code skill that enables AI-assisted research with:
- Automatic data discovery and validation
- Correct API usage (Query vs Core API)
- Point-in-time correctness guarantees
- Common quant analysis patterns

**Option 1: Build from source (recommended for development)**
\```bash
cd skills
./build.sh
ln -s $(pwd)/pointline-research ~/.claude/skills/pointline-research
\```

**Option 2: Install pre-built (easier)**
1. Download `pointline-research.skill` from [latest release](https://github.com/your-org/pointline/releases/latest)
2. Copy to `~/.claude/skills/`
3. Restart Claude Code

**Usage Example:**
\```python
# With Claude Code skill, the AI assistant will guide you:
# "Load BTC trades on Binance Futures for May 1, 2024 and calculate VWAP"

# AI will automatically:
# 1. Check data availability
# 2. Use correct API (Query with decoded=True)
# 3. Apply PIT-correct VWAP calculation
# 4. Ensure deterministic ordering
\```

## Quick Start

\```python
from pointline import research

# Discover available data
exchanges = research.list_exchanges(asset_class="crypto-derivatives")
symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")

# Load market data (Query API - simple and correct)
from pointline.research import query
trades = query.trades("binance-futures", "BTCUSDT", "2024-05-01", "2024-05-02", decoded=True)

# Analyze
import polars as pl
trades = trades.sort("ts_local_us")  # PIT-correct ordering
vwap = (trades["price"] * trades["qty"]).sum() / trades["qty"].sum()
print(f"VWAP: ${vwap:,.2f}")
\```

See [docs/](docs/) for detailed guides.
```

---

## Distribution Channels

### GitHub
- ✅ Source code (always)
- ✅ Pre-built skill (releases)
- ✅ Documentation
- ✅ Issue tracking

### PyPI (if public)
```bash
# Publish to PyPI
python -m build
twine upload dist/*
```

### Internal Channels
- Private PyPI/Artifactory
- Internal documentation wiki
- Slack/email announcements

### Docker Hub (if containerized)
```bash
docker build -t your-org/pointline:latest .
docker push your-org/pointline:latest
```

---

## Maintenance

### When to Update Skill

**Critical (immediate release):**
- Breaking API changes
- Security issues
- Major bugs in guidance

**Regular (with version bumps):**
- New API methods
- New analysis patterns
- Schema updates
- Documentation improvements

**As needed:**
- Minor wording improvements
- Example updates
- Reference additions

### Update Process

1. Update skill source
2. Test thoroughly
3. Commit changes
4. Tag new version
5. CI/CD builds and releases
6. Announce to users

---

This guide ensures your Pointline project and skill ship together seamlessly!
