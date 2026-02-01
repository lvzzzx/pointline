# Bronze Layer Prehooks Design

## Problem Statement

Certain vendors (e.g., Quant360) deliver data in archive formats (.7z, .zip) that require reorganization before ingestion. Currently, this is a manual preprocessing step that users must remember to run.

## Proposed Solution: Bronze Prehooks

### Architecture

**Prehook**: A vendor-specific preparation step that runs automatically before bronze file discovery.

**Trigger Points:**
1. `pointline bronze discover` - Before scanning for files
2. `pointline ingest run` - Before discovering pending files
3. `LocalBronzeSource.list_files()` - At the data access layer

### Design Options

#### Option 1: Detection-Based Auto-Reorganization (Recommended)

**Concept:** When scanning bronze directories, automatically detect archives and reorganize them.

**Pros:**
- Zero user intervention required
- Self-healing: if archives exist, they're automatically processed
- Idempotent: skips already-reorganized files

**Cons:**
- Could be slow on first run (but only runs once per archive)
- Less explicit about what's happening

**Implementation:**
```python
class LocalBronzeSource:
    def __init__(self, root_path: Path, auto_reorganize: bool = True):
        self.root_path = root_path
        self.auto_reorganize = auto_reorganize

    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        # Prehook: Detect and reorganize archives
        if self.auto_reorganize:
            self._run_reorganization_prehooks()

        # Then proceed with normal file discovery
        for p in self.root_path.glob(glob_pattern):
            ...

    def _run_reorganization_prehooks(self):
        """Detect archives and reorganize if needed."""
        # Detect vendor from directory structure or archive patterns
        vendor = self._detect_vendor()

        if vendor == "quant360":
            self._reorganize_quant360_archives()
        elif vendor == "binance":
            self._reorganize_binance_archives()
        # ... other vendors

    def _detect_vendor(self) -> str | None:
        """Auto-detect vendor from directory name or archive patterns."""
        if self.root_path.name in ["quant360", "data.quant360.com"]:
            return "quant360"

        # Check for vendor-specific archive patterns
        if list(self.root_path.glob("*_new_STK_*.7z")):
            return "quant360"

        return None

    def _reorganize_quant360_archives(self):
        """Reorganize quant360 .7z archives if present."""
        archives = list(self.root_path.glob("*.7z"))
        if not archives:
            return  # Already reorganized or no archives

        # Run reorganization script
        # Only processes new/unprocessed archives
        ...
```

#### Option 2: Configured Prehook Registry

**Concept:** User configures which prehooks to run per vendor.

**Configuration** (`~/.pointline/config.yaml`):
```yaml
bronze:
  prehooks:
    quant360:
      enabled: true
      script: scripts/reorganize_quant360.sh
      patterns:
        - "*.7z"
    binance:
      enabled: false
      script: scripts/reorganize_binance_archives.sh
```

**Pros:**
- Explicit configuration
- Easy to enable/disable per vendor
- Custom script paths

**Cons:**
- Requires manual configuration
- More complex initial setup

#### Option 3: Hybrid - Auto-detect with Override

**Concept:** Automatic by default, but allow explicit control.

**CLI Flags:**
```bash
# Auto-reorganize (default)
pointline bronze discover --pending-only

# Skip prehooks
pointline bronze discover --pending-only --no-prehook

# Force re-reorganization
pointline bronze discover --pending-only --force-prehook

# Explicit reorganize (manual control)
pointline bronze reorganize --source-dir ~/archives --bronze-root ~/data/lake/bronze
```

**Pros:**
- Best of both worlds
- Automatic for typical usage
- Explicit control when needed

**Cons:**
- More implementation complexity

### Recommended Approach: Option 3 (Hybrid)

**Implementation Plan:**

1. **Add prehook infrastructure to LocalBronzeSource**
2. **Vendor detection logic** based on:
   - Directory name patterns
   - Archive file patterns
   - Explicit vendor parameter
3. **Smart caching** to avoid re-reorganizing:
   - Track reorganized archives in manifest
   - Skip if target files already exist
4. **CLI integration:**
   - `--no-prehook` flag to skip
   - `--force-prehook` to re-run
5. **Keep explicit command** for manual control

### Performance Considerations

**First-Time Cost:**
- Reorganizing 3000-symbol archive: ~2-3 minutes (bash) vs ~3-5 hours (Python)
- Amortized over many ingestion runs

**Subsequent Runs:**
- Check if reorganization needed: <1 second (file existence checks)
- Skip if already done: no overhead

**Optimization:**
- Create `.reorganized` marker files after completion
- Check markers before scanning for archives

### Migration Path

**Phase 1: Add prehook infrastructure** (current sprint)
- Implement auto-detection in LocalBronzeSource
- Add --no-prehook flag
- Keep explicit `bronze reorganize` command

**Phase 2: Vendor abstraction** (future)
- Vendor-specific prehook classes
- Plugin architecture for new vendors

**Phase 3: Configuration system** (future)
- User-configurable prehooks
- Custom script paths

## Example Workflows

### Automatic (Recommended for Most Users)

```bash
# Just run ingestion - prehook handles reorganization automatically
pointline ingest run --table l3_orders --exchange szse --date 2024-09-30

# Discovers pending files, auto-reorganizes archives if found
pointline bronze discover --pending-only
```

### Manual Control (Advanced Users)

```bash
# Explicitly reorganize upfront
pointline bronze reorganize --source-dir ~/archives --bronze-root ~/data/lake/bronze

# Then run ingestion without prehook
pointline ingest run --table l3_orders --no-prehook
```

### Debugging

```bash
# Force re-reorganization
pointline bronze discover --force-prehook

# Skip prehook to see raw state
pointline bronze discover --no-prehook
```

## Open Questions

1. **Where to store reorganization state?**
   - Option A: In ingest_manifest (reuse existing tracking)
   - Option B: Separate `.bronze_state/` directory
   - Option C: Marker files alongside archives

2. **How to handle partial reorganizations?**
   - Option A: All-or-nothing per archive
   - Option B: Per-symbol checkpointing (more complex)

3. **Should prehooks be vendor-specific or data-type-specific?**
   - Quant360 has different formats for different exchanges (SZSE vs SSE)
   - Could need exchange-specific logic

## Risks and Mitigations

**Risk:** Automatic reorganization surprises users
**Mitigation:** Log clearly when prehook runs, make it opt-out with `--no-prehook`

**Risk:** Performance hit on every discovery
**Mitigation:** Smart caching with marker files

**Risk:** Complex debugging when prehook fails
**Mitigation:** Detailed logging, separate `bronze reorganize` command for isolation

## Success Metrics

- **Developer Experience:** `ingest run` works without manual preprocessing
- **Performance:** <1s overhead when no reorganization needed
- **Reliability:** Idempotent - can run multiple times safely
