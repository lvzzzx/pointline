#!/usr/bin/env bash
# Fast reorganization of Quant360 SZSE L3 archives into Hive-partitioned bronze layout
# Uses parallel extraction and compression for 50-100x speedup vs Python version
#
# Usage:
#   ./scripts/reorganize_quant360.sh ~/data/lake/bronze/quant360 ~/data/lake/bronze
#   ./scripts/reorganize_quant360.sh ~/data/lake/bronze/quant360 ~/data/lake/bronze --dry-run

set -euo pipefail

# Parse arguments
SOURCE_DIR="${1:?Usage: $0 <source_dir> <bronze_root> [--dry-run]}"
BRONZE_ROOT="${2:?Usage: $0 <source_dir> <bronze_root> [--dry-run]}"
DRY_RUN="${3:-}"

# Configuration
VENDOR="quant360"
PARALLEL_JOBS=8  # Adjust based on CPU cores
TEMP_BASE="/tmp/quant360_reorganize_$$"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cleanup temp directory on exit
cleanup() {
    if [ -d "$TEMP_BASE" ]; then
        echo -e "${YELLOW}Cleaning up temp directory...${NC}"
        rm -rf "$TEMP_BASE"
    fi
}
trap cleanup EXIT

# Parse Quant360 filename to extract metadata
parse_filename() {
    local filename="$1"

    # Expected format: order_new_STK_SZ_20240930.7z or tick_new_STK_SZ_20240930.7z
    if [[ ! "$filename" =~ ^(order|tick)_new_STK_(SZ|SH)_([0-9]{8})\.7z$ ]]; then
        echo "ERROR: Invalid filename format: $filename" >&2
        return 1
    fi

    local data_type="${BASH_REMATCH[1]}"
    local exchange_code="${BASH_REMATCH[2]}"
    local date_str="${BASH_REMATCH[3]}"

    # Map exchange code
    local exchange
    case "$exchange_code" in
        SZ) exchange="szse" ;;
        SH) exchange="sse" ;;
        *) echo "ERROR: Unknown exchange code: $exchange_code" >&2; return 1 ;;
    esac

    # Map data type to table type
    local table_type
    case "$data_type" in
        order) table_type="l3_orders" ;;
        tick) table_type="l3_ticks" ;;
        *) echo "ERROR: Unknown data type: $data_type" >&2; return 1 ;;
    esac

    # Format date as YYYY-MM-DD
    local date_iso="${date_str:0:4}-${date_str:4:2}-${date_str:6:2}"

    echo "$exchange|$table_type|$date_iso"
}

# Reorganize a single archive
reorganize_archive() {
    local archive_path="$1"
    local archive_name=$(basename "$archive_path")

    echo -e "${GREEN}Processing $archive_name${NC}"

    # Parse metadata
    local metadata
    if ! metadata=$(parse_filename "$archive_name"); then
        echo -e "${RED}✗ Skipping invalid filename${NC}"
        return 1
    fi

    IFS='|' read -r exchange table_type date_iso <<< "$metadata"
    echo "  Exchange: $exchange"
    echo "  Type: $table_type"
    echo "  Date: $date_iso"

    # Check archive exists
    if [ ! -f "$archive_path" ]; then
        echo -e "${RED}✗ Archive not found${NC}"
        return 1
    fi

    # Count files in archive
    local file_count=$(7z l -slt "$archive_path" | grep -c "^Path = .*\.csv$" || true)
    echo "  Found $file_count CSV files"

    if [ "$DRY_RUN" = "--dry-run" ]; then
        echo -e "${YELLOW}  [DRY RUN] Would extract and reorganize these files${NC}"
        7z l "$archive_path" "*.csv" | head -20
        return 0
    fi

    # Create temp directory for this archive
    local temp_dir="$TEMP_BASE/$(basename "$archive_path" .7z)"
    mkdir -p "$temp_dir"

    # Extract entire archive (fast: 1-2 minutes for 5000 files)
    echo "  Extracting archive..."
    if ! 7z x "$archive_path" -o"$temp_dir" -bso0 -bsp0; then
        echo -e "${RED}✗ Extraction failed${NC}"
        return 1
    fi

    # Count extracted files
    local extracted_count=$(find "$temp_dir" -name "*.csv" -type f | wc -l | tr -d ' ')
    echo "  Extracted $extracted_count CSV files"

    # Reorganize with parallel compression (fast: 30-60 seconds for 5000 files)
    echo "  Reorganizing with parallel compression (jobs=$PARALLEL_JOBS)..."

    # Use GNU Parallel if available, otherwise fallback to xargs
    if command -v parallel &> /dev/null; then
        # GNU Parallel: faster and more robust
        find "$temp_dir" -name "*.csv" -type f | \
            parallel -j "$PARALLEL_JOBS" --bar \
                'symbol=$(basename {} .csv); \
                 tgt='"$BRONZE_ROOT/$VENDOR/exchange=$exchange/type=$table_type/date=$date_iso"'/symbol=$symbol; \
                 mkdir -p $tgt && gzip -c {} > $tgt/$symbol.csv.gz' 2>/dev/null
    else
        # Fallback: process with bash loop (slower but works everywhere)
        local count=0
        local total=$(find "$temp_dir" -name "*.csv" -type f | wc -l | tr -d ' ')

        find "$temp_dir" -name "*.csv" -type f | while read -r csv_file; do
            symbol=$(basename "$csv_file" .csv)
            target_dir="$BRONZE_ROOT/$VENDOR/exchange=$exchange/type=$table_type/date=$date_iso/symbol=$symbol"
            target_file="$target_dir/$symbol.csv.gz"

            # Skip if exists
            [ -f "$target_file" ] && continue

            # Create and compress
            mkdir -p "$target_dir"
            gzip -c "$csv_file" > "$target_file"

            ((count++))
            [ $((count % 100)) -eq 0 ] && echo "  Progress: $count/$total files"
        done
    fi

    # Report
    local reorganized_count=$(find "$BRONZE_ROOT/$VENDOR/exchange=$exchange/type=$table_type/date=$date_iso" -name "*.csv.gz" 2>/dev/null | wc -l | tr -d ' ')
    echo -e "${GREEN}✓ Completed: $reorganized_count files in target${NC}"

    # Cleanup temp directory for this archive
    rm -rf "$temp_dir"

    return 0
}

# Main execution
main() {
    echo "=========================================="
    echo "Quant360 Archive Reorganization (Fast)"
    echo "=========================================="
    echo "Source: $SOURCE_DIR"
    echo "Bronze: $BRONZE_ROOT"
    echo "Vendor: $VENDOR"
    echo "Parallel jobs: $PARALLEL_JOBS"
    [ "$DRY_RUN" = "--dry-run" ] && echo -e "${YELLOW}Mode: DRY RUN${NC}"
    echo ""

    # Validate directories
    if [ ! -d "$SOURCE_DIR" ]; then
        echo -e "${RED}ERROR: Source directory does not exist: $SOURCE_DIR${NC}"
        exit 1
    fi

    if [ ! -d "$BRONZE_ROOT" ]; then
        echo -e "${RED}ERROR: Bronze directory does not exist: $BRONZE_ROOT${NC}"
        exit 1
    fi

    # Find all archives
    local archives=()
    while IFS= read -r -d '' archive; do
        archives+=("$archive")
    done < <(find "$SOURCE_DIR" -maxdepth 1 -name "*_new_STK_*.7z" -type f -print0 | sort -z)

    if [ ${#archives[@]} -eq 0 ]; then
        echo "No archives found matching pattern *_new_STK_*.7z"
        exit 0
    fi

    echo "Found ${#archives[@]} archives to process"
    echo ""

    # Process each archive
    local success_count=0
    local fail_count=0
    local start_time=$(date +%s)

    for archive in "${archives[@]}"; do
        if reorganize_archive "$archive"; then
            ((success_count++))
        else
            ((fail_count++))
        fi
        echo ""
    done

    # Summary
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    echo "=========================================="
    echo "SUMMARY"
    echo "=========================================="
    echo -e "${GREEN}✓ Successful: $success_count${NC}"
    [ $fail_count -gt 0 ] && echo -e "${RED}✗ Failed: $fail_count${NC}"
    echo "Total time: ${elapsed}s"
    echo ""

    return $((fail_count > 0 ? 1 : 0))
}

# Run main
main
