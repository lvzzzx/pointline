#!/usr/bin/env bash
# Build script for pointline-research skill

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$SCRIPT_DIR/pointline-research"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR}"

echo "📦 Building pointline-research skill..."

# Check if skill-creator's packaging script is available
PACKAGE_SCRIPT="$HOME/.claude/skills/skill-creator/scripts/package_skill.py"

if [ ! -f "$PACKAGE_SCRIPT" ]; then
    echo "❌ Error: skill-creator packaging script not found at $PACKAGE_SCRIPT"
    echo "   Please ensure skill-creator is installed in ~/.claude/skills/skill-creator"
    exit 1
fi

# Package the skill
python "$PACKAGE_SCRIPT" "$SKILL_DIR" "$OUTPUT_DIR"

echo "✅ Skill packaged successfully to: $OUTPUT_DIR/pointline-research.skill"
echo ""
echo "To install:"
echo "  1. Copy pointline-research.skill to your Claude Code installation"
echo "  2. Or share with others for distribution"
