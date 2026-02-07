#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOURCE_DIR="$REPO_ROOT/skills/persona-router"
CODEX_HOME_DIR="${CODEX_HOME:-$HOME/.codex}"
TARGET_DIR="$CODEX_HOME_DIR/skills/persona-router"

if [[ ! -f "$SOURCE_DIR/SKILL.md" ]]; then
  echo "Error: source skill not found at $SOURCE_DIR"
  exit 1
fi

mkdir -p "$CODEX_HOME_DIR/skills"
rm -rf "$TARGET_DIR"
cp -R "$SOURCE_DIR" "$TARGET_DIR"

echo "Synced persona-router skill"
echo "  source: $SOURCE_DIR"
echo "  target: $TARGET_DIR"
