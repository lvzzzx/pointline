#!/bin/bash

# Pre-commit hook: runs ruff + pytest before git commit commands
# Only triggers on git commit, not other Bash commands

COMMAND=$(jq -r '.tool_input.command' < /dev/stdin)

# Only check git commit commands
if [[ "$COMMAND" != *"git commit"* ]]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 0

# Run ruff check
if ! ruff check --fix . 2>&1; then
  jq -n '{
    "hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": "ruff check failed. Fix lint issues before committing."
    }
  }'
  exit 0
fi

# Run ruff format
if ! ruff format . 2>&1; then
  jq -n '{
    "hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": "ruff format failed. Fix formatting before committing."
    }
  }'
  exit 0
fi

# Run pytest (fast fail)
if ! python -m pytest -x -q 2>&1; then
  jq -n '{
    "hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": "Tests failed. Fix failing tests before committing."
    }
  }'
  exit 0
fi

# All checks passed
exit 0
