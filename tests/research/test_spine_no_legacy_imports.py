from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_PREFIXES = (
    "pointline.research",
    "pointline.research.",
)


def _iter_spine_files() -> list[Path]:
    root = Path("pointline/v2/research")
    return sorted(path for path in root.rglob("*.py") if "spine" in path.name)


def test_v2_spine_has_no_legacy_research_imports() -> None:
    violations: list[str] = []

    for path in _iter_spine_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    target = alias.name
                    if any(target.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
                        violations.append(f"{path}: import {target}")
            elif isinstance(node, ast.ImportFrom):
                target = node.module or ""
                if any(target.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
                    violations.append(f"{path}: from {target} import ...")

    assert not violations, "Found forbidden legacy imports:\n" + "\n".join(violations)
