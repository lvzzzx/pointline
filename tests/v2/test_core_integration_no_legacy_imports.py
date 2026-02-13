from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_PREFIXES = (
    "pointline.io.base_repository",
    "pointline.io.delta_manifest_repo",
    "pointline.io.vendors",
    "pointline.services.generic_ingestion_service",
    "pointline.tables",
)


def _iter_v2_files() -> list[Path]:
    return sorted(Path("pointline/v2").rglob("*.py"))


def test_v2_runtime_has_no_legacy_storage_imports() -> None:
    violations: list[str] = []

    for path in _iter_v2_files():
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
