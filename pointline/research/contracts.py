"""Schema contracts and validators for research pipeline payloads."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:  # pragma: no cover - exercised when dependency is available
    from jsonschema import Draft202012Validator
except Exception:  # pragma: no cover - fallback path is tested
    Draft202012Validator = None  # type: ignore[assignment]


class SchemaValidationError(ValueError):
    """Raised when a payload fails schema validation."""


def _schema_root() -> Path:
    return Path(__file__).resolve().parents[2] / "schemas"


def load_schema(schema_filename: str) -> dict[str, Any]:
    """Load a JSON schema from the repo-level schemas directory."""
    schema_path = _schema_root() / schema_filename
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_against_schema(payload: dict[str, Any], schema_filename: str) -> None:
    """Validate a payload against a schema file.

    Raises:
        SchemaValidationError: if schema validation fails.
    """
    schema = load_schema(schema_filename)
    if Draft202012Validator is not None:
        validator = Draft202012Validator(schema)
        errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
        if not errors:
            return

        rendered = []
        for err in errors:
            loc = ".".join(str(part) for part in err.path) if err.path else "<root>"
            rendered.append(f"{loc}: {err.message}")
        raise SchemaValidationError("; ".join(rendered))

    # Fallback validator for environments without jsonschema installed.
    _validate_node(payload, schema, "<root>")


def _validate_node(value: Any, schema: dict[str, Any], path: str) -> None:
    schema_type = schema.get("type")
    if schema_type == "object":
        if not isinstance(value, dict):
            raise SchemaValidationError(f"{path}: expected object")
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                raise SchemaValidationError(f"{path}: missing required key '{key}'")

        properties = schema.get("properties", {})
        additional_allowed = schema.get("additionalProperties", True)
        if additional_allowed is False:
            extras = set(value.keys()) - set(properties.keys())
            if extras:
                raise SchemaValidationError(f"{path}: unexpected properties {sorted(extras)}")

        for key, key_schema in properties.items():
            if key in value:
                _validate_node(value[key], key_schema, f"{path}.{key}")

        for constraint in schema.get("anyOf", []):
            if _matches_required_keys(value, constraint.get("required", [])):
                return
        if schema.get("anyOf"):
            raise SchemaValidationError(f"{path}: does not satisfy anyOf required constraints")
        return

    if schema_type == "array":
        if not isinstance(value, list):
            raise SchemaValidationError(f"{path}: expected array")
        min_items = schema.get("minItems")
        if isinstance(min_items, int) and len(value) < min_items:
            raise SchemaValidationError(f"{path}: expected at least {min_items} items")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for idx, item in enumerate(value):
                _validate_node(item, item_schema, f"{path}[{idx}]")
        return

    if schema_type == "string":
        if not isinstance(value, str):
            raise SchemaValidationError(f"{path}: expected string")
        min_len = schema.get("minLength")
        if isinstance(min_len, int) and len(value) < min_len:
            raise SchemaValidationError(f"{path}: expected minLength {min_len}")
    elif schema_type == "integer":
        if not isinstance(value, int):
            raise SchemaValidationError(f"{path}: expected integer")
        minimum = schema.get("minimum")
        if minimum is not None and value < minimum:
            raise SchemaValidationError(f"{path}: must be >= {minimum}")
    elif schema_type == "number":
        if not isinstance(value, int | float):
            raise SchemaValidationError(f"{path}: expected number")
        minimum = schema.get("minimum")
        if minimum is not None and value < minimum:
            raise SchemaValidationError(f"{path}: must be >= {minimum}")
    elif schema_type == "boolean":
        if not isinstance(value, bool):
            raise SchemaValidationError(f"{path}: expected boolean")

    if "const" in schema and value != schema["const"]:
        raise SchemaValidationError(f"{path}: expected const value {schema['const']!r}")

    enum = schema.get("enum")
    if isinstance(enum, list) and value not in enum:
        raise SchemaValidationError(f"{path}: expected one of {enum}")

    # Handle union types represented via oneOf.
    one_of = schema.get("oneOf")
    if isinstance(one_of, list):
        for option in one_of:
            try:
                _validate_node(value, option, path)
                return
            except SchemaValidationError:
                continue
        raise SchemaValidationError(f"{path}: does not satisfy oneOf constraints")


def _matches_required_keys(value: dict[str, Any], required: list[str]) -> bool:
    return all(key in value for key in required)


def validate_quant_research_input_v2(payload: dict[str, Any]) -> None:
    """Validate Quant Research input payload v2."""
    validate_against_schema(payload, "quant_research_input.v2.json")


def validate_quant_research_output_v2(payload: dict[str, Any]) -> None:
    """Validate Quant Research output payload v2."""
    validate_against_schema(payload, "quant_research_output.v2.json")


def validate_quant_research_workflow_input_v2(payload: dict[str, Any]) -> None:
    """Validate Quant Research workflow input payload v2."""
    validate_against_schema(payload, "quant_research_workflow_input.v2.json")


def validate_quant_research_workflow_output_v2(payload: dict[str, Any]) -> None:
    """Validate Quant Research workflow output payload v2."""
    validate_against_schema(payload, "quant_research_workflow_output.v2.json")
