"""Canonical v2 schema registry."""

from __future__ import annotations

from pointline.schemas.control import CONTROL_SPECS
from pointline.schemas.dimensions import DIMENSION_SPECS
from pointline.schemas.events import EVENT_SPECS
from pointline.schemas.events_cn import CN_EVENT_SPECS
from pointline.schemas.events_tardis import TARDIS_EVENT_SPECS
from pointline.schemas.types import TableSpec

TABLE_SPECS: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        *EVENT_SPECS,
        *CN_EVENT_SPECS,
        *TARDIS_EVENT_SPECS,
        *DIMENSION_SPECS,
        *CONTROL_SPECS,
    )
}


def get_table_spec(name: str) -> TableSpec:
    try:
        return TABLE_SPECS[name]
    except KeyError as exc:
        available = ", ".join(sorted(TABLE_SPECS))
        raise KeyError(f"Unknown v2 table spec '{name}'. Available: {available}") from exc


def list_table_specs() -> tuple[str, ...]:
    return tuple(sorted(TABLE_SPECS))


def list_specs() -> tuple[TableSpec, ...]:
    return tuple(TABLE_SPECS[name] for name in list_table_specs())
