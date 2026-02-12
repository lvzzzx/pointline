"""Registry for canonical table-domain implementations."""

from __future__ import annotations

import threading
from dataclasses import replace

from pointline.tables.domain_contract import (
    AnyTableDomain,
    DimensionTableDomain,
    DomainKind,
    EventTableDomain,
    TableSpec,
)

_REGISTRY_LOCK = threading.Lock()
_DOMAIN_REGISTRY: dict[str, AnyTableDomain] = {}
_BOOTSTRAPPED = False


def _bootstrap_domains() -> None:
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    # Import order is lexical for linting; both imports are required for registration side effects.
    import pointline.dim_symbol  # noqa: F401
    import pointline.tables  # noqa: F401

    _BOOTSTRAPPED = True


def register_domain(domain: AnyTableDomain) -> None:
    """Register a canonical domain implementation for a table."""
    table_name = domain.spec.table_name
    _validate_spec(domain.spec)
    with _REGISTRY_LOCK:
        if table_name in _DOMAIN_REGISTRY:
            raise ValueError(f"Domain for table '{table_name}' is already registered")
        _DOMAIN_REGISTRY[table_name] = domain


def get_domain(table_name: str) -> AnyTableDomain:
    """Get the registered domain for a table."""
    _bootstrap_domains()
    if table_name not in _DOMAIN_REGISTRY:
        raise KeyError(
            f"No domain registered for '{table_name}'. "
            f"Available domains: {sorted(_DOMAIN_REGISTRY.keys())}"
        )
    return _DOMAIN_REGISTRY[table_name]


def get_event_domain(table_name: str) -> EventTableDomain:
    """Get an event-table domain, failing fast for non-event tables."""
    domain = get_domain(table_name)
    if domain.spec.table_kind != "event":
        raise TypeError(
            f"Table '{table_name}' is '{domain.spec.table_kind}', not an event table. "
            "Use get_dimension_domain() for dimension tables."
        )
    return domain


def get_dimension_domain(table_name: str) -> DimensionTableDomain:
    """Get a dimension-table domain, failing fast for non-dimension tables."""
    domain = get_domain(table_name)
    if domain.spec.table_kind != "dimension":
        raise TypeError(
            f"Table '{table_name}' is '{domain.spec.table_kind}', not a dimension table. "
            "Use get_event_domain() for event tables."
        )
    return domain


def list_domains(*, kind: DomainKind | None = None) -> list[str]:
    """List registered table-domain names, optionally filtered by kind."""
    _bootstrap_domains()
    if kind is None:
        return sorted(_DOMAIN_REGISTRY.keys())
    return sorted(
        name for name, domain in _DOMAIN_REGISTRY.items() if domain.spec.table_kind == kind
    )


def list_event_domains() -> list[str]:
    """List registered event-table domain names."""
    return list_domains(kind="event")


def list_dimension_domains() -> list[str]:
    """List registered dimension-table domain names."""
    return list_domains(kind="dimension")


def get_table_spec(table_name: str) -> TableSpec:
    """Get canonical table spec for a table domain."""
    domain = get_domain(table_name)
    return replace(domain.spec)


def list_table_specs(*, kind: DomainKind | None = None) -> list[TableSpec]:
    """List canonical table specs for registered domains."""
    _bootstrap_domains()
    names = list_domains(kind=kind)
    return [replace(_DOMAIN_REGISTRY[name].spec) for name in names]


def _validate_spec(spec: TableSpec) -> None:
    if spec.has_date != ("date" in spec.schema):
        raise ValueError(
            f"Table '{spec.table_name}' has inconsistent date metadata: "
            f"spec.has_date={spec.has_date}, schema_has_date={'date' in spec.schema}"
        )
