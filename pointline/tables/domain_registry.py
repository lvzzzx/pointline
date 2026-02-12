"""Registry for canonical table-domain implementations."""

from __future__ import annotations

import threading
from dataclasses import replace

from pointline.tables.domain_contract import TableDomain, TableSpec

_REGISTRY_LOCK = threading.Lock()
_DOMAIN_REGISTRY: dict[str, TableDomain] = {}
_BOOTSTRAPPED = False


def _bootstrap_domains() -> None:
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    # Importing pointline.tables triggers event-table module imports and registrations.
    import pointline.tables  # noqa: F401

    _BOOTSTRAPPED = True


def register_domain(domain: TableDomain) -> None:
    """Register a canonical domain implementation for a table."""
    table_name = domain.spec.table_name
    with _REGISTRY_LOCK:
        if table_name in _DOMAIN_REGISTRY:
            raise ValueError(f"Domain for table '{table_name}' is already registered")
        _DOMAIN_REGISTRY[table_name] = domain


def get_domain(table_name: str) -> TableDomain:
    """Get the registered domain for a table."""
    _bootstrap_domains()
    if table_name not in _DOMAIN_REGISTRY:
        raise KeyError(
            f"No domain registered for '{table_name}'. "
            f"Available domains: {sorted(_DOMAIN_REGISTRY.keys())}"
        )
    return _DOMAIN_REGISTRY[table_name]


def list_domains() -> list[str]:
    """List registered table-domain names."""
    _bootstrap_domains()
    return sorted(_DOMAIN_REGISTRY.keys())


def get_table_spec(table_name: str) -> TableSpec:
    """Get canonical table spec for a table domain."""
    domain = get_domain(table_name)
    return replace(domain.spec)


def list_table_specs() -> list[TableSpec]:
    """List canonical table specs for all domains."""
    _bootstrap_domains()
    return [replace(_DOMAIN_REGISTRY[name].spec) for name in sorted(_DOMAIN_REGISTRY.keys())]
