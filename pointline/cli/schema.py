"""``pointline schema`` — introspect table specs."""

from __future__ import annotations

import argparse
import json


def register(subparsers: argparse._SubParsersAction) -> None:
    schema_parser = subparsers.add_parser("schema", help="Inspect table schemas")
    sub = schema_parser.add_subparsers(dest="schema_command")

    # schema list
    list_p = sub.add_parser("list", help="List all registered table specs")
    list_p.set_defaults(handler=_handle_list)

    # schema show <table>
    show_p = sub.add_parser("show", help="Show details of a table spec")
    show_p.add_argument("table", help="Table name (e.g. trades, dim_symbol)")
    show_p.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )
    show_p.set_defaults(handler=_handle_show)


def _handle_list(args: argparse.Namespace) -> int:
    from pointline.schemas.registry import list_specs

    specs = list_specs()
    from pointline.cli._output import print_table

    rows = [
        {
            "name": s.name,
            "kind": s.kind,
            "version": s.schema_version,
            "columns": len(s.column_specs),
            "partition_by": ", ".join(s.partition_by) or "-",
        }
        for s in specs
    ]
    print_table(rows)
    return 0


def _handle_show(args: argparse.Namespace) -> int:
    from pointline.schemas.registry import get_table_spec

    try:
        spec = get_table_spec(args.table)
    except KeyError as exc:
        print(str(exc))
        return 1

    if args.format == "json":
        data = {
            "name": spec.name,
            "kind": spec.kind,
            "schema_version": spec.schema_version,
            "partition_by": list(spec.partition_by),
            "business_keys": list(spec.business_keys),
            "tie_break_keys": list(spec.tie_break_keys),
            "columns": [
                {
                    "name": c.name,
                    "dtype": str(c.dtype),
                    "nullable": c.nullable,
                    "description": c.description,
                    "scale": c.scale,
                }
                for c in spec.column_specs
            ],
        }
        print(json.dumps(data, indent=2))
        return 0

    # Text table format
    print(f"Table: {spec.name}")
    print(f"Kind:  {spec.kind}")
    print(f"Version: {spec.schema_version}")
    print(f"Partition by: {', '.join(spec.partition_by) or '-'}")
    print(f"Business keys: {', '.join(spec.business_keys)}")
    print(f"Tie-break keys: {', '.join(spec.tie_break_keys)}")
    print()

    from pointline.cli._output import print_table

    rows = [
        {
            "name": c.name,
            "dtype": str(c.dtype),
            "nullable": "yes" if c.nullable else "",
            "scale": str(c.scale) if c.scale else "",
            "description": c.description,
        }
        for c in spec.column_specs
    ]
    print_table(rows)
    return 0
