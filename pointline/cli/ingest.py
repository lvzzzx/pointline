"""``pointline ingest`` — single-file ingestion through the v2 pipeline."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("ingest", help="Ingest a single Bronze file into Silver")
    p.add_argument("file", type=Path, help="Path to the Bronze file (CSV, CSV.gz)")
    p.add_argument(
        "--vendor",
        required=True,
        choices=["tardis", "quant360"],
        help="Vendor that produced the file",
    )
    p.add_argument("--data-type", required=True, help="Stream data type (e.g. trades, quotes)")
    p.add_argument("--silver-root", type=Path, default=None, help="Silver data root directory")
    p.add_argument(
        "--exchange",
        default=None,
        help="Exchange code (required for quant360, optional for tardis)",
    )
    p.add_argument(
        "--symbol",
        default=None,
        help="Symbol (required for quant360, optional for tardis)",
    )
    p.add_argument("--trading-date", default=None, help="Trading date (YYYY-MM-DD)")
    p.add_argument("--force", action="store_true", help="Skip idempotency check")
    p.add_argument("--dry-run", action="store_true", help="Parse and validate without writing")
    p.set_defaults(handler=_handle)


def _file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def _handle(args: argparse.Namespace) -> int:
    from datetime import date

    from pointline.cli._config import resolve_root, resolve_silver_root
    from pointline.cli._stores import build_stores
    from pointline.protocols import BronzeFileMetadata

    bronze_path = Path(args.file).expanduser().resolve()
    if not bronze_path.exists():
        print(f"error: file not found: {bronze_path}")
        return 1

    root = resolve_root(getattr(args, "root", None))
    silver_root = resolve_silver_root(args.silver_root, root=root)

    # Build BronzeFileMetadata
    stat = bronze_path.stat()
    trading_date = date.fromisoformat(args.trading_date) if args.trading_date else None
    meta = BronzeFileMetadata(
        vendor=args.vendor,
        data_type=args.data_type,
        bronze_file_path=str(bronze_path),
        file_size_bytes=stat.st_size,
        last_modified_ts=int(stat.st_mtime * 1_000_000),
        sha256=_file_sha256(bronze_path),
        date=trading_date,
    )

    # Build parser
    parser = _make_parser(args, bronze_path)

    # Build stores
    stores = build_stores(silver_root)
    dim_symbol_df = stores["dimension"].load_dim_symbol()

    if dim_symbol_df.is_empty():
        print("warning: dim_symbol is empty — PIT coverage will quarantine all rows")

    from pointline.ingestion.pipeline import ingest_file

    result = ingest_file(
        meta,
        parser=parser,
        manifest_repo=stores["manifest"],
        writer=stores["event"],
        dim_symbol_df=dim_symbol_df,
        quarantine_store=stores["quarantine"],
        force=args.force,
        dry_run=args.dry_run,
    )

    # Report
    print(f"Status:           {result.status}")
    print(f"Rows total:       {result.row_count:,}")
    print(f"Rows written:     {result.rows_written:,}")
    print(f"Rows quarantined: {result.rows_quarantined:,}")
    if result.file_id is not None:
        print(f"File ID:          {result.file_id}")
    if result.trading_date_min:
        print(f"Trading dates:    {result.trading_date_min} -> {result.trading_date_max}")
    if result.skipped:
        print("  (skipped — already ingested)")
    if result.error_message:
        print(f"  Error: {result.error_message}")

    if result.status in ("failed",):
        return 2
    return 0


def _make_parser(args: argparse.Namespace, bronze_path: Path):
    """Build a ``Parser`` callable for the given vendor/data_type."""

    if args.vendor == "tardis":
        return _make_tardis_parser(args.data_type, bronze_path)
    elif args.vendor == "quant360":
        if not args.exchange or not args.symbol:
            raise SystemExit("error: --exchange and --symbol are required for quant360 vendor")
        return _make_quant360_parser(args.data_type, bronze_path, args.exchange, args.symbol)
    else:
        raise SystemExit(f"error: unsupported vendor '{args.vendor}'")


def _make_tardis_parser(data_type: str, bronze_path: Path):
    def parser(meta):
        import polars as pl

        from pointline.vendors.tardis import get_tardis_parser

        stream_parser = get_tardis_parser(data_type)
        raw = pl.read_csv(bronze_path)
        return stream_parser(raw)

    return parser


def _make_quant360_parser(data_type: str, bronze_path: Path, exchange: str, symbol: str):
    def parser(meta):
        import polars as pl

        from pointline.vendors.quant360 import get_quant360_stream_parser

        stream_parser = get_quant360_stream_parser(data_type)
        raw = pl.read_csv(bronze_path, infer_schema_length=0, try_parse_dates=False)
        if raw.is_empty():
            return raw
        return stream_parser(raw, exchange=exchange, symbol=symbol)

    return parser
