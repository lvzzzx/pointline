# dim_symbol Sync (CLI Spec)

This document defines a `pointline dim-symbol sync` command that updates the
`silver.dim_symbol` table from a canonical metadata source using SCD Type 2 rules.

## Goal
Provide a deterministic, auditable way to keep `dim_symbol` current without coupling
market-data ingestion to metadata discovery.

## Command (Proposed)
```
pointline dim-symbol sync \
  --source <file|dir|api> \
  --format <csv|parquet|json> \
  --table-path <path> \
  --effective-ts <unix_us|now> \
  --strict \
  --dry-run
```

## Inputs
Minimum required columns:
- `exchange_id`
- `exchange_symbol`
- `base_asset`
- `quote_asset`
- `asset_type`
- `tick_size`
- `lot_size`
- `price_increment`
- `amount_increment`
- `contract_size`

Optional:
- `valid_from_ts` (filled from `--effective-ts` if missing)

## Canonical Field Mapping (Tardis Instruments Metadata API)
The table below maps the Tardis instruments metadata payload to `dim_symbol` fields.

| dim_symbol column | Tardis field | Notes |
| --- | --- | --- |
| `exchange_id` | `exchange` | Map via internal exchange dictionary (e.g., `binance -> 2`). |
| `exchange_symbol` | `datasetId` | Prefer `datasetId` over `id` for stability. |
| `base_asset` | `baseCurrency` | Required. |
| `quote_asset` | `quoteCurrency` | Required. |
| `asset_type` | `type` | Map: `spot=0`, `perpetual/perp=1`, `future=2`, `option=3`. |
| `tick_size` | `priceIncrement` | Tardis price increment. |
| `lot_size` | `amountIncrement` | Tardis amount increment. |
| `price_increment` | `priceIncrement` | Same as `tick_size` for tick-based encoding. |
| `amount_increment` | `amountIncrement` | Same as `lot_size`. |
| `contract_size` | `contractSize` / `contractMultiplier` | Use `contractMultiplier` for futures/perps. Default to `1.0` if missing. |
| `valid_from_ts` | `availableSince` | ISO-8601 UTC → µs. If missing, use sync time. |

If `active=false`, close the current row by setting `valid_until_ts=now` and
`is_current=false` instead of deleting.

## Changes Field (SCD2 Boundaries)
Some exchanges return a `changes` array with historical metadata shifts. Use it to
create SCD2 boundaries when present.

Example:
```
"changes":[{"until":"2022-02-15T03:30:00.000Z","priceIncrement":0.01}]
```

Interpretation:
- Version A: `priceIncrement=0.01` valid from `availableSince` until the `until` timestamp.
- Version B: the current payload values valid from that `until` timestamp onward.

## Behavior
1. Load metadata from the source.
2. Validate required columns and types.
3. If `valid_from_ts` missing, set it to `--effective-ts`.
4. Apply SCD2 upsert against current `silver.dim_symbol`.
5. Write the full updated table.

## Output
Print a summary:
- rows read
- rows inserted
- rows closed (old versions)
- current total row count

## Exit Codes
- `0` success
- `1` validation errors
- `2` source not found/unreadable
- `3` write failure

## Notes
- This command is the canonical way to update reference metadata.
- Market-data ingestion should depend on `dim_symbol` being complete for the day.
