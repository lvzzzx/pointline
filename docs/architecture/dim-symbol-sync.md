# dim_symbol Sync (CLI Spec)

This document defines a `pointline dim-symbol sync` command that updates the
`silver.dim_symbol` table from a canonical metadata source using SCD Type 2 rules.

## Goal
Provide a deterministic, auditable way to keep `dim_symbol` current without coupling
market-data ingestion to metadata discovery.

## Command (Proposed)
```
pointline dim-symbol sync \
  --source <file|api> \
  --exchange <exchange> \
  --symbol <instrument-id> \
  --filter <json> \
  --api-key <token> \
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

### API mode (`--source api`)
When `--source=api`, metadata is fetched from the Tardis Instruments Metadata API.
`--exchange` is required. Use `--symbol` to fetch a single instrument; otherwise the
API list endpoint is used. The optional `--filter` value must be a JSON object
serialized as a string; it is passed through as the `filter` query parameter.

`--api-key` defaults to the `TARDIS_API_KEY` environment variable.

Endpoints:
- `GET https://api.tardis.dev/v1/instruments/:exchange/:symbol_id`
- `GET https://api.tardis.dev/v1/instruments/:exchange?filter=<url-encoded json>`

Authentication:
- `Authorization: Bearer <API_KEY>`

## Canonical Field Mapping (Tardis Instruments Metadata API)
The table below maps the Tardis instruments metadata payload to `dim_symbol` fields.

| dim_symbol column | Tardis field | Notes |
| --- | --- | --- |
| `exchange_id` | `exchange` | Map via internal exchange dictionary in `pointline.config` (e.g., `binance -> 2`). |
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
Some exchanges return a `changes` array with historical metadata shifts.
This allows reconstruction of the true history (backfilling).

Example:
```
"changes":[{"until":"2022-02-15T03:30:00.000Z","priceIncrement":0.01}]
```

**Handling Strategy:**
The `changes` array represents a timeline of values. To apply them correctly:
1.  **Unroll** the `changes` array into a chronological list of distinct state intervals (valid_from, valid_until, values).
2.  **Sequential Apply**: Do not batch-apply these as a single upsert against the current state, as they may overlap or conflict.
    *   Either apply them sequentially in `valid_from` order.
    *   Or (preferred for "sync") perform a **full history rebuild** for the symbol if the source provides the complete history via `changes`.

API note: the `changes` field is not guaranteed to be present for all instruments,
and its data quality can vary by exchange (only `contractMultiplier` changes are
guaranteed accurate). Prefer `--rebuild` when using API responses that include
`changes`, and fall back to incremental updates otherwise. 

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
