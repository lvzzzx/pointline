# dim_asset_stats Table Design & CoinGecko Integration Plan

## Overview

A daily dimension table tracking asset-level statistics, primarily `circulating_supply`, updated via CoinGecko API. This table enables research queries that require market cap calculations, supply analysis, and other asset-level metrics.

## Table Schema

### `silver.dim_asset_stats`

**Storage:** Single unpartitioned Delta table (small size, similar to `dim_symbol`)

**Design Decision:** Daily snapshot table (one row per asset per date) rather than SCD Type 2, because:
- Circulating supply changes are infrequent (typically once per day)
- Historical queries need "what was the supply on date X?" - snapshot model fits perfectly
- Simpler than SCD Type 2 for this use case
- Easy to backfill historical data

**Partitioning Decision:** Unpartitioned (like `dim_symbol`), because:
- Small dimension table: ~100-500 assets × ~365 days/year = ~36K-180K rows/year (very manageable)
- Simpler joins: no partition pruning needed, direct column filtering on `date` is efficient
- Consistent with `dim_symbol` pattern (both are reference/dimension tables)
- Delta Lake column pruning handles date filtering efficiently without physical partitioning
- Easier maintenance: single table, simpler MERGE operations
- Partitioning overhead not justified for this table size

**Natural Key:** `(base_asset, date)`

| Column | Type | Description |
|--------|------|-------------|
| **base_asset** | string | Asset ticker (e.g., `BTC`, `ETH`) - matches `dim_symbol.base_asset` |
| **date** | date | UTC date |
| **coingecko_coin_id** | string | CoinGecko API coin identifier (e.g., `bitcoin`, `ethereum`) |
| **circulating_supply** | f64 | Circulating supply in native units (e.g., BTC, ETH) |
| **total_supply** | f64 | Total supply (if available from CoinGecko) |
| **max_supply** | f64 | Maximum supply (if available, null for uncapped assets) |
| **market_cap_usd** | f64 | Market cap in USD (optional, for convenience) |
| **fully_diluted_valuation_usd** | f64 | FDV in USD (optional) |
| **updated_at_ts** | i64 | Timestamp when CoinGecko last updated this data (µs) |
| **fetched_at_ts** | i64 | Timestamp when we fetched from CoinGecko API (µs) |
| **source** | string | Always `coingecko` for now |

**Partitioning:** None (unpartitioned) - small dimension table, similar to `dim_symbol`. Date filtering is efficient via column pruning.

**Join Pattern:**
```sql
-- Join with dim_symbol to get asset stats for symbols
SELECT 
    s.*,
    a.circulating_supply,
    a.market_cap_usd
FROM silver.dim_symbol s
JOIN silver.dim_asset_stats a
  ON s.base_asset = a.base_asset
  AND DATE(FROM_UNIXTIME(s.valid_from_ts / 1_000_000)) = a.date
WHERE s.is_current = true
```

## CoinGecko API Integration

### API Endpoints

#### 1. Individual Coin Endpoint (Daily Sync)
**Endpoint:** `GET /api/v3/coins/{coin_id}`

**Key Fields:**
- `market_data.circulating_supply`
- `market_data.total_supply`
- `market_data.max_supply`
- `market_data.market_cap.usd`
- `market_data.fully_diluted_valuation.usd`
- `last_updated` (ISO 8601 timestamp)

**Use Case:** Daily syncs for current data

#### 2. Circulating Supply Chart Endpoint (Historical Backfill) ⭐ **RECOMMENDED**
**Endpoint:** `GET /pro-api.coingecko.com/api/v3/coins/{id}/circulating_supply_chart`  
**Reference:** [CoinGecko API Docs](https://docs.coingecko.com/reference/coins-id-circulating-supply-chart)

**Benefits:**
- ✅ **One API call per asset** instead of one per day per asset
- ✅ **Much faster** for historical backfills (e.g., 1 year = 1 call vs 365 calls)
- ✅ **Avoids rate limits** - dramatically fewer requests
- ✅ **Data from June 22, 2019** onwards
- ✅ **Automatic granularity:** 5-minutely (1 day), hourly (2-90 days), daily (91+ days)

**Parameters:**
- `days`: Number of days or `"max"` for all available data
- `interval`: Optional `"5m"`, `"hourly"`, or `"daily"` (auto-selected if omitted)

**Response Format:**
```json
{
  "circulating_supply": [
    [1712448000000, "19675268.0"],  // [timestamp_ms, supply_value]
    [1712534400000, "19675268.0"]
  ]
}
```

**Note:** Requires Pro/Enterprise API key. Falls back to daily sync method if no API key provided.

### Rate Limits

- **Free tier:** 10-50 calls/minute (varies)
- **Pro tier:** Higher limits
- **Recommendation:** Implement rate limiting with exponential backoff

### Asset Mapping

Need to map `base_asset` (from `dim_symbol`) to CoinGecko `coin_id`:

| base_asset | coingecko_coin_id | Notes |
|------------|-------------------|-------|
| BTC | bitcoin | |
| ETH | ethereum | |
| BNB | binancecoin | Binance Coin |
| SOL | solana | |
| XRP | ripple | |
| ADA | cardano | |
| TRX | tron | |
| UNI | uniswap | |
| ... | ... | |

**Implementation:** Create `ASSET_TO_COINGECKO_MAP` in `pointline/config.py` or separate mapping file.

## Implementation Plan

### Phase 1: Core Infrastructure

1. **Schema Definition** (`pointline/dim_asset_stats.py`)
   - Define `SCHEMA` dict (Polars types)
   - Define `normalize_dim_asset_stats_schema()` function
   - Similar pattern to `dim_symbol.py`

2. **CoinGecko Client** (`pointline/io/vendor/coingecko.py`)
   - `CoinGeckoClient` class
   - `fetch_asset_stats(coin_id: str) -> dict`
   - Rate limiting and retry logic
   - Error handling for missing coins

3. **Asset Mapping** (`pointline/config.py`)
   - `ASSET_TO_COINGECKO_MAP: dict[str, str]`
   - `get_coingecko_coin_id(base_asset: str) -> str | None`

4. **Table Path** (`pointline/config.py`)
   - Add `"dim_asset_stats": "silver/dim_asset_stats"` to `TABLE_PATHS`

### Phase 2: Service Layer

5. **DimAssetStatsService** (`pointline/services/dim_asset_stats_service.py`)
   - Extends `BaseService`
   - `sync_daily(date: date) -> None` - fetch and upsert for a single date
   - `sync_date_range(start_date: date, end_date: date) -> None` - batch sync
   - `backfill_historical(start_date: date, end_date: date) -> None`
   - Uses `BaseDeltaRepository` without partitioning (unpartitioned table)

### Phase 3: CLI Integration

6. **CLI Commands** (`pointline/cli.py`)
   ```bash
   pointline dim-asset-stats sync --date 2024-01-15
   pointline dim-asset-stats sync --date-range 2024-01-01 2024-01-31
   pointline dim-asset-stats backfill --start-date 2020-01-01 --end-date 2024-01-01
   ```

### Phase 4: Automation

7. **Daily Sync Job**
   - Cron job or scheduled task
   - Runs once per day (e.g., 02:00 UTC)
   - Syncs previous day's data (CoinGecko updates with ~24h delay)
   - Optionally syncs current day for real-time updates

## Data Quality Considerations

1. **Missing Assets:** Some assets in `dim_symbol` may not exist in CoinGecko
   - Log warnings, skip gracefully
   - Consider manual mapping for major assets

2. **Data Freshness:** CoinGecko updates once per 24h
   - `fetched_at_ts` tracks when we pulled data
   - `updated_at_ts` tracks CoinGecko's last update
   - Can detect stale data

3. **Null Handling:** Some assets have `null` for `max_supply` (uncapped)
   - Store as `null` in database
   - Document in schema

4. **Idempotency:** Daily sync should be idempotent
   - Use `MERGE` operation on `(base_asset, date)` key
   - Re-running same date should update if CoinGecko data changed

## Example Usage

```python
import polars as pl
from pointline.config import get_table_path

# Read asset stats for a date range
df = pl.read_delta(str(get_table_path("dim_asset_stats")))
stats = df.filter(
    (pl.col("date") >= "2024-01-01") &
    (pl.col("date") <= "2024-01-31") &
    (pl.col("base_asset") == "BTC")
)

# Join with dim_symbol for research
symbols = pl.read_delta(str(get_table_path("dim_symbol")))
research = symbols.join(
    stats,
    left_on="base_asset",
    right_on="base_asset",
    how="left"
).filter(pl.col("is_current") == True)
```

## Future Enhancements

1. **Additional Metrics:** Add more CoinGecko fields (volume, price, etc.)
2. **Multiple Sources:** Support other data providers (CoinMarketCap, etc.)
3. **Real-time Updates:** WebSocket integration for live updates
4. **Derived Metrics:** Calculate supply inflation rates, market cap ratios, etc.

## Dependencies

- `requests` or `httpx` for HTTP client
- `ratelimit` library for rate limiting (optional)
- CoinGecko API key (optional, but recommended for higher rate limits)

## Testing Strategy

1. **Unit Tests:** Mock CoinGecko API responses
2. **Integration Tests:** Test against CoinGecko sandbox (if available)
3. **Data Quality Tests:** Verify schema, null handling, date ranges
4. **Backfill Tests:** Test historical data sync
