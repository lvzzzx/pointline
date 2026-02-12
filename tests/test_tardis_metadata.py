from datetime import datetime, timezone

from pointline.io.vendors.tardis.mapper import build_updates_from_instruments


def _to_us(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def test_build_updates_from_instruments_basic() -> None:
    instruments = [
        {
            "datasetId": "BTC-PERPETUAL",
            "baseCurrency": "BTC",
            "quoteCurrency": "USD",
            "type": "perpetual",
            "priceIncrement": 0.5,
            "amountIncrement": 1.0,
            "contractMultiplier": 10.0,
            "availableSince": "2024-01-01T00:00:00.000Z",
        }
    ]

    df = build_updates_from_instruments(instruments, exchange="binance", rebuild=False)

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["exchange_id"] == 1
    assert row["exchange_symbol"] == "BTC-PERPETUAL"
    assert row["asset_type"] == 1
    assert row["tick_size"] == 0.5
    assert row["lot_size"] == 1.0
    assert row["contract_size"] == 10.0
    assert row["valid_from_ts"] == _to_us("2024-01-01T00:00:00.000Z")


def test_build_updates_from_instruments_rebuild_changes() -> None:
    instruments = [
        {
            "datasetId": "ETH-PERPETUAL",
            "baseCurrency": "ETH",
            "quoteCurrency": "USD",
            "type": "perpetual",
            "priceIncrement": 0.3,
            "amountIncrement": 2.0,
            "contractMultiplier": 1.0,
            "availableSince": "2024-01-01T00:00:00.000Z",
            "changes": [
                {
                    "until": "2024-01-02T00:00:00.000Z",
                    "priceIncrement": 0.1,
                },
                {
                    "until": "2024-01-03T00:00:00.000Z",
                    "priceIncrement": 0.2,
                },
            ],
        }
    ]

    df = build_updates_from_instruments(instruments, exchange="binance", rebuild=True)

    assert df.height == 3
    rows = sorted(df.to_dicts(), key=lambda row: row["valid_from_ts"])
    assert rows[0]["valid_from_ts"] == _to_us("2024-01-01T00:00:00.000Z")
    assert rows[0]["tick_size"] == 0.1
    assert rows[1]["valid_from_ts"] == _to_us("2024-01-02T00:00:00.000Z")
    assert rows[1]["tick_size"] == 0.2
    assert rows[2]["valid_from_ts"] == _to_us("2024-01-03T00:00:00.000Z")
    assert rows[2]["tick_size"] == 0.3


def test_build_updates_from_instruments_rebuild_cumulative_changes() -> None:
    # Current state: tick=0.3, lot=10.0
    # Until Jan 3: lot was 5.0
    # Until Jan 2: tick was 0.1
    # Expected Timeline:
    # [Jan 1, Jan 2): tick=0.1, lot=5.0  <-- Both should be old!
    # [Jan 2, Jan 3): tick=0.3, lot=5.0  <-- Tick updated, lot still old
    # [Jan 3, Now ): tick=0.3, lot=10.0 <-- Both current
    instruments = [
        {
            "datasetId": "BTC-PERP",
            "baseCurrency": "BTC",
            "quoteCurrency": "USD",
            "type": "perpetual",
            "priceIncrement": 0.3,
            "amountIncrement": 10.0,
            "contractMultiplier": 1.0,
            "availableSince": "2024-01-01T00:00:00.000Z",
            "changes": [
                {
                    "until": "2024-01-02T00:00:00.000Z",
                    "priceIncrement": 0.1,
                },
                {
                    "until": "2024-01-03T00:00:00.000Z",
                    "amountIncrement": 5.0,
                },
            ],
        }
    ]

    df = build_updates_from_instruments(instruments, exchange="binance", rebuild=True)

    assert df.height == 3
    rows = sorted(df.to_dicts(), key=lambda row: row["valid_from_ts"])

    # Row 1: [Jan 1, Jan 2)
    assert rows[0]["valid_from_ts"] == _to_us("2024-01-01T00:00:00.000Z")
    assert rows[0]["tick_size"] == 0.1
    assert rows[0]["lot_size"] == 5.0

    # Row 2: [Jan 2, Jan 3)
    assert rows[1]["valid_from_ts"] == _to_us("2024-01-02T00:00:00.000Z")
    assert rows[1]["tick_size"] == 0.3
    assert rows[1]["lot_size"] == 5.0

    # Row 3: [Jan 3, Now)
    assert rows[2]["valid_from_ts"] == _to_us("2024-01-03T00:00:00.000Z")
    assert rows[2]["tick_size"] == 0.3
    assert rows[2]["lot_size"] == 10.0
