from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import polars as pl

from pointline.config import EXCHANGE_MAP, TYPE_MAP


@dataclass(frozen=True)
class TardisInstrument:
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    asset_type: int
    tick_size: float
    lot_size: float
    price_increment: float
    amount_increment: float
    contract_size: float
    valid_from_ts: int


def _parse_iso_to_us(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _resolve_exchange_id(exchange: str) -> int:
    if exchange not in EXCHANGE_MAP:
        raise ValueError(
            f"Exchange '{exchange}' not in EXCHANGE_MAP; update pointline.config"
        )
    return EXCHANGE_MAP[exchange]


def _instrument_state(
    record: dict[str, Any],
    *,
    effective_ts: int | None,
) -> TardisInstrument:
    exchange_symbol = record.get("datasetId") or record.get("id")
    if not exchange_symbol:
        raise ValueError("Instrument is missing datasetId/id.")

    base_asset = record.get("baseCurrency")
    quote_asset = record.get("quoteCurrency")
    if not base_asset or not quote_asset:
        raise ValueError(f"Instrument {exchange_symbol} missing base/quote currency.")

    type_raw = record.get("type")
    if type_raw not in TYPE_MAP:
        raise ValueError(f"Instrument {exchange_symbol} has unsupported type '{type_raw}'.")

    price_increment = record.get("priceIncrement")
    amount_increment = record.get("amountIncrement")
    if price_increment is None or amount_increment is None:
        raise ValueError(f"Instrument {exchange_symbol} missing price/amount increment.")

    contract_size = record.get("contractMultiplier")
    if contract_size is None:
        contract_size = 1.0

    available_since = record.get("availableSince")
    if available_since:
        valid_from_ts = _parse_iso_to_us(available_since)
    elif effective_ts is not None:
        valid_from_ts = effective_ts
    else:
        raise ValueError(
            f"Instrument {exchange_symbol} missing availableSince and effective_ts."
        )

    return TardisInstrument(
        exchange_symbol=exchange_symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        asset_type=TYPE_MAP[type_raw],
        tick_size=float(price_increment),
        lot_size=float(amount_increment),
        price_increment=float(price_increment),
        amount_increment=float(amount_increment),
        contract_size=float(contract_size),
        valid_from_ts=valid_from_ts,
    )


def _apply_change_fields(state: dict[str, Any], change: dict[str, Any]) -> dict[str, Any]:
    updated = dict(state)
    if change.get("priceIncrement") is not None:
        updated["tick_size"] = float(change["priceIncrement"])
        updated["price_increment"] = float(change["priceIncrement"])
    if change.get("amountIncrement") is not None:
        updated["lot_size"] = float(change["amountIncrement"])
        updated["amount_increment"] = float(change["amountIncrement"])
    if change.get("contractMultiplier") is not None:
        updated["contract_size"] = float(change["contractMultiplier"])
    return updated


def _history_rows(
    record: dict[str, Any],
    *,
    exchange_id: int,
    effective_ts: int | None,
) -> list[dict[str, Any]]:
    state = _instrument_state(record, effective_ts=effective_ts)
    base_row = {
        "exchange_id": exchange_id,
        "exchange_symbol": state.exchange_symbol,
        "base_asset": state.base_asset,
        "quote_asset": state.quote_asset,
        "asset_type": state.asset_type,
        "tick_size": state.tick_size,
        "lot_size": state.lot_size,
        "price_increment": state.price_increment,
        "amount_increment": state.amount_increment,
        "contract_size": state.contract_size,
    }

    changes = record.get("changes") or []
    if not changes:
        return [{**base_row, "valid_from_ts": state.valid_from_ts}]

    sorted_changes = sorted(changes, key=lambda item: _parse_iso_to_us(item["until"]))
    rows: list[dict[str, Any]] = []
    start_ts = state.valid_from_ts
    for change in sorted_changes:
        until_ts = _parse_iso_to_us(change["until"])
        row_state = _apply_change_fields(base_row, change)
        rows.append({**row_state, "valid_from_ts": start_ts})
        start_ts = until_ts

    rows.append({**base_row, "valid_from_ts": start_ts})
    return rows


def build_updates_from_instruments(
    instruments: list[dict[str, Any]],
    *,
    exchange: str,
    effective_ts: int | None = None,
    rebuild: bool = False,
) -> pl.DataFrame:
    """Transform raw Tardis instrument records into a Polars DataFrame."""
    exchange_id = _resolve_exchange_id(exchange)

    rows: list[dict[str, Any]] = []
    for record in instruments:
        if rebuild:
            rows.extend(
                _history_rows(record, exchange_id=exchange_id, effective_ts=effective_ts)
            )
        else:
            state = _instrument_state(record, effective_ts=effective_ts)
            rows.append(
                {
                    "exchange_id": exchange_id,
                    "exchange_symbol": state.exchange_symbol,
                    "base_asset": state.base_asset,
                    "quote_asset": state.quote_asset,
                    "asset_type": state.asset_type,
                    "tick_size": state.tick_size,
                    "lot_size": state.lot_size,
                    "price_increment": state.price_increment,
                    "amount_increment": state.amount_increment,
                    "contract_size": state.contract_size,
                    "valid_from_ts": state.valid_from_ts,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "exchange_id": pl.Int64,
                "exchange_symbol": pl.Utf8,
                "base_asset": pl.Utf8,
                "quote_asset": pl.Utf8,
                "asset_type": pl.Int64,
                "tick_size": pl.Float64,
                "lot_size": pl.Float64,
                "price_increment": pl.Float64,
                "amount_increment": pl.Float64,
                "contract_size": pl.Float64,
                "valid_from_ts": pl.Int64,
            }
        )

    return pl.DataFrame(rows)
