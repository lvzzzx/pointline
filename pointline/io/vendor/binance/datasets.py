from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from pointline.config import get_bronze_root

logger = logging.getLogger(__name__)

BINANCE_PUBLIC_BASE_URL = "https://data.binance.vision"

_EXCHANGE_TO_MARKET_PATH = {
    "binance": "spot",
    "binance-futures": "futures/um",
    "binance-coin-futures": "futures/cm",
    "binance-usd-m": "futures/um",
    "binance-um": "futures/um",
    "binance-cm": "futures/cm",
}

_EXCHANGE_TO_MARKET_LABEL = {
    "binance": "spot",
    "binance-futures": "usd_m",
    "binance-coin-futures": "coin_m",
    "binance-usd-m": "usd_m",
    "binance-um": "usd_m",
    "binance-cm": "coin_m",
}

_ALLOWED_TIMEFRAMES = {"daily", "monthly"}
DEFAULT_BINANCE_TEMPLATE = (
    "{market}/exchange={exchange}/type={data_type}/date={date}/symbol={symbol}/"
    "interval={interval}/{filename}"
)


@dataclass
class BinanceDownloadResult:
    downloaded: list[Path] = field(default_factory=list)
    skipped: list[Path] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def download_binance_klines(
    *,
    exchange: str,
    symbols: list[str],
    interval: str,
    from_date: str,
    to_date: str,
    timeframe: str | None = None,
    download_dir: str | Path | None = None,
    filename_template: str | None = None,
    base_url: str = BINANCE_PUBLIC_BASE_URL,
    concurrency: int = 5,
    overwrite: bool = False,
    verify_checksum: bool = False,
    timeout_s: int = 30,
) -> BinanceDownloadResult:
    """
    Download Binance public kline data into local storage.

    Args:
        exchange: Normalized exchange name ("binance", "binance-futures", "binance-coin-futures").
        symbols: Exchange symbols to download (e.g., ["BTCUSDT"]).
        interval: Kline interval (e.g., "1h", "1d").
        from_date: Start date YYYY-MM-DD (inclusive).
        to_date: End date YYYY-MM-DD (non-inclusive).
        timeframe: "daily" or "monthly". Defaults to "monthly" for 1h, else "daily".
        download_dir: Root directory to store files under.
        filename_template: Optional template with {exchange},{symbol},{interval},{date},
            {timeframe},{filename},{market},{market_path},{data_type}. When omitted,
            use a vendor-first Hive layout under download_dir. For monthly downloads,
            {date} is set to the first day of the month (YYYY-MM-01) to keep Hive
            partitions parseable.
        base_url: Base URL for Binance public data.
        concurrency: Number of concurrent downloads.
        overwrite: Overwrite existing files if True.
        verify_checksum: Verify .CHECKSUM if True (sha256).
        timeout_s: Request timeout in seconds.
    """
    market_path = _resolve_market_path(exchange)
    market_label = _resolve_market_label(exchange)
    if timeframe is None:
        timeframe = "monthly" if interval == "1h" else "daily"
    timeframe = timeframe.lower().strip()
    if timeframe not in _ALLOWED_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Use 'daily' or 'monthly'.")

    if not symbols:
        raise ValueError("symbols must be non-empty")

    if not interval:
        raise ValueError("interval is required for klines")

    start = _parse_date(from_date)
    end = _parse_date(to_date)
    if start >= end:
        raise ValueError("from_date must be before to_date")

    if download_dir is None:
        download_dir = get_bronze_root("binance_vision")
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    tasks: list[tuple[str, Path]] = []
    for symbol in symbols:
        for label in _iter_time_labels(start, end, timeframe):
            filename = f"{symbol}-{interval}-{label}.zip"
            remote_path = _build_remote_path(
                market_path=market_path,
                timeframe=timeframe,
                symbol=symbol,
                interval=interval,
                filename=filename,
            )
            url = f"{base_url.rstrip('/')}/{remote_path}"
            partition_date = label if timeframe == "daily" else f"{label}-01"
            dest = _build_local_path(
                download_dir=download_dir,
                exchange=exchange,
                symbol=symbol,
                interval=interval,
                timeframe=timeframe,
                filename=filename,
                market_label=market_label,
                market_path=market_path,
                data_type="klines",
                label=label,
                partition_date=partition_date,
                template=filename_template,
                remote_path=remote_path,
            )
            tasks.append((url, dest))
            logger.debug("Queued download: %s -> %s", url, dest)

    result = BinanceDownloadResult()
    if not tasks:
        logger.info("No download tasks generated for %s %s", exchange, symbols)
        return result

    logger.info(
        "Starting download of %d files for %s (concurrency=%d)",
        len(tasks),
        exchange,
        concurrency,
    )

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(
                _download_one,
                url=url,
                dest=dest,
                overwrite=overwrite,
                verify_checksum=verify_checksum,
                timeout_s=timeout_s,
            )
            for url, dest in tasks
        ]
        for future in as_completed(futures):
            status, payload = future.result()
            if status == "downloaded":
                result.downloaded.append(payload)
                logger.debug("Downloaded: %s", payload)
            elif status == "skipped":
                result.skipped.append(payload)
            elif status == "missing":
                result.missing.append(payload)
                logger.warning("File not found (404): %s", payload)
            else:
                result.failed.append(payload)
                logger.error("Download failed: %s - %s", payload[0], payload[1])

    logger.info(
        "Download complete: %d downloaded, %d skipped, %d missing, %d failed",
        len(result.downloaded),
        len(result.skipped),
        len(result.missing),
        len(result.failed),
    )
    return result


def _resolve_market_path(exchange: str) -> str:
    normalized = exchange.lower().strip()
    if normalized not in _EXCHANGE_TO_MARKET_PATH:
        raise ValueError(
            f"Exchange '{exchange}' not supported for Binance public data. "
            f"Supported: {sorted(_EXCHANGE_TO_MARKET_PATH)}"
        )
    return _EXCHANGE_TO_MARKET_PATH[normalized]


def _resolve_market_label(exchange: str) -> str:
    normalized = exchange.lower().strip()
    if normalized not in _EXCHANGE_TO_MARKET_LABEL:
        raise ValueError(
            f"Exchange '{exchange}' not supported for Binance public data. "
            f"Supported: {sorted(_EXCHANGE_TO_MARKET_LABEL)}"
        )
    return _EXCHANGE_TO_MARKET_LABEL[normalized]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_time_labels(start: date, end: date, timeframe: str) -> Iterable[str]:
    if timeframe == "daily":
        current = start
        while current < end:
            yield current.strftime("%Y-%m-%d")
            current += timedelta(days=1)
        return

    # Monthly timeframe: include all months that have at least one day in range
    # Start from the first day of start's month
    current = date(start.year, start.month, 1)
    # End at the first day of the month AFTER end's month to include partial months
    end_month = date(end.year + 1, 1, 1) if end.month == 12 else date(end.year, end.month + 1, 1)

    while current < end_month:
        yield current.strftime("%Y-%m")
        current = _add_month(current)


def _add_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _build_remote_path(
    *,
    market_path: str,
    timeframe: str,
    symbol: str,
    interval: str,
    filename: str,
) -> str:
    return f"data/{market_path}/{timeframe}/klines/{symbol}/{interval}/{filename}"


def _build_local_path(
    *,
    download_dir: Path,
    exchange: str,
    symbol: str,
    interval: str,
    timeframe: str,
    filename: str,
    market_label: str,
    market_path: str,
    data_type: str,
    label: str,
    partition_date: str,
    template: str | None,
    remote_path: str,
) -> Path:
    if template:
        relative = template.format(
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            date=partition_date,
            timeframe=timeframe,
            filename=filename,
            market=market_label,
            market_path=market_path,
            data_type=data_type,
        )
        return download_dir / relative

    relative = DEFAULT_BINANCE_TEMPLATE.format(
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        date=partition_date,
        timeframe=timeframe,
        filename=filename,
        market=market_label,
        market_path=market_path,
        data_type=data_type,
    )
    return download_dir / relative


def _download_one(
    *,
    url: str,
    dest: Path,
    overwrite: bool,
    verify_checksum: bool,
    timeout_s: int,
) -> tuple[str, Path | str | tuple[str, str]]:
    if dest.exists() and not overwrite:
        return ("skipped", dest)

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")

    try:
        with requests.get(url, stream=True, timeout=timeout_s) as response:
            if response.status_code == 404:
                return ("missing", url)
            response.raise_for_status()
            with tmp_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        tmp_path.replace(dest)

        if verify_checksum:
            _verify_checksum(dest, url, timeout_s=timeout_s)

        return ("downloaded", dest)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return ("failed", (url, str(exc)))


def _verify_checksum(file_path: Path, url: str, *, timeout_s: int) -> None:
    checksum_url = f"{url}.CHECKSUM"
    with requests.get(checksum_url, timeout=timeout_s) as response:
        response.raise_for_status()
        checksum_text = response.text.strip()

    expected = checksum_text.split()[0]
    actual = _sha256_file(file_path)
    if actual != expected:
        file_path.unlink(missing_ok=True)
        raise ValueError(f"Checksum mismatch for {file_path.name}: {actual} != {expected}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
