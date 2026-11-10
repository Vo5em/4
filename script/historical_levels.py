"""Thin Bybit V5 + trendln adapter for horizontal support/resistance levels."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import importlib
import logging
import time

import aiohttp


BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
_BYBIT_INTERVALS = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360",
    "12h": "720", "1d": "D", "1w": "W", "1M": "M",
}
logger = logging.getLogger(__name__)


class HistoricalLevelError(RuntimeError):
    """Bybit candle data or the trendln output is unavailable or invalid."""


def _candle_end_ms(start_ms: int, interval: str) -> int:
    if interval == "M":
        start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        next_month = 1 if start.month == 12 else start.month + 1
        year = start.year + (1 if start.month == 12 else 0)
        return int(datetime(year, next_month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    seconds = {
        "1": 60, "3": 180, "5": 300, "15": 900, "30": 1800,
        "60": 3600, "120": 7200, "240": 14400, "360": 21600,
        "720": 43200, "D": 86400, "W": 604800,
    }[interval]
    return start_ms + seconds * 1000


async def fetch_closed_bybit_candles(
    symbol: str,
    timeframe: str,
    limit: int,
    *,
    now_ms: int | None = None,
) -> list[dict]:
    """Fetch closed linear-contract candles only, oldest first."""
    if timeframe not in _BYBIT_INTERVALS:
        raise ValueError(f"Unsupported Bybit timeframe: {timeframe}")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 5 <= limit <= 1000:
        raise ValueError("Bybit kline limit must be between 5 and 1000")
    timeout = aiohttp.ClientTimeout(total=10)
    params = {
        "category": "linear", "symbol": symbol.upper(),
        "interval": _BYBIT_INTERVALS[timeframe], "limit": limit,
    }
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(BYBIT_KLINE_URL, params=params) as response:
            response.raise_for_status()
            payload = await response.json()
    if payload.get("retCode") != 0:
        raise HistoricalLevelError(
            f"Bybit kline returned retCode={payload.get('retCode')}"
        )
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    raw = payload.get("result", {}).get("list")
    if not isinstance(raw, list):
        raise HistoricalLevelError("Bybit kline response has no candle list")
    candles = []
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            continue
        try:
            start_ms = int(row[0])
            open_, high, low, close, volume = map(float, (row[1], row[2], row[3], row[4], row[5]))
        except (TypeError, ValueError):
            continue
        if _candle_end_ms(start_ms, _BYBIT_INTERVALS[timeframe]) > now_ms:
            continue
        if not all(value > 0 for value in (open_, high, low, close)):
            continue
        candles.append({
            "start_time": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            "open": open_, "high": high, "low": low, "close": close,
            "volume": volume,
        })
    candles.sort(key=lambda candle: candle["start_time"])
    if len(candles) < 5:
        raise HistoricalLevelError("Fewer than five closed Bybit candles are available")
    return candles


def calculate_horizontal_levels(
    candles: list[dict], timeframe: str, *, pctbound: float = 0.05,
    min_touches: int = 1, extmethod: str = "METHOD_NUMDIFF", accuracy: int = 2,
) -> tuple[dict, ...]:
    """Return trendln-provided price, support/resistance type, and touch count."""
    try:
        trendln = importlib.import_module("trendln")
    except ImportError as error:
        raise HistoricalLevelError("trendln is not installed") from error
    try:
        supports, resistances = trendln.get_horizontal_levels(
            ([candle["low"] for candle in candles],
             [candle["high"] for candle in candles]),
            pctbound=float(pctbound),
            min_touches=min_touches,
            extmethod=extmethod,
            accuracy=accuracy,
        )
    except Exception as error:
        raise HistoricalLevelError(
            f"trendln.get_horizontal_levels failed: {type(error).__name__}"
        ) from error
    levels = []
    for level_type, items in (("SUPPORT", supports), ("RESISTANCE", resistances)):
        for item in items:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                raise HistoricalLevelError("trendln returned an invalid horizontal level")
            try:
                price = float(item[0])
                touches = int(item[1])
            except (TypeError, ValueError) as error:
                raise HistoricalLevelError("trendln returned non-numeric level data") from error
            if price <= 0 or touches < 1:
                raise HistoricalLevelError("trendln returned invalid level price or touch count")
            levels.append({
                "price": price, "type": level_type, "timeframe": timeframe,
                "touch_count": touches,
            })
    return tuple(levels)


async def fetch_historical_level_snapshot(symbol: str, settings) -> dict:
    """Fetch and calculate levels with the exact settings used for this snapshot."""
    applied_settings = {
        "historical_level_timeframe": settings.historical_level_timeframe,
        "historical_level_candle_count": settings.historical_level_candle_count,
        "historical_level_refresh_seconds": settings.historical_level_refresh_seconds,
        "historical_level_pctbound": settings.historical_level_pctbound,
        "historical_level_min_touches": settings.historical_level_min_touches,
        "historical_level_extmethod": settings.historical_level_extmethod,
        "historical_level_accuracy": settings.historical_level_accuracy,
    }
    candles = await fetch_closed_bybit_candles(
        symbol, settings.historical_level_timeframe,
        settings.historical_level_candle_count,
    )
    levels = await asyncio.to_thread(
        calculate_horizontal_levels,
        candles,
        settings.historical_level_timeframe,
        pctbound=settings.historical_level_pctbound,
        min_touches=settings.historical_level_min_touches,
        extmethod=settings.historical_level_extmethod,
        accuracy=settings.historical_level_accuracy,
    )
    return {
        "levels": levels,
        "candles_analyzed": len(candles),
        "settings": applied_settings,
    }


async def fetch_historical_levels(symbol: str, settings) -> tuple[dict, ...]:
    """Backward-compatible levels-only adapter."""
    snapshot = await fetch_historical_level_snapshot(symbol, settings)
    return snapshot["levels"]
