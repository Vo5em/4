"""Diagnostic counting of separated price interactions with horizontal levels.

This module does not find or move levels. Inputs are Trendln levels and the
closed candles used for their calculation.
"""

from __future__ import annotations

from datetime import datetime
from math import isfinite
from typing import Iterable


def count_independent_touches(
    level_price: float,
    candles: Iterable[dict],
    *,
    touch_zone_bps: float = 10.0,
    reset_bps: float = 30.0,
    min_separation_candles: int = 3,
) -> int | None:
    """Count separate OHLC interactions; return None when no usable candles exist.

    A candle touches when its high-low range intersects the symmetric zone around
    the level. An interaction is re-armed only by a later non-touching closed
    candle whose close is beyond the wider reset distance. A return before the
    minimum separation is treated as part of the existing interaction.
    """
    if isinstance(level_price, bool) or not isinstance(level_price, (int, float)):
        raise ValueError("level_price must be a positive finite number")
    if not isfinite(level_price) or level_price <= 0:
        raise ValueError("level_price must be a positive finite number")
    for name, value in (("touch_zone_bps", touch_zone_bps), ("reset_bps", reset_bps)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a non-negative finite number")
    if reset_bps <= touch_zone_bps:
        raise ValueError("reset_bps must be greater than touch_zone_bps")
    if (
        isinstance(min_separation_candles, bool)
        or not isinstance(min_separation_candles, int)
        or min_separation_candles < 1
    ):
        raise ValueError("min_separation_candles must be a positive integer")

    usable = []
    for candle in candles:
        if candle.get("is_closed", candle.get("closed", True)) is False:
            continue
        try:
            low, high, close = float(candle["low"]), float(candle["high"]), float(candle["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if not all(isfinite(value) and value > 0 for value in (low, high, close)) or low > high:
            continue
        usable.append((candle, low, high, close))
    if not usable:
        return None
    if all(isinstance(item[0].get("start_time"), datetime) for item in usable):
        usable.sort(key=lambda item: item[0]["start_time"])

    zone = level_price * touch_zone_bps / 10000.0
    reset = level_price * reset_bps / 10000.0
    zone_low, zone_high = level_price - zone, level_price + zone
    reset_low, reset_high = level_price - reset, level_price + reset

    touches = 0
    active = False
    last_touch_index: int | None = None
    for index, (_candle, low, high, close) in enumerate(usable):
        intersects_zone = low <= zone_high and high >= zone_low
        if active:
            separated = (
                last_touch_index is not None
                and index - last_touch_index >= min_separation_candles
            )
            confirmed_departure = close >= reset_high or close <= reset_low
            if not intersects_zone and separated and confirmed_departure:
                active = False
            continue

        if intersects_zone:
            if last_touch_index is None or index - last_touch_index >= min_separation_candles:
                touches += 1
                last_touch_index = index
            # A too-early return is still part of the previous interaction.
            active = True

    return touches
