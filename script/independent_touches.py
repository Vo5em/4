"""Diagnostic counting of separated price interactions with horizontal levels.

This module does not find or move levels. Inputs are Trendln levels and the
closed candles used for their calculation. The event analyzer also records why
each touch was counted and the retreat separating it from the previous test.
"""

from __future__ import annotations

from datetime import datetime
from math import isfinite
from typing import Iterable


def analyze_independent_touches(
    level_price: float,
    candles: Iterable[dict],
    *,
    touch_zone_bps: float = 10.0,
    reset_bps: float = 30.0,
    min_separation_candles: int = 3,
) -> dict | None:
    """Return independent-touch events and their parameters.

    ``None`` means no usable closed candles were supplied; a valid sample with
    no touches returns ``{"count": 0, "touches": []}``.
    """
    _validate(level_price, touch_zone_bps, reset_bps, min_separation_candles)
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

    events: list[dict] = []
    active = False
    last_touch_index: int | None = None
    active_event: dict | None = None
    max_departure_price: float | None = None
    max_departure_bps = 0.0
    reset_confirming_close: float | None = None

    for index, (candle, low, high, close) in enumerate(usable):
        intersects_zone = low <= zone_high and high >= zone_low
        timestamp = _time_value(candle)

        if active:
            if intersects_zone and active_event is not None:
                _extend_event(active_event, index, timestamp, low, high)
            separated = (
                last_touch_index is not None
                and index - last_touch_index >= min_separation_candles
            )
            confirmed_departure = close >= reset_high or close <= reset_low
            if not intersects_zone:
                max_departure_price, max_departure_bps = _update_departure(
                    level_price, low, high, max_departure_price, max_departure_bps,
                )
            if not intersects_zone and separated and confirmed_departure:
                reset_confirming_close = close
                active = False
            continue

        if intersects_zone:
            separation = None if last_touch_index is None else index - last_touch_index
            counted = last_touch_index is None or separation >= min_separation_candles
            if counted:
                reason = (
                    "FIRST_ZONE_INTERACTION"
                    if last_touch_index is None else
                    "ZONE_REENTRY_AFTER_CONFIRMED_RESET_CLOSE"
                )
                event = {
                    "touch_number": len(events) + 1,
                    "level_price": float(level_price),
                    "first_candle_at": timestamp,
                    "last_interaction_candle_at": timestamp,
                    "touch_range_low": low,
                    "touch_range_high": high,
                    "confirmation_reason": reason,
                    "reset_close_required_bps": float(reset_bps),
                    "reset_confirming_close_before_this_touch": reset_confirming_close,
                    "max_departure_before_this_touch_price": max_departure_price,
                    "max_departure_before_this_touch_bps": max_departure_bps if last_touch_index is not None else None,
                    "candles_between_previous_tests": None if separation is None else max(0, separation - 1),
                    "index_distance_from_previous_test": separation,
                    "candle_index": index,
                }
                if events:
                    previous = events[-1]
                    previous["max_departure_before_next_touch_price"] = max_departure_price
                    previous["max_departure_before_next_touch_bps"] = max_departure_bps
                    previous["candles_to_next_test"] = max(0, separation - 1)
                events.append(event)
                active_event = event
                last_touch_index = index
                max_departure_price = None
                max_departure_bps = 0.0
                reset_confirming_close = None
            elif active_event is not None:
                # A return too soon to be a separate test remains part of the
                # previous interaction and does not increment the count.
                _extend_event(active_event, index, timestamp, low, high)
            active = True
        elif last_touch_index is not None:
            max_departure_price, max_departure_bps = _update_departure(
                level_price, low, high, max_departure_price, max_departure_bps,
            )

    if events:
        events[-1]["max_departure_to_sample_end_price"] = max_departure_price
        events[-1]["max_departure_to_sample_end_bps"] = max_departure_bps
    for event in events:
        event.pop("candle_index", None)

    return {
        "count": len(events),
        "touches": events,
        "parameters": {
            "touch_zone_bps": float(touch_zone_bps),
            "touch_reset_bps": float(reset_bps),
            "touch_min_separation_candles": int(min_separation_candles),
        },
    }


def count_independent_touches(
    level_price: float,
    candles: Iterable[dict],
    *,
    touch_zone_bps: float = 10.0,
    reset_bps: float = 30.0,
    min_separation_candles: int = 3,
) -> int | None:
    """Return the established independent-touch count; None means no data."""
    analysis = analyze_independent_touches(
        level_price, candles, touch_zone_bps=touch_zone_bps,
        reset_bps=reset_bps, min_separation_candles=min_separation_candles,
    )
    return None if analysis is None else analysis["count"]


def _update_departure(
    level_price: float,
    low: float,
    high: float,
    previous_price: float | None,
    previous_bps: float,
) -> tuple[float | None, float]:
    # Track wick excursion through non-touching candles, while reset itself is
    # still confirmed only by a closed candle's close.
    candidate = max((low, high), key=lambda value: abs(value - level_price))
    candidate_bps = abs(candidate - level_price) / level_price * 10000.0
    if candidate_bps > previous_bps:
        return candidate, candidate_bps
    return previous_price, previous_bps


def _extend_event(event: dict, index: int, timestamp: str | None, low: float, high: float) -> None:
    event["last_interaction_candle_at"] = timestamp
    event["touch_range_low"] = min(event["touch_range_low"], low)
    event["touch_range_high"] = max(event["touch_range_high"], high)
    # This index is internal and removed from the public result.
    event["candle_index"] = index


def _time_value(candle: dict) -> str | None:
    value = candle.get("start_time")
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _validate(level_price, touch_zone_bps, reset_bps, min_separation_candles) -> None:
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
