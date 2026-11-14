"""Diagnostic market phase and phase-local touch counts for Trendln levels.

This module does not find levels and its output must not be fed to strategies.
Only closed OHLC candles are inspected, in chronological order.
"""

from __future__ import annotations

from datetime import datetime
from math import isfinite
from typing import Iterable

from .independent_touches import count_independent_touches


def analyze_level_phase(
    level_price: float,
    candles: Iterable[dict],
    *,
    break_confirm_bps: float = 20.0,
    break_confirm_candles: int = 2,
    touch_zone_bps: float = 10.0,
    touch_reset_bps: float = 30.0,
    touch_min_separation_candles: int = 3,
) -> dict:
    """Classify the latest closed-candle phase around a fixed horizontal level.

    A phase flip requires N consecutive closes at least ``break_confirm_bps``
    beyond the level. Wicks never confirm a flip. The confirming candle is a
    boundary, not a touch. Moving away on the same side does not end a phase;
    ``touch_reset_bps`` is used only by the independent touch counter to decide
    whether a later return is a separate interaction.

    If the level was already on one side at the beginning of the available
    window and neither a confirmed flip nor a stale-away boundary is observed,
    the phase start predates the lookback and its touch count is unknown.
    """
    _validate(level_price, break_confirm_bps, break_confirm_candles,
              touch_zone_bps, touch_reset_bps, touch_min_separation_candles)
    rows = []
    for candle in candles:
        if candle.get("is_closed", candle.get("closed", True)) is False:
            continue
        try:
            low, high, close = (float(candle[name]) for name in ("low", "high", "close"))
        except (KeyError, TypeError, ValueError):
            continue
        if not all(isfinite(v) and v > 0 for v in (low, high, close)) or low > high:
            continue
        rows.append({**candle, "low": low, "high": high, "close": close})
    if not rows:
        return {
            "freshness_state": "NO_DATA", "freshness_reason": "Нет закрытых OHLC-свечей.",
            "current_phase_touch_count": None, "current_phase_started_at": None,
            "last_confirmed_transition_at": None, "current_phase_side": None,
            "current_role": "NO_DATA",
            "structure_state": "UNKNOWN",
            "current_phase_touch_parameters": {
                "touch_zone_bps": float(touch_zone_bps),
                "touch_reset_bps": float(touch_reset_bps),
                "min_separation_candles": int(touch_min_separation_candles),
            },
        }
    if all(isinstance(row.get("start_time"), datetime) for row in rows):
        rows.sort(key=lambda row: row["start_time"])

    threshold = level_price * break_confirm_bps / 10000.0
    upper, lower = level_price + threshold, level_price - threshold
    zone = level_price * touch_zone_bps / 10000.0
    zone_low, zone_high = level_price - zone, level_price + zone

    def close_side(row):
        if row["close"] >= upper:
            return "above"
        if row["close"] <= lower:
            return "below"
        return None

    initial_side = close_side(rows[0])
    phase_side = initial_side
    phase_started_at = None
    boundary_index = None
    last_transition_at = None
    phase_origin_known = False
    transition_seen = False
    status = "UNCONFIRMED"
    reason = "Начало текущей фазы находится за пределами доступного окна."

    if initial_side is None:
        # The first clear close establishes an observable baseline, but cannot
        # prove how long that phase existed before this dataset.
        first_clear = next((i for i, row in enumerate(rows) if close_side(row)), None)
        if first_clear is not None:
            phase_side = close_side(rows[first_clear])
            boundary_index = first_clear

    run_side = None
    run_start = None
    run_length = 0

    for index, row in enumerate(rows):
        side = close_side(row)
        if phase_side is None:
            if side is not None:
                phase_side = side
                boundary_index = index
            continue

        intersects_zone = row["low"] <= zone_high and row["high"] >= zone_low
        opposite = "below" if phase_side == "above" else "above"
        if side == opposite:
            if run_side != side:
                run_side, run_start, run_length = side, index, 1
            else:
                run_length += 1
            if run_length >= break_confirm_candles:
                phase_side = opposite
                boundary_index = index
                phase_started_at = _time_value(row)
                last_transition_at = phase_started_at
                transition_seen = True
                phase_origin_known = True
                status = "CONFIRMED_AFTER_BREAK"
                reason = f"Подтверждён переход: {break_confirm_candles} закрытия подряд за уровнем на {break_confirm_bps:g} bps."
                run_side = run_start = None
                run_length = 0
                continue
        else:
            run_side = run_start = None
            run_length = 0

    last_side = close_side(rows[-1])
    current_role = "AT_LEVEL" if last_side is None else ("SUPPORT" if last_side == "above" else "RESISTANCE")
    if phase_origin_known and boundary_index is not None:
        phase_candles = rows[boundary_index + 1:]
        phase_touches = count_independent_touches(
            level_price, phase_candles, touch_zone_bps=touch_zone_bps,
            reset_bps=touch_reset_bps,
            min_separation_candles=touch_min_separation_candles,
        )
    else:
        phase_touches = None
    if status == "UNCONFIRMED" and phase_origin_known:
        status = "CONFIRMED"
        reason = "Текущая сторона уровня наблюдается, подтверждённого перехода в доступном окне нет."
    structure_state = (
        "BROKEN" if transition_seen else
        "CONFIRMED" if phase_origin_known else "UNCONFIRMED"
    )
    return {
        "freshness_state": status,
        "structure_state": structure_state,
        "freshness_reason": reason,
        "current_role": current_role,
        "current_phase_side": phase_side,
        "current_phase_started_at": phase_started_at,
        "last_confirmed_transition_at": last_transition_at,
        "current_phase_touch_count": phase_touches,
        "phase_origin_known": phase_origin_known,
        "break_confirm_bps": float(break_confirm_bps),
        "break_confirm_candles": int(break_confirm_candles),
        "current_phase_touch_parameters": {
            "touch_zone_bps": float(touch_zone_bps),
            "touch_reset_bps": float(touch_reset_bps),
            "min_separation_candles": int(touch_min_separation_candles),
        },
    }


def _time_value(candle: dict):
    value = candle.get("start_time")
    return value.isoformat() if isinstance(value, datetime) else None


def _validate(level, confirm_bps, confirm_candles, zone_bps, reset_bps, separation):
    if isinstance(level, bool) or not isinstance(level, (int, float)) or not isfinite(level) or level <= 0:
        raise ValueError("level_price must be positive and finite")
    for name, value in (("break_confirm_bps", confirm_bps), ("touch_zone_bps", zone_bps), ("touch_reset_bps", reset_bps)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
            raise ValueError(f"{name} must be non-negative and finite")
    if reset_bps <= zone_bps:
        raise ValueError("touch_reset_bps must be greater than touch_zone_bps")
    if isinstance(confirm_candles, bool) or not isinstance(confirm_candles, int) or confirm_candles < 1:
        raise ValueError("break_confirm_candles must be a positive integer")
    if isinstance(separation, bool) or not isinstance(separation, int) or separation < 1:
        raise ValueError("touch_min_separation_candles must be a positive integer")
