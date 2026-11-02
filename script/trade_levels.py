"""Pure virtual trade level calculations for wall-based strategies.

Keep bps policy here so per-pair/per-strategy level settings can be added
without changing TradingEngine or TradeManager lifecycle code.
"""

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


BOUNCE_STRATEGY_ID = "bounce"
BREAKOUT_STRATEGY_ID = "breakout"
BPS_DENOMINATOR = Decimal("10000")


def _round_to_tick(value: Decimal, tick_size: Decimal, rounding: str) -> Decimal:
    ticks = (value / tick_size).to_integral_value(rounding=rounding)
    return ticks * tick_size


def calculate_trade_levels(
    *,
    strategy_id: str,
    wall_side: str,
    wall_price: float | Decimal,
    tick_size: float | Decimal,
    entry_distance_bps: float | Decimal = 1.5,
    bounce_stop_distance_bps: float | Decimal = 2.0,
) -> dict[str, Decimal | str]:
    """Calculate tick-aligned Entry/TP/SL for the approved stage-one rules.

    BID entries sit above the wall and ASK entries below it. Entry trigger
    direction is determined by wall side, independently from LONG/SHORT.
    """
    wall = Decimal(str(wall_price))
    tick = Decimal(str(tick_size))
    entry_bps = Decimal(str(entry_distance_bps))
    stop_bps = Decimal(str(bounce_stop_distance_bps))
    if wall <= 0 or tick <= 0:
        raise ValueError("wall_price and tick_size must be positive")
    if wall_side not in {"buy", "sell"}:
        raise ValueError("wall_side must be 'buy' or 'sell'")
    if entry_bps < 0:
        raise ValueError("entry_distance_bps must not be negative")
    if strategy_id not in {BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID}:
        raise ValueError(f"Unsupported wall strategy: {strategy_id}")

    entry_raw = wall * (
        Decimal(1) + entry_bps / BPS_DENOMINATOR
        if wall_side == "buy"
        else Decimal(1) - entry_bps / BPS_DENOMINATOR
    )
    entry = _round_to_tick(
        entry_raw, tick,
        ROUND_CEILING if wall_side == "buy" else ROUND_FLOOR,
    )
    trigger = "cross_down" if wall_side == "buy" else "cross_up"

    if strategy_id == BOUNCE_STRATEGY_ID:
        if stop_bps < 0:
            raise ValueError("bounce_stop_distance_bps must not be negative")
        if wall_side == "buy":
            side = "long"
            stop_raw = wall * (Decimal(1) - stop_bps / BPS_DENOMINATOR)
            stop = _round_to_tick(stop_raw, tick, ROUND_FLOOR)
            take_profit = entry + (entry - stop)
            take_profit = _round_to_tick(take_profit, tick, ROUND_CEILING)
        else:
            side = "short"
            stop_raw = wall * (Decimal(1) + stop_bps / BPS_DENOMINATOR)
            stop = _round_to_tick(stop_raw, tick, ROUND_CEILING)
            take_profit = entry - (stop - entry)
            take_profit = _round_to_tick(take_profit, tick, ROUND_FLOOR)
    else:
        if wall_side == "sell":
            side = "long"
            tp_raw = wall * (Decimal(1) + Decimal("5") / BPS_DENOMINATOR)
            take_profit = _round_to_tick(tp_raw, tick, ROUND_CEILING)
            stop = entry - (take_profit - entry)
        else:
            side = "short"
            tp_raw = wall * (Decimal(1) - Decimal("5") / BPS_DENOMINATOR)
            take_profit = _round_to_tick(tp_raw, tick, ROUND_FLOOR)
            stop = entry + (entry - take_profit)

    if side == "long" and not stop < entry < take_profit:
        raise ValueError("rounded LONG levels are not ordered SL < Entry < TP")
    if side == "short" and not take_profit < entry < stop:
        raise ValueError("rounded SHORT levels are not ordered TP < Entry < SL")
    if any(value <= 0 for value in (entry, stop, take_profit)):
        raise ValueError("rounded trade levels must be positive")

    return {
        "side": side,
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": take_profit,
        "entry_trigger_direction": trigger,
    }
