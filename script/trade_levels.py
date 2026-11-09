"""Pure virtual trade level calculations for wall-based strategies.

Keep bps policy here so per-pair/per-strategy level settings can be added
without changing TradingEngine or TradeManager lifecycle code.
"""

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


BOUNCE_STRATEGY_ID = "bounce"
BREAKOUT_STRATEGY_ID = "breakout"
BPS_DENOMINATOR = Decimal("10000")
ENTRY_DISTANCE_BPS = Decimal("1.5")
FALSE_BREAKOUT_PENDING_TIMEOUT_SECONDS = 900


def effective_trade_level_bps(pair_settings, trading_settings) -> tuple[float, float]:
    """Resolve independent pair overrides against global defaults."""
    take_profit = (
        trading_settings.default_take_profit_bps
        if pair_settings.take_profit_bps is None
        else pair_settings.take_profit_bps
    )
    stop_loss = (
        trading_settings.default_stop_loss_bps
        if pair_settings.stop_loss_bps is None
        else pair_settings.stop_loss_bps
    )
    return float(take_profit), float(stop_loss)


def effective_strategy_trade_level_bps(
    pair_settings, trading_settings, strategy_id: str,
) -> tuple[float, float]:
    """Resolve strategy-specific pair overrides, then legacy pair/global defaults."""
    if strategy_id not in {"bounce", "breakout"}:
        return effective_trade_level_bps(pair_settings, trading_settings)
    prefix = strategy_id
    take_profit = getattr(pair_settings, f"{prefix}_take_profit_bps", None)
    stop_loss = getattr(pair_settings, f"{prefix}_stop_loss_bps", None)
    legacy_tp, legacy_sl = effective_trade_level_bps(pair_settings, trading_settings)
    return (
        float(legacy_tp if take_profit is None else take_profit),
        float(legacy_sl if stop_loss is None else stop_loss),
    )


def estimate_trade_outcomes(
    *,
    side: str,
    entry_price: float | Decimal,
    take_profit_price: float | Decimal,
    stop_loss_price: float | Decimal,
    position_notional_usdt: float | Decimal,
    taker_fee_rate: float | Decimal,
) -> dict[str, Decimal]:
    """Estimate outcomes using the supplied per-fill simulation fee rate."""
    try:
        entry = Decimal(str(entry_price))
        take_profit = Decimal(str(take_profit_price))
        stop_loss = Decimal(str(stop_loss_price))
        notional = Decimal(str(position_notional_usdt))
        fee_rate = Decimal(str(taker_fee_rate))
    except Exception as error:
        raise ValueError("outcome inputs must be numeric") from error
    if side not in {"long", "short"}:
        raise ValueError("side must be long or short")
    if any(not value.is_finite() or value <= 0 for value in (
        entry, take_profit, stop_loss, notional,
    )):
        raise ValueError("prices and position notional must be positive and finite")
    if not fee_rate.is_finite() or not Decimal(0) <= fee_rate < Decimal(1):
        raise ValueError("taker_fee_rate must be finite and in [0, 1)")
    direction = Decimal(1) if side == "long" else Decimal(-1)
    entry_fee = notional * fee_rate

    def outcome(exit_price: Decimal) -> tuple[Decimal, Decimal]:
        gross = notional * (exit_price - entry) / entry * direction
        exit_notional = notional * exit_price / entry
        exit_fee = exit_notional * fee_rate
        return gross, gross - entry_fee - exit_fee

    tp_gross, tp_net = outcome(take_profit)
    sl_gross, sl_net = outcome(stop_loss)
    net_risk = abs(sl_net)
    net_reward_risk = tp_net / net_risk if net_risk else Decimal(0)
    return {
        "tp_gross_pnl_usdt": tp_gross,
        "tp_net_pnl_usdt": tp_net,
        "sl_gross_pnl_usdt": sl_gross,
        "sl_net_pnl_usdt": sl_net,
        "entry_fee_usdt": entry_fee,
        "tp_exit_fee_usdt": notional * take_profit / entry * fee_rate,
        "sl_exit_fee_usdt": notional * stop_loss / entry * fee_rate,
        "net_reward_risk": net_reward_risk,
    }


def _round_to_tick(value: Decimal, tick_size: Decimal, rounding: str) -> Decimal:
    ticks = (value / tick_size).to_integral_value(rounding=rounding)
    return ticks * tick_size


def calculate_trade_levels(
    *,
    strategy_id: str,
    wall_side: str,
    wall_price: float | Decimal,
    tick_size: float | Decimal,
    take_profit_bps: float | Decimal,
    stop_loss_bps: float | Decimal,
    entry_distance_bps: float | Decimal = ENTRY_DISTANCE_BPS,
) -> dict[str, Decimal | str]:
    """Calculate tick-aligned Entry/TP/SL from the approved bps settings.

    BID entries sit above the wall and ASK entries below it. Entry trigger
    direction is determined by wall side, independently from LONG/SHORT.
    """
    try:
        wall = Decimal(str(wall_price))
        tick = Decimal(str(tick_size))
        entry_bps = Decimal(str(entry_distance_bps))
        tp_bps = Decimal(str(take_profit_bps))
        sl_bps = Decimal(str(stop_loss_bps))
    except Exception as error:
        raise ValueError("wall, tickSize and bps settings must be numeric") from error
    if not wall.is_finite() or not tick.is_finite():
        raise ValueError("wall_price and tick_size must be finite")
    if wall <= 0 or tick <= 0:
        raise ValueError("wall_price and tick_size must be positive")
    if wall_side not in {"buy", "sell"}:
        raise ValueError("wall_side must be 'buy' or 'sell'")
    if not entry_bps.is_finite() or entry_bps < 0:
        raise ValueError("entry_distance_bps must be finite and not negative")
    if not tp_bps.is_finite() or tp_bps <= 0:
        raise ValueError("take_profit_bps must be a positive finite number")
    if not sl_bps.is_finite() or sl_bps <= 0:
        raise ValueError("stop_loss_bps must be a positive finite number")
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
        side = "long" if wall_side == "buy" else "short"
    else:
        side = "long" if wall_side == "sell" else "short"

    if side == "long":
        tp_raw = entry * (Decimal(1) + tp_bps / BPS_DENOMINATOR)
        sl_raw = entry * (Decimal(1) - sl_bps / BPS_DENOMINATOR)
        take_profit = _round_to_tick(tp_raw, tick, ROUND_CEILING)
        stop = _round_to_tick(sl_raw, tick, ROUND_FLOOR)
    else:
        tp_raw = entry * (Decimal(1) - tp_bps / BPS_DENOMINATOR)
        sl_raw = entry * (Decimal(1) + sl_bps / BPS_DENOMINATOR)
        take_profit = _round_to_tick(tp_raw, tick, ROUND_FLOOR)
        stop = _round_to_tick(sl_raw, tick, ROUND_CEILING)

    if side == "long" and not stop < entry < take_profit:
        raise ValueError("rounded LONG levels are not ordered SL < Entry < TP")
    if side == "short" and not take_profit < entry < stop:
        raise ValueError("rounded SHORT levels are not ordered TP < Entry < SL")
    if any(value <= 0 for value in (entry, stop, take_profit)):
        raise ValueError("rounded trade levels must be positive")
    if strategy_id == BOUNCE_STRATEGY_ID:
        stop_beyond_wall = stop < wall if wall_side == "buy" else stop > wall
        if not stop_beyond_wall:
            raise ValueError(
                "Bounce SL does not extend beyond the wall after tick rounding"
            )
    else:
        target_reaches_wall = take_profit <= wall if wall_side == "buy" else take_profit >= wall
        if not target_reaches_wall:
            raise ValueError(
                "Breakout TP is before the wall and does not reach the breakout level"
            )

    return {
        "side": side,
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": take_profit,
        "entry_trigger_direction": trigger,
    }


def calculate_false_breakout_points(
    *, wall_side: str, wall_price, tick_size, point_1_offset_bps, point_2_distance_bps,
) -> tuple[Decimal, Decimal]:
    """Calculate tick-aligned False Breakout points measured successively."""
    wall = Decimal(str(wall_price))
    tick = Decimal(str(tick_size))
    offset = Decimal(str(point_1_offset_bps)) / BPS_DENOMINATOR
    distance = Decimal(str(point_2_distance_bps)) / BPS_DENOMINATOR
    if wall <= 0 or tick <= 0 or wall_side not in {"buy", "sell"}:
        raise ValueError("wall, tickSize and side must be valid")
    if offset <= 0 or distance <= 0 or offset >= 1 or distance >= 1:
        raise ValueError("False Breakout point distances must be positive and below 10000 bps")
    direction = Decimal(1) if wall_side == "sell" else Decimal(-1)
    rounding = ROUND_CEILING if direction > 0 else ROUND_FLOOR
    point_1 = _round_to_tick(wall * (Decimal(1) + direction * offset), tick, rounding)
    point_2 = _round_to_tick(
        point_1 * (Decimal(1) + direction * distance), tick, rounding
    )
    if direction > 0 and not wall < point_1 < point_2:
        raise ValueError("Rounded ASK False Breakout points are not strictly increasing")
    if direction < 0 and not point_2 < point_1 < wall:
        raise ValueError("Rounded BID False Breakout points are not strictly decreasing")
    return point_1, point_2


def calculate_levels_from_entry(
    *, side: str, entry_price, tick_size, take_profit_bps, stop_loss_bps,
) -> dict:
    """Calculate tick-rounded TP/SL from a known virtual fill price."""
    entry = Decimal(str(entry_price))
    tick = Decimal(str(tick_size))
    tp_bps = Decimal(str(take_profit_bps)) / BPS_DENOMINATOR
    sl_bps = Decimal(str(stop_loss_bps)) / BPS_DENOMINATOR
    if side not in {"long", "short"} or entry <= 0 or tick <= 0 or tp_bps <= 0 or sl_bps <= 0:
        raise ValueError("False Breakout fill levels are invalid")
    if side == "long":
        take_profit = _round_to_tick(entry * (1 + tp_bps), tick, ROUND_CEILING)
        stop_loss = _round_to_tick(entry * (1 - sl_bps), tick, ROUND_FLOOR)
        if not stop_loss < entry < take_profit:
            raise ValueError("Rounded LONG TP/SL are not ordered around fill")
    else:
        take_profit = _round_to_tick(entry * (1 - tp_bps), tick, ROUND_FLOOR)
        stop_loss = _round_to_tick(entry * (1 + sl_bps), tick, ROUND_CEILING)
        if not take_profit < entry < stop_loss:
            raise ValueError("Rounded SHORT TP/SL are not ordered around fill")
    return {"entry_price": entry, "take_profit": take_profit, "stop_loss": stop_loss}
