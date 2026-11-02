"""Virtual USDT-linear isolated-margin calculations using Bybit risk tiers."""

from dataclasses import dataclass
from decimal import Decimal


class UnsafePlan(ValueError):
    pass


MODEL_VERSION = "virtual-isolated-usdt-v1"


def virtual_liquidation_price(*, side: str, entry: Decimal, stake: Decimal,
                              leverage: Decimal, mmr: Decimal,
                              close_fee: Decimal) -> Decimal:
    """Experimental isolated-margin estimate; not an exchange liquidation quote."""
    if side not in {"long", "short"} or min(entry, stake, leverage) <= 0:
        raise ValueError("invalid virtual position parameters")
    if not Decimal(0) <= mmr < Decimal(1) or not Decimal(0) <= close_fee < Decimal(1):
        raise ValueError("MMR and close fee must be in [0, 1)")
    notional = stake * leverage
    qty = notional / entry
    deduction = stake
    if side == "long":
        return (entry * qty - deduction - mmr * entry * qty) / (qty * (1 - close_fee - mmr))
    return (entry * qty + deduction) / (qty * (1 + close_fee + mmr))


def virtual_pnl(*, side: str, entry: Decimal, exit: Decimal, notional: Decimal,
                entry_fee_rate: Decimal, exit_fee_rate: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    direction = Decimal(1) if side == "long" else Decimal(-1)
    gross = notional * (exit - entry) / entry * direction
    fees = notional * entry_fee_rate + notional * (exit / entry) * exit_fee_rate
    return gross, fees, gross - fees


@dataclass(frozen=True)
class BybitTier:
    risk_limit: Decimal
    max_leverage: Decimal
    mmr: Decimal
    mm_deduction: Decimal


def liquidation_price(
    *, side: str, entry: Decimal, notional: Decimal, leverage: Decimal,
    tier: BybitTier, taker_fee: Decimal = Decimal("0.00055"),
    extra_margin: Decimal = Decimal("0"),
) -> Decimal:
    """Bybit's current USDT isolated LP formula; trigger source is Mark Price."""
    if side not in {"long", "short"} or min(entry, notional, leverage) <= 0:
        raise ValueError("invalid isolated position parameters")
    qty = notional / entry
    if side == "long":
        numerator = (
            entry * qty - entry * qty / leverage
            - extra_margin / (Decimal(1) - taker_fee) - tier.mm_deduction
        )
        denominator = qty - qty * tier.mmr
    else:
        numerator = (
            entry * qty + entry * qty / leverage
            + extra_margin / (Decimal(1) + taker_fee) + tier.mm_deduction
        )
        denominator = qty + qty * tier.mmr
    if denominator <= 0:
        raise ValueError("invalid Bybit risk tier")
    return numerator / denominator


def choose_maximum_safe_leverage(
    *, side: str, entry: Decimal, stop: Decimal, stake: Decimal,
    min_leverage: Decimal, max_leverage: Decimal, leverage_step: Decimal,
    tiers: list[BybitTier], safety_buffer_bps: Decimal,
    taker_fee: Decimal = Decimal("0.00055"),
) -> tuple[Decimal, Decimal, Decimal]:
    """Choose only the maximum exchange-allowed leverage; never step down to pass SL."""
    if min_leverage <= 0 or max_leverage < min_leverage or leverage_step <= 0:
        raise ValueError("invalid instrument leverage filter")
    count = int((max_leverage - min_leverage) / leverage_step)
    candidates = [min_leverage + leverage_step * i for i in range(count + 1)]
    if not candidates or candidates[-1] != max_leverage:
        candidates.append(max_leverage)
    for leverage in reversed(candidates):
        notional = stake * leverage
        matching = [tier for tier in tiers if notional <= tier.risk_limit]
        if not matching:
            continue
        tier = min(matching, key=lambda item: item.risk_limit)
        if leverage > tier.max_leverage:
            continue
        liq = liquidation_price(
            side=side, entry=entry, notional=notional, leverage=leverage,
            tier=tier, taker_fee=taker_fee,
        )
        buffer = entry * safety_buffer_bps / Decimal(10000)
        safe = stop - liq >= buffer if side == "long" else liq - stop >= buffer
        if safe:
            return leverage, notional, liq
        # Per requested policy, the max exchange-allowed leverage is authoritative.
        raise UnsafePlan("maximum Bybit leverage puts liquidation inside the SL safety buffer")
    raise UnsafePlan("no Bybit risk tier allows the position size and leverage")
