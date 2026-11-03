"""Fee model used by virtual trades; it never describes exchange execution."""

from dataclasses import dataclass


MAKER_MAKER_SIMULATION = "MAKER_MAKER_SIMULATION"
LEGACY_SAVED_RATE = "LEGACY_SAVED_RATE"


@dataclass(frozen=True)
class CommissionSnapshot:
    model: str
    maker_fee_rate: float
    taker_fee_rate: float
    applied_fee_rate: float


def maker_maker_simulation(maker_fee_rate: float, taker_fee_rate: float) -> CommissionSnapshot:
    """Freeze configured exchange rates and the rate actually charged in simulation."""
    return CommissionSnapshot(
        model=MAKER_MAKER_SIMULATION,
        maker_fee_rate=float(maker_fee_rate),
        taker_fee_rate=float(taker_fee_rate),
        applied_fee_rate=float(maker_fee_rate),
    )
