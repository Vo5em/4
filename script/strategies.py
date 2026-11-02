from dataclasses import dataclass
import re
from typing import Protocol

from .settings import PairSettings

DEFAULT_STRATEGY_ID = "stable_wall_v1"
BOUNCE_STRATEGY_ID = "bounce"
BREAKOUT_STRATEGY_ID = "breakout"


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    last_price: float
    stable_walls: tuple[dict, ...]
    wall_events: tuple[dict, ...]
    settings: PairSettings
    now: float | None = None
    price_updated: bool = True


@dataclass(frozen=True)
class TradeSignal:
    strategy_id: str
    symbol: str
    wall_side: str
    wall_price: float
    wall_id: str
    wall_size: float
    wall_ratio: float
    side: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    entry_mode: str | None = None
    entry_trigger_direction: str | None = None


class TradingStrategy(Protocol):
    """Contract implemented by each independently pluggable strategy."""

    strategy_id: str

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        """Return new virtual-trade signals for the current market snapshot."""


class StrategyRegistry:
    def __init__(self) -> None:
        self._strategies: dict[str, TradingStrategy] = {}

    @property
    def strategy_ids(self) -> tuple[str, ...]:
        return tuple(self._strategies)

    def register(self, strategy: TradingStrategy) -> None:
        strategy_id = getattr(strategy, "strategy_id", None)
        if not isinstance(strategy_id, str) or re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,59}", strategy_id
        ) is None:
            raise ValueError(
                "strategy_id must start with a letter or digit and contain only "
                "letters, digits, dots, underscores, or hyphens (up to 60 characters)"
            )
        if strategy.strategy_id in self._strategies:
            raise ValueError(f"Strategy already registered: {strategy.strategy_id}")
        self._strategies[strategy.strategy_id] = strategy

    def get_enabled(self, strategy_ids: set[str] | None = None) -> tuple[TradingStrategy, ...]:
        if strategy_ids is None:
            return tuple(self._strategies.values())
        unknown = strategy_ids.difference(self._strategies)
        if unknown:
            raise KeyError(f"Unknown strategies: {', '.join(sorted(unknown))}")
        return tuple(
            strategy
            for key, strategy in self._strategies.items()
            if key in strategy_ids
        )


class StableWallStrategy:
    """Legacy identifier kept for persisted configurations; stability alone is not a signal."""

    strategy_id = DEFAULT_STRATEGY_ID

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        return self.evaluate_with_diagnostics(context)[0]

    def evaluate_with_diagnostics(
        self, context: StrategyContext
    ) -> tuple[list[TradeSignal], list[dict]]:
        # This persisted legacy strategy is intentionally observational: wall
        # stability by itself has never been an entry signal.
        return [], [{
            "strategy_id": self.strategy_id,
            "wall_id": _wall_id(context.symbol, wall),
            "side": wall["side"], "price": float(wall["price"]),
            "notional": float(wall.get("current_size", wall.get("initial_size", 0))),
            "age_seconds": float(wall.get("age", 0)),
            "wall_passed": True, "evaluated": True,
            "signal_formed": False, "signal_accepted": False,
            "conditions": [],
            "reasons": ["Наблюдательный режим: стабильная стена сама по себе не создаёт сигнал."],
        } for wall in context.stable_walls]


def _wall_id(symbol: str, wall: dict) -> str:
    identity = wall.get("first_seen_at", wall.get("first_seen", "unknown"))
    return f"{symbol}:{wall['side']}:{float(wall['price']):.12g}:{identity}"


class BounceStrategy:
    """Maintain a pending plan for the nearest stable BID and ASK walls."""

    strategy_id = BOUNCE_STRATEGY_ID

    def __init__(self) -> None:
        self._states: dict[str, dict] = {}

    def reset_symbol(self, symbol: str) -> None:
        prefix = f"{symbol.upper()}:"
        self._states = {key: value for key, value in self._states.items() if not key.startswith(prefix)}

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        return self.evaluate_with_diagnostics(context)[0]

    def evaluate_with_diagnostics(
        self, context: StrategyContext
    ) -> tuple[list[TradeSignal], list[dict]]:
        return _wall_plan_signals(context, self.strategy_id)


class BreakoutStrategy:
    """Maintain a pending plan ahead of the nearest stable BID and ASK walls."""

    strategy_id = BREAKOUT_STRATEGY_ID

    def __init__(self) -> None:
        self._states: dict[str, dict] = {}

    def reset_symbol(self, symbol: str) -> None:
        prefix = f"{symbol.upper()}:"
        self._states = {key: value for key, value in self._states.items() if not key.startswith(prefix)}

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        return self.evaluate_with_diagnostics(context)[0]

    def evaluate_with_diagnostics(
        self, context: StrategyContext
    ) -> tuple[list[TradeSignal], list[dict]]:
        return _wall_plan_signals(context, self.strategy_id)


def _wall_plan_signals(
    context: StrategyContext, strategy_id: str
) -> tuple[list[TradeSignal], list[dict]]:
    traces = []
    walls = [wall for wall in context.stable_walls if wall.get("event") not in {"REMOVED", "CONSUMED"}]
    eligible: dict[str, tuple[float, dict]] = {}
    for wall in walls:
        side = wall.get("side")
        price = float(wall.get("price") or 0)
        age = float(wall.get("age") or 0)
        wall_id = _wall_id(context.symbol, wall)
        trace = {
            "strategy_id": strategy_id, "wall_id": wall_id, "side": side,
            "price": price,
            "notional": float(wall.get("current_size", wall.get("initial_size", 0))),
            "age_seconds": age, "wall_passed": True, "evaluated": True,
            "signal_formed": False, "signal_accepted": False,
            "conditions": [], "reasons": [],
        }
        traces.append(trace)
        minimum_age = max(40.0, context.settings.min_wall_age_seconds)
        age_ok = age >= minimum_age
        side_ok = (side == "buy" and price < context.last_price) or (
            side == "sell" and price > context.last_price
        )
        distance_bps = abs(context.last_price - price) / context.last_price * 10000
        distance_ok = distance_bps <= context.settings.max_wall_distance_bps
        trace["conditions"].extend((
            {"condition": "stable_wall_age", "passed": age_ok,
             "actual_seconds": age, "threshold_seconds": minimum_age},
            {"condition": "wall_on_valid_side_of_price", "passed": side_ok,
             "actual_wall_price": price, "current_price": context.last_price},
            {"condition": "max_wall_distance_bps", "passed": distance_ok,
             "actual_bps": distance_bps,
             "threshold_bps": context.settings.max_wall_distance_bps},
        ))
        if not age_ok:
            trace["reasons"].append("Стена ещё не достигла минимального стабильного возраста.")
        elif not side_ok:
            trace["reasons"].append("BID должен быть ниже цены, ASK — выше цены.")
        elif not distance_ok:
            trace["reasons"].append("Стена находится дальше допустимой дистанции поиска.")
        else:
            distance = abs(context.last_price - price)
            selected = eligible.get(side)
            if selected is None or distance < selected[0]:
                eligible[side] = (distance, wall)

    signals = []
    for side, (_, wall) in eligible.items():
        wall_id = _wall_id(context.symbol, wall)
        trace = next(row for row in traces if row["wall_id"] == wall_id)
        trade_side = (
            "long" if (strategy_id == BOUNCE_STRATEGY_ID and side == "buy")
            or (strategy_id == BREAKOUT_STRATEGY_ID and side == "sell")
            else "short"
        )
        trigger = "cross_down" if side == "buy" else "cross_up"
        trace["nearest_wall_passed"] = True
        trace["signal_formed"] = True
        trace["signal_accepted"] = True
        trace["reasons"].append("Ближайшая стабильная стена выбрана; создан план ожидания Entry.")
        signals.append(TradeSignal(
            strategy_id=strategy_id, symbol=context.symbol, wall_side=side,
            wall_price=float(wall["price"]), wall_id=wall_id,
            wall_size=float(wall.get("current_size", wall.get("initial_size", 0))),
            wall_ratio=float(wall.get("wall_ratio", 0)), side=trade_side,
            entry_mode="cross", entry_trigger_direction=trigger,
        ))
    for trace in traces:
        if not trace["signal_accepted"] and not trace["reasons"]:
            trace["reasons"].append("Есть более близкая подходящая стена этой стороны.")
    return signals, traces


def build_strategy_registry() -> StrategyRegistry:
    """Build the strategies available to the application at runtime.

    Register newly implemented strategies here. Per-pair activation remains
    controlled by persisted settings and the Telegram admin panel.
    """
    registry = StrategyRegistry()
    registry.register(StableWallStrategy())
    registry.register(BounceStrategy())
    registry.register(BreakoutStrategy())
    return registry
