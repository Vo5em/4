from dataclasses import dataclass
from math import isfinite
import re
from time import monotonic
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
    """Initial strategy: prepare one signal per stable wall, deduplicated by ID."""

    strategy_id = DEFAULT_STRATEGY_ID

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        signals = []
        for wall in context.stable_walls:
            side = wall["side"]
            price = float(wall["price"])
            instance = wall.get("first_seen_at", wall.get("first_seen", "unknown"))
            wall_id = f"{context.symbol}:{side}:{price:.12g}:{instance}"
            signals.append(
                TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=context.symbol,
                    wall_side=side,
                    wall_price=price,
                    wall_id=wall_id,
                    wall_size=float(wall["current_size"]),
                    wall_ratio=float(wall.get("wall_ratio", 0.0)),
                )
            )
        return signals


def _wall_id(symbol: str, wall: dict) -> str:
    identity = wall.get("first_seen_at", wall.get("first_seen", "unknown"))
    return f"{symbol}:{wall['side']}:{float(wall['price']):.12g}:{identity}"


def _risk_levels(
    *, strategy_id: str, side: str, wall_side: str, wall_price: float,
    entry_price: float, settings: PairSettings, stable_walls: tuple[dict, ...],
) -> tuple[float, float] | None:
    if side not in {"long", "short"} or not all(
        isfinite(value) and value > 0 for value in (wall_price, entry_price)
    ):
        return None
    direction = 1 if side == "long" else -1
    if strategy_id == BOUNCE_STRATEGY_ID:
        buffer = settings.stop_distance_bps / 10000
        stop = wall_price * (1 - buffer if wall_side == "buy" else 1 + buffer)
        risk = abs(entry_price - stop)
    else:
        # For a breakout, the invalidation level is the crossed wall itself.
        stop = wall_price
        risk = abs(entry_price - wall_price)
    if not isfinite(risk) or risk <= 0:
        return None
    target = entry_price + direction * risk * 1.5
    if (side == "long" and not stop < entry_price < target) or (
        side == "short" and not target < entry_price < stop
    ):
        return None

    if strategy_id == BOUNCE_STRATEGY_ID:
        obstacles = [
            float(wall["price"])
            for wall in stable_walls
            if wall.get("side") == ("sell" if side == "long" else "buy")
            and wall.get("price") is not None
            and (float(wall["price"]) > entry_price if side == "long" else float(wall["price"]) < entry_price)
            and (float(wall["price"]) < target if side == "long" else float(wall["price"]) > target)
        ]
        if obstacles:
            obstacle = min(obstacles) if side == "long" else max(obstacles)
            target = obstacle * (1 - 0.00001 if side == "long" else 1 + 0.00001)
            potential = direction * (target - entry_price)
            if potential < 1.5 * risk:
                return None
    return stop, target


class BounceStrategy:
    """Trade only after a stable wall is approached and price rejects it."""

    strategy_id = BOUNCE_STRATEGY_ID

    def __init__(self) -> None:
        self._states: dict[str, dict] = {}

    def reset_symbol(self, symbol: str) -> None:
        prefix = f"{symbol.upper()}:"
        self._states = {key: value for key, value in self._states.items() if not key.startswith(prefix)}

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        if not context.price_updated:
            return []
        now = context.now if context.now is not None else monotonic()
        live_ids = {_wall_id(context.symbol, wall) for wall in context.stable_walls}
        removed_ids = {
            _wall_id(context.symbol, event)
            for event in context.wall_events
            if event.get("event") in {"REMOVED", "CONSUMED"}
        }
        for wall_id in set(self._states).difference(live_ids).union(removed_ids):
            self._states.pop(wall_id, None)

        signals = []
        for wall in context.stable_walls:
            if float(wall.get("age", 0)) < 40:
                continue
            wall_id = _wall_id(context.symbol, wall)
            state = self._states.setdefault(wall_id, {"approached_at": None})
            price = context.last_price
            level = float(wall["price"])
            proximity = abs(level - price) / price * 10000
            if proximity <= context.settings.entry_distance_bps and state["approached_at"] is None:
                # A support approach comes from above; resistance comes from below.
                if (wall["side"] == "buy" and price >= level) or (
                    wall["side"] == "sell" and price <= level
                ):
                    state["approached_at"] = now
                    state["touch_price"] = price
                    continue
            if state["approached_at"] is None or now <= state["approached_at"]:
                continue
            threshold = context.settings.bounce_reversal_bps / 10000
            confirmed = (
                price >= level * (1 + threshold)
                if wall["side"] == "buy"
                else price <= level * (1 - threshold)
            )
            if not confirmed:
                continue
            side = "long" if wall["side"] == "buy" else "short"
            levels = _risk_levels(
                strategy_id=self.strategy_id, side=side, wall_side=wall["side"],
                wall_price=level, entry_price=price, settings=context.settings,
                stable_walls=tuple(item for item in context.stable_walls if item is not wall),
            )
            if levels is None:
                continue
            signals.append(TradeSignal(
                strategy_id=self.strategy_id, symbol=context.symbol,
                wall_side=wall["side"], wall_price=level, wall_id=wall_id,
                wall_size=float(wall["current_size"]), wall_ratio=float(wall.get("wall_ratio", 0)),
                side=side, entry_price=price, stop_loss=levels[0], take_profit=levels[1],
                entry_mode="market",
            ))
        return signals


class BreakoutStrategy:
    """Require price to hold beyond a stable wall for a configured duration."""

    strategy_id = BREAKOUT_STRATEGY_ID

    def __init__(self) -> None:
        self._states: dict[str, dict] = {}

    def reset_symbol(self, symbol: str) -> None:
        prefix = f"{symbol.upper()}:"
        self._states = {key: value for key, value in self._states.items() if not key.startswith(prefix)}

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        if not context.price_updated:
            return []
        now = context.now if context.now is not None else monotonic()
        events = {
            _wall_id(context.symbol, event): event
            for event in context.wall_events
            if event.get("event") in {"REMOVED", "CONSUMED"}
        }
        for wall_id, event in events.items():
            if event.get("event") == "REMOVED":
                self._states.pop(wall_id, None)
            elif wall_id in self._states:
                self._states[wall_id]["consumed"] = True
        live = {_wall_id(context.symbol, wall) for wall in context.stable_walls}
        for wall_id in list(self._states):
            if wall_id not in live and not self._states[wall_id].get("consumed"):
                self._states.pop(wall_id, None)

        signals = []
        walls_by_id = {_wall_id(context.symbol, wall): wall for wall in context.stable_walls}
        walls_by_id.update({key: event for key, event in events.items() if event.get("event") == "CONSUMED"})
        for wall_id, wall in walls_by_id.items():
            if float(wall.get("age", 0)) < 40:
                continue
            state = self._states.setdefault(wall_id, {
                "beyond_since": None,
                "last_beyond_at": None,
                "consumed": wall.get("event") == "CONSUMED",
            })
            level = float(wall["price"])
            buffer = context.settings.breakout_buffer_bps / 10000
            side = "long" if wall["side"] == "sell" else "short"
            beyond = context.last_price >= level * (1 + buffer) if side == "long" else context.last_price <= level * (1 - buffer)
            if not beyond:
                state["beyond_since"] = None
                state["last_beyond_at"] = None
                continue
            last_beyond_at = state["last_beyond_at"]
            if (
                state["beyond_since"] is None
                or last_beyond_at is None
                or now - last_beyond_at > context.settings.breakout_max_update_gap_seconds
            ):
                state["beyond_since"] = now
                state["last_beyond_at"] = now
                continue
            state["last_beyond_at"] = now
            if now - state["beyond_since"] < context.settings.breakout_confirm_seconds:
                continue
            levels = _risk_levels(
                strategy_id=self.strategy_id, side=side, wall_side=wall["side"],
                wall_price=level, entry_price=context.last_price, settings=context.settings,
                stable_walls=tuple(item for item in context.stable_walls if item is not wall),
            )
            if levels is None:
                continue
            signals.append(TradeSignal(
                strategy_id=self.strategy_id, symbol=context.symbol,
                wall_side=wall["side"], wall_price=level, wall_id=wall_id,
                wall_size=float(wall.get("current_size", wall.get("initial_size", 0))),
                wall_ratio=float(wall.get("wall_ratio", 0)), side=side,
                entry_price=context.last_price, stop_loss=levels[0], take_profit=levels[1],
                entry_mode="market",
            ))
        return signals


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
