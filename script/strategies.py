from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Protocol
import logging

from .settings import PairSettings
from .trade_levels import (
    FALSE_BREAKOUT_PENDING_TIMEOUT_SECONDS,
    calculate_false_breakout_points, calculate_levels_from_entry,
)

DEFAULT_STRATEGY_ID = "stable_wall_v1"
BOUNCE_STRATEGY_ID = "bounce"
BREAKOUT_STRATEGY_ID = "breakout"
FALSE_BREAKOUT_STRATEGY_ID = "false_breakout"
COMBINED_STRATEGY_ID = "combined"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    last_price: float
    stable_walls: tuple[dict, ...]
    wall_events: tuple[dict, ...]
    settings: PairSettings
    now: float | None = None
    price_updated: bool = True
    historical_levels: tuple[dict, ...] = ()
    trade_states: tuple[dict, ...] = ()
    tick_size: float | None = None
    default_take_profit_bps: float = 5.0
    default_stop_loss_bps: float = 5.0


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
    level_policy_strategy_id: str | None = None
    take_profit_bps: float | None = None
    stop_loss_bps: float | None = None
    false_breakout_phase: str | None = None
    tick_size: float | None = None


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
    """Maintain one pending plan for each eligible stable BID and ASK wall."""

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
        return _wall_plan_signals(context, self.strategy_id, level_policy=self.strategy_id)


class BreakoutStrategy:
    """Maintain one pending plan for each eligible stable BID and ASK wall."""

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
        return _wall_plan_signals(context, self.strategy_id, level_policy=self.strategy_id)


class CombinedStrategy:
    """Choose exactly one existing wall strategy according to library touches."""

    strategy_id = COMBINED_STRATEGY_ID

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        return self.evaluate_with_diagnostics(context)[0]

    def evaluate_with_diagnostics(self, context: StrategyContext):
        return _wall_plan_signals(context, self.strategy_id, combined=True)


class FalseBreakoutStrategy:
    """Track a wall-triggered two-point scenario, then emit one reverse PENDING."""

    strategy_id = FALSE_BREAKOUT_STRATEGY_ID

    def __init__(self) -> None:
        self._scenarios: dict[str, dict[str, dict]] = {}
        self._previous_prices: dict[str, float] = {}
        self._counters: dict[str, dict[str, int]] = {}

    def restore_symbol_state(self, symbol: str, state: dict | None) -> None:
        symbol = symbol.upper()
        scenarios = (state or {}).get("scenarios", {})
        self._scenarios[symbol] = {
            str(key): dict(value) for key, value in scenarios.items()
            if isinstance(value, dict)
        }
        self._previous_prices.pop(symbol, None)
        self._counters[symbol] = {
            key: int(value) for key, value in (state or {}).get("counters", {}).items()
            if key in {"created", "cancelled", "completed"}
        }

    def export_symbol_state(self, symbol: str) -> dict:
        symbol = symbol.upper()
        return {
            "scenarios": {
                key: dict(value) for key, value in sorted(self._scenarios.get(symbol, {}).items())
            },
            "counters": dict(self._counters.get(symbol, {})),
        }

    def reset_price_tracking(self, symbol: str) -> None:
        self._previous_prices.pop(symbol.upper(), None)

    def reset_symbol(self, symbol: str) -> None:
        self.reset_price_tracking(symbol)

    def evaluate(self, context: StrategyContext) -> list[TradeSignal]:
        return self.evaluate_with_diagnostics(context)[0]

    def evaluate_with_diagnostics(self, context: StrategyContext):
        symbol = context.symbol.upper()
        scenarios = self._scenarios.setdefault(symbol, {})
        counters = self._counters.setdefault(symbol, {"created": 0, "cancelled": 0, "completed": 0})
        now = datetime.now(timezone.utc)
        previous_price = self._previous_prices.get(symbol)
        signals: list[TradeSignal] = []
        traces = []
        stable_by_id = {_wall_id(symbol, wall): wall for wall in context.stable_walls}
        trades_by_wall = {
            str(trade.get("wall_id")): trade for trade in context.trade_states
            if str(trade.get("symbol", "")).upper() == symbol
            and trade.get("strategy_id") == self.strategy_id
        }

        point_1_bps = context.settings.false_breakout_point_1_offset_bps
        point_2_bps = context.settings.false_breakout_point_2_distance_bps
        observation_timeout = context.settings.false_breakout_observation_timeout_seconds
        created_this_update = set()
        if (context.tick_size and point_1_bps is not None and point_2_bps is not None
                and observation_timeout is not None):
            for wall_id, wall in stable_by_id.items():
                level = nearest_matching_level(
                    wall, context.historical_levels,
                    context.settings.max_wall_level_distance_bps,
                )
                if level is None or wall_id in scenarios:
                    continue
                try:
                    point_1, point_2 = calculate_false_breakout_points(
                        wall_side=wall["side"], wall_price=wall["price"],
                        tick_size=context.tick_size,
                        point_1_offset_bps=point_1_bps,
                        point_2_distance_bps=point_2_bps,
                    )
                except (ValueError, ArithmeticError):
                    continue
                scenario = {
                    "wall_id": wall_id, "wall_side": wall["side"],
                    "wall_price": float(wall["price"]),
                    "wall_size": float(wall.get("current_size", wall.get("initial_size", 0))),
                    "wall_ratio": float(wall.get("wall_ratio", 0)),
                    "level_price": float(level["price"]),
                    "level_type": level["type"], "touch_count": int(level["touch_count"]),
                    "timeframe": level.get("timeframe"),
                    "level_distance_bps": float(level["distance_bps"]),
                    "point_1": float(point_1), "point_2": float(point_2),
                    "point_1_offset_bps": float(point_1_bps),
                    "point_2_distance_bps": float(point_2_bps),
                    "observation_timeout_seconds": float(observation_timeout) if observation_timeout else None,
                    "created_at": now.isoformat(), "status": "OBSERVING",
                }
                scenarios[wall_id] = scenario
                created_this_update.add(wall_id)
                counters["created"] = counters.get("created", 0) + 1
                logger.info("%s false_breakout scenario=%s status=OBSERVING", symbol, wall_id)

        for wall_id, scenario in list(scenarios.items()):
            status = scenario.get("status")
            trade = trades_by_wall.get(wall_id)
            if status in {"PENDING", "OPEN"} and trade is not None:
                if trade.get("status") == "open" and status != "OPEN":
                    self._set_status(symbol, scenario, "OPEN")
                elif trade.get("status") in {"closed", "cancelled"} and status not in {"COMPLETED", "CANCELLED"}:
                    terminal = "COMPLETED" if trade.get("status") == "closed" else "CANCELLED"
                    self._set_status(symbol, scenario, terminal)
                    if terminal == "CANCELLED":
                        counters["cancelled"] = counters.get("cancelled", 0) + 1
                    else:
                        counters["completed"] = counters.get("completed", 0) + 1
                elif trade.get("status") == "pending" and status == "PENDING":
                    signal = self._pending_signal(context, scenario)
                    if signal is not None:
                        signals.append(signal)
            elif status == "PENDING":
                signal = self._pending_signal(context, scenario)
                if signal is not None:
                    signals.append(signal)
            if status in {"OBSERVING", "POINT_1_REACHED"}:
                timeout = scenario.get("observation_timeout_seconds")
                try:
                    created_at = datetime.fromisoformat(scenario["created_at"])
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                except (KeyError, TypeError, ValueError):
                    created_at, timeout = now, 0
                if timeout is not None and (now - created_at).total_seconds() >= float(timeout):
                    self._set_status(symbol, scenario, "CANCELLED")
                    counters["cancelled"] = counters.get("cancelled", 0) + 1
                    continue
                if context.price_updated and wall_id not in created_this_update:
                    self._advance_scenario(symbol, scenario, previous_price, context.last_price)
                if scenario.get("status") == "PENDING":
                    signal = self._pending_signal(context, scenario)
                    if signal is not None:
                        signals.append(signal)
            traces.append({
                "strategy_id": self.strategy_id, "wall_id": wall_id,
                "side": scenario.get("wall_side"), "price": scenario.get("wall_price"),
                "wall_passed": wall_id in stable_by_id or status in {"POINT_1_REACHED", "POINT_2_REACHED", "PENDING", "OPEN"},
                "evaluated": True, "signal_formed": scenario.get("status") == "PENDING",
                "signal_accepted": scenario.get("status") == "PENDING",
                "false_breakout_phase": scenario.get("status"),
                "historical_level": {
                    "price": scenario.get("level_price"), "type": scenario.get("level_type"),
                    "touch_count": scenario.get("touch_count"), "timeframe": scenario.get("timeframe"),
                    "distance_bps": scenario.get("level_distance_bps"),
                },
                "point_1": scenario.get("point_1"), "point_2": scenario.get("point_2"),
                "conditions": [], "reasons": [],
            })
        if context.price_updated:
            self._previous_prices[symbol] = float(context.last_price)
        self._prune_terminal(symbol, stable_by_id)
        return signals, traces

    def diagnostics(self, symbol: str, trades: tuple[dict, ...] = ()) -> dict:
        scenarios = self._scenarios.get(symbol.upper(), {})
        statuses = [item.get("status") for item in scenarios.values()]
        symbol_trades = [trade for trade in trades if trade.get("symbol", "").upper() == symbol.upper()
                         and trade.get("strategy_id") == self.strategy_id]
        return {
            "observing": sum(status == "OBSERVING" for status in statuses),
            "point_1_reached": sum(status == "POINT_1_REACHED" for status in statuses),
            "point_2_reached": sum(status == "PENDING" for status in statuses),
            "pending": sum(trade.get("status") == "pending" for trade in symbol_trades),
            "open": sum(trade.get("status") == "open" for trade in symbol_trades),
            "cancelled_scenarios": self._counters.get(symbol.upper(), {}).get("cancelled", 0),
            "completed_scenarios": self._counters.get(symbol.upper(), {}).get("completed", 0),
        }

    def _advance_scenario(self, symbol, scenario, previous_price, price):
        if previous_price is None:
            return
        p1, p2 = float(scenario["point_1"]), float(scenario["point_2"])
        side = scenario["wall_side"]
        if scenario["status"] == "OBSERVING":
            crossed = (previous_price < p1 <= price) if side == "sell" else (previous_price > p1 >= price)
            if crossed:
                self._set_status(symbol, scenario, "POINT_1_REACHED")
        elif scenario["status"] == "POINT_1_REACHED":
            crossed = (previous_price < p2 <= price) if side == "sell" else (previous_price > p2 >= price)
            if crossed:
                self._set_status(symbol, scenario, "PENDING")

    def _pending_signal(self, context, scenario):
        if not context.tick_size:
            return None
        side = "short" if scenario["wall_side"] == "sell" else "long"
        entry = float(scenario["point_1"])
        tp_bps = (
            context.settings.false_breakout_take_profit_bps
            or context.settings.take_profit_bps
            or context.default_take_profit_bps
        )
        sl_bps = (
            context.settings.false_breakout_stop_loss_bps
            or context.settings.stop_loss_bps
            or context.default_stop_loss_bps
        )
        try:
            levels = calculate_levels_from_entry(
                side=side, entry_price=entry, tick_size=context.tick_size,
                take_profit_bps=tp_bps, stop_loss_bps=sl_bps,
            )
        except (ValueError, ArithmeticError):
            return None
        trigger = "cross_down" if scenario["wall_side"] == "sell" else "cross_up"
        return TradeSignal(
            strategy_id=self.strategy_id, symbol=context.symbol,
            wall_side=scenario["wall_side"], wall_price=float(scenario["wall_price"]),
            wall_id=scenario["wall_id"], wall_size=float(scenario["wall_size"]),
            wall_ratio=float(scenario["wall_ratio"]), side=side,
            entry_price=float(levels["entry_price"]),
            stop_loss=float(levels["stop_loss"]), take_profit=float(levels["take_profit"]),
            entry_mode="cross", entry_trigger_direction=trigger,
            level_policy_strategy_id=self.strategy_id,
            take_profit_bps=float(tp_bps), stop_loss_bps=float(sl_bps),
            false_breakout_phase="PENDING",
            tick_size=float(context.tick_size),
        )

    @staticmethod
    def _set_status(symbol, scenario, status):
        previous = scenario.get("status")
        if previous == status:
            return
        scenario["status"] = status
        scenario["status_changed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("%s false_breakout scenario=%s status=%s->%s", symbol, scenario.get("wall_id"), previous, status)

    def _prune_terminal(self, symbol, stable_by_id):
        scenarios = self._scenarios.get(symbol, {})
        for wall_id, scenario in list(scenarios.items()):
            if scenario.get("status") in {"COMPLETED", "CANCELLED"} and wall_id not in stable_by_id:
                del scenarios[wall_id]


def nearest_matching_level(wall: dict, levels: tuple[dict, ...], maximum_bps: float):
    expected_type = "SUPPORT" if wall.get("side") == "buy" else "RESISTANCE"
    wall_price = float(wall.get("price") or 0)
    matches = []
    for level in levels:
        if str(level.get("type", "")).upper() != expected_type:
            continue
        try:
            level_price = float(level["price"])
            touches = int(level["touch_count"])
        except (KeyError, TypeError, ValueError):
            continue
        if wall_price <= 0 or level_price <= 0 or touches < 1:
            continue
        distance_bps = abs(wall_price - level_price) / level_price * 10000
        if distance_bps <= maximum_bps:
            matches.append((distance_bps, level_price, level, touches))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]))
    distance, _, level, touches = matches[0]
    return {**level, "distance_bps": distance, "touch_count": touches}


def _wall_plan_signals(
    context: StrategyContext, strategy_id: str, *, level_policy: str | None = None,
    combined: bool = False,
) -> tuple[list[TradeSignal], list[dict]]:
    traces = []
    walls = [wall for wall in context.stable_walls if wall.get("event") not in {"REMOVED", "CONSUMED"}]
    eligible: dict[str, dict] = {}
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
        level = nearest_matching_level(
            wall, context.historical_levels,
            context.settings.max_wall_level_distance_bps,
        )
        trace["historical_level"] = level
        level_ok = level is not None
        if not level_ok:
            trace["reasons"].append("Нет подходящего исторического уровня нужного типа в допустимой дистанции.")
        else:
            trace["conditions"].append({
                "condition": "historical_level_distance_bps", "passed": True,
                "actual_bps": level["distance_bps"],
                "threshold_bps": context.settings.max_wall_level_distance_bps,
                "level_price": level["price"], "level_type": level["type"],
                "touch_count": level["touch_count"],
                "timeframe": level.get("timeframe"),
            })
        minimum_age = max(40.0, context.settings.min_wall_age_seconds)
        age_ok = age >= minimum_age
        side_ok = (side == "buy" and price < context.last_price) or (
            side == "sell" and price > context.last_price
        )
        distance_bps = abs(context.last_price - price) / context.last_price * 10000
        distance_ok = distance_bps <= context.settings.max_wall_distance_bps
        trace["conditions"].extend((
            {"condition": "historical_level_match", "passed": level_ok},
            {"condition": "stable_wall_age", "passed": age_ok,
             "actual_seconds": age, "threshold_seconds": minimum_age},
            {"condition": "wall_on_valid_side_of_price", "passed": side_ok,
             "actual_wall_price": price, "current_price": context.last_price},
            {"condition": "max_wall_distance_bps", "passed": distance_ok,
             "actual_bps": distance_bps,
             "threshold_bps": context.settings.max_wall_distance_bps},
        ))
        policy = level_policy or strategy_id
        if combined and level is not None:
            policy = BOUNCE_STRATEGY_ID if level["touch_count"] < 2 else BREAKOUT_STRATEGY_ID
            trace["combined_choice"] = policy
        touches_ok = level is not None and (
            level["touch_count"] < 2 if policy == BOUNCE_STRATEGY_ID
            else level["touch_count"] >= 2
        )
        trace["conditions"].append({
            "condition": "historical_touch_count", "passed": touches_ok,
            "actual": level["touch_count"] if level else None,
            "rule": "<2" if policy == BOUNCE_STRATEGY_ID else ">=2",
        })
        if not level_ok:
            continue
        if not touches_ok:
            trace["reasons"].append(
                f"Уровень имеет {level['touch_count']} касаний; правило {strategy_id} не выполнено."
            )
        elif not age_ok:
            trace["reasons"].append("Стена ещё не достигла минимального стабильного возраста.")
        elif not side_ok:
            trace["reasons"].append("BID должен быть ниже цены, ASK — выше цены.")
        elif not distance_ok:
            trace["reasons"].append("Стена находится дальше допустимой дистанции поиска.")
        else:
            eligible[wall_id] = wall

    signals = []
    for wall_id, wall in eligible.items():
        side = wall["side"]
        trace = next(row for row in traces if row["wall_id"] == wall_id)
        policy = trace.get("combined_choice") if combined else (level_policy or strategy_id)
        trade_side = (
            "long" if (policy == BOUNCE_STRATEGY_ID and side == "buy")
            or (policy == BREAKOUT_STRATEGY_ID and side == "sell")
            else "short"
        )
        trigger = "cross_down" if side == "buy" else "cross_up"
        trace["signal_formed"] = True
        trace["signal_accepted"] = True
        trace["reasons"].append("Подходящая стабильная стена выбрана; создан план ожидания Entry.")
        signals.append(TradeSignal(
            strategy_id=strategy_id, symbol=context.symbol, wall_side=side,
            wall_price=float(wall["price"]), wall_id=wall_id,
            wall_size=float(wall.get("current_size", wall.get("initial_size", 0))),
            wall_ratio=float(wall.get("wall_ratio", 0)), side=trade_side,
            entry_mode="cross", entry_trigger_direction=trigger,
            level_policy_strategy_id=policy,
        ))
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
    registry.register(FalseBreakoutStrategy())
    registry.register(CombinedStrategy())
    return registry
