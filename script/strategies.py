from dataclasses import dataclass
import re
from typing import Protocol

from .settings import PairSettings

DEFAULT_STRATEGY_ID = "stable_wall_v1"


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    last_price: float
    stable_walls: tuple[dict, ...]
    wall_events: tuple[dict, ...]
    settings: PairSettings


@dataclass(frozen=True)
class TradeSignal:
    strategy_id: str
    symbol: str
    wall_side: str
    wall_price: float
    wall_id: str
    wall_size: float
    wall_ratio: float


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


def build_strategy_registry() -> StrategyRegistry:
    """Build the strategies available to the application at runtime.

    Register newly implemented strategies here. Per-pair activation remains
    controlled by persisted settings and the Telegram admin panel.
    """
    registry = StrategyRegistry()
    registry.register(StableWallStrategy())
    return registry
