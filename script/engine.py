import asyncio
from enum import Enum
from time import monotonic

from .settings import PairSettings, TradingSettings
from .execution import ExecutionBackend
from .strategies import (
    DEFAULT_STRATEGY_ID,
    StrategyContext,
    StrategyRegistry,
    build_strategy_registry,
)
from .trade_manager import TradeManager


class EngineMode(str, Enum):
    RUNNING = "running"
    ENTRIES_PAUSED = "entries_paused"
    STOPPING = "stopping"
    STOPPED = "stopped"


class TradingEngine:
    """Coordinates strategies and virtual execution independently of Telegram."""

    def __init__(
        self,
        pair_settings: dict[str, PairSettings],
        trading_settings: TradingSettings | None = None,
        strategy_registry: StrategyRegistry | None = None,
        enabled_strategies: dict[str, set[str]] | None = None,
        trade_manager: ExecutionBackend | None = None,
        repository=None,
    ) -> None:
        self.pair_settings = {key.upper(): value for key, value in pair_settings.items()}
        self.trading_settings = trading_settings or TradingSettings()
        self.strategy_registry = strategy_registry or build_strategy_registry()
        if strategy_registry is None:
            self._default_enabled_strategies = {DEFAULT_STRATEGY_ID}
        else:
            self._default_enabled_strategies = set()
        self.enabled_strategies = {
            symbol.upper(): strategy_ids
            for symbol, strategy_ids in (enabled_strategies or {}).items()
        }
        unknown_pair_settings = set(self.enabled_strategies).difference(
            self.pair_settings
        )
        if unknown_pair_settings:
            raise ValueError(
                "Strategy configuration references unknown pairs: "
                + ", ".join(sorted(unknown_pair_settings))
            )
        for strategy_ids in self.enabled_strategies.values():
            self.strategy_registry.get_enabled(strategy_ids)
        self.strategy_errors: dict[str, dict[str, str]] = {}
        self.trade_manager = trade_manager or TradeManager(self.trading_settings)
        self.repository = repository
        self._pending_persistence: dict[str, dict] = {}
        self.state_lock = asyncio.Lock()
        self._persistence_lock = asyncio.Lock()
        self._last_market_price_save: dict[str, float] = {}
        self.mode = EngineMode.STOPPED
        self._workers: set[asyncio.Task] = set()
        self.feed_status = {symbol: "stopped" for symbol in self.pair_settings}

    def start(self) -> None:
        if self.mode == EngineMode.STOPPING:
            raise RuntimeError("Engine is currently stopping")
        self.mode = EngineMode.RUNNING

    def register_worker(self, task: asyncio.Task) -> None:
        if self.mode not in {EngineMode.RUNNING, EngineMode.ENTRIES_PAUSED}:
            raise RuntimeError("Workers can only be registered while the engine is active")
        self._workers.add(task)
        task.add_done_callback(self._workers.discard)

    def restore_trade_state(
        self,
        trades: list[dict],
        balance_usdt: float,
        pending_persistence: dict[str, dict],
    ) -> None:
        """Restore a snapshot and reconnect retry entries to restored trade objects."""
        self.trade_manager.restore_state(trades, balance_usdt)
        trades_by_id = {
            trade["signal_id"]: trade for trade in self.trade_manager.trades
        }
        self._pending_persistence = {
            signal_id: trades_by_id.get(signal_id, dict(trade))
            for signal_id, trade in pending_persistence.items()
        }

    def pause_new_entries(self) -> None:
        if self.mode == EngineMode.STOPPED:
            raise RuntimeError("Start the engine before pausing entries")
        if self.mode == EngineMode.STOPPING:
            raise RuntimeError("Engine is currently stopping")
        self.mode = EngineMode.ENTRIES_PAUSED

    def resume_new_entries(self) -> None:
        if self.mode != EngineMode.ENTRIES_PAUSED:
            raise RuntimeError("New entries are not paused")
        self.mode = EngineMode.RUNNING

    async def stop_all(self) -> list[dict]:
        async with self.state_lock:
            return await self._stop_all_locked()

    async def close_open_trades(self) -> list[dict]:
        """Close active positions while leaving the engine and pending entries running."""
        async with self.state_lock:
            previous_trades = [dict(trade) for trade in self.trade_manager.trades]
            previous_balance = self.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value) for key, value in self._pending_persistence.items()
            }
            try:
                events = self.trade_manager.close_open_trades()
                if not await self.persist_events(events):
                    raise RuntimeError("Could not persist closed virtual trade state")
            except Exception:
                self.restore_trade_state(
                    previous_trades, previous_balance, previous_pending_persistence
                )
                raise
            return events

    async def _stop_all_locked(self) -> list[dict]:
        if self.mode == EngineMode.STOPPED:
            return []
        previous_mode = self.mode
        previous_trades = [dict(trade) for trade in self.trade_manager.trades]
        previous_balance = self.trade_manager.balance_usdt
        previous_pending_persistence = {
            key: dict(value) for key, value in self._pending_persistence.items()
        }
        self.mode = EngineMode.STOPPING
        try:
            events = self.trade_manager.close_all()
            if not await self.persist_events(events):
                raise RuntimeError("Could not persist final virtual trade state")
        except Exception:
            self.restore_trade_state(
                previous_trades, previous_balance, previous_pending_persistence
            )
            self.mode = previous_mode
            raise
        await self._stop_workers_locked()
        self.mode = EngineMode.STOPPED
        await self.persist_control_state()
        return events

    async def _stop_workers_locked(self) -> None:
        """Stop market feeds while the caller holds state_lock."""
        try:
            current_task = asyncio.current_task()
        except RuntimeError:  # Supports synchronous coroutine-driven unit tests.
            current_task = None
        workers = [task for task in self._workers if task is not current_task]
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._workers.clear()
        for symbol in self.feed_status:
            self.feed_status[symbol] = "stopped"

    async def persist_events(self, events: list[dict]) -> bool:
        async with self._persistence_lock:
            for event in events:
                trade = event.get("trade")
                if trade is not None:
                    self._pending_persistence[trade["signal_id"]] = trade
            if self.repository is None:
                self._pending_persistence.clear()
                return True
            if not self._pending_persistence:
                return True
            snapshot_refs = dict(self._pending_persistence)
            snapshot = [dict(trade) for trade in snapshot_refs.values()]
            balance_snapshot = self.trade_manager.balance_usdt
            try:
                await self.repository.save_trades(
                    snapshot,
                    balance_snapshot,
                    engine_mode=self.mode.value,
                )
            except Exception as error:
                print(f"Database trade save failed: {type(error).__name__}")
                return False
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    async def persist_control_state(self) -> None:
        if self.repository is None:
            return
        async with self._persistence_lock:
            await self.repository.save_runtime_state(
                engine_mode=self.mode.value,
                balance_usdt=self.trade_manager.balance_usdt,
                trading_settings=self.trading_settings,
                initialized=True,
            )

    async def persist_pair_settings(
        self, settings: PairSettings, events: list[dict]
    ) -> bool:
        """Atomically persist pair settings and trades changed by that update."""
        async with self._persistence_lock:
            for event in events:
                trade = event.get("trade")
                if trade is not None:
                    self._pending_persistence[trade["signal_id"]] = trade
            if self.repository is None:
                self._pending_persistence.clear()
                return True
            snapshot_refs = dict(self._pending_persistence)
            snapshot = [dict(trade) for trade in snapshot_refs.values()]
            try:
                await self.repository.save_pair_settings_with_trades(
                    settings,
                    snapshot,
                    self.trade_manager.balance_usdt,
                    self.mode.value,
                )
            except Exception as error:
                print(f"Database pair update failed: {type(error).__name__}")
                return False
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    async def persist_enabled_strategies(
        self, symbol: str, strategy_ids: set[str], events: list[dict]
    ) -> bool:
        """Atomically save strategy selection and any cancelled entries."""
        async with self._persistence_lock:
            for event in events:
                trade = event.get("trade")
                if trade is not None:
                    self._pending_persistence[trade["signal_id"]] = trade
            if self.repository is None:
                self._pending_persistence.clear()
                return True
            snapshot_refs = dict(self._pending_persistence)
            snapshot = [dict(trade) for trade in snapshot_refs.values()]
            try:
                await self.repository.save_enabled_strategies_with_trades(
                    symbol,
                    strategy_ids,
                    snapshot,
                    self.trade_manager.balance_usdt,
                    self.mode.value,
                )
            except Exception as error:
                print(f"Database strategy update failed: {type(error).__name__}")
                return False
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    async def persist_market_price(
        self,
        symbol: str,
        price: float,
        *,
        min_interval_seconds: float = 5.0,
    ) -> None:
        if self.repository is None:
            return
        symbol = symbol.upper()
        now = monotonic()
        if now - self._last_market_price_save.get(symbol, 0.0) < min_interval_seconds:
            return
        self._last_market_price_save[symbol] = now
        try:
            await self.repository.save_market_price(symbol, price)
        except Exception as error:
            print(f"Database market price save failed: {type(error).__name__}")

    async def process_market_update(
        self,
        symbol: str,
        last_price: float,
        stable_walls: list[dict] | tuple[dict, ...],
        wall_events: list[dict] | tuple[dict, ...] = (),
        wall_candidates: list[dict] | tuple[dict, ...] | None = None,
    ) -> list[dict]:
        """Serialize a market-state transition and its persistence as one unit."""
        async with self.state_lock:
            events = self.on_market_update(
                symbol, last_price, stable_walls, wall_events, wall_candidates
            )
            await self.persist_events(events)
            await self.persist_market_price(symbol, last_price)
            return events

    def on_market_update(
        self,
        symbol: str,
        last_price: float,
        stable_walls: list[dict] | tuple[dict, ...],
        wall_events: list[dict] | tuple[dict, ...] = (),
        wall_candidates: list[dict] | tuple[dict, ...] | None = None,
    ) -> list[dict]:
        symbol = symbol.upper()
        if self.mode in {EngineMode.STOPPING, EngineMode.STOPPED}:
            return []

        events = []
        settings = self.pair_settings.get(symbol)
        if settings is not None and not settings.enabled:
            events.extend(
                self.trade_manager.cancel_pending_for_symbol(
                    symbol, "PAIR_DISABLED"
                )
            )
        elif self.mode == EngineMode.ENTRIES_PAUSED:
            events.extend(
                self.trade_manager.cancel_pending_for_symbol(
                    symbol, "ENTRIES_PAUSED"
                )
            )
        for wall_event in wall_events:
            if wall_event.get("event") in {"REMOVED", "CONSUMED"}:
                events.extend(
                    self.trade_manager.cancel_pending_for_wall(
                        symbol,
                        wall_event["side"],
                        wall_event["price"],
                        f"WALL_{wall_event['event']}",
                    )
                )
        if wall_candidates is not None:
            valid_walls = {
                (wall["side"], float(wall["price"]))
                for wall in wall_candidates
            }
            events.extend(
                self.trade_manager.cancel_pending_not_in_wall_set(
                    symbol,
                    valid_walls,
                    "WALL_NO_LONGER_QUALIFIES",
                )
            )
        events.extend(self.trade_manager.on_price(symbol, last_price))
        if settings is None or not settings.enabled or self.mode != EngineMode.RUNNING:
            return events
        if self.repository is not None and self._pending_persistence:
            return events

        context = StrategyContext(
            symbol=symbol,
            last_price=last_price,
            stable_walls=tuple(stable_walls),
            wall_events=tuple(wall_events),
            settings=settings,
        )
        strategy_ids = self.enabled_strategies.get(
            symbol, self._default_enabled_strategies
        )
        for strategy in self.strategy_registry.get_enabled(strategy_ids):
            try:
                signals = strategy.evaluate(context)
            except Exception as error:
                self.strategy_errors.setdefault(symbol, {})[
                    strategy.strategy_id
                ] = type(error).__name__
                print(
                    f"Strategy {strategy.strategy_id} failed for {symbol}: "
                    f"{type(error).__name__}"
                )
                continue
            symbol_errors = self.strategy_errors.get(symbol)
            if symbol_errors is not None:
                symbol_errors.pop(strategy.strategy_id, None)
                if not symbol_errors:
                    self.strategy_errors.pop(symbol, None)
            for signal in signals:
                trade = self.trade_manager.create_trade(
                    signal,
                    settings,
                    stake_usdt=self.trading_settings.stake_usdt,
                )
                if trade is not None:
                    events.append({"event": "TRADE_CREATED", "trade": trade})
        return events

    def get_status(self) -> dict:
        stats = self.trade_manager.get_statistics()
        return {
            "mode": self.mode.value,
            "feed_status": dict(self.feed_status),
            "strategy_errors": {
                symbol: dict(errors)
                for symbol, errors in self.strategy_errors.items()
            },
            "persistence_pending": len(self._pending_persistence),
            "enabled_pairs": [
                symbol for symbol, settings in self.pair_settings.items() if settings.enabled
            ],
            **stats,
        }

    def get_enabled_strategy_ids(self, symbol: str) -> set[str]:
        return set(
            self.enabled_strategies.get(
                symbol.upper(), self._default_enabled_strategies
            )
        )

    def should_run_market_feed(self, symbol: str) -> bool:
        symbol = symbol.upper()
        settings = self.pair_settings.get(symbol)
        if settings is None:
            return False
        return self.trade_manager.has_active_symbol(symbol) or (
            settings.enabled and bool(self.get_enabled_strategy_ids(symbol))
        )
