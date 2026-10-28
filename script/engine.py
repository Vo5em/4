import asyncio
import logging
from datetime import datetime, timezone
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

logger = logging.getLogger(__name__)

class StrategyPersistenceError(RuntimeError):
    """A pair strategy setting could not be committed to the repository."""


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
        strategy_versions: dict[str, int] | None = None,
        trade_manager: ExecutionBackend | None = None,
        repository=None,
        last_market_update_at: dict[str, datetime] | None = None,
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
        self.strategy_versions = {
            symbol.upper(): int(version)
            for symbol, version in (strategy_versions or {}).items()
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
        self.feed_errors: dict[str, dict | None] = {
            symbol: None for symbol in self.pair_settings
        }
        self.last_market_update_at = {
            symbol.upper(): timestamp
            for symbol, timestamp in (last_market_update_at or {}).items()
        }
        # Runtime-only heartbeat: restored prices must not make a newly started
        # algorithm look active before it processes a fresh market update.
        self.last_algorithm_update_at: dict[str, datetime] = {}
        self.market_analysis = {
            symbol: {
                "potential_wall_count": None,
                "stable_wall_count": None,
                "updated_at": None,
            }
            for symbol in self.pair_settings
        }
        self.database_status = "unknown" if repository is not None else "not_configured"
        self.database_last_success_at: datetime | None = None
        self.database_last_error: dict | None = None

    def start(self) -> None:
        if self.mode == EngineMode.STOPPING:
            raise RuntimeError("Engine is currently stopping")
        self.mode = EngineMode.RUNNING

    def register_worker(self, task: asyncio.Task) -> None:
        if self.mode not in {EngineMode.RUNNING, EngineMode.ENTRIES_PAUSED}:
            raise RuntimeError("Workers can only be registered while the engine is active")
        self._workers.add(task)
        task.add_done_callback(self._workers.discard)

    def reset_strategy_state(self, symbol: str) -> None:
        for strategy in self.strategy_registry.get_enabled():
            reset = getattr(strategy, "reset_symbol", None)
            if reset is not None:
                reset(symbol)

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    def set_feed_status(self, symbol: str, status: str, error: Exception | None = None) -> None:
        symbol = symbol.upper()
        self.feed_status[symbol] = status
        if error is not None:
            self.feed_errors[symbol] = {
                "message": str(error),
                "type": type(error).__name__,
                "at": self._utc_now(),
            }
        elif status == "connected":
            self.feed_errors[symbol] = None

    def record_database_success(self) -> None:
        if self.repository is None:
            return
        self.database_status = "connected"
        self.database_last_success_at = self._utc_now()
        self.database_last_error = None

    def record_database_error(self, error: Exception) -> None:
        if self.repository is None:
            return
        self.database_status = "error"
        self.database_last_error = {
            "message": str(error),
            "type": type(error).__name__,
            "at": self._utc_now(),
        }

    def _record_market_analysis(
        self,
        symbol: str,
        stable_walls: list[dict] | tuple[dict, ...],
        wall_candidates: list[dict] | tuple[dict, ...] | None,
    ) -> None:
        symbol = symbol.upper()
        metrics = self.market_analysis.setdefault(
            symbol,
            {"potential_wall_count": None, "stable_wall_count": None, "updated_at": None},
        )
        if wall_candidates is not None:
            metrics["potential_wall_count"] = len(wall_candidates)
            metrics["stable_wall_count"] = len(stable_walls)
            metrics["updated_at"] = self._utc_now()

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
                self.record_database_error(error)
                print(f"Database trade save failed: {type(error).__name__}")
                return False
            self.record_database_success()
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    async def persist_control_state(self) -> None:
        if self.repository is None:
            return
        async with self._persistence_lock:
            try:
                await self.repository.save_runtime_state(
                    engine_mode=self.mode.value,
                    balance_usdt=self.trade_manager.balance_usdt,
                    trading_settings=self.trading_settings,
                    initialized=True,
                )
            except Exception as error:
                self.record_database_error(error)
                raise
            self.record_database_success()

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
                self.record_database_error(error)
                print(f"Database pair update failed: {type(error).__name__}")
                return False
            self.record_database_success()
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    async def persist_enabled_strategies(
        self, symbol: str, strategy_ids: set[str], events: list[dict],
        admin_user_id: int | None = None,
        expected_version: int | None = None,
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
                save = self.repository.save_enabled_strategies_with_trades
                args = (
                    symbol, strategy_ids, snapshot,
                    self.trade_manager.balance_usdt, self.mode.value,
                )
                if admin_user_id is None and expected_version is None:
                    await save(*args)
                else:
                    await save(
                        *args, admin_user_id=admin_user_id,
                        expected_version=expected_version,
                    )
            except Exception as error:
                self.record_database_error(error)
                logger.exception("Database strategy update failed for %s", symbol)
                raise StrategyPersistenceError(
                    "Could not commit pair strategy configuration"
                ) from error
            self.record_database_success()
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
            self.record_database_success()
        except Exception as error:
            self.record_database_error(error)
            print(f"Database market price save failed: {type(error).__name__}")

    async def process_market_update(
        self,
        symbol: str,
        last_price: float,
        stable_walls: list[dict] | tuple[dict, ...],
        wall_events: list[dict] | tuple[dict, ...] = (),
        wall_candidates: list[dict] | tuple[dict, ...] | None = None,
        *,
        price_updated: bool = True,
    ) -> list[dict]:
        """Serialize a market-state transition and its persistence as one unit."""
        async with self.state_lock:
            symbol = symbol.upper()
            events = self.on_market_update(
                symbol, last_price, stable_walls, wall_events, wall_candidates,
                price_updated=price_updated,
            )
            now = self._utc_now()
            self.last_market_update_at[symbol] = now
            self.last_algorithm_update_at[symbol] = now
            self._record_market_analysis(symbol, stable_walls, wall_candidates)
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
        *,
        price_updated: bool = True,
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
        if not price_updated:
            return events

        context = StrategyContext(
            symbol=symbol,
            last_price=last_price,
            stable_walls=tuple(stable_walls),
            wall_events=tuple(wall_events),
            settings=settings,
            now=monotonic(),
            price_updated=price_updated,
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
            "feed_errors": {
                symbol: dict(error) if error is not None else None
                for symbol, error in self.feed_errors.items()
            },
            "last_market_update_at": dict(self.last_market_update_at),
            "market_analysis": {
                symbol: dict(metrics)
                for symbol, metrics in self.market_analysis.items()
            },
            "algorithm_status": self.get_algorithm_status(),
            "database_status": self.database_status,
            "database_last_success_at": self.database_last_success_at,
            "database_last_error": (
                dict(self.database_last_error)
                if self.database_last_error is not None
                else None
            ),
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

    def get_algorithm_status(self) -> str:
        if self.mode in {EngineMode.STOPPED, EngineMode.STOPPING}:
            return "stopped"
        required = [
            symbol
            for symbol in self.pair_settings
            if self.should_run_market_feed(symbol)
        ]
        if not required:
            return "stopped"
        if any(self.feed_errors.get(symbol) for symbol in required):
            return "error"
        if any(
            strategy_errors
            for symbol, strategy_errors in self.strategy_errors.items()
            if symbol in required
        ):
            return "error"
        now = self._utc_now()
        if any(
            self.feed_status.get(symbol) == "connected"
            and self.last_algorithm_update_at.get(symbol) is not None
            and (now - self.last_algorithm_update_at[symbol]).total_seconds() <= 30
            for symbol in required
        ):
            return "working"
        if any(self.feed_status.get(symbol) == "reconnecting" for symbol in required):
            return "error"
        return "waiting"

    def get_entry_block_reason(self, symbol: str) -> str:
        symbol = symbol.upper()
        settings = self.pair_settings.get(symbol)
        if settings is None:
            return "Пара не настроена."
        if self.mode == EngineMode.STOPPED or self.mode == EngineMode.STOPPING:
            return "Движок остановлен."
        if self.mode == EngineMode.ENTRIES_PAUSED:
            return "Новые входы на паузе."
        if not settings.enabled:
            return "Пара выключена."
        if not self.get_enabled_strategy_ids(symbol):
            return "Для пары не выбраны стратегии."
        if self.feed_status.get(symbol) != "connected":
            error = self.feed_errors.get(symbol)
            return (
                f"Рыночные данные: {error['message']}"
                if error
                else "Ожидание рыночных данных."
            )
        if not self.last_market_update_at.get(symbol):
            return "Рынок ещё не обработан алгоритмом."
        if self.repository is not None and self._pending_persistence:
            return "Ожидается сохранение состояния сделок в PostgreSQL."
        analysis = self.market_analysis.get(symbol, {})
        potential = analysis.get("potential_wall_count")
        stable = analysis.get("stable_wall_count")
        if potential is None:
            return "Нет данных анализа стен."
        if potential == 0:
            return "Нет потенциальных стен, прошедших текущие фильтры."
        if stable == 0:
            return "Потенциальные стены ещё не подтвердили устойчивость."
        stats = self.trade_manager.get_statistics()
        if stats["available_balance_usdt"] < self.trading_settings.stake_usdt:
            return "Недостаточно свободного виртуального баланса для ставки."
        if symbol in self.strategy_errors:
            return "Ошибка оценки одной или нескольких стратегий."
        return "Текущая стратегия ещё не сформировала новый сигнал."

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
