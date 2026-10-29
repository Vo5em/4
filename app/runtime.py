import asyncio
from dataclasses import replace
import logging

from script.engine import EngineMode, StrategyPersistenceError, TradingEngine
from script.settings import PairSettings, TradingSettings, normalize_pair_symbol

logger = logging.getLogger(__name__)


class StrategyVersionConflict(RuntimeError):
    """The strategy buttons were rendered for an older pair configuration."""


class StrategyWorkerSyncError(RuntimeError):
    """The selection committed, but its market worker could not be synchronized."""


class RuntimeController:
    """Application lifecycle facade used by startup and admin callbacks."""

    def __init__(self, engine: TradingEngine) -> None:
        self.engine = engine
        self._lifecycle_lock = asyncio.Lock()
        self._workers_by_symbol: dict[str, asyncio.Task] = {}

    async def start(self, preserve_mode: bool = False) -> None:
        async with self._lifecycle_lock, self.engine.state_lock:
            previous_mode = self.engine.mode
            if previous_mode == EngineMode.STOPPING:
                raise RuntimeError("Engine is currently stopping")
            if self.engine.mode == EngineMode.STOPPED and preserve_mode:
                return
            previous_trades = [
                dict(trade) for trade in self.engine.trade_manager.trades
            ]
            previous_balance = self.engine.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value)
                for key, value in self.engine._pending_persistence.items()
            }
            disabled_pair_events = []
            if preserve_mode:
                # Persisted pending entries belong to an earlier process/feed
                # generation. Keep positions and balance, but rebuild all new
                # entry decisions from a newly synchronized market snapshot.
                for symbol in self.engine.pair_settings:
                    disabled_pair_events.extend(
                        self.engine.trade_manager.cancel_pending_for_symbol(
                            symbol, "PROCESS_RESTARTED"
                        )
                    )
            for symbol, settings in self.engine.pair_settings.items():
                if not settings.enabled:
                    disabled_pair_events.extend(
                        self.engine.trade_manager.cancel_pending_for_symbol(
                            symbol, "PAIR_DISABLED"
                        )
                    )
                enabled_strategies = self.engine.get_enabled_strategy_ids(symbol)
                disabled_strategy_ids = {
                    trade["strategy_id"]
                    for trade in self.engine.trade_manager.get_pending_trades()
                    if trade["symbol"] == symbol
                    and trade["strategy_id"] not in enabled_strategies
                }
                for strategy_id in disabled_strategy_ids:
                    disabled_pair_events.extend(
                        self.engine.trade_manager.cancel_pending_for_strategy(
                            symbol, strategy_id, "STRATEGY_DISABLED"
                        )
                    )
            if disabled_pair_events and not await self.engine.persist_events(
                disabled_pair_events
            ):
                self.engine.restore_trade_state(
                    previous_trades,
                    previous_balance,
                    previous_pending_persistence,
                )
                raise RuntimeError("Could not persist startup trade reconciliation")
            if self.engine.mode == EngineMode.ENTRIES_PAUSED and not preserve_mode:
                self.engine.resume_new_entries()
            elif self.engine.mode == EngineMode.STOPPED:
                self.engine.start()
            if self.engine.mode != previous_mode:
                try:
                    await self.engine.persist_control_state()
                except Exception:
                    self.engine.mode = previous_mode
                    raise
            await self._sync_workers()

    async def _sync_workers(self) -> None:
        from script.market_data import market_data_worker

        for symbol, task in list(self._workers_by_symbol.items()):
            if task.done() or not self.engine.should_run_market_feed(symbol):
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                self._workers_by_symbol.pop(symbol, None)

        if self.engine.mode not in {EngineMode.RUNNING, EngineMode.ENTRIES_PAUSED}:
            return
        for symbol in self.engine.pair_settings:
            if (
                self.engine.should_run_market_feed(symbol)
                and symbol not in self._workers_by_symbol
            ):
                task = asyncio.create_task(
                    market_data_worker(
                        symbol, self.engine, self.engine.pair_settings[symbol]
                    ),
                    name=f"market-data-{symbol.lower()}",
                )
                self.engine.register_worker(task)
                self._workers_by_symbol[symbol] = task

    async def pause_new_entries(self) -> None:
        async with self._lifecycle_lock, self.engine.state_lock:
            previous_mode = self.engine.mode
            previous_trades = [
                dict(trade) for trade in self.engine.trade_manager.trades
            ]
            previous_balance = self.engine.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value)
                for key, value in self.engine._pending_persistence.items()
            }
            self.engine.pause_new_entries()
            events = []
            for symbol in self.engine.pair_settings:
                events.extend(
                    self.engine.trade_manager.cancel_pending_for_symbol(
                        symbol, "ENTRIES_PAUSED"
                    )
                )
            if events:
                if not await self.engine.persist_events(events):
                    self.engine.restore_trade_state(
                        previous_trades,
                        previous_balance,
                        previous_pending_persistence,
                    )
                    self.engine.mode = previous_mode
                    raise RuntimeError("Could not persist cancelled pending entries")
            else:
                try:
                    await self.engine.persist_control_state()
                except Exception:
                    self.engine.mode = previous_mode
                    raise

    async def resume_new_entries(self) -> None:
        async with self._lifecycle_lock, self.engine.state_lock:
            previous_mode = self.engine.mode
            self.engine.resume_new_entries()
            try:
                await self.engine.persist_control_state()
            except Exception:
                self.engine.mode = previous_mode
                raise

    async def update_stake(self, stake_usdt: float) -> None:
        async with self._lifecycle_lock, self.engine.state_lock:
            settings = TradingSettings(
                stake_usdt=stake_usdt,
                initial_balance_usdt=self.engine.trading_settings.initial_balance_usdt,
                max_open_trades=self.engine.trading_settings.max_open_trades,
            )
            if self.engine.repository is not None:
                try:
                    await self.engine.repository.save_trading_settings(settings)
                except Exception as error:
                    self.engine.record_database_error(error)
                    raise
                self.engine.record_database_success()
            self.engine.trading_settings = settings
            self.engine.trade_manager.trading_settings = settings

    async def update_max_open_trades(self, maximum: int) -> None:
        async with self._lifecycle_lock, self.engine.state_lock:
            settings = TradingSettings(
                stake_usdt=self.engine.trading_settings.stake_usdt,
                initial_balance_usdt=self.engine.trading_settings.initial_balance_usdt,
                max_open_trades=maximum,
            )
            if self.engine.repository is not None:
                try:
                    await self.engine.repository.save_trading_settings(settings)
                except Exception as error:
                    self.engine.record_database_error(error)
                    raise
                self.engine.record_database_success()
            self.engine.trading_settings = settings
            self.engine.trade_manager.trading_settings = settings

    async def update_pair_settings(self, settings: PairSettings) -> None:
        symbol = settings.symbol.upper()
        settings = replace(settings, symbol=symbol)
        async with self._lifecycle_lock, self.engine.state_lock:
            previous_trades = [
                dict(trade) for trade in self.engine.trade_manager.trades
            ]
            previous_balance = self.engine.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value)
                for key, value in self.engine._pending_persistence.items()
            }
            events = []
            if not settings.enabled:
                events = self.engine.trade_manager.cancel_pending_for_symbol(
                    symbol, "PAIR_DISABLED"
                )
            if not await self.engine.persist_pair_settings(settings, events):
                if events:
                    self.engine.restore_trade_state(
                        previous_trades,
                        previous_balance,
                        previous_pending_persistence,
                    )
                raise RuntimeError("Could not persist pair settings and trade state")
            self.engine.pair_settings[symbol] = settings
            await self._sync_workers()

    async def update_enabled_strategies(
        self, symbol: str, strategy_ids: set[str], admin_user_id: int | None = None,
        expected_version: int | None = None,
    ) -> set[str]:
        symbol = normalize_pair_symbol(symbol)
        selected = set(strategy_ids)
        async with self._lifecycle_lock, self.engine.state_lock:
            if symbol not in self.engine.pair_settings:
                raise ValueError(f"Pair {symbol} is not configured")
            self.engine.strategy_registry.get_enabled(selected)
            if len(selected) > 1:
                raise ValueError("Select only one trading strategy per pair")
            previous = self.engine.get_enabled_strategy_ids(symbol)
            current_version = self.engine.strategy_versions.get(symbol, 0)
            if previous == selected:
                return set(previous)
            if expected_version is not None and expected_version != current_version:
                raise StrategyVersionConflict(
                    "The selected pair strategy changed after this keyboard was rendered"
                )
            previous_trades = [
                dict(trade) for trade in self.engine.trade_manager.trades
            ]
            previous_balance = self.engine.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value)
                for key, value in self.engine._pending_persistence.items()
            }
            events = []
            for strategy_id in previous - selected:
                events.extend(
                    self.engine.trade_manager.cancel_pending_for_strategy(
                        symbol, strategy_id, "STRATEGY_DISABLED"
                    )
                )
            try:
                await self.engine.persist_enabled_strategies(
                    symbol, selected, events, admin_user_id=admin_user_id,
                    expected_version=expected_version,
                )
            except Exception:
                if events:
                    self.engine.restore_trade_state(
                        previous_trades,
                        previous_balance,
                        previous_pending_persistence,
                    )
                raise
            self.engine.enabled_strategies[symbol] = selected
            if previous != selected:
                self.engine.strategy_versions[symbol] = current_version + 1
            self.engine.reset_strategy_state(symbol)
            try:
                await self._sync_workers()
            except Exception as error:
                self.engine.set_feed_status(symbol, "error", error)
                logger.exception(
                    "Strategy for %s was committed, but its market worker could not be synchronized",
                    symbol,
                )
                raise StrategyWorkerSyncError(
                    "Strategy was committed but the market worker could not be synchronized"
                ) from error
            return set(selected)

    async def add_pair(self, symbol: str) -> PairSettings:
        symbol = normalize_pair_symbol(symbol)
        async with self._lifecycle_lock, self.engine.state_lock:
            if symbol in self.engine.pair_settings:
                raise ValueError(f"Pair {symbol} is already configured")
            settings = PairSettings(symbol=symbol)
            if self.engine.repository is not None:
                try:
                    await self.engine.repository.save_pair_settings(settings)
                except Exception as error:
                    self.engine.record_database_error(error)
                    raise
                self.engine.record_database_success()
            self.engine.pair_settings[symbol] = settings
            self.engine.feed_status[symbol] = "stopped"
            self.engine.feed_errors[symbol] = None
            await self._sync_workers()
            return settings

    async def stop_all(self) -> list[dict]:
        async with self._lifecycle_lock:
            return await self.engine.stop_all()

    async def close_open_trades(self) -> list[dict]:
        async with self._lifecycle_lock:
            return await self.engine.close_open_trades()

    async def shutdown(self) -> None:
        """Stop feeds and persist state without closing virtual trades."""
        async with self._lifecycle_lock, self.engine.state_lock:
            await self.engine._stop_workers_locked()
            if self.engine._pending_persistence and not await self.engine.persist_events([]):
                raise RuntimeError("Could not flush pending trade state during shutdown")
            await self.engine.persist_control_state()
