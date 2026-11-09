import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import logging

from config import CANCELLED_TRADE_CLEANUP_INTERVAL_SECONDS
from script.engine import EngineMode, StrategyPersistenceError, TradingEngine
from script.settings import PairSettings, TradingSettings, normalize_pair_symbol
from script.trade_levels import (
    effective_strategy_trade_level_bps, effective_trade_level_bps,
)

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
        self._cancelled_cleanup_task: asyncio.Task | None = None
        self._false_breakout_timeout_task: asyncio.Task | None = None

    def start_false_breakout_timeout_service(self) -> None:
        if self._false_breakout_timeout_task is not None and not self._false_breakout_timeout_task.done():
            return
        self._false_breakout_timeout_task = asyncio.create_task(
            self._false_breakout_timeout_loop(), name="false-breakout-pending-timeout"
        )

    async def _false_breakout_timeout_loop(self) -> None:
        while True:
            try:
                await self.engine.expire_false_breakout_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("False Breakout PENDING expiry check failed")
            await asyncio.sleep(1.0)

    async def _stop_false_breakout_timeout_service(self) -> None:
        task = self._false_breakout_timeout_task
        self._false_breakout_timeout_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def start_cancelled_trade_cleanup(self) -> None:
        """Start the single app-lifetime cleanup service if it is not running."""
        if self._cancelled_cleanup_task is not None and not self._cancelled_cleanup_task.done():
            return
        self._cancelled_cleanup_task = asyncio.create_task(
            self._cancelled_trade_cleanup_loop(),
            name="cancelled-trade-cleanup",
        )

    async def _cancelled_trade_cleanup_loop(self) -> None:
        while True:
            started_at = asyncio.get_running_loop().time()
            await self._cleanup_cancelled_once()
            elapsed = asyncio.get_running_loop().time() - started_at
            await asyncio.sleep(max(0.0, CANCELLED_TRADE_CLEANUP_INTERVAL_SECONDS - elapsed))

    async def _cleanup_cancelled_once(self) -> int | None:
        try:
            removed = await self.engine.cleanup_cancelled_trades()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.engine.record_database_error(error)
            logger.exception(
                "Cancelled-plan cleanup failed at UTC %s (%s)",
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                type(error).__name__,
            )
            return None
        logger.info(
            "Cancelled-plan cleanup completed at %s; deleted=%d",
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            removed,
        )
        return removed

    async def _stop_cancelled_trade_cleanup(self) -> None:
        task = self._cancelled_cleanup_task
        self._cancelled_cleanup_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

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
                # Keep persisted plans across process restart, but require a
                # fresh safe-side observation and a new Entry crossing. The
                # planner will reconcile each plan against the first
                # synchronized wall snapshot for its symbol.
                for symbol in self.engine.pair_settings:
                    self.engine.trade_manager.disarm_pending_for_symbol(symbol)
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
                leverage=self.engine.trading_settings.leverage,
                default_take_profit_bps=self.engine.trading_settings.default_take_profit_bps,
                default_stop_loss_bps=self.engine.trading_settings.default_stop_loss_bps,
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
                leverage=self.engine.trading_settings.leverage,
                default_take_profit_bps=self.engine.trading_settings.default_take_profit_bps,
                default_stop_loss_bps=self.engine.trading_settings.default_stop_loss_bps,
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
            previous_settings = self.engine.pair_settings.get(symbol)
            events = []
            if not settings.enabled:
                events = self.engine.trade_manager.cancel_pending_for_symbol(
                    symbol, "PAIR_DISABLED"
                )
            elif previous_settings is not None and (
                effective_trade_level_bps(previous_settings, self.engine.trading_settings)
                != effective_trade_level_bps(settings, self.engine.trading_settings)
                or any(
                    effective_strategy_trade_level_bps(
                        previous_settings, self.engine.trading_settings, strategy_id
                    ) != effective_strategy_trade_level_bps(
                        settings, self.engine.trading_settings, strategy_id
                    )
                    for strategy_id in ("bounce", "breakout")
                )
            ):
                self.engine.pair_settings[symbol] = settings
                self.engine.trade_manager.set_pair_settings_for_price_check(settings)
                events = self.engine.replan_after_level_settings_change(symbol)
            if not await self.engine.persist_pair_settings(settings, events):
                if previous_settings is not None:
                    self.engine.pair_settings[symbol] = previous_settings
                    self.engine.trade_manager.set_pair_settings_for_price_check(
                        previous_settings
                    )
                self.engine.restore_trade_state(
                    previous_trades,
                    previous_balance,
                    previous_pending_persistence,
                )
                raise RuntimeError("Could not persist pair settings and trade state")
            self.engine.pair_settings[symbol] = settings
            self.engine.trade_manager.set_pair_settings_for_price_check(settings)
            await self._sync_workers()

    async def update_trade_level_defaults(
        self,
        *,
        take_profit_bps: float | None = None,
        stop_loss_bps: float | None = None,
    ) -> TradingSettings:
        """Save global bps defaults and atomically re-plan affected PENDINGs."""
        async with self._lifecycle_lock, self.engine.state_lock:
            previous_settings = self.engine.trading_settings
            updated = replace(
                previous_settings,
                default_take_profit_bps=(
                    previous_settings.default_take_profit_bps
                    if take_profit_bps is None else take_profit_bps
                ),
                default_stop_loss_bps=(
                    previous_settings.default_stop_loss_bps
                    if stop_loss_bps is None else stop_loss_bps
                ),
            )
            if updated == previous_settings:
                return updated
            previous_trades = [
                dict(trade) for trade in self.engine.trade_manager.trades
            ]
            previous_balance = self.engine.trade_manager.balance_usdt
            previous_pending_persistence = {
                key: dict(value)
                for key, value in self.engine._pending_persistence.items()
            }
            self.engine.trading_settings = updated
            self.engine.trade_manager.trading_settings = updated
            events = []
            for symbol in self.engine.pair_settings:
                events.extend(
                    self.engine.replan_after_level_settings_change(symbol)
                )
            if not await self.engine.persist_trading_settings(updated, events):
                self.engine.trading_settings = previous_settings
                self.engine.trade_manager.trading_settings = previous_settings
                self.engine.restore_trade_state(
                    previous_trades,
                    previous_balance,
                    previous_pending_persistence,
                )
                raise RuntimeError("Could not persist global TP/SL settings and trade state")
            return updated

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

    async def close_virtual_trade(self, signal_id: str) -> dict:
        return await self.engine.close_virtual_trade(signal_id)

    async def cancel_pending_trade(self, signal_id: str) -> dict:
        return await self.engine.cancel_pending_trade(signal_id)

    async def update_open_trade_levels(
        self, signal_id: str, *, take_profit: float, stop_loss: float
    ) -> dict:
        return await self.engine.update_open_trade_levels(
            signal_id, take_profit=take_profit, stop_loss=stop_loss
        )

    async def update_open_trade_level(
        self, signal_id: str, *, field: str, value: float
    ) -> dict:
        return await self.engine.update_open_trade_level(
            signal_id, field=field, value=value
        )

    async def shutdown(self) -> None:
        """Stop feeds and persist state without closing virtual trades."""
        await self._stop_cancelled_trade_cleanup()
        await self._stop_false_breakout_timeout_service()
        async with self._lifecycle_lock, self.engine.state_lock:
            await self.engine._stop_workers_locked()
            if self.engine._pending_persistence and not await self.engine.persist_events([]):
                raise RuntimeError("Could not flush pending trade state during shutdown")
            await self.engine.persist_control_state()
