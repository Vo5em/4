import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from time import monotonic, time

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

# Bybit linear tickers publish at 100 ms and full-depth orderbooks at 200 ms.
# Ten missed ticker messages / five missed book messages form a 1 s safety
# bound; this is a freshness contract, not a claim that network delivery is real-time.
BYBIT_LINEAR_TICKER_PUSH_INTERVAL_SECONDS = 0.1
BYBIT_FULL_ORDERBOOK_PUSH_INTERVAL_SECONDS = 0.2
MARKET_PRICE_STALE_AFTER_SECONDS = 10 * BYBIT_LINEAR_TICKER_PUSH_INTERVAL_SECONDS
MARKET_BOOK_STALE_AFTER_SECONDS = 5 * BYBIT_FULL_ORDERBOOK_PUSH_INTERVAL_SECONDS
MARKET_PRICE_SOURCE = "Bybit V5 linear ticker lastPrice"

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
        self.market_data_health: dict[str, dict] = {
            symbol: {
                "price_received_monotonic": None,
                "price_received_at": None,
                "price_event_time_ms": None,
                "price_value_updated_at_ms": None,
                "price_value_updated_monotonic": None,
                "price_message_time_ms": None,
                "price_sequence": None,
                "price_trade_id": None,
                "price_delivery_lag_ms": None,
                "book_received_monotonic": None,
                "book_received_at": None,
                "book_message_time_ms": None,
                "book_cts_ms": None,
                "book_sequence": None,
                "book_update_id": None,
                "book_delivery_lag_ms": None,
                "book_synchronized": False,
                "connection_generation": None,
            }
            for symbol in self.pair_settings
        }
        self.entry_event_cutoff_ms: dict[str, int] = {}
        self.entry_event_receive_cutoff: dict[str, float] = {}
        self.market_reconciliation_required: set[str] = set()
        self._stale_data_warning_at: dict[str, float] = {}
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
        self.reset_all_strategy_state()

    def resume_new_entries(self) -> None:
        if self.mode != EngineMode.ENTRIES_PAUSED:
            raise RuntimeError("New entries are not paused")
        self.mode = EngineMode.RUNNING
        local_cutoff_ms = int(time() * 1000)
        receive_cutoff = monotonic()
        clock_offsets = getattr(self, "exchange_clock_offsets_ms", {})
        for symbol in self.pair_settings:
            offset = clock_offsets.get((symbol.upper(), "tickers"))
            self.entry_event_cutoff_ms[symbol] = (
                local_cutoff_ms - int(offset) if offset is not None else local_cutoff_ms
            )
            self.entry_event_receive_cutoff[symbol] = receive_cutoff
            self.market_reconciliation_required.add(symbol)
        self.reset_all_strategy_state()

    def reset_all_strategy_state(self) -> None:
        for symbol in self.pair_settings:
            self.reset_strategy_state(symbol)

    def _market_health(self, symbol: str) -> dict:
        return self.market_data_health.setdefault(
            symbol.upper(),
            {
                "price_received_monotonic": None,
                "price_received_at": None,
                "price_event_time_ms": None,
                "price_value_updated_at_ms": None,
                "price_value_updated_monotonic": None,
                "price_message_time_ms": None,
                "price_sequence": None,
                "price_trade_id": None,
                "price_delivery_lag_ms": None,
                "book_received_monotonic": None,
                "book_received_at": None,
                "book_message_time_ms": None,
                "book_cts_ms": None,
                "book_sequence": None,
                "book_update_id": None,
                "book_delivery_lag_ms": None,
                "book_synchronized": False,
                "connection_generation": None,
            },
        )

    def record_price_market_data(
        self, symbol: str, *, event_time_ms: int, message_time_ms: int,
        sequence: int | None, trade_id: str, received_monotonic: float,
        received_at: datetime, delivery_lag_ms: float,
        value_updated_at_ms: int | None = None,
        value_updated_monotonic: float | None = None,
    ) -> None:
        health = self._market_health(symbol)
        health.update(
            price_received_monotonic=received_monotonic,
            price_received_at=received_at,
            price_event_time_ms=event_time_ms,
            price_message_time_ms=message_time_ms,
            price_sequence=sequence,
            price_trade_id=trade_id,
            price_delivery_lag_ms=max(0.0, float(delivery_lag_ms)),
        )
        if value_updated_at_ms is not None:
            health["price_value_updated_at_ms"] = int(value_updated_at_ms)
            health["price_value_updated_monotonic"] = value_updated_monotonic

    def record_orderbook_market_data(
        self, symbol: str, *, message_time_ms: int, cts_ms: int | None,
        sequence: int, update_id: int, received_monotonic: float,
        received_at: datetime, delivery_lag_ms: float, synchronized: bool,
        generation: int,
    ) -> None:
        health = self._market_health(symbol)
        health.update(
            book_received_monotonic=received_monotonic,
            book_received_at=received_at,
            book_message_time_ms=message_time_ms,
            book_cts_ms=cts_ms,
            book_sequence=sequence,
            book_update_id=update_id,
            book_delivery_lag_ms=max(0.0, float(delivery_lag_ms)),
            book_synchronized=bool(synchronized),
            connection_generation=generation,
        )

    def mark_orderbook_unsynchronized(self, symbol: str) -> None:
        self._market_health(symbol)["book_synchronized"] = False

    def get_market_data_health(self, symbol: str) -> dict:
        health = dict(self._market_health(symbol))
        now_mono = monotonic()
        now_ms = int(time() * 1000)
        price_received = health["price_received_monotonic"]
        book_received = health["book_received_monotonic"]
        price_age = (
            max(0.0, now_mono - price_received)
            + max(0, health["price_message_time_ms"] - health["price_event_time_ms"]) / 1000
            if price_received is not None else None
        )
        book_age = (
            max(0.0, now_mono - book_received)
            + max(0, health["book_message_time_ms"] - health["book_cts_ms"]) / 1000
            if book_received is not None and health["book_cts_ms"] is not None else
            max(0.0, now_mono - book_received) if book_received is not None else None
        )
        health["price_age_seconds"] = price_age
        health["book_age_seconds"] = book_age
        health["price_fresh"] = (
            price_age is not None
            and price_age <= MARKET_PRICE_STALE_AFTER_SECONDS
            and (health["price_delivery_lag_ms"] or 0) <= MARKET_PRICE_STALE_AFTER_SECONDS * 1000
        )
        health["book_fresh"] = (
            book_age is not None
            and book_age <= MARKET_BOOK_STALE_AFTER_SECONDS
            and (health["book_delivery_lag_ms"] or 0) <= MARKET_BOOK_STALE_AFTER_SECONDS * 1000
        )
        health["fresh"] = (
            health["price_fresh"] and health["book_fresh"]
            and health["book_synchronized"]
        )
        health["exchange_price_age_seconds"] = (
            max(0.0, now_ms - health["price_event_time_ms"]) / 1000
            if health["price_event_time_ms"] is not None else None
        )
        health["price_value_age_seconds"] = (
            max(0.0, now_mono - health["price_value_updated_monotonic"])
            if health["price_value_updated_monotonic"] is not None else None
        )
        return health

    def _has_market_data_metadata(self, symbol: str) -> bool:
        health = self._market_health(symbol)
        return (
            health["price_received_monotonic"] is not None
            or health["book_received_monotonic"] is not None
        )

    def _entry_event_is_after_resume(self, symbol: str) -> bool:
        cutoff = self.entry_event_cutoff_ms.get(symbol)
        if cutoff is None:
            return True
        health = self._market_health(symbol)
        event_time = health["price_event_time_ms"]
        received = health["price_received_monotonic"]
        receive_cutoff = self.entry_event_receive_cutoff.get(symbol)
        return (
            event_time is not None and event_time > cutoff
            and received is not None
            and (receive_cutoff is None or received > receive_cutoff)
        )

    async def stop_all(self) -> list[dict]:
        async with self.state_lock:
            return await self._stop_all_locked()

    async def close_open_trades(self) -> list[dict]:
        """Close active positions while leaving the engine and pending entries running."""
        async with self.state_lock:
            self._require_fresh_exit_prices()
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
        self._require_fresh_exit_prices()
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

    def _require_fresh_exit_prices(self) -> None:
        open_symbols = {trade["symbol"] for trade in self.trade_manager.get_open_trades()}
        stale = [
            symbol for symbol in sorted(open_symbols)
            if not self._has_market_data_metadata(symbol)
            or not self.get_market_data_health(symbol)["price_fresh"]
        ]
        if stale:
            raise RuntimeError(
                "Cannot close virtual positions using stale market prices for: "
                + ", ".join(stale)
            )

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
                self._log_trade_lifecycle_events(events)
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
            self._log_trade_lifecycle_events(events)
            for signal_id, trade_ref in snapshot_refs.items():
                if self._pending_persistence.get(signal_id) is trade_ref:
                    self._pending_persistence.pop(signal_id, None)
            return True

    def _log_trade_lifecycle_events(self, events: list[dict]) -> None:
        for event in events:
            trade = event.get("trade")
            if not trade or event.get("event") == "TRADE_CREATED":
                continue
            symbol = str(trade.get("symbol", "")).upper()
            health = self.get_market_data_health(symbol)
            event_type = event.get("event")
            fill_price = event.get("fill_price")
            if fill_price is None and event_type == "TRADE_CLOSED":
                fill_price = trade.get("exit_price")
            price_source = event.get("price_source")
            if price_source is None and event_type in {"ENTRY_ACTIVATED", "TRADE_CLOSED"}:
                price_source = MARKET_PRICE_SOURCE
            logger.info(
                "%s %s trade_id=%s signal_id=%s strategy=%s side=%s wall=%s@%s "
                "entry=%s trigger=%s fill=%s sl=%s tp=%s source=%s "
                "event_time_ms=%s received_at=%s processed_at=%s price_age=%s "
                "book_age=%s synced=%s generation=%s mode=%s reason=%s",
                symbol, event_type, trade.get("id"),
                trade.get("signal_id"), trade.get("strategy_id"),
                trade.get("side"), trade.get("wall_side"), trade.get("wall_price"),
                trade.get("entry_price"), event.get("trigger_price"),
                fill_price,
                trade.get("stop_loss"), trade.get("take_profit"),
                price_source or "n/a",
                health.get("price_event_time_ms"), health.get("price_received_at"),
                self._utc_now().isoformat(timespec="milliseconds"),
                health.get("price_age_seconds"), health.get("book_age_seconds"),
                health.get("book_synchronized"), health.get("connection_generation"),
                self.mode.value, trade.get("close_reason"),
            )

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
                self._log_trade_lifecycle_events(events)
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
            self._log_trade_lifecycle_events(events)
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
                self._log_trade_lifecycle_events(events)
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
            self._log_trade_lifecycle_events(events)
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
        health_known = self._has_market_data_metadata(symbol)
        health = self.get_market_data_health(symbol) if health_known else None
        accepted_price_update = bool(
            price_updated and (health is None or health["price_fresh"])
        )
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
        # Do not reuse the last trade price for book-only refreshes. A stop or
        # target is evaluated only once for each accepted fresh publicTrade tick.
        if accepted_price_update:
            events.extend(self.trade_manager.on_price(symbol, last_price))
        if settings is None or not settings.enabled or self.mode != EngineMode.RUNNING:
            return events
        if self.repository is not None and self._pending_persistence:
            return events
        if not accepted_price_update or (health is not None and not health["fresh"]):
            if health is not None and not health["fresh"]:
                now = monotonic()
                last_warn = self._stale_data_warning_at.get(symbol, 0.0)
                if now - last_warn >= 30:
                    logger.warning(
                        "%s refusing entries on stale/unsynchronized market data: "
                        "price_age=%s book_age=%s book_synced=%s generation=%s",
                        symbol, health["price_age_seconds"], health["book_age_seconds"],
                        health["book_synchronized"], health["connection_generation"],
                    )
                    self._stale_data_warning_at[symbol] = now
            return events
        if not self._entry_event_is_after_resume(symbol):
            health = self._market_health(symbol)
            logger.info(
                "%s ignored pre-resume entry event: trade_id=%s event_time=%s cutoff=%s",
                symbol, health["price_trade_id"], health["price_event_time_ms"],
                self.entry_event_cutoff_ms.get(symbol),
            )
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
                    health = self.get_market_data_health(symbol) if health_known else {}
                    logger.info(
                        "%s TRADE_CREATED trade_id=%s signal_id=%s strategy=%s side=%s "
                        "wall=%s@%s signal_entry=%s virtual_entry=%s sl=%s tp=%s "
                        "price_source=%s event_time_ms=%s received_at=%s processed_at=%s "
                        "price_age=%s book_age=%s book_synced=%s generation=%s mode=%s",
                        symbol, trade.get("id"), trade.get("signal_id"),
                        trade.get("strategy_id"), trade.get("side"),
                        trade.get("wall_side"), trade.get("wall_price"),
                        signal.entry_price, trade.get("entry_price"),
                        trade.get("stop_loss"), trade.get("take_profit"),
                        MARKET_PRICE_SOURCE,
                        health.get("price_event_time_ms"),
                        health.get("price_received_at"),
                        self._utc_now().isoformat(timespec="milliseconds"),
                        health.get("price_age_seconds"), health.get("book_age_seconds"),
                        health.get("book_synchronized"),
                        health.get("connection_generation"), self.mode.value,
                    )
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
            "market_data_health": {
                symbol: self.get_market_data_health(symbol)
                for symbol in self.pair_settings
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
            and (
                not self._has_market_data_metadata(symbol)
                or self.get_market_data_health(symbol)["fresh"]
            )
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
        if self._has_market_data_metadata(symbol):
            health = self.get_market_data_health(symbol)
            if not health["book_synchronized"]:
                return "Локальный стакан не синхронизирован."
            if not health["fresh"]:
                price_age = health["price_age_seconds"]
                book_age = health["book_age_seconds"]
                return (
                    "Рыночные данные устарели: цена "
                    f"{price_age:.2f} с" if price_age is not None else
                    "Рыночные данные устарели: цена нет данных"
                ) + (
                    ", стакан "
                    f"{book_age:.2f} с." if book_age is not None else
                    ", стакан нет данных."
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
