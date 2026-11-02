import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from math import isfinite
from time import monotonic, time

from .settings import PairSettings, TradingSettings
from .execution import ExecutionBackend
from .strategies import (
    DEFAULT_STRATEGY_ID,
    StrategyContext,
    StrategyRegistry,
    build_strategy_registry,
    _wall_id,
)
from .trade_manager import TradeManager
from .trade_levels import calculate_trade_levels, effective_trade_level_bps

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
        diagnostic_only: bool = False,
        diagnostic_symbols: set[str] | None = None,
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
        self.instrument_tick_sizes: dict[str, float] = {}
        self.repository = repository
        self.diagnostic_only = diagnostic_only
        self.diagnostic_symbols = {
            symbol.upper() for symbol in (diagnostic_symbols or set())
        }
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
                "price_value": None,
                "price_source": None,
                "price_connection_generation": None,
                "active_connection_generation": None,
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
        self.latest_stable_walls: dict[str, tuple[dict, ...]] = {}
        self.latest_wall_events: dict[str, tuple[dict, ...]] = {}
        self.latest_market_prices: dict[str, float] = {}
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
        self.wall_diagnostics = {
            symbol: {
                "feed_state": "stopped",
                "book_synchronized": False,
                "last_update_at": None,
                "price": None,
                "bid": [],
                "ask": [],
                "stable_count": 0,
                "rejected": {},
                "reason": "Ожидание синхронизированного стакана.",
                "planner_decision": "нет данных",
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
        self.wall_diagnostics.setdefault(symbol, {})["feed_state"] = status
        if status != "connected":
            self._market_health(symbol)["book_synchronized"] = False
            self.trade_manager.disarm_pending_for_symbol(symbol)
            self.wall_diagnostics[symbol]["book_synchronized"] = False
            self.wall_diagnostics[symbol]["reason"] = (
                "WebSocket не подключён; прежний стакан помечен недействительным."
            )
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
                "price_value": None,
                "price_source": None,
                "price_connection_generation": None,
                "active_connection_generation": None,
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
        price_value: float, source: str, connection_generation: int,
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
            price_value=float(price_value),
            price_source=source,
            price_connection_generation=connection_generation,
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
            active_connection_generation=generation,
        )

    def mark_orderbook_unsynchronized(
        self, symbol: str, generation: int | None = None
    ) -> None:
        health = self._market_health(symbol)
        health["book_synchronized"] = False
        self.trade_manager.disarm_pending_for_symbol(symbol)
        diagnostics = self.wall_diagnostics.setdefault(symbol.upper(), {})
        diagnostics["book_synchronized"] = False
        diagnostics["reason"] = "Стакан не синхронизирован; анализ стен заблокирован."
        if generation is not None:
            health["active_connection_generation"] = generation

    def record_wall_analysis(self, symbol: str, last_price: float, result: dict) -> None:
        """Expose the live scanner/tracker result to diagnostics and Telegram."""
        symbol = symbol.upper()
        tracked = result.get("tracked_walls", [])
        rejected = {
            key: value for key, value in result.get("filter_counts", {}).items()
            if key not in {"input_levels", "candidates"} and value
        }
        if not result.get("nearby_levels"):
            rejected["no_levels_within_search_distance"] = 1
        stable = result.get("stable_walls", [])
        if not result.get("nearby_levels"):
            reason = "Нет уровней в дистанции поиска."
        elif not result.get("candidates"):
            reason = "Кандидаты отклонены текущими фильтрами."
        elif not stable:
            reason = "Кандидаты ещё не набрали время устойчивости."
        else:
            reason = "Есть стабильные стены."
        self.wall_diagnostics[symbol] = {
            "feed_state": self.feed_status.get(symbol, "нет данных"),
            "book_synchronized": bool(self.get_market_data_health(symbol)["book_synchronized"]),
            "last_update_at": self._utc_now(),
            "updated_monotonic": monotonic(),
            "price": float(last_price),
            "bid": [dict(item) for item in tracked if item.get("side") == "buy"],
            "ask": [dict(item) for item in tracked if item.get("side") == "sell"],
            "stable_count": len(stable),
            "rejected": rejected,
            "reason": reason,
            "planner_decision": "обрабатывается",
        }

    def get_wall_diagnostics(self, symbol: str | None = None) -> dict:
        symbols = [symbol.upper()] if symbol else list(self.pair_settings)
        result = {}
        now = monotonic()
        for pair in symbols:
            current = self.wall_diagnostics.get(pair)
            if current is None:
                continue
            snapshot = dict(current)
            updated_monotonic = snapshot.pop("updated_monotonic", None)
            elapsed = max(0.0, now - updated_monotonic) if updated_monotonic is not None else 0.0
            for side in ("bid", "ask"):
                snapshot[side] = [
                    {
                        **wall,
                        "age_seconds": wall["age_seconds"] + elapsed,
                        "progress_percent": min(
                            100.0,
                            (wall["age_seconds"] + elapsed) / wall["stability_seconds"] * 100,
                        ),
                    }
                    for wall in snapshot.get(side, [])
                ]
            result[pair] = snapshot
        return result

    def replan_after_level_settings_change(self, symbol: str) -> list[dict]:
        """Reconcile PENDING plans against the latest safe wall snapshot.

        This reuses the registered strategies and the normal planner path with
        price-trigger processing disabled. If no current synchronized snapshot
        is available, old plans are cancelled and the next market update will
        create plans from fresh data.
        """
        symbol = symbol.upper()
        if (
            self.mode != EngineMode.RUNNING
            or symbol not in self.pair_settings
            or not self.pair_settings[symbol].enabled
            or symbol not in self.latest_stable_walls
            or symbol not in self.latest_market_prices
        ):
            return self.trade_manager.cancel_pending_for_symbol(
                symbol, "LEVEL_SETTINGS_CHANGED_WAITING_FOR_MARKET"
            )
        if self._has_market_data_metadata(symbol):
            health = self.get_market_data_health(symbol)
            if not health["fresh"]:
                return self.trade_manager.cancel_pending_for_symbol(
                    symbol, "LEVEL_SETTINGS_CHANGED_MARKET_STALE"
                )
        return self.on_market_update(
            symbol,
            self.latest_market_prices[symbol],
            self.latest_stable_walls[symbol],
            self.latest_wall_events.get(symbol, ()),
            self.latest_stable_walls[symbol],
            price_updated=False,
        )

    def _prepare_planner_wall_rows(
        self, symbol: str, stable_walls, strategy_ids: set[str], additional_walls=()
    ) -> dict[str, dict]:
        diagnostics = self.wall_diagnostics.setdefault(symbol.upper(), {})
        diagnostics["planner_strategies"] = sorted(strategy_ids)
        rows = {}
        for wall in (*stable_walls, *additional_walls):
            identity = _wall_id(symbol, wall)
            rows[identity] = {
                "symbol": symbol.upper(),
                "wall_id": identity,
                "side": wall.get("side"),
                "price": float(wall.get("price", 0)),
                "notional": float(wall.get("current_size", wall.get("initial_size", 0))),
                "age_seconds": float(wall.get("age", 0)),
                "event": wall.get("event"),
                "strategies": [],
            }
        diagnostics["planner_wall_decisions"] = list(rows.values())
        return rows

    def _record_planner_blocked(
        self, symbol: str, stable_walls, strategy_ids: set[str], reason: str
    ) -> None:
        rows = self._prepare_planner_wall_rows(symbol, stable_walls, strategy_ids)
        for row in rows.values():
            row["strategies"] = [{
                "strategy_id": strategy_id,
                "wall_passed": False,
                "evaluated": False,
                "signal_formed": False,
                "signal_accepted": False,
                "reasons": [reason],
                "conditions": [],
            } for strategy_id in sorted(strategy_ids)]
        diagnostics = self.wall_diagnostics.setdefault(symbol.upper(), {})
        diagnostics["planner_wall_decisions"] = list(rows.values())
        diagnostics["planner_decision"] = reason
        diagnostics["planner_evaluated"] = True

    def _record_strategy_traces(
        self, symbol: str, traces: list[dict], rows: dict[str, dict]
    ) -> None:
        for trace in traces:
            row = rows.get(trace.get("wall_id"))
            if row is not None:
                row["strategies"].append(dict(trace))
        self.wall_diagnostics.setdefault(symbol.upper(), {})[
            "planner_wall_decisions"
        ] = list(rows.values())

    def _record_trade_plan_result(
        self, symbol: str, wall_id: str, strategy_id: str, trade: dict | None,
        rejection_reason: str | None,
    ) -> None:
        diagnostics = self.wall_diagnostics.setdefault(symbol.upper(), {})
        for wall in diagnostics.get("planner_wall_decisions", []):
            if wall.get("wall_id") != wall_id:
                continue
            for strategy in wall.get("strategies", []):
                if strategy.get("strategy_id") != strategy_id:
                    continue
                strategy["pending_created"] = trade is not None
                if trade is not None:
                    strategy["pending_signal_id"] = trade.get("signal_id")
                    strategy["trade_planner_reason"] = "PENDING создан"
                else:
                    strategy["trade_planner_reason"] = rejection_reason or "TradeManager отклонил сигнал без причины."
                    strategy.setdefault("reasons", []).append(strategy["trade_planner_reason"])
                return

    @staticmethod
    def _summarize_strategy_rejections(wall_rows: list[dict]) -> str:
        from collections import Counter

        grouped = {}
        for wall in wall_rows:
            for strategy in wall.get("strategies", []):
                if strategy.get("signal_accepted"):
                    continue
                reasons = strategy.get("reasons") or ["Сигнал не сформирован."]
                reason = str(reasons[0])
                grouped.setdefault(strategy.get("strategy_id", "strategy"), Counter())[reason] += 1
        if not grouped:
            return "Нет стабильных стен или стратегии не оценивались."
        parts = []
        for strategy_id, reasons in grouped.items():
            detail = ", ".join(f"{reason} ({count})" for reason, count in reasons.most_common(2))
            parts.append(f"{strategy_id}: {detail}")
        return "Сигнал не сформирован · " + "; ".join(parts)

    def _record_strategy_failure(
        self, symbol: str, strategy_id: str, stable_walls, error: Exception,
    ) -> None:
        rows = self.wall_diagnostics.setdefault(symbol.upper(), {}).get(
            "planner_wall_decisions", []
        )
        for wall in stable_walls:
            identity = _wall_id(symbol, wall)
            row = next((item for item in rows if item.get("wall_id") == identity), None)
            if row is None:
                row = {
                    "symbol": symbol.upper(), "wall_id": identity,
                    "side": wall.get("side"), "price": float(wall.get("price", 0)),
                    "notional": float(wall.get("current_size", wall.get("initial_size", 0))),
                    "age_seconds": float(wall.get("age", 0)), "strategies": [],
                }
                rows.append(row)
            row["strategies"].append({
                "strategy_id": strategy_id, "wall_passed": True,
                "evaluated": False, "signal_formed": False,
                "signal_accepted": False,
                "reasons": [f"Ошибка стратегии: {type(error).__name__}."],
                "conditions": [],
            })
        diagnostics = self.wall_diagnostics.setdefault(symbol.upper(), {})
        diagnostics["planner_wall_decisions"] = rows
        diagnostics["planner_evaluated"] = True
        diagnostics["planner_decision"] = f"Ошибка стратегии {strategy_id}: {type(error).__name__}."

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
            and health["price_source"] == MARKET_PRICE_SOURCE
            and health["price_value"] is not None
            and health["price_connection_generation"] is not None
            and health["price_connection_generation"] == health["active_connection_generation"]
        )
        health["book_fresh"] = (
            book_age is not None
            and book_age <= MARKET_BOOK_STALE_AFTER_SECONDS
            and (health["book_delivery_lag_ms"] or 0) <= MARKET_BOOK_STALE_AFTER_SECONDS * 1000
        )
        health["fresh"] = (
            health["price_fresh"] and health["book_fresh"]
            and health["book_synchronized"]
            and health["connection_generation"] == health["active_connection_generation"]
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
        stale = []
        for symbol in sorted(open_symbols):
            health = self.get_market_data_health(symbol)
            manager_price = self.trade_manager.last_prices.get(symbol)
            if (
                not self._has_market_data_metadata(symbol)
                or not health["price_fresh"]
                or manager_price is None
                or health.get("price_value") is None
                or Decimal(str(manager_price)) != Decimal(str(health["price_value"]))
            ):
                stale.append(symbol)
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
                "entry=%s trigger=%s fill=%s close_price=%s sl=%s tp=%s source=%s "
                "event_time_ms=%s received_at=%s closed_at=%s processed_at=%s price_age=%s "
                "book_age=%s synced=%s generation=%s mode=%s reason=%s",
                symbol, event_type, trade.get("id"),
                trade.get("signal_id"), trade.get("strategy_id"),
                trade.get("side"), trade.get("wall_side"), trade.get("wall_price"),
                trade.get("entry_price"), event.get("trigger_price"),
                fill_price,
                trade.get("exit_price") if event_type == "TRADE_CLOSED" else None,
                trade.get("stop_loss"), trade.get("take_profit"),
                price_source or "n/a",
                health.get("price_event_time_ms"), health.get("price_received_at"),
                trade.get("closed_at") if event_type == "TRADE_CLOSED" else None,
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

    async def persist_trading_settings(
        self, settings: TradingSettings, events: list[dict]
    ) -> bool:
        """Atomically persist global settings and their PENDING replacements."""
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
                await self.repository.save_trading_settings_with_trades(
                    settings, snapshot, self.trade_manager.balance_usdt,
                    self.mode.value,
                )
            except Exception as error:
                self.record_database_error(error)
                logger.exception("Database global trade-level settings update failed")
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
            previous_trades = [
                dict(trade) for trade in self.trade_manager.trades
            ]
            previous_balance = self.trade_manager.balance_usdt
            previous_prices = dict(self.trade_manager.last_prices)
            previous_pending_persistence = {
                key: dict(value) for key, value in self._pending_persistence.items()
            }
            events = self.on_market_update(
                symbol, last_price, stable_walls, wall_events, wall_candidates,
                price_updated=price_updated,
            )
            now = self._utc_now()
            self.last_market_update_at[symbol] = now
            self.last_algorithm_update_at[symbol] = now
            self._record_market_analysis(symbol, stable_walls, wall_candidates)
            diagnostics = self.wall_diagnostics.get(symbol)
            if diagnostics is not None and not self.diagnostic_only:
                created = [event for event in events if event.get("event") == "TRADE_CREATED"]
                diagnostics["planner_decision"] = (
                    f"PENDING создан: {created[0]['trade']['signal_id']}"
                    if created else diagnostics.get("planner_decision")
                    if diagnostics.get("planner_evaluated")
                    else self.get_entry_block_reason(symbol)
                )
            if not await self.persist_events(events):
                # Do not leave a replacement/open/close visible in memory when
                # its corresponding PostgreSQL transaction failed. The next
                # market update will derive the desired transition again.
                self.restore_trade_state(
                    previous_trades, previous_balance,
                    previous_pending_persistence,
                )
                self.trade_manager.last_prices = previous_prices
                self.trade_manager.discard_deferred_entry_tick(symbol)
                if diagnostics is not None:
                    diagnostics["planner_decision"] = (
                        "Сохранение торгового перехода в PostgreSQL не выполнено; "
                        "изменение сделки и баланса отменено в памяти."
                    )
                return []
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
            self._record_planner_blocked(
                symbol, stable_walls, set(), "Движок остановлен; стратегии не запускались."
            )
            return []
        try:
            numeric_price = float(last_price)
            decimal_price = Decimal(str(last_price))
        except (TypeError, ValueError, InvalidOperation):
            logger.warning("%s ignoring invalid market price %r", symbol, last_price)
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol),
                "Некорректная цена; стратегии не запускались.",
            )
            return []
        if not isfinite(numeric_price) or decimal_price <= 0:
            logger.warning("%s ignoring invalid market price %r", symbol, last_price)
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol),
                "Некорректная цена; стратегии не запускались.",
            )
            return []
        self.latest_stable_walls[symbol] = tuple(dict(wall) for wall in stable_walls)
        self.latest_wall_events[symbol] = tuple(dict(event) for event in wall_events)
        self.latest_market_prices[symbol] = numeric_price

        if self.diagnostic_only:
            diagnostics = self.wall_diagnostics.setdefault(symbol, {})
            settings = self.pair_settings.get(symbol)
            if settings is None:
                self._record_planner_blocked(symbol, stable_walls, set(), "Пара не настроена.")
                return []
            if not settings.enabled:
                self._record_planner_blocked(symbol, stable_walls, set(), "Пара выключена; стратегии не оценивались.")
                return []
            strategy_ids = self.get_enabled_strategy_ids(symbol)
            if not strategy_ids:
                self._record_planner_blocked(symbol, stable_walls, set(), "Для пары не выбраны стратегии.")
                return []
            consumed_walls = [event for event in wall_events if event.get("event") == "CONSUMED"]
            rows = self._prepare_planner_wall_rows(
                symbol, stable_walls, strategy_ids, consumed_walls
            )
            context = StrategyContext(
                symbol=symbol,
                last_price=last_price,
                stable_walls=tuple(stable_walls),
                wall_events=tuple(wall_events),
                settings=settings,
                now=monotonic(),
                price_updated=price_updated,
            )
            planned = 0
            for strategy in self.strategy_registry.get_enabled(strategy_ids):
                try:
                    evaluate_with_diagnostics = getattr(strategy, "evaluate_with_diagnostics", None)
                    if callable(evaluate_with_diagnostics):
                        signals, traces = evaluate_with_diagnostics(context)
                    else:
                        signals = strategy.evaluate(context)
                        traces = [{
                            "strategy_id": strategy.strategy_id,
                            "wall_id": _wall_id(symbol, wall),
                            "side": wall["side"], "price": float(wall["price"]),
                            "wall_passed": True, "evaluated": True,
                            "signal_formed": False,
                            "reasons": ["Стратегия не предоставляет покомпонентную диагностику."],
                            "conditions": [],
                        } for wall in stable_walls]
                    planned += len(signals)
                    self._record_strategy_traces(symbol, traces, rows)
                except Exception as error:
                    self._record_strategy_failure(symbol, strategy.strategy_id, stable_walls, error)
                    continue
            else:
                diagnostics["planner_decision"] = (
                    f"Стратегии сформировали бы {planned} сигнал(ов); "
                    "создание сделок отключено в диагностическом режиме."
                    if planned else
                    self._summarize_strategy_rejections(
                        diagnostics.get("planner_wall_decisions", [])
                    )
                )
                diagnostics["planner_evaluated"] = True
            return []

        events = []
        settings = self.pair_settings.get(symbol)
        health_known = self._has_market_data_metadata(symbol)
        health = self.get_market_data_health(symbol) if health_known else None
        health_matches_price = False
        if health is not None and health.get("price_value") is not None:
            try:
                health_matches_price = (
                    decimal_price == Decimal(str(health["price_value"]))
                )
            except (InvalidOperation, TypeError, ValueError):
                health_matches_price = False
        confirmed_price_update = bool(
            price_updated and health is not None
            and health["price_fresh"] and health_matches_price
        )
        if health is not None and not health["fresh"]:
            # A gap in either the price or synchronized book stream breaks the
            # continuous crossing observation. Require a fresh safe-side tick
            # before a later Entry crossing can activate a pending plan.
            self.trade_manager.disarm_pending_for_symbol(symbol)
            self.trade_manager.discard_deferred_entry_tick(symbol)
        # Direct strategy unit callers may supply a synthetic price without
        # feed metadata. Such calls can exercise strategy logic, but never
        # manage or close a live position.
        strategy_price_valid = confirmed_price_update or (
            price_updated and health is None
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
        if settings is not None:
            self.trade_manager.set_pair_settings_for_price_check(settings)
        eligible_pending_ids = {
            trade["signal_id"] for trade in self.trade_manager.trades
            if trade["symbol"] == symbol and trade["status"] == "pending"
        }
        if confirmed_price_update:
            for trade in self.trade_manager.trades:
                if trade["symbol"] == symbol and trade["status"] in {"pending", "open"}:
                    logger.debug(
                        "trade monitor check trade_id=%s symbol=%s side=%s market_price=%s "
                        "tp=%s sl=%s event_ts=%s age=%.3fs source=%s result=checking",
                        trade.get("id", trade.get("signal_id")), symbol, trade["side"],
                        last_price, trade["take_profit"], trade["stop_loss"],
                        health.get("price_event_time_ms"), health["price_age_seconds"],
                        health.get("price_source"),
                    )
            events.extend(self.trade_manager.on_price(
                symbol, last_price, process_entries=False
            ))
        elif price_updated and any(
            trade["symbol"] == symbol and trade["status"] == "open"
            for trade in self.trade_manager.trades
        ):
            now = monotonic()
            warning_key = f"{symbol}:open-trade-price-rejected"
            last_warn = self._stale_data_warning_at.get(warning_key, 0.0)
            if now - last_warn >= 30:
                reason = (
                    "missing market metadata" if health is None else
                    "input price does not match the confirmed ticker" if not health_matches_price else
                    "stale price or old WebSocket generation"
                )
                logger.warning(
                    "%s open trade check skipped: %s; event_ts=%s age=%s source=%s "
                    "price=%s confirmed_price=%s generation=%s/%s",
                    symbol, reason,
                    health.get("price_event_time_ms") if health else None,
                    health.get("price_age_seconds") if health else None,
                    health.get("price_source") if health else None,
                    last_price,
                    health.get("price_value") if health else None,
                    health.get("price_connection_generation") if health else None,
                    health.get("active_connection_generation") if health else None,
                )
                self._stale_data_warning_at[warning_key] = now
        if settings is None or not settings.enabled or self.mode != EngineMode.RUNNING:
            reason = (
                "Пара не настроена." if settings is None else
                "Пара выключена." if not settings.enabled else
                "Режим движка не RUNNING; новые сигналы не передаются TradeManager."
            )
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol), reason
            )
            self.trade_manager.discard_deferred_entry_tick(symbol)
            return events
        if self.repository is not None and self._pending_persistence:
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol),
                f"Trade Planner приостановлен: в PostgreSQL ожидают сохранения {len(self._pending_persistence)} записи.",
            )
            self.trade_manager.discard_deferred_entry_tick(symbol)
            return events
        if (health is not None and not health["fresh"]) or (
            health is None and not strategy_price_valid and price_updated
        ):
            reason = (
                self.get_entry_block_reason(symbol)
                if health is not None and not health["fresh"] else
                "Рыночные данные не подтверждены; стратегии не запускались."
            )
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
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol), reason
            )
            self.trade_manager.discard_deferred_entry_tick(symbol)
            return events
        if not self._entry_event_is_after_resume(symbol):
            health = self._market_health(symbol)
            logger.info(
                "%s ignored pre-resume entry event: trade_id=%s event_time=%s cutoff=%s",
                symbol, health["price_trade_id"], health["price_event_time_ms"],
                self.entry_event_cutoff_ms.get(symbol),
            )
            self._record_planner_blocked(
                symbol, stable_walls, self.get_enabled_strategy_ids(symbol),
                "Ценовое событие предшествует возобновлению входов.",
            )
            self.trade_manager.discard_deferred_entry_tick(symbol)
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
        consumed_walls = [event for event in wall_events if event.get("event") == "CONSUMED"]
        rows = self._prepare_planner_wall_rows(
            symbol, stable_walls, strategy_ids, consumed_walls
        )
        signals_found = 0
        trades_created = 0
        trade_rejections = []
        for strategy in self.strategy_registry.get_enabled(strategy_ids):
            try:
                evaluate_with_diagnostics = getattr(strategy, "evaluate_with_diagnostics", None)
                if callable(evaluate_with_diagnostics):
                    signals, traces = evaluate_with_diagnostics(context)
                else:
                    signals = strategy.evaluate(context)
                    traces = [{
                        "strategy_id": strategy.strategy_id,
                        "wall_id": _wall_id(symbol, wall),
                        "side": wall["side"], "price": float(wall["price"]),
                        "wall_passed": True, "evaluated": True,
                        "signal_formed": False, "signal_accepted": False,
                        "reasons": ["Стратегия не предоставляет покомпонентную диагностику."],
                        "conditions": [],
                    } for wall in stable_walls]
                signals_found += len(signals)
                self._record_strategy_traces(symbol, traces, rows)
            except Exception as error:
                self.strategy_errors.setdefault(symbol, {})[
                    strategy.strategy_id
                ] = type(error).__name__
                print(
                    f"Strategy {strategy.strategy_id} failed for {symbol}: "
                    f"{type(error).__name__}"
                )
                self._record_strategy_failure(symbol, strategy.strategy_id, stable_walls, error)
                continue
            symbol_errors = self.strategy_errors.get(symbol)
            if symbol_errors is not None:
                symbol_errors.pop(strategy.strategy_id, None)
                if not symbol_errors:
                    self.strategy_errors.pop(symbol, None)
            desired_slots = {"buy": None, "sell": None}
            for signal in signals:
                desired_slots[signal.wall_side] = signal
            tick_size = self.instrument_tick_sizes.get(symbol)
            for wall_side in ("buy", "sell"):
                signal = desired_slots[wall_side]
                existing = self.trade_manager.get_pending_for_slot(
                    symbol, strategy.strategy_id, wall_side
                )
                if signal is None:
                    if existing is not None:
                        reason = "WALL_NO_LONGER_QUALIFIES" if stable_walls else "NO_STABLE_WALL"
                        events.extend(self.trade_manager.cancel_pending_for_slot(
                            symbol, strategy.strategy_id, wall_side, reason
                        ))
                    continue
                requires_bybit_tick = strategy.strategy_id in {"bounce", "breakout"}
                if requires_bybit_tick and (tick_size is None or tick_size <= 0):
                    trade_rejections.append("Не получен tickSize Bybit; уровни плана не рассчитаны.")
                    if existing is not None:
                        events.extend(self.trade_manager.cancel_pending_for_slot(
                            symbol, strategy.strategy_id, wall_side, "TICK_SIZE_UNAVAILABLE"
                        ))
                    continue
                if strategy.strategy_id in {"bounce", "breakout"}:
                    take_profit_bps, stop_loss_bps = effective_trade_level_bps(
                        settings, self.trading_settings
                    )
                    try:
                        levels = calculate_trade_levels(
                            strategy_id=strategy.strategy_id,
                            wall_side=wall_side,
                            wall_price=signal.wall_price,
                            tick_size=tick_size,
                            take_profit_bps=take_profit_bps,
                            stop_loss_bps=stop_loss_bps,
                        )
                    except ValueError as error:
                        reason = f"Уровни невалидны: {error}"
                        trade_rejections.append(reason)
                        if existing is not None:
                            events.extend(self.trade_manager.cancel_pending_for_slot(
                                symbol, strategy.strategy_id, wall_side,
                                "INVALID_TRADE_LEVELS",
                            ))
                        self._record_trade_plan_result(
                            symbol, signal.wall_id, strategy.strategy_id, None, reason
                        )
                        continue
                elif all(value is not None for value in (
                    signal.entry_price, signal.stop_loss, signal.take_profit
                )):
                    levels = {
                        "side": signal.side,
                        "entry_price": signal.entry_price,
                        "stop_loss": signal.stop_loss,
                        "take_profit": signal.take_profit,
                        "entry_trigger_direction": signal.entry_trigger_direction or (
                            "cross_down" if wall_side == "buy" else "cross_up"
                        ),
                    }
                else:
                    trade_rejections.append(
                        f"Стратегия {strategy.strategy_id} не предоставила готовые уровни сделки."
                    )
                    continue
                if self.trade_manager.has_matching_pending_plan(signal, levels, settings):
                    self._record_trade_plan_result(
                        symbol, signal.wall_id, strategy.strategy_id, existing, None
                    )
                    continue
                if existing is not None:
                    events.extend(self.trade_manager.cancel_pending_for_slot(
                        symbol, strategy.strategy_id, wall_side, "PLAN_REPLACED"
                    ))
                from uuid import uuid4
                payload = {
                    "strategy_id": signal.strategy_id,
                    "symbol": signal.symbol,
                    "wall_side": signal.wall_side,
                    "wall_price": signal.wall_price,
                    "wall_id": signal.wall_id,
                    "wall_size": signal.wall_size,
                    "wall_ratio": signal.wall_ratio,
                    **levels,
                    "entry_mode": signal.entry_mode or "cross",
                    "entry_trigger_direction": levels["entry_trigger_direction"],
                    "reference_price": last_price,
                    "signal_id": f"plan:{uuid4().hex}",
                }
                try:
                    trade = self.trade_manager.create_trade(
                        payload, settings, stake_usdt=self.trading_settings.stake_usdt
                    )
                except (ValueError, ArithmeticError) as error:
                    trade = None
                    self.trade_manager.last_create_rejection_reason = (
                        f"Некорректные уровни плана: {error}"
                    )
                rejection_reason = getattr(self.trade_manager, "last_create_rejection_reason", None)
                self._record_trade_plan_result(
                    symbol, signal.wall_id, strategy.strategy_id, trade, rejection_reason
                )
                if trade is not None:
                    trades_created += 1
                    events.append({"event": "TRADE_CREATED", "trade": trade})
                    logger.info(
                        "%s PENDING created strategy=%s wall=%s@%s side=%s entry=%s sl=%s tp=%s trigger=%s tick=%s",
                        symbol, strategy.strategy_id, wall_side, signal.wall_price,
                        trade["side"], trade["entry_price"], trade["stop_loss"],
                        trade["take_profit"], trade["entry_trigger_direction"], tick_size,
                    )
                else:
                    trade_rejections.append(rejection_reason or "TradeManager отклонил план без причины.")
        if confirmed_price_update:
            events.extend(self.trade_manager.on_price(
                symbol, last_price, process_open=False,
                eligible_signal_ids=eligible_pending_ids,
            ))
        diagnostics = self.wall_diagnostics.setdefault(symbol, {})
        diagnostics["planner_evaluated"] = True
        if trade_rejections:
            diagnostics["planner_decision"] = (
                f"Сигнал(ы) стратегии сформированы, но TradeManager отказал: "
                + "; ".join(dict.fromkeys(trade_rejections))
            )
        elif signals_found:
            diagnostics["planner_decision"] = (
                f"Стратегии сформировали {signals_found} сигнал(ов); "
                f"TradeManager создал PENDING: {trades_created}."
            )
        elif not strategy_ids:
            diagnostics["planner_decision"] = "Для пары не выбраны стратегии."
        else:
            diagnostics["planner_decision"] = self._summarize_strategy_rejections(
                diagnostics.get("planner_wall_decisions", [])
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
            "wall_diagnostics": self.get_wall_diagnostics(),
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
        if self.diagnostic_only:
            return symbol in self.diagnostic_symbols
        return self.trade_manager.has_active_symbol(symbol) or (
            settings.enabled and bool(self.get_enabled_strategy_ids(symbol))
        )
