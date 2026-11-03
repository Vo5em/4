from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from math import isfinite
from threading import RLock

from .settings import PairSettings, TradingSettings


class TradeManager:
    """In-memory virtual execution and lifecycle manager; never sends orders."""

    def __init__(self, trading_settings: TradingSettings | None = None):
        self.trading_settings = trading_settings or TradingSettings()
        self.trades: list[dict] = []
        self.last_prices: dict[str, float] = {}
        self._used_signal_ids: set[str] = set()
        self._create_lock = RLock()
        self.balance_usdt = self.trading_settings.initial_balance_usdt
        self.last_create_rejection_reason: str | None = None
        self._deferred_entry_ticks: dict[str, float | None] = {}
        self._manual_cancelled_wall_blocks: dict[str, tuple[str, str, str, float]] = {}
        self._manual_block_state_initialized = False

    def restore_state(
        self,
        trades: list[dict],
        balance_usdt: float,
        last_prices: dict[str, float] | None = None,
        manual_plan_blocks: list[dict] | None = None,
    ) -> None:
        current_manual_blocks = dict(self._manual_cancelled_wall_blocks)
        self.trades = [dict(trade) for trade in trades]
        for trade in self.trades:
            if trade.get("accounting_version") is None:
                # Existing deployed rows are initialized as version 1 by the
                # additive schema migration; this also covers old in-memory
                # fixtures without changing their recorded PnL.
                trade["accounting_version"] = 1
            # Rows written before margin accounting used stake as their whole
            # notional and did not charge fees. Preserve that interpretation;
            # most importantly, leave recorded historical profit untouched.
            stake = float(trade.get("stake_usdt") or 0.0)
            notional = trade.get("position_notional_usdt")
            if notional is None:
                notional = stake
                trade["position_notional_usdt"] = notional
            if trade.get("leverage") is None:
                trade["leverage"] = notional / stake if stake > 0 else 1.0
            entry_fee = trade.get("entry_fee_usdt")
            if entry_fee is None:
                entry_fee = 0.0
            trade["entry_fee"] = trade.get("entry_fee", entry_fee) or 0.0
            trade["entry_fee_usdt"] = trade.get("entry_fee_usdt", entry_fee) or 0.0
            trade["exit_fee"] = trade.get("exit_fee", trade.get("exit_fee_usdt") or 0.0) or 0.0
            trade["gross_pnl"] = trade.get("gross_pnl", trade.get("gross_pnl_usdt"))
            trade["taker_fee_rate"] = trade.get("taker_fee_rate", trade.get("fee_rate") or 0.0) or 0.0
            if trade.get("entry_trigger_direction") not in {"cross_down", "cross_up"}:
                trade["entry_trigger_direction"] = (
                    "cross_down" if trade.get("wall_side") == "buy" else "cross_up"
                )
            if trade.get("status") == "pending":
                # A restart has no continuous price-crossing history.
                trade["entry_armed"] = False
            else:
                trade.setdefault("entry_armed", False)
            trade.setdefault("entry_fill_price", None)
            trade["entry_previous_price"] = None
        self._used_signal_ids = {trade["signal_id"] for trade in self.trades}
        cancelled_manual_trades = {
            trade["signal_id"]: (
                str(trade.get("symbol", "")).upper(),
                str(trade.get("strategy_id", "")),
                str(trade.get("wall_side", "")),
                float(trade.get("wall_price", 0)),
            )
            for trade in self.trades
            if trade.get("status") == "cancelled"
            and trade.get("close_reason") == "MANUAL_CANCELLED"
            and trade.get("wall_price") is not None
        }
        if manual_plan_blocks is not None:
            self._manual_cancelled_wall_blocks = {
                str(block["signal_id"]): (
                    str(block["symbol"]).upper(), str(block["strategy_id"]),
                    str(block["wall_side"]), float(block["wall_price"]),
                )
                for block in manual_plan_blocks
            }
        elif self._manual_block_state_initialized:
            self._manual_cancelled_wall_blocks = {
                signal_id: key for signal_id, key in current_manual_blocks.items()
                if signal_id in cancelled_manual_trades
            }
        else:
            self._manual_cancelled_wall_blocks = cancelled_manual_trades
        self._manual_block_state_initialized = True
        self.balance_usdt = float(balance_usdt)
        if last_prices is not None:
            self.last_prices = {
                symbol.upper(): float(price)
                for symbol, price in last_prices.items()
                if isfinite(float(price)) and float(price) > 0
            }

    def get_wall_lifecycle_identities(self) -> dict[tuple[str, float], str]:
        """Return durable lifecycle IDs that can be reused when a feed restarts.

        Signal IDs already persist in ``virtual_trades``. Wall lifecycle IDs
        are encoded after the strategy prefix, so no database schema change is
        needed to recognize a still-present wall after process restart.
        """
        identities: dict[tuple[str, float], str] = {}
        for trade in self.trades:
            wall_id = str(trade.get("wall_id") or self._wall_id_from_signal_id(trade))
            parts = wall_id.split(":", 3)
            if len(parts) != 4 or parts[0].upper() != str(trade.get("symbol", "")).upper():
                continue
            if parts[1] != trade.get("wall_side"):
                continue
            try:
                price = float(parts[2])
            except ValueError:
                continue
            if price != float(trade.get("wall_price", 0)) or not parts[3]:
                continue
            identities[(parts[1], price)] = parts[3]
        return identities

    def create_trade(
        self,
        signal,
        pair_settings: PairSettings,
        *,
        stake_usdt: float | None = None,
    ) -> dict | None:
        # The engine serializes market updates, but callers and future adapters
        # may invoke execution concurrently. Keep duplicate checks and insertion
        # in one critical section.
        with self._create_lock:
            self.last_create_rejection_reason = None
            return self._create_trade_unlocked(
                signal, pair_settings, stake_usdt=stake_usdt
            )

    def _create_trade_unlocked(
        self,
        signal,
        pair_settings: PairSettings,
        *,
        stake_usdt: float | None = None,
    ) -> dict | None:
        self.set_pair_settings_for_price_check(pair_settings)
        data = signal if isinstance(signal, dict) else {
            "strategy_id": signal.strategy_id,
            "symbol": signal.symbol,
            "wall_side": signal.wall_side,
            "wall_price": signal.wall_price,
            "wall_id": signal.wall_id,
            "wall_size": signal.wall_size,
            "wall_ratio": signal.wall_ratio,
            "side": signal.side,
            "entry_price": signal.entry_price,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "entry_mode": signal.entry_mode,
            "entry_trigger_direction": getattr(signal, "entry_trigger_direction", None),
            "reference_price": getattr(signal, "reference_price", None),
            "signal_id": f"{signal.strategy_id}:{signal.wall_id}",
        }
        signal_id = data.get("signal_id") or data["wall_id"]
        symbol = str(data["symbol"]).upper()
        wall_price = float(data["wall_price"])
        wall_size = float(data["wall_size"])
        wall_ratio = float(data.get("wall_ratio", 0.0))
        if not signal_id or not symbol:
            raise ValueError("signal_id and symbol must not be empty")
        if symbol != pair_settings.symbol.upper():
            raise ValueError("signal symbol does not match pair settings")
        if (
            not all(isfinite(value) for value in (wall_price, wall_size, wall_ratio))
            or wall_price <= 0
            or wall_size <= 0
            or wall_ratio < 0
        ):
            raise ValueError("wall values must be finite and within valid ranges")
        if signal_id in self._used_signal_ids:
            self.last_create_rejection_reason = "Сигнал с таким signal_id уже обрабатывался ранее."
            return None
        wall_id = str(data.get("wall_id") or signal_id)
        if any(
            str(trade.get("wall_id") or self._wall_id_from_signal_id(trade)) == wall_id
            and trade.get("status") in {"pending", "open", "closed"}
            for trade in self.trades
        ):
            self.last_create_rejection_reason = "Эта идентичность стены уже дала сделку со статусом pending/open/closed."
            return None
        if any(
            trade["symbol"] == symbol
            and trade["wall_side"] == data["wall_side"]
            and trade["wall_price"] == wall_price
            and trade["status"] in {"pending", "open"}
            for trade in self.trades
        ):
            self.last_create_rejection_reason = "Для этой пары, стороны и цены стены уже есть pending/open сделка."
            return None

        stake = float(
            self.trading_settings.stake_usdt
            if stake_usdt is None
            else stake_usdt
        )
        leverage = float(pair_settings.leverage or self.trading_settings.leverage)
        position_notional = stake * leverage
        reserved = sum(
            trade["stake_usdt"]
            for trade in self.trades
            if trade["status"] in {"pending", "open"}
        )
        available = self.balance_usdt - reserved
        if not isfinite(stake) or stake <= 0:
            self.last_create_rejection_reason = f"Некорректная ставка: {stake!r} USDT."
            return None
        if available < stake:
            self.last_create_rejection_reason = (
                f"Недостаточно свободного баланса: доступно {available:.4g} USDT, "
                f"требуется {stake:.4g} USDT маржи."
            )
            return None

        wall_side = data["wall_side"]
        if wall_side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported wall side: {wall_side}")
        trade_side = data.get("side") or ("long" if wall_side == "buy" else "short")
        entry_price = data.get("entry_price")
        stop_loss = data.get("stop_loss")
        take_profit = data.get("take_profit")
        if entry_price is None or stop_loss is None or take_profit is None:
            raise ValueError("Trade levels must be calculated before TradeManager creates a plan")
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        take_profit = float(take_profit)
        if trade_side not in {"long", "short"} or not all(
            isfinite(float(value)) and float(value) > 0
            for value in (entry_price, stop_loss, take_profit)
        ):
            raise ValueError("trade levels must be positive finite numbers")
        if (trade_side == "long" and not stop_loss < entry_price < take_profit) or (
            trade_side == "short" and not take_profit < entry_price < stop_loss
        ):
            raise ValueError("trade levels are on the wrong side of entry")
        created_at = datetime.now(timezone.utc)
        immediate = data.get("entry_mode") == "market"
        trigger_direction = data.get("entry_trigger_direction") or (
            "cross_down" if wall_side == "buy" else "cross_up"
        )
        if trigger_direction not in {"cross_down", "cross_up"}:
            raise ValueError("entry_trigger_direction must be cross_down or cross_up")
        reference_price = data.get("reference_price")
        if reference_price is None:
            reference_price = self.last_prices.get(symbol)
        armed = (
            reference_price is not None
            and ((wall_side == "buy" and float(reference_price) > entry_price)
                 or (wall_side == "sell" and float(reference_price) < entry_price))
        )
        trade = {
            "signal_id": signal_id,
            "strategy_id": data["strategy_id"],
            "symbol": symbol,
            "side": trade_side,
            "wall_side": wall_side,
            "wall_price": wall_price,
            "wall_id": wall_id,
            "wall_size": wall_size,
            "wall_ratio": wall_ratio,
            "stake_usdt": stake,
            "leverage": leverage,
            "position_notional_usdt": position_notional,
            "maker_fee_rate": pair_settings.maker_fee_rate,
            "taker_fee_rate": pair_settings.taker_fee_rate,
            "fee_rate": pair_settings.taker_fee_rate,
            "entry_fee": 0.0,
            "entry_fee_usdt": 0.0,
            "exit_fee": 0.0,
            "exit_fee_usdt": 0.0,
            "gross_pnl": 0.0,
            "gross_pnl_usdt": 0.0,
            "accounting_version": 1,
            "net_pnl": 0.0,
            "margin_return_pct": 0.0,
            "entry_price": entry_price,
            "entry_fill_price": entry_price if immediate else None,
            "entry_trigger_direction": trigger_direction,
            "entry_armed": bool(armed) if not immediate else True,
            "entry_previous_price": float(reference_price) if reference_price is not None else None,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "status": "open" if immediate else "pending",
            "created_at": created_at,
            "opened_at": created_at if immediate else None,
            "closed_at": None,
            "exit_price": None,
            "close_reason": None,
            "profit_usdt": 0.0,
            "profit_percent": 0.0,
        }
        if immediate:
            trade["entry_fee"] = position_notional * pair_settings.taker_fee_rate
            trade["entry_fee_usdt"] = trade["entry_fee"]
            self.balance_usdt -= trade["entry_fee"]
        self.trades.append(trade)
        self._used_signal_ids.add(signal_id)
        return trade

    @staticmethod
    def _wall_id_from_signal_id(trade: dict) -> str:
        signal_id = str(trade.get("signal_id", ""))
        strategy_id = str(trade.get("strategy_id", ""))
        prefix = f"{strategy_id}:"
        return signal_id[len(prefix):] if signal_id.startswith(prefix) else signal_id

    def on_price(
        self, symbol: str, price: float, *, process_entries: bool = True,
        process_open: bool = True, eligible_signal_ids: set[str] | None = None,
    ) -> list[dict]:
        with self._create_lock:
            return self._on_price_unlocked(
                symbol, price, process_entries=process_entries,
                process_open=process_open, eligible_signal_ids=eligible_signal_ids,
            )

    def _on_price_unlocked(
        self, symbol: str, price: float, *, process_entries: bool = True,
        process_open: bool = True, eligible_signal_ids: set[str] | None = None,
    ) -> list[dict]:
        try:
            decimal_price = Decimal(str(price))
        except (InvalidOperation, TypeError, ValueError):
            raise ValueError("price must be a positive finite number") from None
        if not decimal_price.is_finite() or decimal_price <= 0:
            raise ValueError("price must be a positive finite number")
        numeric_price = float(decimal_price)
        symbol = symbol.upper()
        previous = self.last_prices.get(symbol)
        if process_entries:
            previous = self._deferred_entry_ticks.pop(symbol, previous)
        self.last_prices[symbol] = numeric_price
        if not process_entries:
            self._deferred_entry_ticks[symbol] = previous
        events = []
        pending = sorted(
            (trade for trade in self.trades
             if trade["symbol"] == symbol and trade["status"] == "pending"),
            key=lambda trade: (
                trade.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
                0 if trade.get("wall_side") == "buy" else 1,
                trade.get("signal_id", ""),
            ),
        )
        for trade in pending:
            if not process_entries:
                continue
            if eligible_signal_ids is not None and trade["signal_id"] not in eligible_signal_ids:
                trade["entry_previous_price"] = numeric_price
                continue
            entry = Decimal(str(trade["entry_price"]))
            trigger_previous = previous
            if trigger_previous is None:
                trigger_previous = trade.get("entry_previous_price")
            safe_side = numeric_price > float(entry) if trade["wall_side"] == "buy" else numeric_price < float(entry)
            if not trade.get("entry_armed"):
                if safe_side:
                    trade["entry_armed"] = True
                    events.append({"event": "ENTRY_ARMED", "trade": trade})
                continue
            crossed = False
            if trigger_previous is not None:
                if trade["entry_trigger_direction"] == "cross_down":
                    crossed = trigger_previous > float(entry) and numeric_price <= float(entry)
                else:
                    crossed = trigger_previous < float(entry) and numeric_price >= float(entry)
            if not crossed:
                continue
            open_count = sum(item["status"] == "open" for item in self.trades)
            pair_open_count = sum(
                item["status"] == "open" and item["symbol"] == symbol
                for item in self.trades
            )
            pair_settings = getattr(self, "_current_pair_settings", {}).get(symbol)
            pair_limit = pair_settings.max_open_trades if pair_settings else self.trading_settings.max_open_trades
            if open_count >= self.trading_settings.max_open_trades:
                trade["entry_armed"] = False
                events.append({"event": "ENTRY_BLOCKED", "trade": trade,
                               "reason": "GLOBAL_OPEN_LIMIT"})
                continue
            if pair_open_count >= pair_limit:
                trade["entry_armed"] = False
                events.append({"event": "ENTRY_BLOCKED", "trade": trade,
                               "reason": "PAIR_OPEN_LIMIT"})
                continue
            reserved = sum(
                item["stake_usdt"] for item in self.trades
                if item["status"] in {"pending", "open"}
            )
            entry_fee = trade["position_notional_usdt"] * trade["taker_fee_rate"]
            if self.balance_usdt - reserved < entry_fee:
                trade["entry_armed"] = False
                events.append({"event": "ENTRY_BLOCKED", "trade": trade,
                               "reason": "INSUFFICIENT_BALANCE_FOR_FEE"})
                continue
            trade["status"] = "open"
            trade["opened_at"] = datetime.now(timezone.utc)
            # Last Price crossing is a virtual trigger, not a guaranteed maker
            # fill. Use a conservative taker fee and retain the simulated fill.
            trade["entry_fill_price"] = numeric_price
            trade["entry_fee"] = entry_fee
            trade["entry_fee_usdt"] = entry_fee
            self.balance_usdt -= entry_fee
            events.append({
                "event": "ENTRY_ACTIVATED", "trade": trade,
                "trigger_price": numeric_price,
                "fill_price": numeric_price,
                "price_source": "Bybit V5 linear ticker lastPrice (virtual taker-fee simulation)",
            })

        for trade in self.trades:
            if not process_open or trade["symbol"] != symbol or trade["status"] != "open":
                continue
            reason = self._exit_reason(trade, decimal_price)
            if reason is not None:
                # The ticker event that crossed the boundary is the virtual fill.
                # Persist it as close_price even when it gaps past TP/SL.
                event = self._close_trade(trade, numeric_price, reason)
                event["trigger_price"] = numeric_price
                event["close_price"] = numeric_price
                event["price_source"] = "Bybit V5 linear ticker lastPrice"
                events.append(event)
        for trade in self.trades:
            if trade["symbol"] == symbol and trade["status"] == "pending":
                trade["entry_previous_price"] = numeric_price
        return events

    def discard_deferred_entry_tick(self, symbol: str) -> None:
        self._deferred_entry_ticks.pop(symbol.upper(), None)

    def cancel_pending_for_wall(
        self,
        symbol: str,
        wall_side: str,
        wall_price: float,
        reason: str,
    ) -> list[dict]:
        events = []
        for trade in self.trades:
            if (
                trade["status"] != "pending"
                or trade["symbol"] != symbol.upper()
                or trade["wall_side"] != wall_side
                or trade["wall_price"] != float(wall_price)
            ):
                continue
            trade.update(
                status="cancelled",
                closed_at=datetime.now(timezone.utc),
                close_reason=reason,
            )
            events.append({"event": "ENTRY_CANCELLED", "trade": trade})
        return events

    def cancel_pending_for_symbol(self, symbol: str, reason: str) -> list[dict]:
        symbol = symbol.upper()
        events = []
        for trade in self.trades:
            if trade["symbol"] != symbol or trade["status"] != "pending":
                continue
            trade.update(
                status="cancelled",
                closed_at=datetime.now(timezone.utc),
                close_reason=reason,
            )
            events.append({"event": "ENTRY_CANCELLED", "trade": trade})
        return events

    def cancel_pending_for_strategy(
        self, symbol: str, strategy_id: str, reason: str
    ) -> list[dict]:
        symbol = symbol.upper()
        events = []
        for trade in self.trades:
            if (
                trade["symbol"] != symbol
                or trade["strategy_id"] != strategy_id
                or trade["status"] != "pending"
            ):
                continue
            trade.update(
                status="cancelled",
                closed_at=datetime.now(timezone.utc),
                close_reason=reason,
            )
            events.append({"event": "ENTRY_CANCELLED", "trade": trade})
        return events

    def get_pending_for_slot(self, symbol: str, strategy_id: str, wall_side: str) -> dict | None:
        return next((trade for trade in self.trades if
                     trade["symbol"] == symbol.upper()
                     and trade["strategy_id"] == strategy_id
                     and trade["wall_side"] == wall_side
                     and trade["status"] == "pending"), None)

    def find_trade(self, signal_id: str) -> dict | None:
        return next(
            (trade for trade in self.trades if trade.get("signal_id") == signal_id),
            None,
        )

    def is_wall_manually_cancelled(
        self, symbol: str, strategy_id: str, wall_id: str,
        wall_side: str | None = None, wall_price: float | None = None,
    ) -> bool:
        """Keep an explicitly cancelled wall plan suppressed for its lifecycle."""
        if wall_side is None or wall_price is None:
            return any(
                trade.get("signal_id") in self._manual_cancelled_wall_blocks
                and trade.get("wall_id") == wall_id
                and trade.get("symbol", "").upper() == symbol.upper()
                and trade.get("strategy_id") == strategy_id
                for trade in self.trades
            )
        return any(
            key == (
                symbol.upper(), strategy_id, wall_side or "",
                float(wall_price or 0),
            )
            for key in self._manual_cancelled_wall_blocks.values()
        )

    def get_expired_manual_wall_block_ids(
        self, symbol: str, observed_walls: list[dict] | tuple[dict, ...]
    ) -> list[str]:
        """Identify blocks whose wall is absent from a complete scanner result."""
        symbol = symbol.upper()
        observed = set()
        for wall in observed_walls:
            side = str(wall.get("side", "")).lower()
            try:
                price = float(wall.get("price"))
            except (TypeError, ValueError):
                continue
            if side in {"buy", "sell"} and isfinite(price):
                observed.add((side, price))
        return [
            signal_id for signal_id, key in self._manual_cancelled_wall_blocks.items()
            if key[0] == symbol and (key[2], key[3]) not in observed
        ]

    def release_manual_wall_blocks(self, signal_ids: list[str]) -> None:
        for signal_id in signal_ids:
            self._manual_cancelled_wall_blocks.pop(signal_id, None)

    def cancel_pending_trade(self, signal_id: str) -> dict | None:
        trade = self.find_trade(signal_id)
        if trade is None or trade.get("status") != "pending":
            return None
        trade.update(
            status="cancelled",
            closed_at=datetime.now(timezone.utc),
            close_reason="MANUAL_CANCELLED",
        )
        self._manual_cancelled_wall_blocks[trade["signal_id"]] = (
            str(trade["symbol"]).upper(), str(trade["strategy_id"]),
            str(trade["wall_side"]), float(trade["wall_price"]),
        )
        return {"event": "ENTRY_CANCELLED", "trade": trade}

    def close_trade(self, signal_id: str, price: float) -> dict | None:
        trade = self.find_trade(signal_id)
        if trade is None or trade.get("status") != "open":
            return None
        return self._close_trade(trade, price, "ADMIN_CLOSED")

    def update_open_trade_levels(
        self,
        signal_id: str,
        *,
        take_profit: float,
        stop_loss: float,
        current_price: float,
        tick_size: float,
    ) -> dict | None:
        trade = self.find_trade(signal_id)
        if trade is None or trade.get("status") != "open":
            return None
        try:
            tp, sl, price, tick = map(
                lambda value: Decimal(str(value)),
                (take_profit, stop_loss, current_price, tick_size),
            )
        except (InvalidOperation, TypeError, ValueError):
            raise ValueError("TP, SL, цена и tickSize должны быть числами") from None
        if any(not value.is_finite() or value <= 0 for value in (tp, sl, price, tick)):
            raise ValueError("TP, SL, цена и tickSize должны быть положительными")
        if tp % tick != 0 or sl % tick != 0:
            raise ValueError(f"TP и SL должны соответствовать tickSize {tick}")
        if trade["side"] == "long":
            valid = sl < price < tp
        else:
            valid = tp < price < sl
        if not valid:
            raise ValueError("Для текущей цены TP и SL должны оставаться по правильную сторону сделки")
        trade["take_profit"] = float(tp)
        trade["stop_loss"] = float(sl)
        return {"event": "TRADE_LEVELS_UPDATED", "trade": trade}

    def trade_unrealized_pnl(self, trade: dict, current_price: float) -> float:
        entry_fill = float(trade.get("entry_fill_price") or trade["entry_price"])
        notional = float(trade.get("position_notional_usdt") or trade["stake_usdt"])
        price = float(current_price)
        gross = (
            notional * (price - entry_fill) / entry_fill
            * (1 if trade["side"] == "long" else -1)
        )
        entry_fee = float(trade.get("entry_fee", trade.get("entry_fee_usdt") or 0.0))
        exit_fee = (
            notional * price / entry_fill
            * float(trade.get("taker_fee_rate", trade.get("fee_rate") or 0.0))
        )
        return gross - entry_fee - exit_fee

    def cancel_pending_for_slot(
        self, symbol: str, strategy_id: str, wall_side: str, reason: str
    ) -> list[dict]:
        trade = self.get_pending_for_slot(symbol, strategy_id, wall_side)
        if trade is None:
            return []
        trade.update(status="cancelled", closed_at=datetime.now(timezone.utc), close_reason=reason)
        return [{"event": "ENTRY_CANCELLED", "trade": trade}]

    def disarm_pending_for_symbol(self, symbol: str) -> None:
        for trade in self.trades:
            if trade["symbol"] == symbol.upper() and trade["status"] == "pending":
                trade["entry_armed"] = False

    def set_pair_settings_for_price_check(self, settings: PairSettings) -> None:
        if not hasattr(self, "_current_pair_settings"):
            self._current_pair_settings = {}
        self._current_pair_settings[settings.symbol.upper()] = settings

    def has_matching_pending_plan(self, signal, levels: dict, pair_settings: PairSettings) -> bool:
        data = signal if isinstance(signal, dict) else signal.__dict__
        existing = self.get_pending_for_slot(
            data["symbol"], data["strategy_id"], data["wall_side"]
        )
        if existing is None:
            return False
        expected = {
            "wall_id": data["wall_id"],
            "wall_price": float(data["wall_price"]),
            "side": levels["side"],
            "entry_price": float(levels["entry_price"]),
            "stop_loss": float(levels["stop_loss"]),
            "take_profit": float(levels["take_profit"]),
            "leverage": float(pair_settings.leverage or self.trading_settings.leverage),
            "stake_usdt": float(self.trading_settings.stake_usdt),
        }
        return all(existing.get(key) == value for key, value in expected.items())

    def cancel_pending_not_in_wall_set(
        self,
        symbol: str,
        valid_walls: set[tuple[str, float]],
        reason: str,
    ) -> list[dict]:
        symbol = symbol.upper()
        events = []
        for trade in self.trades:
            key = (trade["wall_side"], float(trade["wall_price"]))
            if (
                trade["symbol"] != symbol
                or trade["status"] != "pending"
                or key in valid_walls
            ):
                continue
            trade.update(
                status="cancelled",
                closed_at=datetime.now(timezone.utc),
                close_reason=reason,
            )
            events.append({"event": "ENTRY_CANCELLED", "trade": trade})
        return events

    def has_active_symbol(self, symbol: str) -> bool:
        symbol = symbol.upper()
        return any(
            trade["symbol"] == symbol and trade["status"] in {"pending", "open"}
            for trade in self.trades
        )

    @staticmethod
    def _exit_reason(trade: dict, price: float) -> str | None:
        market_price = Decimal(str(price))
        take_profit = Decimal(str(trade["take_profit"]))
        stop_loss = Decimal(str(trade["stop_loss"]))
        if trade["side"] == "long":
            if market_price <= stop_loss:
                return "STOP_LOSS"
            if market_price >= take_profit:
                return "TAKE_PROFIT"
        else:
            if market_price >= stop_loss:
                return "STOP_LOSS"
            if market_price <= take_profit:
                return "TAKE_PROFIT"
        return None

    def _close_trade(self, trade: dict, price: float, reason: str) -> dict:
        direction = 1 if trade["side"] == "long" else -1
        notional = trade.get("position_notional_usdt") or trade["stake_usdt"]
        entry_fee = trade.get("entry_fee", trade.get("entry_fee_usdt") or 0.0)
        fill_entry = float(trade.get("entry_fill_price") or trade["entry_price"])
        gross_pnl = (
            notional
            * (price - fill_entry) / fill_entry
            * direction
        )
        exit_notional = notional * price / fill_entry
        exit_fee = exit_notional * trade.get("taker_fee_rate", trade.get("fee_rate") or 0.0)
        net_pnl = gross_pnl - entry_fee - exit_fee
        trade.update(
            status="closed",
            exit_price=float(price),
            closed_at=datetime.now(timezone.utc),
            close_reason=reason,
            exit_fee=exit_fee,
            exit_fee_usdt=exit_fee,
            gross_pnl=gross_pnl,
            gross_pnl_usdt=gross_pnl,
            net_pnl=net_pnl,
            profit_usdt=net_pnl,
            profit_percent=(net_pnl / trade["stake_usdt"] * 100),
            margin_return_pct=(net_pnl / trade["stake_usdt"] * 100),
        )
        # Balance is cash/equity: the reserved margin is not removed here;
        # closing releases its reservation and applies only net PnL.
        self.balance_usdt += gross_pnl - exit_fee
        return {"event": "TRADE_CLOSED", "trade": trade, "fill_price": float(price)}

    def close_all(self) -> list[dict]:
        """Close open positions at last observed prices and cancel pending entries."""
        open_trades = [trade for trade in self.trades if trade["status"] == "open"]
        missing_prices = sorted(
            {trade["symbol"] for trade in open_trades if trade["symbol"] not in self.last_prices}
        )
        if missing_prices:
            raise RuntimeError(
                "Cannot close open virtual trades without a last price for: "
                + ", ".join(missing_prices)
            )

        events = []
        for trade in self.trades:
            if trade["status"] == "pending":
                trade.update(
                    status="cancelled",
                    closed_at=datetime.now(timezone.utc),
                    close_reason="ENGINE_STOPPED_BEFORE_ENTRY",
                )
                events.append({"event": "ENTRY_CANCELLED", "trade": trade})
            elif trade["status"] == "open":
                events.append(
                    self._close_trade(
                        trade,
                        self.last_prices[trade["symbol"]],
                        "ENGINE_STOPPED",
                    )
                )
        return events

    def close_open_trades(self, reason: str = "ADMIN_CLOSED") -> list[dict]:
        """Close open positions at their last observed price; leave pending entries intact."""
        open_trades = self.get_open_trades()
        missing_prices = sorted(
            {trade["symbol"] for trade in open_trades if trade["symbol"] not in self.last_prices}
        )
        if missing_prices:
            raise RuntimeError(
                "Cannot close open virtual trades without a last price for: "
                + ", ".join(missing_prices)
            )

        return [
            self._close_trade(trade, self.last_prices[trade["symbol"]], reason)
            for trade in open_trades
        ]

    def get_pending_trades(self) -> list[dict]:
        return [trade for trade in self.trades if trade["status"] == "pending"]

    def remove_persisted_cancelled_trades(self, signal_ids: set[str] | list[str]) -> int:
        """Drop only cancelled trades confirmed deleted by the repository."""
        removed_ids = set(signal_ids)
        if not removed_ids:
            return 0
        before = len(self.trades)
        self.trades = [
            trade for trade in self.trades
            if not (
                trade.get("signal_id") in removed_ids
                and trade.get("status") == "cancelled"
            )
        ]
        actually_removed = before - len(self.trades)
        if actually_removed:
            still_present = {trade.get("signal_id") for trade in self.trades}
            self._used_signal_ids.difference_update(removed_ids - still_present)
        return actually_removed

    def get_open_trades(self) -> list[dict]:
        return [trade for trade in self.trades if trade["status"] == "open"]

    def get_statistics(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        closed = [trade for trade in self.trades if trade["status"] == "closed"]
        weekly = []
        for trade in closed:
            closed_at = trade.get("closed_at")
            if closed_at is not None:
                if closed_at.tzinfo is None:
                    closed_at = closed_at.replace(tzinfo=timezone.utc)
                if closed_at >= week_start:
                    weekly.append(trade)
        wins = sum(trade["profit_usdt"] > 0 for trade in closed)
        losses = sum(trade["profit_usdt"] < 0 for trade in closed)
        weekly_wins = sum(trade["profit_usdt"] > 0 for trade in weekly)
        weekly_losses = sum(trade["profit_usdt"] < 0 for trade in weekly)
        open_trades = self.get_open_trades()
        unrealized_profit = sum(
            self.trade_unrealized_pnl(trade, self.last_prices[trade["symbol"]])
            for trade in open_trades
            if trade["symbol"] in self.last_prices
        )
        reserved = sum(
            trade["stake_usdt"]
            for trade in self.trades
            if trade["status"] in {"pending", "open"}
        )
        # Entry fees are already deducted from cash when a position opens.
        # Add them back when combining net unrealized trade PnL with cash, so
        # equity includes that cost exactly once.
        outstanding_entry_fees = sum(
            trade.get("entry_fee", trade.get("entry_fee_usdt") or 0.0)
            for trade in open_trades
        )
        return {
            "balance_usdt": self.balance_usdt,
            "equity_usdt": self.balance_usdt + unrealized_profit + outstanding_entry_fees,
            "available_balance_usdt": self.balance_usdt - reserved,
            "reserved_margin_usdt": reserved,
            "unrealized_profit_usdt": unrealized_profit,
            "stake_usdt": self.trading_settings.stake_usdt,
            "max_open_trades": self.trading_settings.max_open_trades,
            "total_profit_usdt": sum(trade["profit_usdt"] for trade in closed),
            "weekly_profit_usdt": sum(trade["profit_usdt"] for trade in weekly),
            "closed_trades": len(closed),
            "open_trades": len(open_trades),
            "pending_trades": len(self.get_pending_trades()),
            "wins": wins,
            "losses": losses,
            "weekly_closed_trades": len(weekly),
            "weekly_wins": weekly_wins,
            "weekly_losses": weekly_losses,
            "kd_ratio": wins / losses if losses else None,
            "weekly_kd_ratio": weekly_wins / weekly_losses if weekly_losses else None,
            "total_return_percent": (
                (self.balance_usdt - self.trading_settings.initial_balance_usdt)
                / self.trading_settings.initial_balance_usdt * 100
                if self.trading_settings.initial_balance_usdt > 0
                else 0.0
            ),
            "win_rate_percent": wins / len(closed) * 100 if closed else 0.0,
        }

    def get_pair_statistics(self, symbol: str, now: datetime | None = None) -> dict:
        """Return realized and open virtual-trade statistics for one symbol."""
        symbol = symbol.upper()
        now = now or datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        pair_trades = [trade for trade in self.trades if trade["symbol"] == symbol]
        closed = [trade for trade in pair_trades if trade["status"] == "closed"]
        weekly = []
        for trade in closed:
            closed_at = trade.get("closed_at")
            if closed_at is not None:
                if closed_at.tzinfo is None:
                    closed_at = closed_at.replace(tzinfo=timezone.utc)
                if closed_at >= week_start:
                    weekly.append(trade)
        wins = sum(trade["profit_usdt"] > 0 for trade in closed)
        losses = sum(trade["profit_usdt"] < 0 for trade in closed)
        price = self.last_prices.get(symbol)
        open_trades = [trade for trade in pair_trades if trade["status"] == "open"]
        unrealized = 0.0
        if price is not None:
            unrealized = sum(
                self.trade_unrealized_pnl(trade, price)
                for trade in open_trades
            )
        return {
            "closed_trades": len(closed),
            "open_trades": len(open_trades),
            "pending_trades": sum(trade["status"] == "pending" for trade in pair_trades),
            "total_profit_usdt": sum(trade["profit_usdt"] for trade in closed),
            "weekly_profit_usdt": sum(trade["profit_usdt"] for trade in weekly),
            "unrealized_profit_usdt": unrealized if price is not None else None,
            "wins": wins,
            "losses": losses,
            "win_rate_percent": wins / len(closed) * 100 if closed else None,
            "kd_ratio": wins / losses if losses else (float("inf") if wins else None),
        }
