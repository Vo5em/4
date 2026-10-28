from datetime import datetime, timedelta, timezone
from math import isfinite
from threading import Lock

from .formulas import (
    calculate_entry_price,
    calculate_stop_loss_price,
    calculate_x2_price,
)
from .settings import PairSettings, TradingSettings


class TradeManager:
    """In-memory virtual execution and lifecycle manager; never sends orders."""

    def __init__(self, trading_settings: TradingSettings | None = None):
        self.trading_settings = trading_settings or TradingSettings()
        self.trades: list[dict] = []
        self.last_prices: dict[str, float] = {}
        self._used_signal_ids: set[str] = set()
        self._create_lock = Lock()
        self.balance_usdt = self.trading_settings.initial_balance_usdt

    def restore_state(
        self,
        trades: list[dict],
        balance_usdt: float,
        last_prices: dict[str, float] | None = None,
    ) -> None:
        self.trades = [dict(trade) for trade in trades]
        self._used_signal_ids = {trade["signal_id"] for trade in self.trades}
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
            return None
        wall_id = str(data.get("wall_id") or signal_id)
        if any(
            str(trade.get("wall_id") or self._wall_id_from_signal_id(trade)) == wall_id
            and trade.get("status") in {"pending", "open", "closed"}
            for trade in self.trades
        ):
            return None
        if any(
            trade["symbol"] == symbol
            and trade["wall_side"] == data["wall_side"]
            and trade["wall_price"] == wall_price
            and trade["status"] in {"pending", "open"}
            for trade in self.trades
        ):
            return None

        stake = float(
            self.trading_settings.stake_usdt
            if stake_usdt is None
            else stake_usdt
        )
        reserved = sum(
            trade["stake_usdt"]
            for trade in self.trades
            if trade["status"] in {"pending", "open"}
        )
        if not isfinite(stake) or stake <= 0 or self.balance_usdt - reserved < stake:
            return None

        wall_side = data["wall_side"]
        if wall_side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported wall side: {wall_side}")
        trade_side = data.get("side") or ("long" if wall_side == "buy" else "short")
        entry_price = data.get("entry_price")
        if entry_price is None:
            entry_price = calculate_entry_price(
                wall_price, wall_side, pair_settings.entry_distance_bps
            )
        stop_loss = data.get("stop_loss")
        if stop_loss is None:
            stop_loss = calculate_stop_loss_price(
                wall_price, wall_side, pair_settings.stop_distance_bps
            )
        take_profit = data.get("take_profit")
        if take_profit is None:
            take_profit = calculate_x2_price(entry_price, stop_loss, wall_side)
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
            "entry_price": entry_price,
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
        self.trades.append(trade)
        self._used_signal_ids.add(signal_id)
        return trade

    @staticmethod
    def _wall_id_from_signal_id(trade: dict) -> str:
        signal_id = str(trade.get("signal_id", ""))
        strategy_id = str(trade.get("strategy_id", ""))
        prefix = f"{strategy_id}:"
        return signal_id[len(prefix):] if signal_id.startswith(prefix) else signal_id

    def on_price(self, symbol: str, price: float) -> list[dict]:
        if not isfinite(price) or price <= 0:
            raise ValueError("price must be a positive finite number")
        symbol = symbol.upper()
        self.last_prices[symbol] = float(price)
        events = []
        for trade in self.trades:
            if trade["symbol"] != symbol or trade["status"] not in {"pending", "open"}:
                continue

            if trade["status"] == "pending":
                triggered = (
                    price <= trade["entry_price"]
                    if trade["side"] == "long"
                    else price >= trade["entry_price"]
                )
                if not triggered:
                    continue
                trade["status"] = "open"
                trade["opened_at"] = datetime.now(timezone.utc)
                events.append({"event": "ENTRY_ACTIVATED", "trade": trade})

            reason = self._exit_reason(trade, price)
            if reason is not None:
                exit_price = price if reason == "STOP_LOSS" else trade["take_profit"]
                events.append(self._close_trade(trade, exit_price, reason))
        return events

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
        if trade["side"] == "long":
            if price <= trade["stop_loss"]:
                return "STOP_LOSS"
            if price >= trade["take_profit"]:
                return "TAKE_PROFIT"
        else:
            if price >= trade["stop_loss"]:
                return "STOP_LOSS"
            if price <= trade["take_profit"]:
                return "TAKE_PROFIT"
        return None

    def _close_trade(self, trade: dict, price: float, reason: str) -> dict:
        direction = 1 if trade["side"] == "long" else -1
        profit = (
            (price - trade["entry_price"])
            / trade["entry_price"]
            * trade["stake_usdt"]
            * direction
        )
        trade.update(
            status="closed",
            exit_price=float(price),
            closed_at=datetime.now(timezone.utc),
            close_reason=reason,
            profit_usdt=profit,
            profit_percent=(profit / trade["stake_usdt"] * 100),
        )
        self.balance_usdt += profit
        return {"event": "TRADE_CLOSED", "trade": trade}

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
            (self.last_prices[trade["symbol"]] - trade["entry_price"])
            / trade["entry_price"]
            * trade["stake_usdt"]
            * (1 if trade["side"] == "long" else -1)
            for trade in open_trades
            if trade["symbol"] in self.last_prices
        )
        reserved = sum(
            trade["stake_usdt"]
            for trade in self.trades
            if trade["status"] in {"pending", "open"}
        )
        return {
            "balance_usdt": self.balance_usdt,
            "equity_usdt": self.balance_usdt + unrealized_profit,
            "available_balance_usdt": self.balance_usdt - reserved,
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
                (price - trade["entry_price"])
                / trade["entry_price"]
                * trade["stake_usdt"]
                * (1 if trade["side"] == "long" else -1)
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
