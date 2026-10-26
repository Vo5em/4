import time
from collections import deque


class TradeFlow:
    def __init__(self, window_seconds=10):
        self.window_seconds = window_seconds
        self.trades = deque()
        

        # ---------------------------------------------
        # Накопительный объём с момента запуска
        # ---------------------------------------------

        self.total_buy_volume = 0
        self.total_sell_volume = 0

    def add_trade(self, price, quantity, side):
        now = time.monotonic()

        notional = price * quantity

        self.trades.append({
            "time": now,
            "price": price,
            "quantity": quantity,
            "side": side,
            "notional": notional,
        })

        # ---------------------------------------------
        # Накопительный объём
        # ---------------------------------------------

        if side == "Buy":
            self.total_buy_volume += notional

        elif side == "Sell":
            self.total_sell_volume += notional

        self._remove_old_trades(now)

    def _remove_old_trades(self, now):
        cutoff = now - self.window_seconds

        while (
            self.trades
            and self.trades[0]["time"] < cutoff
        ):
            self.trades.popleft()

    def get_flow(self):
        now = time.monotonic()

        self._remove_old_trades(now)

        buy_volume = sum(
            trade["notional"]
            for trade in self.trades
            if trade["side"] == "Buy"
        )

        sell_volume = sum(
            trade["notional"]
            for trade in self.trades
            if trade["side"] == "Sell"
        )

        return {
            # -----------------------------------------
            # Rolling 10 секунд
            # -----------------------------------------

            "buy_volume": buy_volume,
            "sell_volume": sell_volume,

            "buy_volume_per_second": (
                buy_volume / self.window_seconds
            ),

            "sell_volume_per_second": (
                sell_volume / self.window_seconds
            ),

            # -----------------------------------------
            # Накопительный объём
            # -----------------------------------------

            "buy_volume_total": self.total_buy_volume,
            "sell_volume_total": self.total_sell_volume,
        }