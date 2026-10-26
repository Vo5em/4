from formulas import (
    calculate_entry_price,
    calculate_liquidation_price,
    calculate_x2_price,
)


class TradeManager:
    def __init__(
        self,
        entry_distance_bps=1.5,
        liquidation_distance_bps=2,
    ):
        self.entry_distance_bps = entry_distance_bps
        self.liquidation_distance_bps = liquidation_distance_bps

        self.trades = []

    def create_trade(self, wall):
        wall_side = wall["side"]

        if wall_side == "buy":
            trade_side = "long"
        else:
            trade_side = "short"

        entry_price = calculate_entry_price(
            wall_price=wall["price"],
            side=wall_side,
            distance_bps=self.entry_distance_bps,
        )

        liquidation_price = calculate_liquidation_price(
            wall_price=wall["price"],
            side=wall_side,
            distance_bps=self.liquidation_distance_bps,
        )

        x2_price = calculate_x2_price(
            entry_price=entry_price,
            liquidation_price=liquidation_price,
            side=wall_side,
        )

        trade = {
            "symbol": wall.get("symbol"),

            "side": trade_side,
            "wall_side": wall_side,

            "wall_price": wall["price"],
            "wall_size": wall["current_size"],

            "entry_price": entry_price,
            "liquidation_price": liquidation_price,
            "x2_price": x2_price,

            "status": "pending",
        }

        self.trades.append(trade)

        return trade

    def get_pending_trades(self):
        return [
            trade
            for trade in self.trades
            if trade["status"] == "pending"
        ]