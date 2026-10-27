from typing import Protocol

from .settings import PairSettings, TradingSettings


class ExecutionBackend(Protocol):
    """Execution boundary used by the engine.

    The current implementation is ``TradeManager`` and only simulates fills.
    A future Bybit adapter can implement this contract without putting exchange
    account calls into strategy evaluation or public market-data processing.
    """

    trading_settings: TradingSettings
    trades: list[dict]
    last_prices: dict[str, float]
    balance_usdt: float

    def restore_state(
        self,
        trades: list[dict],
        balance_usdt: float,
        last_prices: dict[str, float] | None = None,
    ) -> None: ...

    def create_trade(
        self,
        signal,
        pair_settings: PairSettings,
        *,
        stake_usdt: float | None = None,
    ) -> dict | None: ...

    def on_price(self, symbol: str, price: float) -> list[dict]: ...

    def cancel_pending_for_symbol(self, symbol: str, reason: str) -> list[dict]: ...

    def cancel_pending_for_strategy(
        self, symbol: str, strategy_id: str, reason: str
    ) -> list[dict]: ...

    def cancel_pending_for_wall(
        self, symbol: str, wall_side: str, wall_price: float, reason: str
    ) -> list[dict]: ...

    def cancel_pending_not_in_wall_set(
        self,
        symbol: str,
        valid_walls: set[tuple[str, float]],
        reason: str,
    ) -> list[dict]: ...

    def has_active_symbol(self, symbol: str) -> bool: ...

    def close_all(self) -> list[dict]: ...

    def close_open_trades(self, reason: str = "ADMIN_CLOSED") -> list[dict]: ...

    def get_pending_trades(self) -> list[dict]: ...

    def get_open_trades(self) -> list[dict]: ...

    def get_statistics(self) -> dict: ...
