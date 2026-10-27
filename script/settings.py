import re
from dataclasses import dataclass
from math import isfinite


def normalize_pair_symbol(symbol: str) -> str:
    """Return a canonical Bybit Linear USDT symbol or reject invalid input."""
    if not isinstance(symbol, str):
        raise ValueError("symbol must be text")
    normalized = symbol.strip().upper()
    if re.fullmatch(r"[A-Z0-9]{2,26}USDT", normalized) is None:
        raise ValueError("Use a Bybit Linear USDT symbol such as BTCUSDT")
    return normalized


@dataclass(frozen=True)
class PairSettings:
    """Strategy thresholds that can be configured independently per symbol."""

    symbol: str
    enabled: bool = True
    min_wall_ratio: float = 888.0
    min_wall_notional_usdt: float = 0.0
    max_wall_distance_bps: float = 100.0
    min_wall_age_seconds: float = 20.0
    consumed_ratio: float = 0.9
    entry_distance_bps: float = 1.5
    stop_distance_bps: float = 2.0
    max_open_trades: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_pair_symbol(self.symbol))
        if not isfinite(self.min_wall_ratio) or self.min_wall_ratio <= 0:
            raise ValueError("min_wall_ratio must be positive")
        if not isfinite(self.min_wall_notional_usdt) or self.min_wall_notional_usdt < 0:
            raise ValueError("min_wall_notional_usdt must not be negative")
        if not isfinite(self.max_wall_distance_bps) or self.max_wall_distance_bps < 0:
            raise ValueError("max_wall_distance_bps must not be negative")
        if not isfinite(self.min_wall_age_seconds) or self.min_wall_age_seconds < 0:
            raise ValueError("min_wall_age_seconds must not be negative")
        if not isfinite(self.consumed_ratio) or not 0 < self.consumed_ratio <= 1:
            raise ValueError("consumed_ratio must be greater than 0 and at most 1")
        if (
            not isfinite(self.entry_distance_bps)
            or not isfinite(self.stop_distance_bps)
            or self.entry_distance_bps < 0
            or self.stop_distance_bps < 0
        ):
            raise ValueError("entry and stop distances must not be negative")
        if self.entry_distance_bps >= 10000 or self.stop_distance_bps >= 10000:
            raise ValueError("entry and stop distances must be below 10000 bps")
        if 2 * self.entry_distance_bps + self.stop_distance_bps >= 10000:
            raise ValueError("entry and stop distances produce a non-positive short target")
        if isinstance(self.max_open_trades, bool) or not isinstance(self.max_open_trades, int):
            raise ValueError("max_open_trades must be an integer")
        if self.max_open_trades < 1:
            raise ValueError("max_open_trades must be at least 1")


@dataclass(frozen=True)
class TradingSettings:
    """Global virtual-trading settings shared by all enabled pairs."""

    stake_usdt: float = 10.0
    initial_balance_usdt: float = 1000.0
    max_open_trades: int = 3

    def __post_init__(self) -> None:
        if not isfinite(self.stake_usdt) or self.stake_usdt <= 0:
            raise ValueError("stake_usdt must be positive")
        if not isfinite(self.initial_balance_usdt) or self.initial_balance_usdt < 0:
            raise ValueError("initial_balance_usdt must not be negative")
        if isinstance(self.max_open_trades, bool) or not isinstance(self.max_open_trades, int):
            raise ValueError("max_open_trades must be an integer")
        if self.max_open_trades < 1:
            raise ValueError("max_open_trades must be at least 1")
