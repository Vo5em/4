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
    min_wall_age_seconds: float = 40.0
    consumed_ratio: float = 0.9
    entry_distance_bps: float = 1.5
    stop_distance_bps: float = 2.0
    max_open_trades: int = 1
    bounce_reversal_bps: float = 1.0
    breakout_confirm_seconds: float = 5.0
    breakout_buffer_bps: float = 0.5
    breakout_max_update_gap_seconds: float = 2.0
    leverage: float | None = None
    maker_fee_rate: float = 0.00036
    taker_fee_rate: float = 0.001
    maintenance_margin_rate: float = 0.005
    take_profit_bps: float | None = None
    stop_loss_bps: float | None = None
    bounce_take_profit_bps: float | None = None
    bounce_stop_loss_bps: float | None = None
    breakout_take_profit_bps: float | None = None
    breakout_stop_loss_bps: float | None = None
    historical_level_timeframe: str = "5m"
    historical_level_candle_count: int = 500
    historical_level_refresh_seconds: float = 300.0
    historical_level_pctbound: float = 0.05
    historical_level_min_touches: int = 1
    historical_level_extmethod: str = "METHOD_NUMDIFF"
    historical_level_accuracy: int = 2
    historical_level_touch_zone_bps: float = 10.0
    historical_level_touch_reset_bps: float = 30.0
    historical_level_touch_min_separation_candles: int = 3
    historical_level_break_confirm_bps: float = 20.0
    historical_level_break_confirm_candles: int = 2
    max_wall_level_distance_bps: float = 13.0
    false_breakout_point_1_offset_bps: float | None = None
    false_breakout_point_2_distance_bps: float | None = None
    false_breakout_observation_timeout_seconds: float | None = None
    false_breakout_take_profit_bps: float | None = None
    false_breakout_stop_loss_bps: float | None = None

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
        if not isfinite(self.bounce_reversal_bps) or self.bounce_reversal_bps <= 0:
            raise ValueError("bounce_reversal_bps must be positive")
        if not isfinite(self.breakout_confirm_seconds) or self.breakout_confirm_seconds <= 0:
            raise ValueError("breakout_confirm_seconds must be positive")
        if not isfinite(self.breakout_buffer_bps) or self.breakout_buffer_bps < 0:
            raise ValueError("breakout_buffer_bps must not be negative")
        if not isfinite(self.breakout_max_update_gap_seconds) or self.breakout_max_update_gap_seconds <= 0:
            raise ValueError("breakout_max_update_gap_seconds must be positive")
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
        if self.leverage is not None and (not isfinite(self.leverage) or self.leverage <= 0 or self.leverage > 125):
            raise ValueError("leverage must be positive and no greater than 125")
        if any(not isfinite(rate) or rate < 0 or rate >= 1 for rate in (self.maker_fee_rate, self.taker_fee_rate, self.maintenance_margin_rate)):
            raise ValueError("fee and maintenance margin rates must be in [0, 1)")
        for name, value in (
            ("take_profit_bps", self.take_profit_bps),
            ("stop_loss_bps", self.stop_loss_bps),
            ("bounce_take_profit_bps", self.bounce_take_profit_bps),
            ("bounce_stop_loss_bps", self.bounce_stop_loss_bps),
            ("breakout_take_profit_bps", self.breakout_take_profit_bps),
            ("breakout_stop_loss_bps", self.breakout_stop_loss_bps),
            ("false_breakout_point_1_offset_bps", self.false_breakout_point_1_offset_bps),
            ("false_breakout_point_2_distance_bps", self.false_breakout_point_2_distance_bps),
            ("false_breakout_take_profit_bps", self.false_breakout_take_profit_bps),
            ("false_breakout_stop_loss_bps", self.false_breakout_stop_loss_bps),
        ):
            try:
                valid = value is None or (isfinite(value) and value > 0)
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError(f"{name} override must be a positive finite number or None")
        if self.false_breakout_observation_timeout_seconds is not None:
            try:
                valid = (
                    isfinite(self.false_breakout_observation_timeout_seconds)
                    and self.false_breakout_observation_timeout_seconds > 0
                )
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError("false_breakout_observation_timeout_seconds must be positive or None")
        if not isinstance(self.historical_level_timeframe, str) or self.historical_level_timeframe not in {
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w", "1M"
        }:
            raise ValueError("Unsupported Bybit historical level timeframe")
        if isinstance(self.historical_level_candle_count, bool) or not isinstance(self.historical_level_candle_count, int):
            raise ValueError("historical_level_candle_count must be an integer")
        if not 5 <= self.historical_level_candle_count <= 1000:
            raise ValueError("historical_level_candle_count must be between 5 and 1000")
        if not isfinite(self.historical_level_refresh_seconds) or self.historical_level_refresh_seconds <= 0:
            raise ValueError("historical_level_refresh_seconds must be positive")
        if (
            isinstance(self.historical_level_pctbound, bool)
            or not isinstance(self.historical_level_pctbound, (int, float))
            or not isfinite(self.historical_level_pctbound)
            or self.historical_level_pctbound <= 0
        ):
            raise ValueError("historical_level_pctbound must be a positive finite number")
        object.__setattr__(self, "historical_level_pctbound", float(self.historical_level_pctbound))
        if (
            isinstance(self.historical_level_min_touches, bool)
            or not isinstance(self.historical_level_min_touches, int)
            or self.historical_level_min_touches < 1
        ):
            raise ValueError("historical_level_min_touches must be a positive integer")
        if (
            not isinstance(self.historical_level_extmethod, str)
            or self.historical_level_extmethod not in {
                "METHOD_NUMDIFF", "METHOD_NAIVE", "METHOD_NAIVECONSEC"
            }
        ):
            raise ValueError("Unsupported historical_level_extmethod")
        if (
            isinstance(self.historical_level_accuracy, bool)
            or not isinstance(self.historical_level_accuracy, int)
            or self.historical_level_accuracy <= 0
            or self.historical_level_accuracy % 2 != 0
        ):
            raise ValueError("historical_level_accuracy must be a positive even integer")
        for name in (
            "historical_level_touch_zone_bps",
            "historical_level_touch_reset_bps",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative finite number")
            object.__setattr__(self, name, float(value))
        if self.historical_level_touch_reset_bps <= self.historical_level_touch_zone_bps:
            raise ValueError("historical_level_touch_reset_bps must be greater than touch_zone_bps")
        if (
            isinstance(self.historical_level_touch_min_separation_candles, bool)
            or not isinstance(self.historical_level_touch_min_separation_candles, int)
            or self.historical_level_touch_min_separation_candles < 1
        ):
            raise ValueError("historical_level_touch_min_separation_candles must be a positive integer")
        if (
            isinstance(self.historical_level_break_confirm_bps, bool)
            or not isinstance(self.historical_level_break_confirm_bps, (int, float))
            or not isfinite(self.historical_level_break_confirm_bps)
            or self.historical_level_break_confirm_bps <= 0
        ):
            raise ValueError("historical_level_break_confirm_bps must be positive and finite")
        object.__setattr__(self, "historical_level_break_confirm_bps", float(self.historical_level_break_confirm_bps))
        if (
            isinstance(self.historical_level_break_confirm_candles, bool)
            or not isinstance(self.historical_level_break_confirm_candles, int)
            or self.historical_level_break_confirm_candles < 1
        ):
            raise ValueError("historical_level_break_confirm_candles must be a positive integer")
        if not isfinite(self.max_wall_level_distance_bps) or self.max_wall_level_distance_bps <= 0:
            raise ValueError("max_wall_level_distance_bps must be positive")


@dataclass(frozen=True)
class TradingSettings:
    """Global virtual-trading settings shared by all enabled pairs."""

    stake_usdt: float = 10.0
    initial_balance_usdt: float = 1000.0
    max_open_trades: int = 3
    leverage: float = 25.0
    default_take_profit_bps: float = 5.0
    default_stop_loss_bps: float = 5.0

    def __post_init__(self) -> None:
        if not isfinite(self.stake_usdt) or self.stake_usdt <= 0:
            raise ValueError("stake_usdt must be positive")
        if not isfinite(self.initial_balance_usdt) or self.initial_balance_usdt < 0:
            raise ValueError("initial_balance_usdt must not be negative")
        if isinstance(self.max_open_trades, bool) or not isinstance(self.max_open_trades, int):
            raise ValueError("max_open_trades must be an integer")
        if self.max_open_trades < 1:
            raise ValueError("max_open_trades must be at least 1")
        if not isfinite(self.leverage) or self.leverage <= 0 or self.leverage > 125:
            raise ValueError("leverage must be positive and no greater than 125")
        for name, value in (
            ("default_take_profit_bps", self.default_take_profit_bps),
            ("default_stop_loss_bps", self.default_stop_loss_bps),
        ):
            try:
                valid = isfinite(value) and value > 0
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError(f"{name} must be a positive finite number")
