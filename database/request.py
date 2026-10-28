from sqlalchemy import MetaData, Table, inspect, select

from database.models import (
    MarketPrice,
    PairStrategyConfig,
    StrategyChangeEvent,
    RuntimeState,
    TradingPairSetting,
    VirtualTrade,
    async_main,
    engine,
    async_session,
    utc_now,
)
from script.settings import PairSettings, TradingSettings, normalize_pair_symbol
from script.strategies import DEFAULT_STRATEGY_ID


async def initialize_database(
    default_trading_settings: TradingSettings,
    default_pair_settings: dict[str, PairSettings],
) -> dict:
    await async_main()
    legacy_symbols = await read_legacy_symbol_rows()
    async with async_session() as session:
        state = await session.get(RuntimeState, 1)
        if state is None:
            state = RuntimeState(
                id=1,
                balance_usdt=default_trading_settings.initial_balance_usdt,
                initial_balance_usdt=default_trading_settings.initial_balance_usdt,
                stake_usdt=default_trading_settings.stake_usdt,
                max_open_trades=default_trading_settings.max_open_trades,
                engine_mode="stopped",
                initialized=False,
            )
            session.add(state)

        imported_symbols = _legacy_pair_settings(legacy_symbols)
        for settings in imported_symbols:
            if await session.get(TradingPairSetting, settings.symbol) is None:
                session.add(_pair_row(settings))
        for symbol, settings in default_pair_settings.items():
            if await session.get(TradingPairSetting, symbol) is None:
                session.add(_pair_row(settings))
        await session.commit()

        state = await session.get(RuntimeState, 1)
        pair_rows = (await session.scalars(select(TradingPairSetting))).all()
        strategy_rows = (await session.scalars(select(PairStrategyConfig))).all()
        configured_symbols = {row.symbol for row in strategy_rows}
        missing_strategy_rows = [
            PairStrategyConfig(
                symbol=row.symbol,
                enabled_strategy_ids=[DEFAULT_STRATEGY_ID],
                version=1,
            )
            for row in pair_rows
            if row.symbol not in configured_symbols
        ]
        if missing_strategy_rows:
            for strategy_row in missing_strategy_rows:
                session.add(strategy_row)
            await session.commit()
            strategy_rows = (await session.scalars(select(PairStrategyConfig))).all()
        trade_rows = (await session.scalars(select(VirtualTrade))).all()
        market_price_rows = (await session.scalars(select(MarketPrice))).all()
        configured_strategies = {
            row.symbol: set(row.enabled_strategy_ids)
            for row in strategy_rows
        }
        enabled_strategies = {
            row.symbol: configured_strategies.get(
                row.symbol, {DEFAULT_STRATEGY_ID}
            )
            for row in pair_rows
        }
        return {
            "trading_settings": TradingSettings(
                stake_usdt=state.stake_usdt,
                initial_balance_usdt=state.initial_balance_usdt,
                max_open_trades=state.max_open_trades,
            ),
            "balance_usdt": state.balance_usdt,
            "engine_mode": state.engine_mode,
            "initialized": state.initialized,
            "pair_settings": {
                row.symbol: PairSettings(
                    symbol=row.symbol,
                    enabled=row.enabled,
                    min_wall_ratio=row.min_wall_ratio,
                    min_wall_notional_usdt=row.min_wall_notional_usdt,
                    max_wall_distance_bps=row.max_wall_distance_bps,
                    min_wall_age_seconds=row.min_wall_age_seconds,
                    consumed_ratio=row.consumed_ratio,
                    entry_distance_bps=row.entry_distance_bps,
                    stop_distance_bps=row.stop_distance_bps,
                    max_open_trades=row.max_open_trades,
                    bounce_reversal_bps=getattr(row, "bounce_reversal_bps", 1.0) or 1.0,
                    breakout_confirm_seconds=getattr(row, "breakout_confirm_seconds", 5.0) or 5.0,
                    breakout_buffer_bps=getattr(row, "breakout_buffer_bps", 0.5) or 0.5,
                    breakout_max_update_gap_seconds=getattr(
                        row, "breakout_max_update_gap_seconds", 2.0
                    ) or 2.0,
                )
                for row in pair_rows
            },
            "enabled_strategies": enabled_strategies,
            "strategy_versions": {
                row.symbol: int(getattr(row, "version", 1) or 1)
                for row in strategy_rows
            },
            "trades": [_trade_dict(row) for row in trade_rows],
            "last_prices": {
                row.symbol: row.last_price for row in market_price_rows
            },
            "last_price_observed_at": {
                row.symbol: row.observed_at for row in market_price_rows
            },
        }


async def read_legacy_symbol_rows() -> list[dict]:
    """Read the old `simbols` table without changing or dropping it."""
    async with engine.connect() as connection:
        def read(sync_connection):
            inspector = inspect(sync_connection)
            if not inspector.has_table("simbols"):
                return []
            table = Table("simbols", MetaData(), autoload_with=sync_connection)
            required = {"symbol", "enabled"}
            if not required.issubset(table.c.keys()):
                return []
            columns = [table.c.symbol, table.c.enabled]
            if "exchange" in table.c:
                columns.append(table.c.exchange)
            rows = sync_connection.execute(select(*columns)).mappings().all()
            return [dict(row) for row in rows]

        return await connection.run_sync(read)


def _legacy_pair_settings(rows: list[dict]) -> list[PairSettings]:
    """Convert only unambiguous Bybit linear USDT pair records."""
    settings_by_symbol = {}
    for row in rows:
        try:
            symbol = normalize_pair_symbol(str(row.get("symbol") or ""))
        except ValueError:
            continue
        exchange = str(row.get("exchange") or "").strip().lower()
        if exchange not in {"", "bybit"}:
            continue
        settings_by_symbol[symbol] = PairSettings(
            symbol=symbol,
            enabled=bool(row.get("enabled")),
        )
    return list(settings_by_symbol.values())


def _pair_row(settings: PairSettings) -> TradingPairSetting:
    return TradingPairSetting(
        symbol=settings.symbol.upper(),
        enabled=settings.enabled,
        min_wall_ratio=settings.min_wall_ratio,
        min_wall_notional_usdt=settings.min_wall_notional_usdt,
        max_wall_distance_bps=settings.max_wall_distance_bps,
        min_wall_age_seconds=settings.min_wall_age_seconds,
        consumed_ratio=settings.consumed_ratio,
        entry_distance_bps=settings.entry_distance_bps,
        stop_distance_bps=settings.stop_distance_bps,
        max_open_trades=settings.max_open_trades,
        bounce_reversal_bps=settings.bounce_reversal_bps,
        breakout_confirm_seconds=settings.breakout_confirm_seconds,
        breakout_buffer_bps=settings.breakout_buffer_bps,
        breakout_max_update_gap_seconds=settings.breakout_max_update_gap_seconds,
    )


def _trade_dict(row: VirtualTrade) -> dict:
    return {
        "signal_id": row.signal_id,
        "strategy_id": row.strategy_id,
        "symbol": row.symbol,
        "side": row.side,
        "wall_side": row.wall_side,
        "status": row.status,
        "wall_price": row.wall_price,
        "wall_size": row.wall_size,
        "wall_ratio": row.wall_ratio,
        "stake_usdt": row.stake_usdt,
        "entry_price": row.entry_price,
        "stop_loss": row.stop_loss,
        "take_profit": row.take_profit,
        "exit_price": row.exit_price,
        "profit_usdt": row.profit_usdt,
        "profit_percent": row.profit_percent,
        "close_reason": row.close_reason,
        "created_at": row.created_at,
        "opened_at": row.opened_at,
        "closed_at": row.closed_at,
    }


async def save_runtime_state(
    *,
    engine_mode: str,
    balance_usdt: float,
    trading_settings: TradingSettings,
    initialized: bool = True,
) -> None:
    async with async_session() as session:
        state = await session.get(RuntimeState, 1)
        if state is None:
            state = RuntimeState(
                id=1,
                balance_usdt=balance_usdt,
                initial_balance_usdt=trading_settings.initial_balance_usdt,
                max_open_trades=trading_settings.max_open_trades,
                stake_usdt=trading_settings.stake_usdt,
            )
            session.add(state)
        state.engine_mode = engine_mode
        state.balance_usdt = balance_usdt
        state.initial_balance_usdt = trading_settings.initial_balance_usdt
        state.stake_usdt = trading_settings.stake_usdt
        state.max_open_trades = trading_settings.max_open_trades
        state.initialized = initialized
        await session.commit()


async def save_trades(
    trades: list[dict],
    balance_usdt: float,
    engine_mode: str | None = None,
) -> None:
    if not trades and engine_mode is None:
        return
    async with async_session() as session:
        for trade in trades:
            row = await session.scalar(
                select(VirtualTrade).where(VirtualTrade.signal_id == trade["signal_id"])
            )
            values = {
                key: trade.get(key)
                for key in (
                    "signal_id", "strategy_id", "symbol", "side", "wall_side", "status",
                    "wall_price", "wall_size", "wall_ratio", "stake_usdt", "entry_price",
                    "stop_loss", "take_profit", "exit_price", "profit_usdt", "close_reason",
                    "profit_percent", "created_at", "opened_at", "closed_at",
                )
            }
            if row is None:
                session.add(VirtualTrade(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
        state = await session.get(RuntimeState, 1)
        if state is not None:
            state.balance_usdt = balance_usdt
            if engine_mode is not None:
                state.engine_mode = engine_mode
        await session.commit()


async def save_pair_settings(settings: PairSettings) -> None:
    async with async_session() as session:
        row = await session.get(TradingPairSetting, settings.symbol.upper())
        values = _pair_row(settings)
        if row is None:
            session.add(values)
        else:
            for key in (
                "enabled", "min_wall_ratio", "min_wall_notional_usdt", "max_wall_distance_bps", "min_wall_age_seconds",
                "consumed_ratio", "entry_distance_bps", "stop_distance_bps", "max_open_trades",
                "bounce_reversal_bps", "breakout_confirm_seconds", "breakout_buffer_bps",
                "breakout_max_update_gap_seconds",
            ):
                setattr(row, key, getattr(values, key))
        await session.commit()


async def save_enabled_strategies(symbol: str, strategy_ids: set[str]) -> None:
    symbol = normalize_pair_symbol(symbol)
    async with async_session() as session:
        row = await session.get(PairStrategyConfig, symbol)
        values = sorted(strategy_ids)
        if row is None:
            session.add(
                PairStrategyConfig(
                    symbol=symbol,
                    enabled_strategy_ids=values,
                )
            )
        else:
            row.enabled_strategy_ids = values
        await session.commit()


async def save_enabled_strategies_with_trades(
    symbol: str,
    strategy_ids: set[str],
    trades: list[dict],
    balance_usdt: float,
    engine_mode: str,
    admin_user_id: int | None = None,
    expected_version: int | None = None,
) -> None:
    """Commit a pair's strategy selection and related trade changes together."""
    symbol = normalize_pair_symbol(symbol)
    async with async_session() as session:
        row = await session.get(PairStrategyConfig, symbol)
        selected = sorted(strategy_ids)
        previous = sorted(row.enabled_strategy_ids) if row is not None else []
        current_version = int(getattr(row, "version", 1) or 1) if row is not None else 0
        if expected_version is not None and current_version != expected_version and previous != selected:
            raise RuntimeError("Strategy configuration changed; refresh the strategy panel")
        if row is None:
            session.add(
                PairStrategyConfig(
                    symbol=symbol,
                    enabled_strategy_ids=selected,
                    version=1,
                )
            )
        else:
            row.enabled_strategy_ids = selected
            if previous != selected:
                row.version = current_version + 1

        if previous != selected:
            reasons = {}
            for trade in trades:
                if (
                    trade.get("status") == "cancelled"
                    and trade.get("symbol", "").upper() == symbol
                    and trade.get("strategy_id") in set(previous).difference(selected)
                ):
                    reason = trade.get("close_reason") or "UNKNOWN"
                    reasons[reason] = reasons.get(reason, 0) + 1
            session.add(StrategyChangeEvent(
                symbol=symbol,
                previous_strategy=",".join(previous) or None,
                new_strategy=",".join(selected) or None,
                admin_user_id=admin_user_id,
                result="applied",
                cancelled_plans=sum(reasons.values()),
                cancellation_reasons=reasons,
            ))

        for trade in trades:
            row = await session.scalar(
                select(VirtualTrade).where(
                    VirtualTrade.signal_id == trade["signal_id"]
                )
            )
            values = {
                key: trade.get(key)
                for key in (
                    "signal_id", "strategy_id", "symbol", "side", "wall_side", "status",
                    "wall_price", "wall_size", "wall_ratio", "stake_usdt", "entry_price",
                    "stop_loss", "take_profit", "exit_price", "profit_usdt", "close_reason",
                    "profit_percent", "created_at", "opened_at", "closed_at",
                )
            }
            if row is None:
                session.add(VirtualTrade(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

        state = await session.get(RuntimeState, 1)
        if state is not None:
            state.balance_usdt = balance_usdt
            state.engine_mode = engine_mode
        await session.commit()


async def save_pair_settings_with_trades(
    settings: PairSettings,
    trades: list[dict],
    balance_usdt: float,
    engine_mode: str,
) -> None:
    """Commit pair settings and resulting trade state in one transaction."""
    async with async_session() as session:
        for trade in trades:
            row = await session.scalar(
                select(VirtualTrade).where(VirtualTrade.signal_id == trade["signal_id"])
            )
            values = {
                key: trade.get(key)
                for key in (
                    "signal_id", "strategy_id", "symbol", "side", "wall_side", "status",
                    "wall_price", "wall_size", "wall_ratio", "stake_usdt", "entry_price",
                    "stop_loss", "take_profit", "exit_price", "profit_usdt", "close_reason",
                    "profit_percent", "created_at", "opened_at", "closed_at",
                )
            }
            if row is None:
                session.add(VirtualTrade(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

        pair_row = await session.get(TradingPairSetting, settings.symbol.upper())
        values = _pair_row(settings)
        if pair_row is None:
            session.add(values)
        else:
            for key in (
                "enabled", "min_wall_ratio", "min_wall_notional_usdt", "max_wall_distance_bps",
                "min_wall_age_seconds", "consumed_ratio", "entry_distance_bps",
                "stop_distance_bps", "max_open_trades", "bounce_reversal_bps",
                "breakout_confirm_seconds", "breakout_buffer_bps",
                "breakout_max_update_gap_seconds",
            ):
                setattr(pair_row, key, getattr(values, key))

        state = await session.get(RuntimeState, 1)
        if state is not None:
            state.balance_usdt = balance_usdt
            state.engine_mode = engine_mode
        await session.commit()


async def save_trading_settings(settings: TradingSettings) -> None:
    async with async_session() as session:
        state = await session.get(RuntimeState, 1)
        if state is None:
            state = RuntimeState(
                id=1,
                balance_usdt=settings.initial_balance_usdt,
                initial_balance_usdt=settings.initial_balance_usdt,
                stake_usdt=settings.stake_usdt,
                max_open_trades=settings.max_open_trades,
            )
            session.add(state)
        else:
            state.stake_usdt = settings.stake_usdt
            state.initial_balance_usdt = settings.initial_balance_usdt
            state.max_open_trades = settings.max_open_trades
        await session.commit()


async def save_market_price(symbol: str, last_price: float) -> None:
    async with async_session() as session:
        row = await session.get(MarketPrice, symbol.upper())
        if row is None:
            row = MarketPrice(symbol=symbol.upper(), last_price=last_price)
            session.add(row)
        else:
            row.last_price = last_price
            row.observed_at = utc_now()
        await session.commit()
