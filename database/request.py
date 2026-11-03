from datetime import datetime, timedelta, timezone

from sqlalchemy import MetaData, Table, and_, delete, exists, func, inspect, or_, select

from database.models import (
    MarketPrice,
    ManualPlanBlock,
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
from config import (
    CANCELLED_TRADE_CLEANUP_BATCH_SIZE,
    CANCELLED_TRADE_RETENTION_MINUTES,
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
                default_leverage=default_trading_settings.leverage,
                default_take_profit_bps=default_trading_settings.default_take_profit_bps,
                default_stop_loss_bps=default_trading_settings.default_stop_loss_bps,
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
        market_price_rows = (await session.scalars(select(MarketPrice))).all()
        manual_plan_block_rows = (await session.scalars(select(ManualPlanBlock))).all()
        # Cancelled plans are retained in PostgreSQL for the configured audit
        # period, but do not affect runtime balance or statistics. Load only
        # currently active suppression rows from that status, avoiding a large
        # cancelled-history scan in TradeManager after a restart.
        cancelled_retention_cutoff = utc_now() - timedelta(
            minutes=CANCELLED_TRADE_RETENTION_MINUTES
        )
        trade_rows = (await session.scalars(
            select(VirtualTrade).where(
                or_(
                    VirtualTrade.status != "cancelled",
                    and_(
                        VirtualTrade.status == "cancelled",
                        VirtualTrade.closed_at >= cancelled_retention_cutoff,
                    ),
                    VirtualTrade.signal_id.in_(
                        select(ManualPlanBlock.signal_id)
                    ),
                )
            )
        )).all()
        cancelled_trade_count = int(await session.scalar(
            select(func.count())
            .select_from(VirtualTrade)
            .where(VirtualTrade.status == "cancelled")
        ) or 0)
        if getattr(state, "cancelled_trade_count", 0) != cancelled_trade_count:
            state.cancelled_trade_count = cancelled_trade_count
            await session.commit()
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
                leverage=getattr(state, "default_leverage", 25.0) or 25.0,
                default_take_profit_bps=getattr(
                    state, "default_take_profit_bps", 5.0
                ) or 5.0,
                default_stop_loss_bps=getattr(
                    state, "default_stop_loss_bps", 5.0
                ) or 5.0,
            ),
            "balance_usdt": state.balance_usdt,
            "cancelled_trade_count": cancelled_trade_count,
            "last_cancelled_cleanup_at": state.last_cancelled_cleanup_at,
            "last_cancelled_cleanup_deleted": int(
                getattr(state, "last_cancelled_cleanup_deleted", 0) or 0
            ),
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
                    leverage=getattr(row, "leverage", None),
                    maker_fee_rate=(
                        getattr(row, "maker_fee_rate", None)
                        if getattr(row, "maker_fee_rate", None) is not None else 0.00036
                    ),
                    taker_fee_rate=(
                        getattr(row, "taker_fee_rate", None)
                        if getattr(row, "taker_fee_rate", None) is not None else 0.001
                    ),
                    take_profit_bps=getattr(row, "take_profit_bps", None),
                    stop_loss_bps=getattr(row, "stop_loss_bps", None),
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
            "manual_plan_blocks": [
                {
                    "signal_id": row.signal_id,
                    "symbol": row.symbol,
                    "strategy_id": row.strategy_id,
                    "wall_id": row.wall_id,
                    "wall_side": row.wall_side,
                    "wall_price": row.wall_price,
                }
                for row in manual_plan_block_rows
            ],
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
        leverage=settings.leverage,
        maker_fee_rate=settings.maker_fee_rate,
        taker_fee_rate=settings.taker_fee_rate,
        take_profit_bps=settings.take_profit_bps,
        stop_loss_bps=settings.stop_loss_bps,
    )


def _trade_dict(row: VirtualTrade) -> dict:
    saved_fee_rate = getattr(row, "fee_rate", None)
    stored_taker_rate = getattr(row, "taker_fee_rate", None)
    stored_applied_rate = getattr(row, "applied_fee_rate", None)
    taker_rate = stored_taker_rate if stored_taker_rate is not None else saved_fee_rate
    applied_rate = stored_applied_rate if stored_applied_rate is not None else saved_fee_rate
    return {
        "signal_id": row.signal_id,
        "strategy_id": row.strategy_id,
        "symbol": row.symbol,
        "side": row.side,
        "wall_side": row.wall_side,
        "wall_id": getattr(row, "wall_id", None),
        "status": row.status,
        "wall_price": row.wall_price,
        "wall_size": row.wall_size,
        "wall_ratio": row.wall_ratio,
        "stake_usdt": row.stake_usdt,
        "leverage": getattr(row, "leverage", None),
        "position_notional_usdt": getattr(row, "position_notional_usdt", None),
        "entry_fee_usdt": getattr(row, "entry_fee_usdt", None),
        "exit_fee_usdt": getattr(row, "exit_fee_usdt", None),
        "gross_pnl_usdt": getattr(row, "gross_pnl_usdt", None),
        "maker_fee_rate": getattr(row, "maker_fee_rate", None),
        "taker_fee_rate": taker_rate if taker_rate is not None else 0.0,
        "applied_fee_rate": applied_rate,
        "commission_model": getattr(row, "commission_model", None) or "LEGACY_SAVED_RATE",
        "fee_rate": saved_fee_rate,
        "accounting_version": getattr(row, "accounting_version", None) or 1,
        # Older rows have no accounting metadata; keep their recorded PnL and
        # allow TradeManager's legacy stake-as-notional/zero-fee fallback.
        "entry_fee": getattr(row, "entry_fee_usdt", None) or 0.0,
        "exit_fee": getattr(row, "exit_fee_usdt", None) or 0.0,
        "gross_pnl": getattr(row, "gross_pnl_usdt", None),
        "entry_price": row.entry_price,
        "entry_fill_price": getattr(row, "entry_fill_price", None),
        "entry_trigger_direction": getattr(row, "entry_trigger_direction", None),
        "entry_armed": bool(getattr(row, "entry_armed", False)),
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


_VIRTUAL_TRADE_FIELDS = (
    "signal_id", "strategy_id", "symbol", "side", "wall_side", "status",
    "wall_price", "wall_size", "wall_ratio", "stake_usdt", "entry_price",
    "leverage", "position_notional_usdt", "entry_fee_usdt", "exit_fee_usdt",
    "gross_pnl_usdt", "maker_fee_rate", "taker_fee_rate", "applied_fee_rate",
    "commission_model", "fee_rate", "accounting_version", "stop_loss",
    "take_profit", "entry_fill_price", "wall_id", "entry_trigger_direction",
    "entry_armed", "exit_price", "profit_usdt", "close_reason",
    "profit_percent", "created_at", "opened_at", "closed_at",
)


def _virtual_trade_values(trade: dict, *, is_insert: bool = False) -> dict:
    """Build ORM values without turning an omitted partial field into NULL."""
    values = {key: trade[key] for key in _VIRTUAL_TRADE_FIELDS if key in trade}
    if values.get("accounting_version") is None:
        # `None` is not a valid accounting schema version. On UPDATE it means
        # the caller has no version to change; on INSERT use the current model.
        values.pop("accounting_version", None)
    if is_insert and "accounting_version" not in values:
        # Version 1 is the project's current margin/fee accounting model.
        values["accounting_version"] = 1
    return values


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
                default_leverage=trading_settings.leverage,
                default_take_profit_bps=trading_settings.default_take_profit_bps,
                default_stop_loss_bps=trading_settings.default_stop_loss_bps,
                stake_usdt=trading_settings.stake_usdt,
            )
            session.add(state)
        state.engine_mode = engine_mode
        state.balance_usdt = balance_usdt
        state.initial_balance_usdt = trading_settings.initial_balance_usdt
        state.stake_usdt = trading_settings.stake_usdt
        state.max_open_trades = trading_settings.max_open_trades
        state.default_leverage = trading_settings.leverage
        state.default_take_profit_bps = trading_settings.default_take_profit_bps
        state.default_stop_loss_bps = trading_settings.default_stop_loss_bps
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
            values = _virtual_trade_values(trade, is_insert=row is None)
            if row is None:
                session.add(VirtualTrade(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
            if trade.get("status") == "cancelled" and trade.get("close_reason") == "MANUAL_CANCELLED":
                block = await session.get(ManualPlanBlock, trade["signal_id"])
                if block is None:
                    session.add(ManualPlanBlock(
                        signal_id=trade["signal_id"],
                        symbol=trade["symbol"],
                        strategy_id=trade["strategy_id"],
                        wall_id=trade.get("wall_id"),
                        wall_side=trade["wall_side"],
                        wall_price=trade["wall_price"],
                    ))
        state = await session.get(RuntimeState, 1)
        if state is not None:
            state.balance_usdt = balance_usdt
            if engine_mode is not None:
                state.engine_mode = engine_mode
        await session.commit()


async def release_manual_plan_blocks(signal_ids: list[str]) -> None:
    """Remove active suppressions while preserving their cancelled trade history."""
    if not signal_ids:
        return
    async with async_session() as session:
        await session.execute(
            delete(ManualPlanBlock).where(ManualPlanBlock.signal_id.in_(signal_ids))
        )
        await session.commit()


async def cleanup_cancelled_trades(
    *,
    older_than_minutes: int = CANCELLED_TRADE_RETENTION_MINUTES,
    batch_size: int = CANCELLED_TRADE_CLEANUP_BATCH_SIZE,
    exclude_signal_ids: set[str] | None = None,
    now: datetime | None = None,
) -> dict:
    """Delete one bounded batch of old cancelled plans, preserving active blocks.

    Row locks and SKIP LOCKED avoid waiting on concurrent trade updates. The
    select and delete share one transaction; any failure rolls back on session
    exit. MANUAL_CANCELLED rows referenced by ManualPlanBlock remain durable.
    """
    if older_than_minutes <= 0 or batch_size <= 0:
        raise ValueError("Cleanup retention and batch size must be positive")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cutoff = now - timedelta(minutes=older_than_minutes)
    excluded = set(exclude_signal_ids or ())
    criteria = [
        VirtualTrade.status == "cancelled",
        VirtualTrade.closed_at.is_not(None),
        VirtualTrade.closed_at < cutoff,
        ~exists(
            select(1).where(ManualPlanBlock.signal_id == VirtualTrade.signal_id)
        ),
    ]
    if excluded:
        criteria.append(VirtualTrade.signal_id.not_in(excluded))

    async with async_session() as session:
        candidates = await session.scalars(
            select(VirtualTrade.signal_id)
            .where(*criteria)
            .order_by(VirtualTrade.closed_at, VirtualTrade.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        signal_ids = list(candidates.all())
        if signal_ids:
            await session.execute(
                delete(VirtualTrade).where(VirtualTrade.signal_id.in_(signal_ids))
            )
        remaining_count = int(await session.scalar(
            select(func.count())
            .select_from(VirtualTrade)
            .where(VirtualTrade.status == "cancelled")
        ) or 0)
        state = await session.get(RuntimeState, 1)
        if state is not None:
            state.cancelled_trade_count = remaining_count
            state.last_cancelled_cleanup_at = now
            state.last_cancelled_cleanup_deleted = len(signal_ids)
        await session.commit()
        return {
            "deleted_signal_ids": signal_ids,
            "cancelled_trade_count": remaining_count,
            "last_cleanup_at": now,
            "deleted_count": len(signal_ids),
        }


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
                "leverage", "maker_fee_rate", "taker_fee_rate",
                "take_profit_bps", "stop_loss_bps",
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
                    version=1,
                )
            )
        else:
            if row.enabled_strategy_ids != values:
                row.enabled_strategy_ids = values
                row.version = int(getattr(row, "version", 1) or 1) + 1
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
        if expected_version is not None and current_version != expected_version:
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
            values = _virtual_trade_values(trade, is_insert=row is None)
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
            values = _virtual_trade_values(trade, is_insert=row is None)
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
                "leverage", "maker_fee_rate", "taker_fee_rate",
                "take_profit_bps", "stop_loss_bps",
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
                default_leverage=settings.leverage,
                default_take_profit_bps=settings.default_take_profit_bps,
                default_stop_loss_bps=settings.default_stop_loss_bps,
            )
            session.add(state)
        else:
            state.stake_usdt = settings.stake_usdt
            state.initial_balance_usdt = settings.initial_balance_usdt
            state.max_open_trades = settings.max_open_trades
            state.default_leverage = settings.leverage
            state.default_take_profit_bps = settings.default_take_profit_bps
            state.default_stop_loss_bps = settings.default_stop_loss_bps
        await session.commit()


async def save_trading_settings_with_trades(
    settings: TradingSettings,
    trades: list[dict],
    balance_usdt: float,
    engine_mode: str,
) -> None:
    """Atomically save global TP/SL defaults and re-planned trade rows."""
    async with async_session() as session:
        state = await session.get(RuntimeState, 1)
        if state is None:
            state = RuntimeState(
                id=1,
                balance_usdt=balance_usdt,
                initial_balance_usdt=settings.initial_balance_usdt,
                stake_usdt=settings.stake_usdt,
                max_open_trades=settings.max_open_trades,
                default_leverage=settings.leverage,
                default_take_profit_bps=settings.default_take_profit_bps,
                default_stop_loss_bps=settings.default_stop_loss_bps,
                engine_mode=engine_mode,
            )
            session.add(state)
        else:
            state.balance_usdt = balance_usdt
            state.initial_balance_usdt = settings.initial_balance_usdt
            state.stake_usdt = settings.stake_usdt
            state.max_open_trades = settings.max_open_trades
            state.default_leverage = settings.leverage
            state.default_take_profit_bps = settings.default_take_profit_bps
            state.default_stop_loss_bps = settings.default_stop_loss_bps
            state.engine_mode = engine_mode

        for trade in trades:
            row = await session.scalar(
                select(VirtualTrade).where(VirtualTrade.signal_id == trade["signal_id"])
            )
            values = _virtual_trade_values(trade, is_insert=row is None)
            if row is None:
                session.add(VirtualTrade(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
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
