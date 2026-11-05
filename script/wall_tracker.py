import json
import logging
import time
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


def wall_lifecycle_id(symbol, side, price, first_seen_at):
    return f"{str(symbol).upper()}:{side}:{float(price):.12g}:{first_seen_at or 'unknown'}"


def log_stable_wall_transition(
    *, symbol, generation, wall, previous_status, new_status, reason,
    previous_notional=None, current_notional=None,
    previous_median=None, current_median=None,
    previous_ratio=None, current_ratio=None,
    min_wall_ratio=None, min_wall_notional_usdt=None,
    previous_distance_bps=None, current_distance_bps=None,
    max_wall_distance_bps=None, stable_age_seconds=None,
    active_pending=None, reason_detail=None,
):
    """Emit one structured diagnostic record for a lost STABLE wall."""
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "symbol": str(symbol).upper() if symbol else None,
        "websocket_generation": generation,
        "wall_id": wall_lifecycle_id(
            symbol, wall.get("side"), wall.get("price"), wall.get("first_seen_at")
        ) if symbol else None,
        "side": "BID" if wall.get("side") == "buy" else "ASK",
        "price": wall.get("price"),
        "previous_status": str(previous_status).upper(),
        "new_status": str(new_status).upper(),
        "reason": reason,
        "reason_detail": reason_detail,
        "previous_notional_usdt": previous_notional,
        "current_notional_usdt": current_notional,
        "previous_side_median_usdt": previous_median,
        "current_side_median_usdt": current_median,
        "previous_wall_ratio": previous_ratio,
        "current_wall_ratio": current_ratio,
        "min_wall_ratio": min_wall_ratio,
        "min_wall_notional_usdt": min_wall_notional_usdt,
        "previous_distance_bps": previous_distance_bps,
        "current_distance_bps": current_distance_bps,
        "max_wall_distance_bps": max_wall_distance_bps,
        "stable_age_seconds": stable_age_seconds,
        "active_pending": active_pending,
    }
    logger.warning(
        "wall_state_transition=%s",
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
    )


class WallTracker:
    def __init__(
        self,
        min_age_seconds=20,
        consumed_ratio=0.9,
    ):
        self.min_age_seconds = min_age_seconds
        self.consumed_ratio = consumed_ratio



        self.walls = {}
        self._diagnostic_distance_bps = {}
        self._restored_lifecycle_identities = {}
        self.qualification_transitions = {"lost": 0, "restored": 0}

    def seed_lifecycle_identities(self, identities):
        """Reuse persisted identity on first sight of a wall after restart.

        Stability age still starts at zero after a new feed connection. If the
        wall disappears, the saved identity is consumed and a later lifecycle
        at the same price receives a new identity.
        """
        self._restored_lifecycle_identities = {
            (side, float(price)): str(identity)
            for (side, price), identity in identities.items()
        }

    def update(
        self,
        detected_walls,
        orderbook,
        trade_flow,
        *,
        diagnostic_context=None,
    ):
        now = time.monotonic()
        diagnostic_context = diagnostic_context or {}

        events = []

        # ---------------------------------------------
        # Какие стены прямо сейчас проходят 888x
        # ---------------------------------------------

        current_candidates = {
            (wall["side"], wall["price"]): wall
            for wall in detected_walls
        }

        # ---------------------------------------------
        # 1. Добавляем новые стены-кандидаты
        # ---------------------------------------------

        for wall in detected_walls:

            key = (
                wall["side"],
                wall["price"],
            )

            if key in self.walls:
                continue

            self.walls[key] = {
                "price": wall["price"],
                "side": wall["side"],

                "initial_size": wall["notional"],
                "current_size": wall["notional"],
                "wall_ratio": wall.get("wall_ratio", 0.0),
                "median_notional": wall.get("median_notional"),
                "qualifies": True,

                "first_seen": now,
                "first_seen_at": self._restored_lifecycle_identities.pop(
                    key, datetime.now(timezone.utc).isoformat()
                ),
                "last_seen": now,

                "aggressive_volume": 0,
                "last_relevant_total": (
                    trade_flow.get("sell_volume_by_price_total", {}).get(wall["price"], 0)
                    if wall["side"] == "buy"
                    else trade_flow.get("buy_volume_by_price_total", {}).get(wall["price"], 0)
                ),

                "status": "candidate",
            }
            self._diagnostic_distance_bps[key] = self._distance_bps(
                wall["price"], diagnostic_context.get("last_price")
            )

        # ---------------------------------------------
        # 2. Проверяем все отслеживаемые стены
        # ---------------------------------------------

        for key, wall in list(self.walls.items()):

            # -----------------------------------------
            # Сохраняем стену ниже порога, пока уровень есть в стакане.
            # -----------------------------------------

            # -----------------------------------------
            # Получаем текущий размер стены
            # -----------------------------------------

            previous_size = wall.get("current_size")
            previous_status = wall.get("status", "candidate")
            previous_distance_bps = self._diagnostic_distance_bps.get(key)
            previous_ratio = wall.get("wall_ratio")
            previous_median = wall.get("median_notional")
            current_size = self._get_current_wall_size(
                wall,
                orderbook,
            )

            # -----------------------------------------
            # Стена полностью исчезла из стакана
            # -----------------------------------------

            if current_size is None:

                age = now - wall["first_seen"]
                if str(previous_status).lower() == "stable":
                    self._log_stable_loss(
                        wall, previous_status, "removed", "WALL_REMOVED",
                        previous_size, None, previous_median, previous_ratio,
                        previous_distance_bps,
                        self._distance_bps(
                            wall["price"], diagnostic_context.get("last_price")
                        ),
                        age, diagnostic_context,
                        "Уровень отсутствует в синхронизированном локальном стакане.",
                    )
                current_relevant_total = self._relevant_total(wall, trade_flow)
                flow_delta = self._flow_delta(
                    current_relevant_total, wall["last_relevant_total"]
                )
                wall["aggressive_volume"] += flow_delta
                wall["last_relevant_total"] = current_relevant_total
                wall["current_size"] = 0

                # Stability controls whether a wall can signal a trade, but every
                # tracked disappearance still has a lifecycle event.
                event = self._classify_disappearance(
                    wall,
                    age,
                )

                events.append(event)

                del self.walls[key]
                self._diagnostic_distance_bps.pop(key, None)

                continue

            # -----------------------------------------
            # Стена существует и всё ещё проходит 888x
            # -----------------------------------------

            wall["current_size"] = current_size
            wall["last_seen"] = now
            candidate = current_candidates.get(key)
            was_qualified = bool(wall.get("qualifies", True))
            is_qualified = candidate is not None
            if was_qualified and not is_qualified:
                self.qualification_transitions["lost"] += 1
                if str(previous_status).lower() == "stable":
                    self._log_stable_loss(
                        wall, previous_status, "candidate", None,
                        previous_size, current_size, previous_median, previous_ratio,
                        previous_distance_bps,
                        self._distance_bps(
                            wall["price"], diagnostic_context.get("last_price")
                        ),
                        max(0.0, now - wall["first_seen"]), diagnostic_context,
                    )
                # The only stability clock is the continuous Scanner-qualified
                # period. A ratio/notional/distance miss ends it immediately.
                wall["first_seen"] = now
                wall["status"] = "candidate"
            elif not was_qualified and is_qualified:
                self.qualification_transitions["restored"] += 1
                # Requalification starts a new full period, even when the exact
                # price level remained present in the local book.
                wall["first_seen"] = now
                wall["status"] = "candidate"
            wall["qualifies"] = is_qualified
            self._diagnostic_distance_bps[key] = self._distance_bps(
                wall["price"], diagnostic_context.get("last_price")
            )
            if candidate is not None:
                wall["wall_ratio"] = candidate.get("wall_ratio", wall["wall_ratio"])
                wall["median_notional"] = candidate.get(
                    "median_notional", wall["median_notional"]
                )

            # -----------------------------------------
            # Получаем текущий cumulative flow
            # -----------------------------------------

            current_relevant_total = self._relevant_total(wall, trade_flow)

            # -----------------------------------------
            # Сколько нового aggressive flow появилось
            # -----------------------------------------

            flow_delta = self._flow_delta(
                current_relevant_total, wall["last_relevant_total"]
            )

            # Обновляем точку отсчёта
            wall["last_relevant_total"] = (
                current_relevant_total
            )

            # -----------------------------------------
            # Trade and orderbook WebSocket messages can arrive in either order.
            # Count new exact-price aggressive flow independently of the book
            # delta so a delayed trade message is not lost.
            wall["aggressive_volume"] += flow_delta

            # -----------------------------------------
            # Проверяем возраст стены
            # -----------------------------------------

            age = now - wall["first_seen"]

            if (
                wall["status"] == "candidate"
                and age >= self.min_age_seconds
            ):
                wall["status"] = "stable"

        return events

    @staticmethod
    def _distance_bps(price, last_price):
        try:
            last = float(last_price)
            if last <= 0:
                return None
            return abs(float(price) - last) / last * 10000
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def _log_stable_loss(
        wall, previous_status, new_status, forced_reason,
        previous_notional, current_notional, previous_median, previous_ratio,
        previous_distance_bps, current_distance_bps, stable_age_seconds,
        diagnostic_context, reason_detail=None,
    ):
        side = wall.get("side")
        side_medians = diagnostic_context.get("side_medians") or {}
        current_median = side_medians.get(side)
        min_ratio = diagnostic_context.get("min_wall_ratio")
        min_notional = diagnostic_context.get("min_wall_notional_usdt")
        max_distance = diagnostic_context.get("max_wall_distance_bps")
        current_ratio = None
        try:
            if current_notional is not None and current_median is not None and float(current_median) > 0:
                current_ratio = float(current_notional) / float(current_median)
        except (TypeError, ValueError, ZeroDivisionError):
            current_ratio = None

        reason = forced_reason
        if reason is None:
            required_diagnostics = {
                "last_price", "min_wall_ratio", "min_wall_notional_usdt",
                "max_wall_distance_bps", "side_medians",
            }
            if (
                not required_diagnostics.issubset(diagnostic_context)
                or diagnostic_context.get("last_price") is None
                or min_ratio is None or min_notional is None or max_distance is None
            ):
                reason = "UNKNOWN"
                reason_detail = "Контекст Scanner неполон для установления причины потери квалификации."
            elif current_distance_bps is None:
                reason = "UNKNOWN"
                reason_detail = "Невозможно вычислить текущую дистанцию стены до цены."
            elif current_distance_bps is not None and max_distance is not None and current_distance_bps > float(max_distance):
                reason = "OUT_OF_SCANNER_RANGE"
                reason_detail = "Дистанция текущего уровня выше max_wall_distance_bps."
            elif current_notional is not None and min_notional is not None and float(current_notional) < float(min_notional):
                reason = "WALL_NOTIONAL_FAILED"
                reason_detail = "Текущий notional ниже min_wall_notional_usdt."
            elif current_median is None or float(current_median) <= 0:
                reason = "MEDIAN_UNAVAILABLE"
                reason_detail = "Для текущего снимка Scanner не получил медиану этой стороны."
            elif current_ratio is not None and min_ratio is not None and current_ratio < float(min_ratio):
                reason = "WALL_RATIO_FAILED"
                reason_detail = "Текущий wall_ratio ниже min_wall_ratio."
            else:
                reason = "OTHER"
                reason_detail = (
                    "Scanner не передал уровень, но доступных значений недостаточно "
                    "для установления конкретного фильтра."
                )

        symbol = diagnostic_context.get("symbol")
        identity = wall_lifecycle_id(
            symbol, side, wall.get("price"), wall.get("first_seen_at")
        ) if symbol else None
        pending_ids = diagnostic_context.get("active_pending_wall_ids")
        log_stable_wall_transition(
            symbol=symbol,
            generation=diagnostic_context.get("connection_generation"),
            wall=wall,
            previous_status=previous_status,
            new_status=new_status,
            reason=reason,
            previous_notional=previous_notional,
            current_notional=current_notional,
            previous_median=previous_median,
            current_median=current_median,
            previous_ratio=previous_ratio,
            current_ratio=current_ratio,
            min_wall_ratio=min_ratio,
            min_wall_notional_usdt=min_notional,
            previous_distance_bps=previous_distance_bps,
            current_distance_bps=current_distance_bps,
            max_wall_distance_bps=max_distance,
            stable_age_seconds=stable_age_seconds,
            active_pending=(identity in pending_ids) if pending_ids is not None and identity else None,
            reason_detail=reason_detail,
        )

    # =================================================
    # Получение текущего размера стены
    # =================================================

    @staticmethod
    def _get_current_wall_size(
        wall,
        orderbook,
    ):
        price = wall["price"]

        if wall["side"] == "buy":
            quantity = orderbook.bids.get(price)

        else:
            quantity = orderbook.asks.get(price)

        if quantity is None:
            return None

        return price * quantity

    # =================================================
    # Определение исчезновения стены
    # =================================================

    def _classify_disappearance(
        self,
        wall,
        age,
    ):
        remaining_size = wall["current_size"]

        consumed_size = (
            wall["initial_size"] - remaining_size
        )

        aggressive_volume = wall["aggressive_volume"]

        if consumed_size <= 0:
            event_type = "REMOVED"

        elif aggressive_volume >= (
            consumed_size * self.consumed_ratio
        ):
            event_type = "CONSUMED"

        else:
            event_type = "REMOVED"

        return {
            **wall,

            "event": event_type,
            "age": age,

            "consumed_size": consumed_size,
            "aggressive_volume": aggressive_volume,
        }

    # =================================================
    # Какой flow должен съедать стену
    # =================================================

    @staticmethod
    def _relevant_total(wall, trade_flow):
        if wall["side"] == "buy":
            by_price = trade_flow.get("sell_volume_by_price_total")
            return by_price.get(wall["price"], 0) if by_price is not None else trade_flow.get("sell_volume_total", trade_flow.get("sell_volume", 0))
        by_price = trade_flow.get("buy_volume_by_price_total")
        return by_price.get(wall["price"], 0) if by_price is not None else trade_flow.get("buy_volume_total", trade_flow.get("buy_volume", 0))

    @staticmethod
    def _flow_delta(current_total, previous_total):
        # Per-price totals can restart at zero after their bounded cache evicts a key.
        return current_total - previous_total if current_total >= previous_total else current_total

    # =================================================
    # Получить стабильные стены
    # =================================================

    def get_stable_walls(self):
        now = time.monotonic()

        stable = []

        for wall in self.walls.values():

            if wall["status"] != "stable" or not wall.get("qualifies", True):
                continue

            age = now - wall["first_seen"]

            stable.append({
                **wall,
                "age": age,
            })

        return stable

    # =================================================
    # Получить ближайшие стабильные стены
    # =================================================

    def get_nearest_stable_walls(
        self,
        last_price,
        limit=None,
    ):
        if last_price is None:
            return []

        stable = self.get_stable_walls()

        for wall in stable:

            wall["distance"] = abs(
                wall["price"] - last_price
            )

            wall["distance_bps"] = (
                abs(wall["price"] - last_price)
                / last_price
                * 10000
            )

        stable.sort(
            key=lambda wall: wall["distance"]
        )

        if limit is not None:
            stable = stable[:limit]

        return stable
