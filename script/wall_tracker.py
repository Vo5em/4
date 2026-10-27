import time
from datetime import datetime, timezone


class WallTracker:
    def __init__(
        self,
        min_age_seconds=20,
        consumed_ratio=0.9,
    ):
        self.min_age_seconds = min_age_seconds
        self.consumed_ratio = consumed_ratio



        self.walls = {}

    def update(
        self,
        detected_walls,
        orderbook,
        trade_flow,
    ):
        now = time.monotonic()

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
                "first_seen_at": datetime.now(timezone.utc).isoformat(),
                "last_seen": now,

                "aggressive_volume": 0,
                "last_relevant_total": (
                    trade_flow.get("sell_volume_by_price_total", {}).get(wall["price"], 0)
                    if wall["side"] == "buy"
                    else trade_flow.get("buy_volume_by_price_total", {}).get(wall["price"], 0)
                ),

                "status": "candidate",
            }

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

            current_size = self._get_current_wall_size(
                wall,
                orderbook,
            )

            # -----------------------------------------
            # Стена полностью исчезла из стакана
            # -----------------------------------------

            if current_size is None:

                age = now - wall["first_seen"]
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

                continue

            # -----------------------------------------
            # Стена существует и всё ещё проходит 888x
            # -----------------------------------------

            wall["current_size"] = current_size
            wall["last_seen"] = now
            candidate = current_candidates.get(key)
            wall["qualifies"] = candidate is not None
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
