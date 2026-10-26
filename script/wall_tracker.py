import time


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
            (
                wall["side"],
                wall["price"],
            )
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

                "first_seen": now,
                "last_seen": now,

                "aggressive_volume": 0,
                "last_relevant_total": (
                    trade_flow["sell_volume_total"]
                    if wall["side"] == "buy"
                    else trade_flow["buy_volume_total"]
                ),

                "status": "candidate",
            }

        # ---------------------------------------------
        # 2. Проверяем все отслеживаемые стены
        # ---------------------------------------------

        for key, wall in list(self.walls.items()):

            # -----------------------------------------
            # Стена больше НЕ проходит 888x
            # -----------------------------------------

            if key not in current_candidates:

                del self.walls[key]
                continue

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

                # Если не прожила 20 секунд —
                # это просто исчезнувший кандидат
                if age < self.min_age_seconds:
                    del self.walls[key]
                    continue

                # Если прожила 20+ секунд —
                # классифицируем исчезновение
                event = self._classify_disappearance(
                    wall,
                    trade_flow,
                    age,
                )

                events.append(event)

                del self.walls[key]

                continue

            # -----------------------------------------
            # Стена существует и всё ещё проходит 888x
            # -----------------------------------------

            previous_size = wall["current_size"]

            wall["current_size"] = current_size
            wall["last_seen"] = now

            # -----------------------------------------
            # Отслеживаем уменьшение стены
            # -----------------------------------------

            size_change = previous_size - current_size

            # -----------------------------------------
            # Получаем текущий cumulative flow
            # -----------------------------------------

            if wall["side"] == "buy":
                current_relevant_total = (
                    trade_flow["sell_volume_total"]
                )
            else:
                current_relevant_total = (
                    trade_flow["buy_volume_total"]
                )

            # -----------------------------------------
            # Сколько нового aggressive flow появилось
            # -----------------------------------------

            flow_delta = (
                    current_relevant_total
                    - wall["last_relevant_total"]
            )

            # Обновляем точку отсчёта
            wall["last_relevant_total"] = (
                current_relevant_total
            )

            # Защита от отрицательного значения
            if flow_delta < 0:
                flow_delta = 0

            # -----------------------------------------
            # Если стена уменьшилась —
            # учитываем только новый flow
            # -----------------------------------------

            if size_change > 0:
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
        trade_flow,
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
    def _get_relevant_flow(
        side,
        trade_flow,
    ):
        if side == "buy":
            return trade_flow["sell_volume"]

        return trade_flow["buy_volume"]

    # =================================================
    # Получить стабильные стены
    # =================================================

    def get_stable_walls(self):
        now = time.monotonic()

        stable = []

        for wall in self.walls.values():

            if wall["status"] != "stable":
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