import asyncio
import json

import aiohttp
import websockets

from orderbook import LocalOrderBook
from scanner import find_nearby_levels, find_walls
from trade_flow import TradeFlow
from wall_tracker import WallTracker


BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_URL = "https://api.bybit.com/v5/market/orderbook"

SYMBOL = "BTCUSDT"
CATEGORY = "linear"


async def get_orderbook_snapshot():
    params = {
        "category": CATEGORY,
        "symbol": SYMBOL,
        "limit": 1000,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(
            BYBIT_REST_URL,
            params=params
        ) as response:
            data = await response.json()

    if data["retCode"] != 0:
        raise RuntimeError(
            f"Ошибка Bybit REST: {data}"
        )

    return data["result"]


def find_first_delta_after_snapshot(snapshot, buffer):
    snapshot_seq = snapshot["seq"]

    for delta in buffer:
        if delta["seq"] > snapshot_seq:
            return delta

        if delta["seq"] == snapshot_seq:
            if delta["u"] == snapshot["u"]:
                return delta

            return None

    return None


async def main():
    buffer = []
    last_price = None

    trade_flow = TradeFlow(
        window_seconds=10
    )

    wall_tracker = WallTracker(
        min_age_seconds=20,
        consumed_ratio=0.9,
    )

    # Начальное состояние потока сделок.
    flow = trade_flow.get_flow()

    async with websockets.connect(
        BYBIT_WS_URL
    ) as websocket:

        subscribe_message = {
            "op": "subscribe",
            "args": [
                f"orderbook.full.{SYMBOL}",
                f"publicTrade.{SYMBOL}"
            ]
        }

        await websocket.send(
            json.dumps(subscribe_message)
        )

        print(
            f"WebSocket подключён. "
            f"Слушаем {SYMBOL}...\n"
        )

        # -------------------------------------------------
        # ПОЛУЧАЕМ WEBSOCKET DELTA ДО REST SNAPSHOT
        # -------------------------------------------------

        snapshot_task = asyncio.create_task(
            get_orderbook_snapshot()
        )

        while not snapshot_task.done():
            message = await websocket.recv()
            data = json.loads(message)

            if data.get("topic") != (
                f"orderbook.full.{SYMBOL}"
            ):
                continue

            delta = data["data"]

            buffer.append(delta)

            print(
                f"Buffer: {len(buffer)} | "
                f"u={delta['u']} | "
                f"seq={delta['seq']}"
            )

        # -------------------------------------------------
        # REST SNAPSHOT ГОТОВ
        # -------------------------------------------------

        snapshot = await snapshot_task

        print("\nREST SNAPSHOT ПОЛУЧЕН:")
        print(
            f"u={snapshot['u']} | "
            f"seq={snapshot['seq']}"
        )

        print(
            f"Накоплено delta: {len(buffer)}"
        )

        # -------------------------------------------------
        # СИНХРОНИЗАЦИЯ SNAPSHOT + WEBSOCKET
        # -------------------------------------------------

        first_delta = find_first_delta_after_snapshot(
            snapshot,
            buffer
        )

        if first_delta is None:
            raise RuntimeError(
                "Не удалось синхронизировать "
                "snapshot с WebSocket."
            )

        print("\nСИНХРОНИЗАЦИЯ:")
        print(
            f"Первый delta после snapshot: "
            f"u={first_delta['u']} | "
            f"seq={first_delta['seq']}"
        )

        # -------------------------------------------------
        # СОЗДАЁМ ЛОКАЛЬНЫЙ СТАКАН
        # -------------------------------------------------

        orderbook = LocalOrderBook()

        orderbook.load_snapshot(
            snapshot
        )

        print("\nLOCAL ORDERBOOK СОЗДАН:")
        print(
            f"Bids: {len(orderbook.bids)}"
        )
        print(
            f"Asks: {len(orderbook.asks)}"
        )
        print(
            f"u: {orderbook.update_id}"
        )
        print(
            f"seq: {orderbook.sequence}"
        )

        # -------------------------------------------------
        # ПРИМЕНЯЕМ НАКОПЛЕННЫЕ DELTA
        # -------------------------------------------------

        first_delta_index = buffer.index(
            first_delta
        )

        for delta in buffer[first_delta_index:]:
            orderbook.apply_delta(delta)

        print("\nBUFFER ПРИМЕНЁН:")
        print(
            f"Bids: {len(orderbook.bids)}"
        )
        print(
            f"Asks: {len(orderbook.asks)}"
        )
        print(
            f"u: {orderbook.update_id}"
        )
        print(
            f"seq: {orderbook.sequence}"
        )

        buffer.clear()

        print("\nСТАКАН СИНХРОНИЗИРОВАН.")
        print(
            "Переходим в режим "
            "реального времени...\n"
        )

        # =================================================
        # РЕАЛЬНОЕ ВРЕМЯ
        # =================================================

        while True:

            message = await websocket.recv()
            data = json.loads(message)

            topic = data.get("topic")

            # =================================================
            # ИСПОЛНЕННЫЕ СДЕЛКИ
            # =================================================

            if topic == f"publicTrade.{SYMBOL}":

                for trade in data["data"]:

                    price = float(
                        trade["p"]
                    )

                    quantity = float(
                        trade["v"]
                    )

                    side = trade["S"]

                    last_price = price

                    trade_flow.add_trade(
                        price=price,
                        quantity=quantity,
                        side=side
                    )

                # Получаем поток
                # за последние 10 секунд.

                flow = trade_flow.get_flow()

                print(
                    f"FLOW | "
                    f"buy=${flow['buy_volume']:.0f} | "
                    f"sell=${flow['sell_volume']:.0f} | "
                    f"buy/s=${flow['buy_volume_per_second']:.0f} | "
                    f"sell/s=${flow['sell_volume_per_second']:.0f}"
                )

                continue

            # =================================================
            # ИЗМЕНЕНИЯ СТАКАНА
            # =================================================

            if topic != (
                f"orderbook.full.{SYMBOL}"
            ):
                continue

            delta = data["data"]

            expected_update_id = (
                orderbook.update_id + 1
            )

            if delta["u"] != expected_update_id:

                print(
                    "\nОШИБКА СИНХРОНИЗАЦИИ!"
                )

                print(
                    f"Ожидали "
                    f"u={expected_update_id}, "
                    f"получили "
                    f"u={delta['u']}"
                )

                break

            orderbook.apply_delta(
                delta
            )

            # =================================================
            # ПОИСК УРОВНЕЙ РЯДОМ С ЦЕНОЙ
            # =================================================

            nearby_levels = find_nearby_levels(
                orderbook,
                last_price,
                max_distance_bps=100
            )

            # =================================================
            # ПОИСК НОВЫХ ПОТЕНЦИАЛЬНЫХ СТЕН
            # =================================================

            walls = find_walls(
                nearby_levels,
                flow
            )

            # =================================================
            # WALL TRACKER
            # =================================================

            wall_events = wall_tracker.update(
                detected_walls=walls,
                orderbook=orderbook,
                trade_flow=flow,
            )

            stable_walls = (
                wall_tracker.get_stable_walls()
            )

            nearest_walls = (
                wall_tracker.get_nearest_stable_walls(
                    last_price,
                    limit=10,
                )
            )

            # =================================================
            # ДИАГНОСТИКА СТАКАНА
            # =================================================

            print(
                "TOP BIDS:",
                sorted(
                    orderbook.bids.keys(),
                    reverse=True
                )[:5]
            )

            print(
                "TOP ASKS:",
                sorted(
                    orderbook.asks.keys()
                )[:5]
            )

            print(
                "LAST:",
                last_price
            )

            if last_price is not None:

                min_price = min(
                    min(orderbook.bids),
                    min(orderbook.asks)
                )

                max_price = max(
                    max(orderbook.bids),
                    max(orderbook.asks)
                )

                print(
                    f"Диапазон стакана: "
                    f"{min_price} - {max_price} | "
                    f"last={last_price}"
                )

            print(
                f"Обновление: "
                f"u={orderbook.update_id} | "
                f"seq={orderbook.sequence} | "
                f"price={last_price} | "
                f"nearby={len(nearby_levels)} | "
                f"walls={len(walls)} | "
                f"stable={len(stable_walls)} | "
                f"bids={len(orderbook.bids)} | "
                f"asks={len(orderbook.asks)}"
            )

            # =================================================
            # НОВЫЕ НАЙДЕННЫЕ СТЕНЫ
            # =================================================

            if walls:

                print(
                    "\nНАЙДЕНЫ СТЕНЫ:"
                )

                for wall in walls[:10]:

                    consume_time = (
                        f"{wall['estimated_consumption_time']:.1f}s"
                        if (
                            wall[
                                "estimated_consumption_time"
                            ]
                            is not None
                        )
                        else "N/A"
                    )

                    print(
                        f"{wall['side'].upper()} | "
                        f"price={wall['price']} | "
                        f"size=${wall['notional']:.0f} | "
                        f"ratio={wall['wall_ratio']:.1f}x | "
                        f"flow/s="
                        f"${wall['opposing_flow_per_second']:.0f} | "
                        f"consume≈{consume_time}"
                    )

            # =================================================
            # СТАБИЛЬНЫЕ СТЕНЫ
            # =================================================

            if stable_walls:

                print(
                    "\nСТАБИЛЬНЫЕ СТЕНЫ:"
                )

                for wall in nearest_walls:

                    print(
                        f"{wall['side'].upper()} | "
                        f"price={wall['price']} | "
                        f"size="
                        f"${wall['current_size']:.0f} | "
                        f"age="
                        f"{wall['age']:.1f}s | "
                        f"distance="
                        f"{wall['distance_bps']:.1f}bps"
                    )

            # =================================================
            # СОБЫТИЯ СО СТЕНАМИ
            # =================================================

            if wall_events:

                print(
                    "\nWALL EVENTS:"
                )

                for event in wall_events:

                    print(
                        f"{event['event']} | "
                        f"{event['side'].upper()} | "
                        f"price={event['price']} | "
                        f"initial="
                        f"${event['initial_size']:.0f} | "
                        f"current="
                        f"${event['current_size']:.0f} | "
                        f"consumed="
                        f"${event['consumed_size']:.0f} | "
                        f"flow="
                        f"${event['aggressive_volume']:.0f} | "
                        f"age="
                        f"{event['age']:.1f}s"
                    )


if __name__ == "__main__":
    asyncio.run(main())