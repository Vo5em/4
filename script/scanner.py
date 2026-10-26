from statistics import median

from formulas import calculate_notional


def find_nearby_levels(orderbook, last_price, max_distance_bps=100):
    if last_price is None:
        return []

    levels = []

    for price, quantity in orderbook.bids.items():
        distance_bps = abs(price - last_price) / last_price * 10000

        if distance_bps <= max_distance_bps:
            levels.append({
                "side": "buy",
                "price": price,
                "quantity": quantity,
                "distance_bps": distance_bps,
            })

    for price, quantity in orderbook.asks.items():
        distance_bps = abs(price - last_price) / last_price * 10000

        if distance_bps <= max_distance_bps:
            levels.append({
                "side": "sell",
                "price": price,
                "quantity": quantity,
                "distance_bps": distance_bps,
            })

    return levels


def find_walls(levels, flow, min_wall_ratio=888):
    buy_levels = [
        level for level in levels
        if level["side"] == "buy"
    ]

    sell_levels = [
        level for level in levels
        if level["side"] == "sell"
    ]

    buy_median = median([
        calculate_notional(level["price"], level["quantity"])
        for level in buy_levels
    ]) if buy_levels else None

    sell_median = median([
        calculate_notional(level["price"], level["quantity"])
        for level in sell_levels
    ]) if sell_levels else None

    walls = []

    for level in levels:
        notional = calculate_notional(
            level["price"],
            level["quantity"]
        )

        if level["side"] == "buy":
            side_median = buy_median
            opposing_flow = flow["sell_volume_per_second"]

        else:
            side_median = sell_median
            opposing_flow = flow["buy_volume_per_second"]

        if side_median is None or side_median == 0:
            continue

        wall_ratio = notional / side_median

        if wall_ratio < min_wall_ratio:
            continue

        if opposing_flow > 0:
            estimated_consumption_time = (
                notional / opposing_flow
            )
        else:
            estimated_consumption_time = None

        walls.append({
            **level,
            "notional": notional,
            "median_notional": side_median,
            "wall_ratio": wall_ratio,
            "opposing_flow_per_second": opposing_flow,
            "estimated_consumption_time": estimated_consumption_time,
        })

    return walls