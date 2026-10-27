from statistics import median

from .formulas import calculate_notional


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


def find_walls(
    levels,
    flow,
    min_wall_ratio=888,
    min_wall_notional=0,
    diagnostics=None,
):
    if diagnostics is not None:
        diagnostics.clear()
        diagnostics.update({
            "input_levels": len(levels),
            "below_min_notional": 0,
            "no_side_median": 0,
            "below_min_wall_ratio": 0,
            "candidates": 0,
        })
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
        if notional < min_wall_notional:
            if diagnostics is not None:
                diagnostics["below_min_notional"] += 1
            continue

        if level["side"] == "buy":
            side_median = buy_median
            opposing_flow = flow["sell_volume_per_second"]

        else:
            side_median = sell_median
            opposing_flow = flow["buy_volume_per_second"]

        if side_median is None or side_median == 0:
            if diagnostics is not None:
                diagnostics["no_side_median"] += 1
            continue

        wall_ratio = notional / side_median

        if wall_ratio < min_wall_ratio:
            if diagnostics is not None:
                diagnostics["below_min_wall_ratio"] += 1
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

    if diagnostics is not None:
        diagnostics["candidates"] = len(walls)
    return walls
