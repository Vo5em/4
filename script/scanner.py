from statistics import median

from .formulas import calculate_notional


WALL_RATIO_DIAGNOSTIC_THRESHOLDS = (5, 10, 15, 20, 30, 50, 100)


def diagnose_wall_levels(levels, *, min_wall_notional=0, min_wall_ratio=0, top_n=10):
    """Explain scanner filtering and threshold sensitivity from existing levels."""
    by_side = {
        side: [level for level in levels if level.get("side") == side]
        for side in ("buy", "sell")
    }
    medians = {
        side: median([calculate_notional(item["price"], item["quantity"]) for item in items])
        if items else None
        for side, items in by_side.items()
    }
    evaluated = []
    for level in levels:
        notional = calculate_notional(level["price"], level["quantity"])
        side_median = medians[level["side"]]
        ratio = notional / side_median if side_median else None
        if notional < min_wall_notional:
            rejection = "below_min_notional"
        elif ratio is None:
            rejection = "no_side_median"
        elif ratio < min_wall_ratio:
            rejection = "below_min_wall_ratio"
        else:
            rejection = None
        evaluated.append({
            "side": level["side"], "price": float(level["price"]),
            "notional": float(notional), "distance_bps": float(level.get("distance_bps", 0)),
            "median_notional": float(side_median) if side_median is not None else None,
            "wall_ratio": float(ratio) if ratio is not None else None,
            "rejection": rejection,
        })
    return {
        "side_medians": medians,
        "ratio_sweep": {
            side: {
                threshold: sum(
                    1 for item in evaluated
                    if item["side"] == side and item["notional"] >= min_wall_notional
                    and item["wall_ratio"] is not None and item["wall_ratio"] >= threshold
                )
                for threshold in WALL_RATIO_DIAGNOSTIC_THRESHOLDS
            }
            for side in ("buy", "sell")
        },
        "largest_levels": sorted(evaluated, key=lambda item: item["notional"], reverse=True)[:top_n],
    }


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
