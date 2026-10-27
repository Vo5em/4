def calculate_notional(price, quantity):
    return price * quantity

from statistics import median


def calculate_local_median(levels):
    notionals = [
        calculate_notional(price, quantity)
        for price, quantity in levels
    ]

    if not notionals:
        return None

    return median(notionals)


def prepare_levels(levels):
    return [
        {
            "price": price,
            "quantity": quantity,
            "notional": calculate_notional(price, quantity),
        }
        for price, quantity in levels
    ]

def calculate_entry_price(wall_price, side, distance_bps=1.5):
    if side == "sell":
        return wall_price * (1 - distance_bps / 10000)

    return wall_price * (1 + distance_bps / 10000)


def calculate_stop_loss_price(wall_price, side, distance_bps=2):
    if side == "sell":
        return wall_price * (1 + distance_bps / 10000)

    return wall_price * (1 - distance_bps / 10000)


def calculate_liquidation_price(wall_price, side, distance_bps=2):
    """Compatibility alias; this level is a virtual stop, not liquidation."""
    return calculate_stop_loss_price(wall_price, side, distance_bps)


def calculate_x2_price(entry_price, stop_loss_price, side):
    distance = abs(entry_price - stop_loss_price)

    if side == "sell":
        return entry_price - distance

    return entry_price + distance
