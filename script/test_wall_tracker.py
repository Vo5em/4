import time

from wall_tracker import WallTracker


tracker = WallTracker(
    min_age_seconds=2,
    consumed_ratio=0.9,
)


# --------------------------------------------------
# 1. Стена появилась
# --------------------------------------------------

walls = [
    {
        "side": "buy",
        "price": 85000.0,
        "notional": 2_000_000,
    }
]

flow = {
    "buy_volume": 0,
    "sell_volume": 0,
}

events = tracker.update(walls, flow)

print("1. Стена появилась")
print("events:", events)
print("stable:", tracker.get_stable_walls())


# --------------------------------------------------
# 2. Ждём, чтобы стена стала стабильной
# --------------------------------------------------

print("\nЖдём 3 секунды...")
time.sleep(3)

events = tracker.update(walls, flow)

print("2. Стена прожила > 2 секунд")
print("events:", events)
print("stable:", tracker.get_stable_walls())


# --------------------------------------------------
# 3. Стена уменьшилась
# --------------------------------------------------

walls = [
    {
        "side": "buy",
        "price": 85000.0,
        "notional": 1_500_000,
    }
]

flow = {
    "buy_volume": 0,
    "sell_volume": 500_000,
}

events = tracker.update(walls, flow)

print("\n3. Стена уменьшилась")
print("events:", events)
print("stable:", tracker.get_stable_walls())


# --------------------------------------------------
# 4. Стена исчезла
# --------------------------------------------------

walls = []

flow = {
    "buy_volume": 0,
    "sell_volume": 500_000,
}

events = tracker.update(walls, flow)

print("\n4. Стена исчезла")
print("events:", events)
print("stable:", tracker.get_stable_walls())