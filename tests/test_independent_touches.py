import unittest
from datetime import datetime, timedelta, timezone

from script.independent_touches import count_independent_touches
from script.settings import PairSettings


def candle(low, high, close, *, minute=0, closed=True):
    return {
        "start_time": datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minute),
        "low": low,
        "high": high,
        "close": close,
        "is_closed": closed,
    }


class IndependentTouchCounterTests(unittest.TestCase):
    def test_many_pivots_in_one_continuous_interaction_are_one_touch(self):
        candles = [
            candle(99.95, 100.05, 100.01, minute=0),
            candle(99.97, 100.03, 99.99, minute=1),
            candle(99.94, 100.04, 100.02, minute=2),
        ]
        self.assertEqual(count_independent_touches(100, candles), 1)

    def test_confirmed_departure_and_later_return_count_a_new_touch(self):
        candles = [
            candle(99.95, 100.05, 100.00, minute=0),
            candle(100.25, 100.40, 100.35, minute=1),
            candle(100.25, 100.40, 100.35, minute=2),
            candle(100.25, 100.40, 100.35, minute=3),
            candle(99.96, 100.04, 100.01, minute=4),
        ]
        self.assertEqual(count_independent_touches(100, candles), 2)

    def test_adjacent_candles_near_zone_do_not_create_extra_touches(self):
        candles = [
            candle(99.98, 100.02, 100.00, minute=index)
            for index in range(12)
        ]
        self.assertEqual(count_independent_touches(100, candles), 1)

    def test_zone_contact_without_crossing_level_counts(self):
        # The candle remains above the level, but its range enters the +10 bps zone.
        candles = [candle(100.02, 100.20, 100.10)]
        self.assertEqual(count_independent_touches(100, candles), 1)

    def test_one_wide_candle_crossing_level_is_not_counted_twice(self):
        candles = [candle(99.0, 101.0, 100.0)]
        self.assertEqual(count_independent_touches(100, candles), 1)

    def test_unclosed_candles_are_excluded_and_no_usable_data_is_unavailable(self):
        candles = [candle(99.95, 100.05, 100, closed=False)]
        self.assertIsNone(count_independent_touches(100, candles))
        candles.append(candle(99.95, 100.05, 100, minute=1, closed=True))
        self.assertEqual(count_independent_touches(100, candles), 1)

    def test_calculation_is_sequential_and_does_not_need_future_candles(self):
        candles = [
            candle(99.95, 100.05, 100, minute=0),
            candle(100.25, 100.40, 100.35, minute=1),
            candle(100.25, 100.40, 100.35, minute=2),
            candle(100.25, 100.40, 100.35, minute=3),
            candle(99.95, 100.05, 100, minute=4),
        ]
        self.assertEqual(count_independent_touches(100, candles[:2]), 1)
        self.assertEqual(count_independent_touches(100, candles), 2)

    def test_invalid_reset_zone_relationship_is_rejected(self):
        with self.assertRaises(ValueError):
            count_independent_touches(100, [candle(100, 100, 100)], touch_zone_bps=30, reset_bps=30)

    def test_pair_settings_hold_independent_touch_parameters_per_symbol(self):
        btc = PairSettings("BTCUSDT")
        eth = PairSettings(
            "ETHUSDT", historical_level_touch_zone_bps=8,
            historical_level_touch_reset_bps=35,
            historical_level_touch_min_separation_candles=5,
        )
        self.assertEqual(
            (btc.historical_level_touch_zone_bps, btc.historical_level_touch_reset_bps,
             btc.historical_level_touch_min_separation_candles),
            (10.0, 30.0, 3),
        )
        self.assertEqual(
            (eth.historical_level_touch_zone_bps, eth.historical_level_touch_reset_bps,
             eth.historical_level_touch_min_separation_candles),
            (8.0, 35.0, 5),
        )
        with self.assertRaisesRegex(ValueError, "reset_bps"):
            PairSettings("ETHUSDT", historical_level_touch_zone_bps=30,
                         historical_level_touch_reset_bps=20)


if __name__ == "__main__":
    unittest.main()
