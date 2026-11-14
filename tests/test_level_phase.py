import unittest
from datetime import datetime, timedelta, timezone

from script.level_phase import analyze_level_phase
from script.settings import PairSettings
from script.strategies import StrategyContext, CombinedStrategy


_BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
_INDEX = 0


def c(close, low=None, high=None, closed=True):
    global _INDEX
    stamp = _BASE + timedelta(minutes=_INDEX)
    _INDEX += 1
    return {"close": close, "low": close if low is None else low,
            "high": close if high is None else high, "is_closed": closed,
            "start_time": stamp}


class LevelPhaseTests(unittest.TestCase):
    def test_old_consolidation_excluded_after_confirmed_break_and_return_touches_count(self):
        candles = [
            c(100.3, 100.0, 100.4), c(100.35, 100.0, 100.4),
            c(99.7, 99.6, 99.9), c(99.7, 99.6, 99.9),  # confirmed below
            c(99.5, 99.4, 99.6), c(99.5, 99.4, 99.6), c(99.5, 99.4, 99.6),
            c(100.0, 99.95, 100.05),  # first touch
            c(99.5, 99.4, 99.6), c(99.5, 99.4, 99.6), c(99.5, 99.4, 99.6),
            c(100.0, 99.95, 100.05),  # second touch
        ]
        result = analyze_level_phase(100, candles)
        self.assertEqual(result["freshness_state"], "CONFIRMED_AFTER_BREAK")
        self.assertEqual(result["current_phase_side"], "below")
        self.assertEqual(result["current_phase_touch_count"], 2)

    def test_confirmed_support_resistance_flip(self):
        result = analyze_level_phase(100, [c(100.3), c(100.3), c(99.7), c(99.7)])
        self.assertEqual(result["current_role"], "RESISTANCE")
        self.assertIsNotNone(result["last_confirmed_transition_at"])

    def test_one_new_touch_after_confirmation_is_counted_as_one(self):
        result = analyze_level_phase(100, [
            c(100.3), c(100.3), c(99.7), c(99.7),
            c(99.5), c(99.5), c(99.5), c(100.0, 99.95, 100.05),
        ])
        self.assertEqual(result["current_phase_touch_count"], 1)

    def test_wick_probe_does_not_confirm_transition(self):
        result = analyze_level_phase(100, [c(100.3), c(100.3), c(100.2, 99.5, 100.4)])
        self.assertNotEqual(result["freshness_state"], "CONFIRMED_AFTER_BREAK")
        self.assertIsNone(result["last_confirmed_transition_at"])

    def test_one_close_beyond_break_threshold_does_not_flip_phase(self):
        result = analyze_level_phase(100, [
            c(100.3), c(100.3), c(99.8),
        ])
        self.assertEqual(result["current_phase_side"], "above")
        self.assertIsNone(result["last_confirmed_transition_at"])
        self.assertNotEqual(result["freshness_state"], "CONFIRMED_AFTER_BREAK")

    def test_support_bounces_keep_phase_touches_after_long_departure(self):
        result = analyze_level_phase(100, [
            c(99.5), c(99.5), c(100.3), c(100.3),  # confirmed flip to support
            c(100.0, 99.95, 100.05),
            c(100.5), c(100.5), c(100.5),
            c(100.0, 99.95, 100.05),
            *[c(100.5) for _ in range(10)],
        ])
        self.assertEqual(result["current_phase_touch_count"], 2)
        self.assertEqual(result["freshness_state"], "CONFIRMED_AFTER_BREAK")

    def test_resistance_bounces_keep_phase_touches_after_long_departure(self):
        result = analyze_level_phase(100, [
            c(100.5), c(100.5), c(99.7), c(99.7),  # confirmed flip to resistance
            c(100.0, 99.95, 100.05),
            c(99.5), c(99.5), c(99.5),
            c(100.0, 99.95, 100.05),
            *[c(99.5) for _ in range(10)],
        ])
        self.assertEqual(result["current_phase_touch_count"], 2)
        self.assertEqual(result["freshness_state"], "CONFIRMED_AFTER_BREAK")

    def test_continuous_consolidation_has_unknown_phase_age_and_no_count_fabrication(self):
        result = analyze_level_phase(100, [c(100.3, 99.95, 100.4)] * 12)
        self.assertEqual(result["freshness_state"], "UNCONFIRMED")
        self.assertIsNone(result["current_phase_touch_count"])
        self.assertFalse(result["phase_origin_known"])

    def test_unclosed_future_candle_is_ignored(self):
        result = analyze_level_phase(100, [c(100.3), c(100.3), c(99.5), c(99.5, closed=False)])
        self.assertEqual(result["freshness_state"], "UNCONFIRMED")

    def test_missing_candles_return_none_not_zero(self):
        result = analyze_level_phase(100, [])
        self.assertIsNone(result["current_phase_touch_count"])
        self.assertEqual(result["freshness_state"], "NO_DATA")

    def test_diagnostic_fields_do_not_change_combined_policy(self):
        settings = PairSettings("ETHUSDT")
        wall = {"side": "sell", "price": 101, "age": 50, "first_seen_at": "w",
                "current_size": 1000, "wall_ratio": 900}
        base_level = {"price": 101, "type": "RESISTANCE", "touch_count": 1}
        context = StrategyContext("ETHUSDT", 100, (wall,), (), settings,
                                  historical_levels=(base_level,), tick_size=0.01)
        diagnostic_level = {**base_level, "current_phase_touch_count": 7,
                            "freshness_state": "CONFIRMED_AFTER_BREAK"}
        context_with_diag = StrategyContext("ETHUSDT", 100, (wall,), (), settings,
                                            historical_levels=(diagnostic_level,), tick_size=0.01)
        strategy = CombinedStrategy()
        self.assertEqual(strategy.evaluate(context), strategy.evaluate(context_with_diag))


if __name__ == "__main__":
    unittest.main()
