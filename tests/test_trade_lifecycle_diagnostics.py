import json
import unittest
from unittest.mock import patch

from script.engine import TradingEngine
from script.settings import PairSettings


class TradeLifecycleDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.engine = TradingEngine({"ETHUSDT": PairSettings("ETHUSDT")})
        self.engine.market_data_health["ETHUSDT"].update({
            "price_message_time_ms": 123456,
            "connection_generation": 7,
            "book_synchronized": True,
        })
        self.engine.latest_stable_walls["ETHUSDT"] = ({"wall_id": "w1"},)

    def _trade(self):
        return {
            "id": 12, "signal_id": "plan:combined:w1", "symbol": "ETHUSDT",
            "strategy_id": "combined", "level_policy_strategy_id": "breakout",
            "wall_id": "w1", "side": "long", "wall_side": "sell",
            "wall_price": 101.0, "entry_price": 100.9,
            "entry_trigger_direction": "cross_up", "entry_armed": True,
            "status": "pending", "entry_previous_price": 100.0,
        }

    def test_ticker_cross_log_records_pending_status_and_market_context(self):
        trade = self._trade()
        self.engine.trade_manager.trades.append(trade)
        self.engine.trade_manager.last_prices["ETHUSDT"] = 100.0
        self.engine._entry_eligible_pending_ids["ETHUSDT"] = {trade["signal_id"]}
        with patch("script.engine.logger.warning") as log_info:
            self.engine.record_ticker_entry_candidates("ETHUSDT", 101.0)

        self.assertEqual(log_info.call_count, 1)
        record = json.loads(log_info.call_args.args[1])
        self.assertEqual(record["event"], "ENTRY_CROSS_DETECTED")
        self.assertTrue(record["pending_active_when_ticker_received"])
        self.assertTrue(record["armed"])
        self.assertEqual(record["source"], "ticker")
        self.assertEqual(record["local_order"], 1)
        self.assertEqual(record["exchange_timestamp_ms"], 123456)
        self.assertEqual(record["book_generation"], 7)
        self.assertEqual(record["wall_state"], "STABLE")

    def test_non_crossing_ticker_produces_no_lifecycle_log(self):
        trade = self._trade()
        self.engine.trade_manager.trades.append(trade)
        self.engine.trade_manager.last_prices["ETHUSDT"] = 100.0
        with patch("script.engine.logger.warning") as log_info:
            self.engine.record_ticker_entry_candidates("ETHUSDT", 100.5)
        log_info.assert_not_called()

    def test_lifecycle_state_changes_are_structured_and_include_plan_identity(self):
        trade = self._trade()
        with patch("script.engine.logger.warning") as log_info:
            self.engine._log_trade_lifecycle_events([{
                "event": "ENTRY_CANCELLED", "trade": trade,
                "reason": "WALL_NO_LONGER_QUALIFIES",
                "current_price": 100.5,
            }], event_source="orderbook")
        record = json.loads(log_info.call_args.args[1])
        self.assertEqual(record["event"], "PENDING_CANCELLED")
        self.assertEqual(record["source"], "orderbook")
        self.assertEqual(record["plan_id"], "plan:combined:w1")
        self.assertEqual(record["reason"], "WALL_NO_LONGER_QUALIFIES")
        self.assertEqual(record["wall_state"], "NOT_STABLE")

    def test_disarming_logs_only_the_actual_armed_to_disarmed_transition(self):
        trade = self._trade()
        self.engine.trade_manager.trades.append(trade)
        with patch("script.engine.logger.warning") as log_info:
            first = self.engine.disarm_pending_with_diagnostics(
                "ETHUSDT", "STALE_PRICE", "ticker"
            )
            second = self.engine.disarm_pending_with_diagnostics(
                "ETHUSDT", "STALE_PRICE", "ticker"
            )
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(log_info.call_count, 1)
        record = json.loads(log_info.call_args.args[1])
        self.assertEqual(record["event"], "PENDING_DISARMED")
        self.assertEqual(record["reason"], "STALE_PRICE")
        self.assertFalse(trade["entry_armed"])

    def test_ticker_cross_received_before_book_cancel_is_reconstructable(self):
        trade = self._trade()
        self.engine.trade_manager.trades.append(trade)
        self.engine.trade_manager.last_prices["ETHUSDT"] = 100.0
        self.engine._entry_eligible_pending_ids["ETHUSDT"] = {trade["signal_id"]}
        with patch("script.engine.logger.warning") as log_info:
            self.engine.record_ticker_entry_candidates("ETHUSDT", 101.0)
            self.engine.latest_stable_walls["ETHUSDT"] = ()
            self.engine._log_trade_lifecycle_events([{
                "event": "ENTRY_CANCELLED", "trade": trade,
                "reason": "WALL_NO_LONGER_QUALIFIES",
                "current_price": 101.0,
            }], event_source="orderbook")
        records = [json.loads(call.args[1]) for call in log_info.call_args_list]
        self.assertEqual([row["local_order"] for row in records], [1, 2])
        self.assertEqual([row["source"] for row in records], ["ticker", "orderbook"])
        self.assertTrue(records[0]["pending_active_when_ticker_received"])
        self.assertEqual(records[0]["event"], "ENTRY_CROSS_DETECTED")
        self.assertEqual(records[1]["event"], "PENDING_CANCELLED")

    def test_orderbook_cancel_first_means_later_ticker_has_no_active_plan(self):
        trade = self._trade()
        self.engine.trade_manager.trades.append(trade)
        self.engine.trade_manager.last_prices["ETHUSDT"] = 100.0
        self.engine._entry_eligible_pending_ids["ETHUSDT"] = {trade["signal_id"]}
        with patch("script.engine.logger.warning") as log_info:
            trade.update(status="cancelled", close_reason="WALL_NO_LONGER_QUALIFIES")
            self.engine._log_trade_lifecycle_events([{
                "event": "ENTRY_CANCELLED", "trade": trade,
                "reason": "WALL_NO_LONGER_QUALIFIES",
            }], event_source="orderbook")
            self.engine.record_ticker_entry_candidates("ETHUSDT", 101.0)
        self.assertEqual(log_info.call_count, 1)
        record = json.loads(log_info.call_args.args[1])
        self.assertEqual(record["event"], "PENDING_CANCELLED")


if __name__ == "__main__":
    unittest.main()
