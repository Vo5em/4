import unittest
from decimal import Decimal as D
from script.risk import BybitTier, UnsafePlan, choose_maximum_safe_leverage, liquidation_price

class RiskCalculationTests(unittest.TestCase):
    def setUp(self):
        self.tier = BybitTier(D('10000'), D('20'), D('0.005'), D('0'))

    def test_long_liquidation_uses_tier_mmr_and_fee(self):
        liq = liquidation_price(side='long', entry=D('100'), notional=D('1000'), leverage=D('10'), tier=self.tier)
        self.assertAlmostEqual(float(liq), 90.45, places=2)

    def test_short_liquidation_uses_tier_mmr_and_fee(self):
        liq = liquidation_price(side='short', entry=D('100'), notional=D('1000'), leverage=D('10'), tier=self.tier)
        self.assertGreater(liq, D('100'))

    def test_auto_uses_maximum_safe_exchange_leverage(self):
        lev, notional, liq = choose_maximum_safe_leverage(
            side='long', entry=D('100'), stop=D('95'), stake=D('10'),
            min_leverage=D('1'), max_leverage=D('10'), leverage_step=D('0.5'),
            tiers=[self.tier], safety_buffer_bps=D('5'),
        )
        self.assertEqual((lev, notional), (D('10'), D('100')))
        self.assertLess(liq, D('95'))

    def test_max_leverage_unsafe_plan_is_rejected_not_deleveraged(self):
        with self.assertRaises(UnsafePlan):
            choose_maximum_safe_leverage(
                side='long', entry=D('100'), stop=D('90.5'), stake=D('10'),
                min_leverage=D('1'), max_leverage=D('10'), leverage_step=D('0.5'),
                tiers=[self.tier], safety_buffer_bps=D('5'),
            )

if __name__ == '__main__':
    unittest.main()

