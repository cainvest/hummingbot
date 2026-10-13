import unittest
from decimal import Decimal

from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_balance_logic as bal


class CainvestSpotBalanceLogicTests(unittest.TestCase):
    def test_reference_mid_first_brl_pair(self):
        last = {
            "USDC-BRL": {"buy": Decimal("5.30"), "sell": Decimal("5.30")},
        }
        ref = bal.reference_mid_brl_per_base_for_trading_line(["USDC-BRL"], last)
        self.assertIsNotNone(ref)
        mid, base = ref
        self.assertEqual(base, "USDC")
        self.assertEqual(mid, Decimal("5.30"))

    def test_apply_trading_line_boosts_base(self):
        local = {"BRL": Decimal("53"), "USDC": Decimal("1")}
        ref = (Decimal("5.30"), "USDC")
        bumped = bal.apply_trading_line_to_base_balance(local, Decimal("53"), ref)
        self.assertTrue(bumped)
        self.assertEqual(local["USDC"], Decimal("10"))

    def test_apply_without_mid_noop(self):
        local = {"USDC": Decimal("1")}
        self.assertFalse(
            bal.apply_trading_line_to_base_balance(local, Decimal("999"), None)
        )
        self.assertEqual(local["USDC"], Decimal("1"))

    def test_wallet_exceeds_synthetic(self):
        local = {"USDC": Decimal("100")}
        ref = (Decimal("5"), "USDC")
        bumped = bal.apply_trading_line_to_base_balance(local, Decimal("50"), ref)
        self.assertFalse(bumped)
        self.assertEqual(local["USDC"], Decimal("100"))

    def test_usdt_symbol(self):
        local = {"USDT": Decimal("2")}
        ref = (Decimal("5"), "USDT")
        bal.apply_trading_line_to_base_balance(local, Decimal("100"), ref)
        self.assertEqual(local["USDT"], Decimal("20"))


if __name__ == "__main__":
    unittest.main()
