from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


builder = load_script_module(
    "polyalpha_build_btc_basis_backtest_db_rule_support",
    "scripts/build_btc_basis_backtest_db.py",
)
report = load_script_module(
    "polyalpha_btc_basis_backtest_report",
    "scripts/btc_basis_backtest_report.py",
)


class MarketRuleParsingTests(unittest.TestCase):
    def test_parse_above_rule(self) -> None:
        rule = report.parse_market_rule("Will the price of Bitcoin be above $100,000 on March 31?")
        self.assertEqual(rule.kind, "above")
        self.assertEqual(rule.lower_strike, 100000.0)
        self.assertIsNone(rule.upper_strike)

    def test_parse_below_rule(self) -> None:
        rule = report.parse_market_rule("Will BTC be below $80,500 on April 1?")
        self.assertEqual(rule.kind, "below")
        self.assertEqual(rule.upper_strike, 80500.0)
        self.assertIsNone(rule.lower_strike)

    def test_parse_between_rule(self) -> None:
        rule = report.parse_market_rule("Will Bitcoin be between $90,000 and $95,000 on Friday?")
        self.assertEqual(rule.kind, "between")
        self.assertEqual(rule.lower_strike, 90000.0)
        self.assertEqual(rule.upper_strike, 95000.0)

    def test_parse_k_suffix_rules(self) -> None:
        rule = report.parse_market_rule("Will Bitcoin be above $122K on September 10?")
        self.assertEqual(rule.kind, "above")
        self.assertEqual(rule.lower_strike, 122000.0)

        between = report.parse_market_rule("Will Bitcoin be between $118K and $120K on Friday?")
        self.assertEqual(between.kind, "between")
        self.assertEqual(between.lower_strike, 118000.0)
        self.assertEqual(between.upper_strike, 120000.0)

    def test_unsupported_rule_raises(self) -> None:
        with self.assertRaises(ValueError):
            report.parse_market_rule("Will Bitcoin outperform ETH this week?")

    def test_builder_supports_only_causal_market_questions(self) -> None:
        self.assertTrue(
            builder.supports_causal_market_question(
                "Will the price of Bitcoin be above $100,000 on March 31?"
            )
        )
        self.assertTrue(
            builder.supports_causal_market_question(
                "Will Bitcoin be between $90,000 and $95,000 on Friday?"
            )
        )
        self.assertTrue(
            builder.supports_causal_market_question("Will Bitcoin be above $122K on September 10?")
        )
        self.assertFalse(
            builder.supports_causal_market_question("Will Bitcoin hit $100k again in 2024?")
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
