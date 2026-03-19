from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


builder = load_script_module(
    "polyalpha_build_btc_basis_backtest_db",
    "scripts/build_btc_basis_backtest_db.py",
)


class BtcPriceMarketFilterTests(unittest.TestCase):
    def test_price_only_keeps_price_market(self) -> None:
        event = {
            "title": "Bitcoin above ___ on March 17?",
            "markets": [
                {
                    "question": "Will the price of Bitcoin be above $62,000 on March 17?",
                    "description": "Resolution source is Binance BTC/USDT 1m close.",
                }
            ],
        }
        self.assertTrue(builder.event_matches_market_style(event, "price-only"))

    def test_price_only_rejects_microstrategy_news_event(self) -> None:
        event = {
            "title": "MicroStrategy announces another Bitcoin purchase?",
            "markets": [
                {
                    "question": "Will MicroStrategy announce another Bitcoin purchase this week?",
                    "description": "Company treasury / reserve update.",
                }
            ],
        }
        self.assertFalse(builder.event_matches_market_style(event, "price-only"))

    def test_all_btc_keeps_news_but_price_only_filters_it(self) -> None:
        event = {
            "title": "MicroStrategy announces another Bitcoin purchase?",
            "markets": [
                {
                    "question": "Will MicroStrategy announce another Bitcoin purchase this week?",
                    "description": "Bitcoin treasury update.",
                }
            ],
        }
        self.assertTrue(builder.event_matches_market_style(event, "all-btc"))
        self.assertFalse(builder.event_matches_market_style(event, "price-only"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

