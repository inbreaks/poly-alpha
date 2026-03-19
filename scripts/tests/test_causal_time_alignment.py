from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


builder = load_script_module(
    "polyalpha_build_btc_basis_backtest_db_alignment",
    "scripts/build_btc_basis_backtest_db.py",
)
report = load_script_module(
    "polyalpha_btc_basis_backtest_report_alignment",
    "scripts/btc_basis_backtest_report.py",
)


class CausalTimeAlignmentTests(unittest.TestCase):
    def test_builder_uses_previous_completed_cex_close(self) -> None:
        poly_rows = [
            ("event-1", "market-1", "token-1", "yes", 120_000, 0.42),
        ]
        kline_rows = [
            (60_000, 0.0, 0.0, 0.0, 101_000.0, 0.0),
            (120_000, 0.0, 0.0, 0.0, 102_000.0, 0.0),
        ]

        basis_rows = builder.build_basis_rows(poly_rows, kline_rows)
        self.assertEqual(len(basis_rows), 1)
        self.assertEqual(basis_rows[0][6], 101000.0)

    def test_report_observations_skip_minutes_without_previous_bar(self) -> None:
        contract = report.ContractSpec(
            event_id="event-1",
            market_id="market-1",
            token_id="token-1",
            token_side="yes",
            market_question="Will the price of Bitcoin be above $100,000 on March 31?",
            start_ts_ms=0,
            end_ts_ms=300_000,
            rule=report.MarketRule(kind="above", lower_strike=100000.0),
        )
        observations = report.build_observations_by_minute(
            poly_rows=[
                (60_000, "event-1", "market-1", "token-1", "yes", 0.40),
                (120_000, "event-1", "market-1", "token-1", "yes", 0.42),
            ],
            contract_specs={"token-1": contract},
            cex_close_by_minute={
                60_000: 101000.0,
                120_000: 102000.0,
            },
            sigma_by_minute={
                60_000: 0.02,
                120_000: 0.03,
            },
        )
        self.assertNotIn(60_000, observations)
        self.assertIn(120_000, observations)
        self.assertEqual(observations[120_000][0].cex_price, 101000.0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
