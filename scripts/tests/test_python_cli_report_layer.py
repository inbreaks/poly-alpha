from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


report = load_script_module(
    "polyalpha_btc_basis_backtest_report_cli_mode",
    "scripts/btc_basis_backtest_report.py",
)


class PythonCliReportLayerTests(unittest.TestCase):
    def make_cfg(self, **overrides):
        base = report.RunConfig(
            db_path="data/example.duckdb",
            market_id=None,
            token_id=None,
            start_ts_ms=None,
            end_ts_ms=None,
            trade_start_ts_ms=None,
            initial_capital=100_000.0,
            rolling_window=360,
            entry_z=2.0,
            exit_z=0.5,
            position_units=None,
            position_notional_usd=None,
            max_capital_usage=0.25,
            cex_hedge_ratio=1.0,
            cex_margin_ratio=0.10,
            fee_bps=2.0,
            slippage_bps=10.0,
            report_json=None,
            equity_csv=None,
        )
        for key, value in overrides.items():
            setattr(base, key, value)
        return base

    def test_run_command_builds_rust_replay_command(self) -> None:
        cfg = self.make_cfg(market_id="1559477", start_ts_ms=1_700_000_000_000, end_ts_ms=1_700_000_060_000)
        command = report.build_rust_replay_command(
            cfg,
            rust_report_json_path="/tmp/rust-report.json",
            rust_equity_csv_path="/tmp/rust-equity.csv",
        )

        self.assertEqual(
            command[:8],
            ["cargo", "run", "-q", "-p", "polyalpha-cli", "--", "backtest", "rust-replay"],
        )
        self.assertIn("--market-id", command)
        self.assertIn("1559477", command)
        self.assertIn("--position-notional-usd", command)
        self.assertIn("1000.0", command)
        self.assertIn("--poly-fee-bps", command)
        self.assertIn("--cex-slippage-bps", command)

    def test_run_command_rejects_legacy_only_token_filter(self) -> None:
        cfg = self.make_cfg(token_id="token-yes")
        with self.assertRaises(SystemExit):
            report.build_rust_replay_command(
                cfg,
                rust_report_json_path="/tmp/rust-report.json",
                rust_equity_csv_path="/tmp/rust-equity.csv",
            )

    def test_report_layer_payload_marks_rust_as_fact_source(self) -> None:
        summary = report.BacktestSummary(
            db_path="data/example.duckdb",
            cex_source="binance_usdm_ccxt",
            market_style="price-only",
            rows_processed=10,
            minute_count=5,
            token_count=2,
            market_count=1,
            event_count=1,
            start_ts_ms=1,
            end_ts_ms=2,
            initial_capital=100_000.0,
            ending_equity=100_100.0,
            total_pnl=100.0,
            realized_pnl=100.0,
            unrealized_pnl=0.0,
            poly_realized_pnl=120.0,
            cex_realized_pnl=-20.0,
            max_drawdown=10.0,
            max_gross_exposure=1_000.0,
            max_reserved_margin=100.0,
            max_capital_in_use=200.0,
            max_capital_utilization=0.002,
            trade_count=4,
            poly_trade_count=2,
            cex_trade_count=2,
            round_trip_count=1,
            winning_round_trips=1,
            win_rate=1.0,
            total_fees_paid=1.0,
            total_slippage_paid=2.0,
            poly_fees_paid=0.5,
            poly_slippage_paid=1.0,
            cex_fees_paid=0.5,
            cex_slippage_paid=1.0,
            annualized_sharpe=1.23,
            strategy={"execution_source": report.RUST_REPLAY_FACT_SOURCE},
        )
        points = [
            report.EquityPoint(
                ts_ms=1,
                equity=100_000.0,
                cash=100_000.0,
                available_cash=100_000.0,
                unrealized_pnl=0.0,
                gross_exposure=0.0,
                reserved_margin=0.0,
                capital_in_use=0.0,
                open_positions=0,
            )
        ]
        rows = [
            report.ReportMarketRow(
                market_id="1559477",
                event_id="261299",
                market_question="Will BTC be above $60K?",
                entry_count=1,
                signal_count=2,
                rejected_capital_count=0,
                net_pnl=100.0,
            )
        ]
        payload = report.build_report_layer_payload(
            summary,
            rows,
            points,
            rust_report={"summary": {"total_pnl": 100.0}},
        )

        self.assertEqual(payload["report_metadata"]["fact_source"], report.RUST_REPLAY_FACT_SOURCE)
        self.assertEqual(payload["report_metadata"]["report_mode"], "rust_fact_source_report_layer")
        self.assertIn("rust_replay_report", payload)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
