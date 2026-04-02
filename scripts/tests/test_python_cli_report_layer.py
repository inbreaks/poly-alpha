from __future__ import annotations

import contextlib
import io
import json
import subprocess
import unittest
from unittest import mock

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
            initial_capital=10_000.0,
            rolling_window=600,
            entry_z=4.0,
            exit_z=0.5,
            position_units=None,
            position_notional_usd=200.0,
            max_capital_usage=1.0,
            cex_hedge_ratio=1.0,
            cex_margin_ratio=0.10,
            fee_bps=2.0,
            slippage_bps=50.0,
            poly_slippage_bps=50.0,
            cex_slippage_bps=2.0,
            min_poly_price=0.2,
            max_poly_price=0.5,
            report_json=None,
            equity_csv=None,
        )
        for key, value in overrides.items():
            setattr(base, key, value)
        return base

    def test_runtime_backtest_defaults_match_config_default_toml(self) -> None:
        defaults = report.load_runtime_backtest_defaults()

        self.assertEqual(defaults.initial_capital, 10_000.0)
        self.assertEqual(defaults.rolling_window, 600)
        self.assertEqual(defaults.entry_z, 4.0)
        self.assertEqual(defaults.exit_z, 0.5)
        self.assertEqual(defaults.position_notional_usd, 200.0)
        self.assertEqual(defaults.max_capital_usage, 1.0)
        self.assertEqual(defaults.poly_slippage_bps, 50.0)
        self.assertEqual(defaults.cex_slippage_bps, 2.0)
        self.assertEqual(defaults.band_policy, "configured_band")
        self.assertEqual(defaults.min_poly_price, 0.2)
        self.assertEqual(defaults.max_poly_price, 0.5)

    def test_run_command_builds_rust_replay_command(self) -> None:
        cfg = self.make_cfg(market_id="1559477", start_ts_ms=1_700_000_000_000, end_ts_ms=1_700_000_060_000)
        command = report.build_rust_replay_command(
            cfg,
            rust_report_json_path="/tmp/rust-report.json",
            rust_equity_csv_path="/tmp/rust-equity.csv",
            rust_anomaly_report_json_path="/tmp/rust-anomalies.json",
        )

        self.assertEqual(
            command[:8],
            ["cargo", "run", "-q", "-p", "polyalpha-cli", "--", "backtest", "rust-replay"],
        )
        self.assertIn("--market-id", command)
        self.assertIn("1559477", command)
        self.assertIn("--position-notional-usd", command)
        self.assertIn("200.0", command)
        self.assertIn("--poly-fee-bps", command)
        self.assertIn("--cex-slippage-bps", command)
        self.assertIn("50.0", command)
        self.assertIn("2.0", command)
        self.assertIn("--min-poly-price", command)
        self.assertIn("0.2", command)
        self.assertIn("--max-poly-price", command)
        self.assertIn("0.5", command)
        self.assertIn("--anomaly-report-json", command)
        self.assertIn("/tmp/rust-anomalies.json", command)
        self.assertIn("--fail-on-anomaly", command)

    def test_run_command_rejects_legacy_only_token_filter(self) -> None:
        cfg = self.make_cfg(token_id="token-yes")
        with self.assertRaises(SystemExit):
            report.build_rust_replay_command(
                cfg,
                rust_report_json_path="/tmp/rust-report.json",
                rust_equity_csv_path="/tmp/rust-equity.csv",
                rust_anomaly_report_json_path="/tmp/rust-anomalies.json",
            )

    def test_run_current_noband_command_disables_price_band(self) -> None:
        cfg = self.make_cfg()
        command = report.build_rust_replay_command(
            cfg,
            rust_report_json_path="/tmp/rust-report.json",
            rust_equity_csv_path="/tmp/rust-equity.csv",
            rust_anomaly_report_json_path="/tmp/rust-anomalies.json",
            band_policy="disabled",
        )

        self.assertIn("--band-policy", command)
        self.assertIn("disabled", command)

    def make_summary(self) -> report.BacktestSummary:
        return report.BacktestSummary(
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

    def test_build_report_layer_summary_maps_anomaly_fields(self) -> None:
        summary = report.build_report_layer_summary(
            self.make_cfg(),
            rust_report={
                "summary": {
                    "db_path": "data/example.duckdb",
                    "minute_count": 5,
                    "market_count": 1,
                    "event_count": 1,
                    "start_ts_ms": 1,
                    "end_ts_ms": 2,
                    "initial_capital": 100_000.0,
                    "ending_equity": 100_100.0,
                    "total_pnl": 100.0,
                    "realized_pnl": 100.0,
                    "unrealized_pnl": 0.0,
                    "poly_realized_gross": 120.0,
                    "cex_realized_gross": -20.0,
                    "max_drawdown": 10.0,
                    "max_gross_exposure": 1_000.0,
                    "max_reserved_margin": 100.0,
                    "max_capital_in_use": 200.0,
                    "max_capital_utilization": 0.002,
                    "trade_count": 4,
                    "poly_trade_count": 2,
                    "cex_trade_count": 2,
                    "round_trip_count": 1,
                    "winning_round_trips": 1,
                    "win_rate": 1.0,
                    "total_fees_paid": 1.0,
                    "total_slippage_paid": 2.0,
                    "poly_fees_paid": 0.5,
                    "poly_slippage_paid": 1.0,
                    "cex_fees_paid": 0.5,
                    "cex_slippage_paid": 1.0,
                    "anomaly_count": 2,
                    "max_abs_anomaly_equity_jump_usd": 123.4,
                    "max_anomaly_mark_share": 0.42,
                },
                "anomaly_diagnostics": {
                    "top_anomalies": [
                        {
                            "start_ts_ms": 10,
                            "end_ts_ms": 20,
                            "reason_codes": ["large_equity_jump", "mark_share_high"],
                        }
                    ]
                },
            },
            equity_curve=[
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
                ),
                report.EquityPoint(
                    ts_ms=2,
                    equity=100_100.0,
                    cash=100_100.0,
                    available_cash=100_100.0,
                    unrealized_pnl=0.0,
                    gross_exposure=0.0,
                    reserved_margin=0.0,
                    capital_in_use=0.0,
                    open_positions=0,
                ),
            ],
            token_count=2,
        )

        self.assertEqual(summary.anomaly_count, 2)
        self.assertEqual(summary.max_abs_anomaly_equity_jump_usd, 123.4)
        self.assertEqual(summary.max_anomaly_mark_share, 0.42)
        self.assertEqual(summary.top_anomaly_start_ts_ms, 10)
        self.assertEqual(summary.top_anomaly_end_ts_ms, 20)
        self.assertEqual(summary.top_anomaly_reason_codes, ["large_equity_jump", "mark_share_high"])

    def test_build_replay_anomaly_surface_prefers_dedicated_anomaly_artifact(self) -> None:
        anomaly_surface = report.build_replay_anomaly_surface(
            {
                "summary": {
                    "anomaly_count": 1,
                    "max_abs_anomaly_equity_jump_usd": 11.0,
                    "max_anomaly_mark_share": 0.11,
                },
                "anomaly_diagnostics": {
                    "anomaly_count": 3,
                    "max_abs_equity_jump_usd": 33.0,
                    "max_mark_share": 0.33,
                    "top_anomalies": [
                        {
                            "start_ts_ms": 30,
                            "end_ts_ms": 40,
                            "reason_codes": ["from_report"],
                        }
                    ],
                },
                "anomaly_report": {
                    "diagnostics": {
                        "anomaly_count": 2,
                        "max_abs_equity_jump_usd": 22.0,
                        "max_mark_share": 0.22,
                        "top_anomalies": [
                            {
                                "start_ts_ms": 10,
                                "end_ts_ms": 20,
                                "reason_codes": ["from_artifact"],
                            }
                        ],
                    }
                },
            }
        )

        self.assertEqual(anomaly_surface["anomaly_count"], 2)
        self.assertEqual(anomaly_surface["max_abs_anomaly_equity_jump_usd"], 22.0)
        self.assertEqual(anomaly_surface["max_anomaly_mark_share"], 0.22)
        self.assertEqual(anomaly_surface["top_anomaly"]["start_ts_ms"], 10)
        self.assertEqual(anomaly_surface["top_anomaly"]["reason_codes"], ["from_artifact"])

    def test_run_fact_source_tolerates_anomaly_gate_exit_when_artifacts_exist(self) -> None:
        cfg = self.make_cfg()
        expected_summary = self.make_summary()
        expected_rows: list[report.ReportMarketRow] = []
        expected_points: list[report.EquityPoint] = []

        def fake_subprocess_run(command, **kwargs):
            del kwargs
            report_path = command[command.index("--report-json") + 1]
            anomaly_path = command[command.index("--anomaly-report-json") + 1]
            equity_path = command[command.index("--equity-csv") + 1]
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump({"summary": {"anomaly_count": 1}}, f)
            with open(anomaly_path, "w", encoding="utf-8") as f:
                json.dump({"diagnostics": {"anomaly_count": 1}}, f)
            with open(equity_path, "w", encoding="utf-8") as f:
                f.write("ts_ms,equity,cash,available_cash,unrealized_pnl,gross_exposure,reserved_margin,capital_in_use,open_positions\n")
                f.write("1,100000,100000,100000,0,0,0,0,0\n")
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="replay anomaly detected")

        with (
            mock.patch.object(report, "load_report_context", return_value=({}, 0)),
            mock.patch.object(report, "subprocess") as mocked_subprocess,
            mock.patch.object(report, "load_equity_curve_csv", return_value=expected_points),
            mock.patch.object(report, "build_report_layer_summary", return_value=expected_summary),
            mock.patch.object(report, "build_report_market_rows", return_value=expected_rows),
        ):
            mocked_subprocess.run.side_effect = fake_subprocess_run
            summary, points, rows, rust_report = report.run_rust_replay_fact_source(cfg)

        self.assertIs(summary, expected_summary)
        self.assertIs(points, expected_points)
        self.assertIs(rows, expected_rows)
        self.assertIn("anomaly_report", rust_report)
        self.assertEqual(rust_report["anomaly_report"]["diagnostics"]["anomaly_count"], 1)

    def test_run_fact_source_rejects_unexpected_nonzero_even_with_anomaly_artifacts(self) -> None:
        cfg = self.make_cfg()

        def fake_subprocess_run(command, **kwargs):
            del kwargs
            report_path = command[command.index("--report-json") + 1]
            anomaly_path = command[command.index("--anomaly-report-json") + 1]
            equity_path = command[command.index("--equity-csv") + 1]
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump({"summary": {"anomaly_count": 1}}, f)
            with open(anomaly_path, "w", encoding="utf-8") as f:
                json.dump({"diagnostics": {"anomaly_count": 1}}, f)
            with open(equity_path, "w", encoding="utf-8") as f:
                f.write("ts_ms,equity,cash,available_cash,unrealized_pnl,gross_exposure,reserved_margin,capital_in_use,open_positions\n")
                f.write("1,100000,100000,100000,0,0,0,0,0\n")
            return subprocess.CompletedProcess(command, 2, stdout="", stderr="replay anomaly detected")

        with (
            mock.patch.object(report, "load_report_context", return_value=({}, 0)),
            mock.patch.object(report, "subprocess") as mocked_subprocess,
        ):
            mocked_subprocess.run.side_effect = fake_subprocess_run
            with self.assertRaises(SystemExit):
                report.run_rust_replay_fact_source(cfg)

    def test_run_fact_source_rejects_anomaly_gate_exit_when_anomaly_artifact_missing(self) -> None:
        cfg = self.make_cfg()

        def fake_subprocess_run(command, **kwargs):
            del kwargs
            report_path = command[command.index("--report-json") + 1]
            equity_path = command[command.index("--equity-csv") + 1]
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump({"summary": {"anomaly_count": 1}}, f)
            with open(equity_path, "w", encoding="utf-8") as f:
                f.write("ts_ms,equity,cash,available_cash,unrealized_pnl,gross_exposure,reserved_margin,capital_in_use,open_positions\n")
                f.write("1,100000,100000,100000,0,0,0,0,0\n")
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="replay anomaly detected")

        with (
            mock.patch.object(report, "load_report_context", return_value=({}, 0)),
            mock.patch.object(report, "subprocess") as mocked_subprocess,
        ):
            mocked_subprocess.run.side_effect = fake_subprocess_run
            with self.assertRaises(SystemExit):
                report.run_rust_replay_fact_source(cfg)

    def test_run_fact_source_rejects_anomaly_gate_exit_when_artifact_has_zero_anomalies(self) -> None:
        cfg = self.make_cfg()

        def fake_subprocess_run(command, **kwargs):
            del kwargs
            report_path = command[command.index("--report-json") + 1]
            anomaly_path = command[command.index("--anomaly-report-json") + 1]
            equity_path = command[command.index("--equity-csv") + 1]
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump({"summary": {"anomaly_count": 0}}, f)
            with open(anomaly_path, "w", encoding="utf-8") as f:
                json.dump({"diagnostics": {"anomaly_count": 0}}, f)
            with open(equity_path, "w", encoding="utf-8") as f:
                f.write("ts_ms,equity,cash,available_cash,unrealized_pnl,gross_exposure,reserved_margin,capital_in_use,open_positions\n")
                f.write("1,100000,100000,100000,0,0,0,0,0\n")
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="replay anomaly detected")

        with (
            mock.patch.object(report, "load_report_context", return_value=({}, 0)),
            mock.patch.object(report, "subprocess") as mocked_subprocess,
        ):
            mocked_subprocess.run.side_effect = fake_subprocess_run
            with self.assertRaises(SystemExit):
                report.run_rust_replay_fact_source(cfg)

    def test_run_fact_source_rejects_nonzero_when_artifacts_missing(self) -> None:
        cfg = self.make_cfg()
        with (
            mock.patch.object(report, "load_report_context", return_value=({}, 0)),
            mock.patch.object(report, "subprocess") as mocked_subprocess,
        ):
            mocked_subprocess.run.return_value = subprocess.CompletedProcess(
                args=["cargo"],
                returncode=2,
                stdout="",
                stderr="boom",
            )
            with self.assertRaises(SystemExit):
                report.run_rust_replay_fact_source(cfg)

    def test_report_layer_payload_marks_rust_as_fact_source(self) -> None:
        summary = self.make_summary()
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
            rust_report={
                "summary": {
                    "total_pnl": 100.0,
                    "anomaly_count": 2,
                    "max_abs_anomaly_equity_jump_usd": 123.4,
                    "max_anomaly_mark_share": 0.42,
                },
                "anomaly_diagnostics": {
                    "top_anomalies": [
                        {
                            "start_ts_ms": 10,
                            "end_ts_ms": 20,
                            "reason_codes": ["large_equity_jump", "mark_share_high"],
                        }
                    ]
                },
                "anomaly_report": {
                    "diagnostics": {
                        "anomaly_count": 2,
                        "max_abs_equity_jump_usd": 123.4,
                        "max_mark_share": 0.42,
                        "top_anomalies": [
                            {
                                "start_ts_ms": 10,
                                "end_ts_ms": 20,
                                "reason_codes": ["large_equity_jump", "mark_share_high"],
                            }
                        ],
                    }
                },
            },
        )

        self.assertEqual(payload["report_metadata"]["fact_source"], report.RUST_REPLAY_FACT_SOURCE)
        self.assertEqual(payload["report_metadata"]["report_mode"], "rust_fact_source_report_layer")
        self.assertIn("rust_replay_report", payload)
        self.assertIn("acceptance", payload)
        self.assertEqual(payload["acceptance"]["anomaly"]["anomaly_count"], 2)
        self.assertEqual(payload["acceptance"]["anomaly"]["max_abs_anomaly_equity_jump_usd"], 123.4)
        self.assertEqual(payload["acceptance"]["anomaly"]["max_anomaly_mark_share"], 0.42)
        self.assertEqual(payload["acceptance"]["anomaly"]["top_anomaly"]["start_ts_ms"], 10)
        self.assertEqual(
            payload["acceptance"]["anomaly"]["top_anomaly"]["reason_codes"],
            ["large_equity_jump", "mark_share_high"],
        )
        self.assertEqual(payload["summary"]["anomaly_count"], summary.anomaly_count)
        self.assertEqual(
            payload["summary"]["max_abs_anomaly_equity_jump_usd"],
            summary.max_abs_anomaly_equity_jump_usd,
        )
        self.assertEqual(payload["summary"]["max_anomaly_mark_share"], summary.max_anomaly_mark_share)

    def test_report_layer_summary_prints_anomaly_acceptance(self) -> None:
        summary = self.make_summary()
        cfg = self.make_cfg()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            report.print_report_layer_summary(
                summary,
                cfg,
                market_rows=[],
                rust_report={
                    "summary": {
                        "signal_count": 1,
                        "signal_rejected_capital": 0,
                        "signal_rejected_missing_price": 0,
                        "signal_ignored_existing_position": 0,
                        "close_count": 0,
                        "settlement_count": 0,
                        "anomaly_count": 1,
                        "max_abs_anomaly_equity_jump_usd": 50.0,
                        "max_anomaly_mark_share": 0.2,
                    },
                    "anomaly_diagnostics": {
                        "top_anomalies": [
                            {
                                "start_ts_ms": 1000,
                                "end_ts_ms": 2000,
                                "reason_codes": ["large_equity_jump"],
                            }
                        ]
                    },
                },
            )

        output = out.getvalue()
        self.assertIn("异常检测", output)
        self.assertIn("验收状态", output)
        self.assertIn("触发异常门限", output)
        self.assertIn("异常窗口数", output)
        self.assertIn("large_equity_jump", output)

    def test_main_run_exits_nonzero_when_anomaly_detected(self) -> None:
        cfg = self.make_cfg()
        summary = self.make_summary()
        summary.anomaly_count = 1
        parser = mock.Mock()
        parser.parse_args.return_value = mock.Mock(command="run")

        with (
            mock.patch.object(report, "build_parser", return_value=parser),
            mock.patch.object(report, "build_run_config_from_args", return_value=cfg),
            mock.patch.object(
                report,
                "run_rust_replay_fact_source",
                return_value=(summary, [], [], {"summary": {"anomaly_count": 1}}),
            ),
            mock.patch.object(report, "print_report_layer_summary"),
        ):
            with self.assertRaises(SystemExit) as exc_info:
                report.main()

        self.assertEqual(exc_info.exception.code, 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
