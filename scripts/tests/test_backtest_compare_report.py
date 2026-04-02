from __future__ import annotations

import csv
import json
import os
import tempfile
import unittest

from scripts.tests.module_loader import load_script_module


compare = load_script_module(
    "polyalpha_backtest_compare_report",
    "scripts/backtest_compare_report.py",
)


class BacktestCompareReportTests(unittest.TestCase):
    def test_build_run_row_falls_back_to_nested_strategy_band_policy(self) -> None:
        row = compare.build_run_row(
            "current",
            report={
                "summary": {
                    "total_pnl": 10.0,
                    "ending_equity": 110.0,
                    "strategy": {"band_policy": "configured_band"},
                },
                "rust_replay_report": {
                    "summary": {"band_policy": "configured_band"},
                },
            },
        )

        self.assertEqual(row["band_policy"], "configured_band")

    def test_build_summary_payload_writes_three_way_summary(self) -> None:
        payload = compare.build_summary_payload(
            current_report={"summary": {"total_pnl": 10.0, "ending_equity": 110.0}},
            current_noband_report={"summary": {"total_pnl": 15.0, "ending_equity": 115.0}},
            legacy_report={"summary": {"total_pnl": 25.0, "ending_equity": 125.0}},
        )

        self.assertEqual(payload["rows"][0]["run_kind"], "current")
        self.assertEqual(payload["rows"][1]["run_kind"], "current_noband")
        self.assertEqual(payload["rows"][2]["run_kind"], "legacy")
        self.assertEqual(payload["rows"][1]["total_pnl"], 15.0)

    def test_aggregate_run_equity_series_preserves_initial_equity_before_late_market_start(self) -> None:
        rows = compare.aggregate_run_equity_series(
            {
                "btc": {"equity": [(1, 100.0), (3, 120.0)]},
                "eth": {"equity": [(2, 50.0), (4, 70.0)]},
            },
            run_kind="current",
            all_ts=[1, 2, 3, 4],
        )

        self.assertEqual(
            rows,
            [
                {"kind": "current", "ts_ms": 1, "equity": 150.0, "pnl": 0.0},
                {"kind": "current", "ts_ms": 2, "equity": 150.0, "pnl": 0.0},
                {"kind": "current", "ts_ms": 3, "equity": 170.0, "pnl": 20.0},
                {"kind": "current", "ts_ms": 4, "equity": 190.0, "pnl": 40.0},
            ],
        )

    def test_compare_report_writes_csv_json_svg_and_png_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            current_dir = os.path.join(tempdir, "current")
            current_noband_dir = os.path.join(tempdir, "current_noband")
            legacy_dir = os.path.join(tempdir, "legacy")
            output_dir = os.path.join(tempdir, "out")
            os.makedirs(current_dir)
            os.makedirs(current_noband_dir)
            os.makedirs(legacy_dir)

            self._write_run_artifacts(
                current_dir,
                market="btc",
                total_pnl=10.0,
                ending_equity=110.0,
                band_policy="configured_band",
            )
            self._write_run_artifacts(
                current_noband_dir,
                market="btc",
                total_pnl=15.0,
                ending_equity=115.0,
                band_policy="disabled",
            )
            self._write_run_artifacts(
                legacy_dir,
                market="btc",
                total_pnl=25.0,
                ending_equity=125.0,
                band_policy="legacy_reference",
            )

            compare.main(
                [
                    "--current-dir",
                    current_dir,
                    "--current-noband-dir",
                    current_noband_dir,
                    "--legacy-dir",
                    legacy_dir,
                    "--output-dir",
                    output_dir,
                ]
            )

            summary_csv = os.path.join(output_dir, "summary.csv")
            summary_json = os.path.join(output_dir, "summary.json")
            aggregate_csv = os.path.join(output_dir, "aggregate_equity.csv")
            aggregate_svg = os.path.join(output_dir, "charts", "aggregate-current-vs-current-noband-vs-legacy.svg")
            aggregate_png = os.path.join(output_dir, "charts", "aggregate-current-vs-current-noband-vs-legacy.png")
            per_market_svg = os.path.join(output_dir, "charts", "per-market-current-vs-current-noband-vs-legacy.svg")
            per_market_png = os.path.join(output_dir, "charts", "per-market-current-vs-current-noband-vs-legacy.png")

            self.assertTrue(os.path.exists(summary_csv))
            self.assertTrue(os.path.exists(summary_json))
            self.assertTrue(os.path.exists(aggregate_csv))
            self.assertTrue(os.path.exists(aggregate_svg))
            self.assertTrue(os.path.exists(aggregate_png))
            self.assertTrue(os.path.exists(per_market_svg))
            self.assertTrue(os.path.exists(per_market_png))

            with open(summary_json, encoding="utf-8") as f:
                payload = json.load(f)
            self.assertEqual(payload["markets"][0]["market"], "btc")
            self.assertEqual(payload["markets"][0]["current_noband_total_pnl"], 15.0)

            with open(summary_csv, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(rows[0]["market"], "btc")

            with open(aggregate_svg, encoding="utf-8") as f:
                svg = f.read()
            self.assertIn("<svg", svg)
            self.assertIn("Current NoBand", svg)

    def _write_run_artifacts(
        self,
        run_dir: str,
        *,
        market: str,
        total_pnl: float,
        ending_equity: float,
        band_policy: str,
    ) -> None:
        report_path = os.path.join(run_dir, f"{market}-report.json")
        equity_path = os.path.join(run_dir, f"{market}-equity.csv")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "summary": {
                        "total_pnl": total_pnl,
                        "ending_equity": ending_equity,
                        "win_rate": 0.5,
                        "max_drawdown": 1.0,
                        "trade_count": 2,
                        "total_fees_paid": 0.4,
                        "total_slippage_paid": 0.6,
                        "start_ts_ms": 1,
                        "end_ts_ms": 2,
                        "band_policy": band_policy,
                    }
                },
                f,
            )
        with open(equity_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ts_ms", "equity"])
            writer.writerow([1, 100.0])
            writer.writerow([2, ending_equity])
