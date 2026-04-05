from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.tests.module_loader import load_script_module


sweep = load_script_module(
    "polyalpha_stable_backtest_sweep",
    "scripts/stable_backtest_sweep.py",
)


class StableBacktestSweepTests(unittest.TestCase):
    def test_phase1_matrix_includes_default_band_and_wider_max_range(self) -> None:
        combos = sweep.build_phase1_runs()
        btc = [item for item in combos if item.asset == "btc"]
        self.assertTrue(
            any(
                item.min_poly_price == 0.20 and item.max_poly_price == 0.50
                for item in btc
            )
        )
        self.assertTrue(any(item.max_poly_price == 0.65 for item in btc))

    def test_select_top_phase1_candidates_prefers_stability_and_trade_count_floor(
        self,
    ) -> None:
        rows = [
            sweep.SweepRow("btc", 0.10, 0.50, None, None, 1000.0, 100.0, 0.60, 80, "phase1/a.json"),
            sweep.SweepRow("btc", 0.14, 0.50, None, None, 900.0, 60.0, 0.62, 75, "phase1/b.json"),
            sweep.SweepRow("btc", 0.20, 0.50, None, None, 1100.0, 100.0, 0.59, 30, "phase1/c.json"),
        ]
        selected = sweep.select_top_phase1_candidates(
            rows,
            top_n=2,
            min_round_trip_ratio=0.7,
        )
        self.assertEqual(
            [(item.min_poly_price, item.max_poly_price) for item in selected],
            [(0.14, 0.50), (0.10, 0.50)],
        )

    def test_render_report_document_covers_methodology_recommendations_and_concentration(
        self,
    ) -> None:
        recommendations = {
            "btc": {
                "conservative": {
                    "asset": "btc",
                    "min_poly_price": 0.10,
                    "max_poly_price": 0.45,
                    "entry_z": None,
                    "rolling_window": None,
                    "exit_z": None,
                    "total_pnl": 9219.95,
                    "max_drawdown": 665.95,
                    "win_rate": 0.6298,
                    "round_trip_count": 262,
                    "pnl_over_drawdown": 13.84,
                    "report_path": "phase1/btc-min010-max045.json",
                },
                "balanced": {
                    "asset": "btc",
                    "min_poly_price": 0.10,
                    "max_poly_price": 0.45,
                    "entry_z": None,
                    "rolling_window": None,
                    "exit_z": None,
                    "total_pnl": 9219.95,
                    "max_drawdown": 665.95,
                    "win_rate": 0.6298,
                    "round_trip_count": 262,
                    "pnl_over_drawdown": 13.84,
                    "report_path": "phase1/btc-min010-max045.json",
                },
                "aggressive": {
                    "asset": "btc",
                    "min_poly_price": 0.08,
                    "max_poly_price": 0.45,
                    "entry_z": None,
                    "rolling_window": None,
                    "exit_z": None,
                    "total_pnl": 10749.95,
                    "max_drawdown": 992.37,
                    "win_rate": 0.6121,
                    "round_trip_count": 281,
                    "pnl_over_drawdown": 10.83,
                    "report_path": "phase1/btc-min008-max045.json",
                },
            }
        }
        concentration = {
            "btc": {
                "top5_net_pnl_share": 0.18,
                "top3_entry_share": 0.11,
            }
        }

        report = sweep.render_report_document(recommendations, concentration)

        self.assertIn("# Stable Sweep Report", report)
        self.assertIn("方法", report)
        self.assertIn("BTC", report)
        self.assertIn("集中度", report)
        self.assertIn("0.10-0.45", report)

    def test_render_recommended_config_emits_per_asset_overrides(self) -> None:
        recommendations = {
            "btc": {
                "balanced": {
                    "asset": "btc",
                    "min_poly_price": 0.10,
                    "max_poly_price": 0.45,
                    "entry_z": None,
                    "rolling_window": None,
                    "exit_z": None,
                }
            },
            "eth": {
                "balanced": {
                    "asset": "eth",
                    "min_poly_price": 0.14,
                    "max_poly_price": 0.65,
                    "entry_z": 4.0,
                    "rolling_window": 360,
                    "exit_z": 0.6,
                }
            },
        }

        rendered = sweep.render_recommended_config(recommendations)

        self.assertIn("[strategy.basis.overrides.btc]", rendered)
        self.assertIn('min_poly_price = "0.10"', rendered)
        self.assertIn("[strategy.basis.overrides.eth]", rendered)
        self.assertIn('rolling_window_secs = 21600', rendered)
        self.assertIn('exit_z_score_threshold = "0.6"', rendered)

    def test_build_command_supports_equity_csv_export(self) -> None:
        run = sweep.SweepRun(
            phase="phase1",
            asset="btc",
            db_path="data/btc.duckdb",
            min_poly_price=0.10,
            max_poly_price=0.45,
        )

        command = sweep.build_command(
            run,
            Path("report.json"),
            equity_csv_path=Path("equity.csv"),
        )

        self.assertIn("--equity-csv", command)
        self.assertIn("equity.csv", command)

    def test_compute_drawdown_series_tracks_peak_to_trough(self) -> None:
        drawdown = sweep.compute_drawdown_series([100.0, 110.0, 90.0, 120.0, 115.0])
        self.assertEqual(drawdown, [0.0, 0.0, 20.0, 0.0, 5.0])

    def test_write_report_and_config_outputs_files(self) -> None:
        recommendations = {
            "btc": {
                "balanced": {
                    "asset": "btc",
                    "min_poly_price": 0.10,
                    "max_poly_price": 0.45,
                    "entry_z": None,
                    "rolling_window": None,
                    "exit_z": None,
                }
            }
        }
        concentration = {
            "btc": {
                "top5_net_pnl_share": 0.18,
                "top3_entry_share": 0.11,
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            report_path = Path(tmp_dir) / "stable-report.md"
            config_path = Path(tmp_dir) / "stable.toml"

            sweep.write_report_and_config(
                report_path,
                config_path,
                recommendations,
                concentration,
            )

            self.assertTrue(report_path.exists())
            self.assertTrue(config_path.exists())
            self.assertIn("Stable Sweep Report", report_path.read_text())
            self.assertIn("[strategy.basis.overrides.btc]", config_path.read_text())


if __name__ == "__main__":
    unittest.main()
