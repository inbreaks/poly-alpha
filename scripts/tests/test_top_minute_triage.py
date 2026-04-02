from __future__ import annotations

import csv
import tempfile
import unittest

from scripts.tests.module_loader import load_script_module


triage = load_script_module(
    "polyalpha_top_minute_triage",
    "scripts/top_minute_triage.py",
)


class TopMinuteTriageTests(unittest.TestCase):
    def test_load_top_minutes_csv_excludes_aggregate_delta_from_market_contributions(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = f"{tempdir}/top-minutes.csv"
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "start_ts_ms",
                        "end_ts_ms",
                        "start_utc",
                        "end_utc",
                        "new_xrp_delta",
                        "new_sol_delta",
                        "new_aggregate_delta",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "start_ts_ms": 60_000,
                        "end_ts_ms": 120_000,
                        "start_utc": "1970-01-01T00:01:00+00:00",
                        "end_utc": "1970-01-01T00:02:00+00:00",
                        "new_xrp_delta": 100.0,
                        "new_sol_delta": -20.0,
                        "new_aggregate_delta": 80.0,
                    }
                )

            rows = triage.load_top_minutes_csv(path, run_kind="current_noband")

        self.assertEqual(rows[0].contributions, {"xrp": 100.0, "sol": -20.0})

    def test_build_market_minute_breakdown_tracks_fill_and_mark_components(self) -> None:
        stress = triage.StressConfig(
            poly_fee_bps=2.0,
            cex_fee_bps=2.0,
        )
        trades = [
            triage.TradeRow(
                trade_id="open-1",
                event_id="evt-1",
                market_id="mkt-1",
                market_slug="mkt-1",
                direction="yes_signal open",
                entry_time=30,
                exit_time=180,
                entry_poly_price=0.30,
                exit_poly_price=0.35,
                entry_cex_price=100.0,
                exit_cex_price=108.0,
                poly_shares=100.0,
                cex_qty=2.0,
                delta_at_entry=0.5,
            ),
            triage.TradeRow(
                trade_id="entry-2",
                event_id="evt-1",
                market_id="mkt-1",
                market_slug="mkt-1",
                direction="yes_signal open",
                entry_time=120,
                exit_time=240,
                entry_poly_price=0.20,
                exit_poly_price=0.25,
                entry_cex_price=105.0,
                exit_cex_price=109.0,
                poly_shares=50.0,
                cex_qty=1.0,
                delta_at_entry=-0.1,
            ),
        ]
        market_specs = {
            "mkt-1": triage.MarketSpec(
                event_id="evt-1",
                market_id="mkt-1",
                market_slug="mkt-1",
                market_question="Will XRP be above $3.10?",
            )
        }
        price_series = {
            "mkt-1": {
                60_000: triage.PricePoint(yes_price=0.30, no_price=0.70),
                120_000: triage.PricePoint(yes_price=0.34, no_price=0.66),
            }
        }
        cex_close_by_ts = {
            0: 100.0,
            60_000: 105.0,
        }

        rows = triage.build_market_minute_breakdown(
            trades=trades,
            market_specs=market_specs,
            price_series=price_series,
            cex_close_by_ts=cex_close_by_ts,
            stress=stress,
            start_ts_ms=60_000,
            end_ts_ms=120_000,
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.market_id, "mkt-1")
        self.assertEqual(row.market_question, "Will XRP be above $3.10?")
        self.assertAlmostEqual(row.fill_cash_change_usd, -10.023, places=6)
        self.assertAlmostEqual(row.poly_mark_change_usd, 14.0, places=6)
        self.assertAlmostEqual(row.cex_mark_change_usd, -10.0, places=6)
        self.assertAlmostEqual(row.total_contribution_usd, -6.023, places=6)
        self.assertEqual(row.open_positions_before, 1)
        self.assertEqual(row.open_positions_after, 2)
        self.assertEqual(row.entries_at_end_ts, 1)
        self.assertEqual(row.exits_at_end_ts, 0)
        self.assertEqual(row.yes_price_start, 0.30)
        self.assertEqual(row.yes_price_end, 0.34)
        self.assertEqual(row.cex_price_start, 100.0)
        self.assertEqual(row.cex_price_end, 105.0)

    def test_classify_minute_window_covers_sync_close_entry_and_mark_cases(self) -> None:
        sync_like = triage.classify_minute_window(
            equity_change_usd=12_000.0,
            fill_cash_change_usd=500.0,
            mark_change_usd=11_500.0,
            entries_at_end_ts=0,
            exits_at_end_ts=0,
            sync_reset_market_count=4,
        )
        self.assertEqual(sync_like.code, "sync_reset_like")

        close_cluster = triage.classify_minute_window(
            equity_change_usd=8_000.0,
            fill_cash_change_usd=7_400.0,
            mark_change_usd=600.0,
            entries_at_end_ts=0,
            exits_at_end_ts=6,
            sync_reset_market_count=0,
        )
        self.assertEqual(close_cluster.code, "realized_close_cluster")

        entry_cluster = triage.classify_minute_window(
            equity_change_usd=-6_000.0,
            fill_cash_change_usd=-5_700.0,
            mark_change_usd=-300.0,
            entries_at_end_ts=5,
            exits_at_end_ts=0,
            sync_reset_market_count=0,
        )
        self.assertEqual(entry_cluster.code, "entry_cluster")

        pure_mark = triage.classify_minute_window(
            equity_change_usd=4_000.0,
            fill_cash_change_usd=50.0,
            mark_change_usd=3_950.0,
            entries_at_end_ts=0,
            exits_at_end_ts=0,
            sync_reset_market_count=0,
        )
        self.assertEqual(pure_mark.code, "pure_mark_shock")

    def test_build_triage_payload_embeds_top_contributor_quote_paths(self) -> None:
        top_minutes = [
            triage.TopMinuteWindow(
                start_ts_ms=60_000,
                end_ts_ms=120_000,
                start_utc="1970-01-01T00:01:00+00:00",
                end_utc="1970-01-01T00:02:00+00:00",
                run_kind="current_noband",
                contributions={"xrp": 120.0},
                aggregate_delta_usd=120.0,
            )
        ]
        artifacts = {
            "xrp": triage.RunArtifact(
                market_key="xrp",
                db_path="/tmp/xrp.duckdb",
                stress=triage.StressConfig(poly_fee_bps=2.0, cex_fee_bps=2.0),
                equity_by_ts={
                    60_000: triage.EquitySnapshot(
                        ts_ms=60_000,
                        equity=1_000.0,
                        cash=900.0,
                        unrealized_pnl=100.0,
                        open_positions=1,
                    ),
                    120_000: triage.EquitySnapshot(
                        ts_ms=120_000,
                        equity=1_120.0,
                        cash=900.0,
                        unrealized_pnl=220.0,
                        open_positions=1,
                    ),
                },
                trades=[
                    triage.TradeRow(
                        trade_id="open-1",
                        event_id="evt-1",
                        market_id="mkt-1",
                        market_slug="mkt-1",
                        direction="yes_signal open",
                        entry_time=30,
                        exit_time=180,
                        entry_poly_price=0.30,
                        exit_poly_price=0.35,
                        entry_cex_price=100.0,
                        exit_cex_price=108.0,
                        poly_shares=100.0,
                        cex_qty=2.0,
                        delta_at_entry=0.5,
                    )
                ],
            )
        }

        def fake_context_provider(
            db_path: str,
            market_ids: list[str],
            start_ts_ms: int,
            end_ts_ms: int,
            padding_minutes: int,
        ) -> triage.WindowContext:
            self.assertEqual(db_path, "/tmp/xrp.duckdb")
            self.assertEqual(market_ids, ["mkt-1"])
            self.assertEqual(start_ts_ms, 60_000)
            self.assertEqual(end_ts_ms, 120_000)
            self.assertEqual(padding_minutes, 1)
            return triage.WindowContext(
                market_specs={
                    "mkt-1": triage.MarketSpec(
                        event_id="evt-1",
                        market_id="mkt-1",
                        market_slug="mkt-1",
                        market_question="Will XRP be above $3.10?",
                    )
                },
                price_series={
                    "mkt-1": {
                        0: triage.PricePoint(yes_price=0.28, no_price=0.72),
                        60_000: triage.PricePoint(yes_price=0.30, no_price=0.70),
                        120_000: triage.PricePoint(yes_price=0.34, no_price=0.66),
                        180_000: triage.PricePoint(yes_price=0.36, no_price=0.64),
                    }
                },
                cex_close_by_ts={
                    0: 100.0,
                    60_000: 105.0,
                    120_000: 107.0,
                    180_000: 109.0,
                },
            )

        payload = triage.build_triage_payload(
            top_minutes=top_minutes,
            artifacts=artifacts,
            context_provider=fake_context_provider,
            padding_minutes=1,
            top_market_limit=2,
        )

        self.assertEqual(payload["minutes"][0]["classification"]["code"], "pure_mark_shock")
        market = payload["minutes"][0]["markets"][0]
        self.assertEqual(market["market"], "xrp")
        self.assertEqual(market["top_market_contributors"][0]["market_id"], "mkt-1")
        self.assertEqual(
            market["top_market_contributors"][0]["quote_path"]["poly"][0]["ts_ms"],
            0,
        )
        self.assertEqual(
            market["top_market_contributors"][0]["quote_path"]["cex"][-1]["price"],
            109.0,
        )


if __name__ == "__main__":
    unittest.main()
