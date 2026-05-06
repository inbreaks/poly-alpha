from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.tests.module_loader import load_script_module


sweep = load_script_module(
    "polyalpha_pulse_research_sweep",
    "scripts/pulse_research_sweep.py",
)


class PulseResearchSweepTests(unittest.TestCase):
    def make_runtime_params(self) -> sweep.RuntimeParams:
        return sweep.RuntimeParams(
            opening_request_notional_usd=450.0,
            min_expected_net_pnl_usd=0.1,
            min_net_session_edge_bps=5.0,
            pulse_window_ms=5_000,
            min_claim_price_move_bps=50.0,
            max_fair_claim_move_bps=50.0,
            max_cex_mid_move_bps=50.0,
            min_pulse_score_bps=20.0,
            max_timeout_loss_usd=20.0,
            max_required_hit_rate=0.70,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            poly_open_max_quote_age_ms=500,
            cex_open_max_quote_age_ms=500,
            max_anchor_age_ms=500,
            max_anchor_latency_delta_ms=500,
            base_target_ticks=1,
            medium_target_ticks=2,
            strong_target_ticks=3,
            medium_pulse_score_bps=150.0,
            strong_pulse_score_bps=300.0,
        )

    def test_load_market_catalog_reads_rule_and_settlement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "markets.toml"
            path.write_text(
                """
[[markets]]
symbol = "bitcoin-above-74k-on-april-19"
settlement_timestamp = 1776614400
min_tick_size = "0.01"
cex_price_tick = "0.1"
cex_qty_step = "0.001"

[markets.market_rule]
kind = "above"
lower_strike = "74000"

[[markets]]
symbol = "will-the-price-of-ethereum-be-between-2200-2300-on-april-19"
settlement_timestamp = 1776614400
min_tick_size = "0.01"
cex_price_tick = "0.01"
cex_qty_step = "0.01"

[markets.market_rule]
kind = "between"
lower_strike = "2200"
upper_strike = "2300"
""".strip(),
                encoding="utf-8",
            )

            catalog = sweep.load_market_catalog(path)

            btc = catalog["bitcoin-above-74k-on-april-19"]
            self.assertEqual(btc.asset, "btc")
            self.assertEqual(btc.rule.kind, "above")
            self.assertEqual(btc.rule.lower_strike, 74000.0)
            self.assertIsNone(btc.rule.upper_strike)
            self.assertEqual(btc.settlement_ts_ms, 1776614400 * 1000)
            self.assertEqual(btc.min_tick_size, 0.01)

            eth = catalog["will-the-price-of-ethereum-be-between-2200-2300-on-april-19"]
            self.assertEqual(eth.asset, "eth")
            self.assertEqual(eth.rule.kind, "between")
            self.assertEqual(eth.rule.lower_strike, 2200.0)
            self.assertEqual(eth.rule.upper_strike, 2300.0)

    def test_load_runtime_params_falls_back_for_missing_risk_caps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "runtime.toml"
            path.write_text(
                """
[execution_costs]
poly_fee_bps = "20"
cex_taker_fee_bps = "5"

[paper_slippage]
cex_slippage_bps = "2"

[strategy.market_data]
poly_open_max_quote_age_ms = 500
cex_open_max_quote_age_ms = 500

[strategy.pulse_arb.entry]
min_net_session_edge_bps = "5"
pulse_window_ms = 5000
min_claim_price_move_bps = "50"
max_fair_claim_move_bps = "50"
max_cex_mid_move_bps = "50"
min_pulse_score_bps = "20"

[strategy.pulse_arb.session]
opening_request_notional_usd = "450"
min_expected_net_pnl_usd = "0.5"

[strategy.pulse_arb.exit]
base_target_ticks = 1
medium_target_ticks = 2
strong_target_ticks = 3
medium_pulse_score_bps = "150"
strong_pulse_score_bps = "300"

[strategy.pulse_arb.providers.deribit_primary]
max_anchor_age_ms = 500
max_anchor_latency_delta_ms = 500
""".strip(),
                encoding="utf-8",
            )

            params = sweep.load_runtime_params(path)

            self.assertEqual(params.max_timeout_loss_usd, 20.0)
            self.assertEqual(params.max_required_hit_rate, 0.70)

    def test_interpolated_mark_iv_uses_linear_surface(self) -> None:
        anchor = sweep.AnchorSnapshot(
            ts_ms=1_776_470_400_000,
            index_price=77000.0,
            expiry_ts_ms=1_776_758_400_000,
            atm_iv=55.0,
            points=[
                sweep.SurfacePoint(strike=74000.0, mark_iv=50.0),
                sweep.SurfacePoint(strike=76000.0, mark_iv=54.0),
                sweep.SurfacePoint(strike=78000.0, mark_iv=58.0),
            ],
            latest_received_at_ms=1_776_470_400_100,
        )

        iv = sweep.interpolated_mark_iv(anchor, 77000.0)

        self.assertAlmostEqual(iv, 56.0)

    def test_probability_for_rule_matches_above_and_between(self) -> None:
        anchor = sweep.AnchorSnapshot(
            ts_ms=1_776_470_400_000,
            index_price=77000.0,
            expiry_ts_ms=1_776_758_400_000,
            atm_iv=55.0,
            points=[
                sweep.SurfacePoint(strike=74000.0, mark_iv=50.0),
                sweep.SurfacePoint(strike=76000.0, mark_iv=54.0),
                sweep.SurfacePoint(strike=78000.0, mark_iv=58.0),
            ],
            latest_received_at_ms=1_776_470_400_100,
        )

        above = sweep.MarketRule(kind="above", lower_strike=76000.0, upper_strike=None)
        between = sweep.MarketRule(kind="between", lower_strike=76000.0, upper_strike=78000.0)

        above_prob = sweep.probability_for_rule(
            anchor=anchor,
            rule=above,
            event_expiry_ts_ms=1_776_758_400_000,
            expiry_gap=sweep.ExpiryGapAdjustment(60, 360, 720),
        )
        between_prob = sweep.probability_for_rule(
            anchor=anchor,
            rule=between,
            event_expiry_ts_ms=1_776_758_400_000,
            expiry_gap=sweep.ExpiryGapAdjustment(60, 360, 720),
        )

        self.assertGreater(above_prob, 0.0)
        self.assertLess(above_prob, 1.0)
        self.assertGreater(between_prob, 0.0)
        self.assertLess(between_prob, above_prob)

    def test_signal_tape_latest_before_respects_window(self) -> None:
        tape = sweep.SignalTape()
        tape.push_decimal("yes_ask", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.40, retention_ms=20_000)
        tape.push_decimal("yes_ask", ts_ms=4_000, received_at_ms=4_010, sequence=11, value=0.35, retention_ms=20_000)

        self.assertEqual(tape.latest_decimal_before("yes_ask", 4_000, 5_000), 0.40)
        self.assertIsNone(tape.latest_decimal_before("yes_ask", 4_000, 2_000))

    def test_signal_tape_window_snapshot_captures_extremes(self) -> None:
        tape = sweep.SignalTape()
        tape.push_decimal("no_ask", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.40, retention_ms=20_000)
        tape.push_decimal("no_ask", ts_ms=2_500, received_at_ms=2_510, sequence=11, value=0.37, retention_ms=20_000)
        tape.push_decimal("no_ask", ts_ms=4_200, received_at_ms=4_210, sequence=12, value=0.35, retention_ms=20_000)
        tape.push_decimal("no_ask", ts_ms=4_800, received_at_ms=4_810, sequence=13, value=0.36, retention_ms=20_000)

        snapshot = tape.window_decimal_snapshot("no_ask", 5_000, 5_000)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.sample_count, 4)
        self.assertAlmostEqual(snapshot.first_value, 0.40)
        self.assertAlmostEqual(snapshot.last_value, 0.36)
        self.assertAlmostEqual(snapshot.min_value, 0.35)
        self.assertAlmostEqual(snapshot.max_value, 0.40)
        self.assertEqual(snapshot.min_ts_ms, 4_200)
        self.assertEqual(snapshot.max_ts_ms, 1_000)

    def test_compute_flow_reversion_metrics_detects_first_refill(self) -> None:
        window = sweep.SignalWindowSnapshot(
            first_value=0.40,
            last_value=0.36,
            min_value=0.35,
            max_value=0.40,
            min_ts_ms=4_200,
            max_ts_ms=1_000,
            sample_count=4,
        )

        metrics = sweep.compute_flow_reversion_metrics(
            window=window,
            current_value=0.36,
            now_ms=5_000,
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertAlmostEqual(metrics.window_displacement_bps, 1250.0)
        self.assertAlmostEqual(metrics.residual_gap_bps, 1000.0)
        self.assertAlmostEqual(metrics.refill_ratio, 0.2)
        self.assertEqual(metrics.extreme_age_ms, 800)

    def test_order_book_executable_quotes_cover_buy_and_sell(self) -> None:
        book = sweep.OrderBook(
            bids={0.34: 100.0, 0.33: 200.0},
            asks={0.35: 100.0, 0.36: 400.0},
            ts_ms=1_000,
            received_at_ms=1_010,
            sequence=10,
        )

        buy = book.executable_buy_notional(100.0)
        sell = book.executable_sell_shares(150.0)

        self.assertAlmostEqual(buy["filled_notional_usd"], 100.0)
        self.assertAlmostEqual(round(buy["avg_price"], 6), 0.356436)
        self.assertAlmostEqual(round(buy["filled_shares"], 6), 280.555556)
        self.assertAlmostEqual(round(sell, 6), 0.336667)

    def test_write_checkpoint_persists_summary_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            payload = {"sample_count": 12, "best_pnl": 3.5}

            checkpoint_path = sweep.write_checkpoint(
                output_dir=output_dir,
                stage="signal_matrix",
                label="coarse",
                payload=payload,
            )

            manifest_path = output_dir / "manifest.json"
            self.assertTrue(checkpoint_path.exists())
            self.assertTrue(manifest_path.exists())

            saved = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(saved["payload"], payload)
            self.assertEqual(manifest["latest"]["signal_matrix"], checkpoint_path.name)

    def test_iter_jsonl_zst_ignores_incomplete_terminal_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            plain_path = Path(tmp_dir) / "sample.jsonl"
            plain_path.write_text('{"ok": 1}\n{"also_ok": 2}\n{"broken":"tail\n', encoding="utf-8")
            zst_path = Path(tmp_dir) / "sample.jsonl.zst"
            subprocess.run(["zstd", "-q", "-f", str(plain_path), "-o", str(zst_path)], check=True)

            rows = list(sweep.iter_jsonl_zst(zst_path))

            self.assertEqual(rows, [{"ok": 1}, {"also_ok": 2}])

    def test_flow_first_candidate_uses_window_extreme_not_last_tick(self) -> None:
        params = self.make_runtime_params()
        market = sweep.MarketSpec(
            symbol="bitcoin-above-74k-on-april-19",
            asset="btc",
            settlement_ts_ms=1_776_758_400_000,
            min_tick_size=0.001,
            cex_price_tick=0.1,
            cex_qty_step=0.001,
            rule=sweep.MarketRule(kind="above", lower_strike=74_000.0, upper_strike=None),
        )
        claim_book = sweep.OrderBook(
            bids={0.3595: 2_000.0, 0.3590: 2_000.0},
            asks={0.3600: 400.0, 0.3610: 400.0, 0.3620: 800.0},
            ts_ms=5_000,
            received_at_ms=5_010,
            sequence=20,
        )
        cex_book = sweep.OrderBook(
            bids={76_999.5: 1.0},
            asks={77_000.5: 1.0},
            ts_ms=5_000,
            received_at_ms=5_015,
            sequence=30,
        )
        market_tape = sweep.SignalTape()
        market_tape.push_decimal("no_ask", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.40, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_500, received_at_ms=4_510, sequence=11, value=0.35, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.351, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.4500, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.4502, retention_ms=20_000)
        asset_cex_tape = sweep.SignalTape()
        asset_cex_tape.push_decimal("cex_mid", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=77_000.0, retention_ms=20_000)
        asset_cex_tape.push_decimal("cex_mid", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=77_001.0, retention_ms=20_000)
        anchor = sweep.AnchorSnapshot(
            ts_ms=4_980,
            index_price=77_000.0,
            expiry_ts_ms=1_776_758_400_000,
            atm_iv=55.0,
            points=[
                sweep.SurfacePoint(strike=74_000.0, mark_iv=54.0),
                sweep.SurfacePoint(strike=77_000.0, mark_iv=55.0),
                sweep.SurfacePoint(strike=80_000.0, mark_iv=57.0),
            ],
            latest_received_at_ms=4_990,
        )

        candidate, reason = sweep._evaluate_claim_candidate(
            params=params,
            market=market,
            claim_side="no",
            claim_book=claim_book,
            cex_book=cex_book,
            market_tape=market_tape,
            asset_cex_tape=asset_cex_tape,
            fair_prob_yes=0.45,
            anchor=anchor,
            now_ms=5_020,
            signal_mode="flow-first",
        )

        self.assertIsNone(reason)
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.signal_mode, "flow-first")
        self.assertGreater(candidate.window_displacement_bps, 1_200.0)
        self.assertGreater(candidate.claim_price_move_bps, 900.0)
        self.assertGreater(candidate.realizable_ev_usd, 0.0)

    def test_build_research_feature_row_preserves_window_geometry(self) -> None:
        params = self.make_runtime_params()
        market = sweep.MarketSpec(
            symbol="bitcoin-above-74k-on-april-19",
            asset="btc",
            settlement_ts_ms=1_776_758_400_000,
            min_tick_size=0.001,
            cex_price_tick=0.1,
            cex_qty_step=0.001,
            rule=sweep.MarketRule(kind="above", lower_strike=74_000.0, upper_strike=None),
        )
        claim_book = sweep.OrderBook(
            bids={0.3595: 2_000.0, 0.3590: 2_000.0},
            asks={0.3600: 400.0, 0.3610: 400.0, 0.3620: 800.0},
            ts_ms=5_000,
            received_at_ms=5_010,
            sequence=20,
        )
        cex_book = sweep.OrderBook(
            bids={76_999.5: 1.0},
            asks={77_000.5: 1.0},
            ts_ms=5_000,
            received_at_ms=5_015,
            sequence=30,
        )
        market_tape = sweep.SignalTape()
        market_tape.push_decimal("no_ask", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.40, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_500, received_at_ms=4_510, sequence=11, value=0.35, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.351, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.4500, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.4502, retention_ms=20_000)
        asset_cex_tape = sweep.SignalTape()
        asset_cex_tape.push_decimal("cex_mid", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=77_000.0, retention_ms=20_000)
        asset_cex_tape.push_decimal("cex_mid", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=77_001.0, retention_ms=20_000)
        anchor = sweep.AnchorSnapshot(
            ts_ms=4_980,
            index_price=77_000.0,
            expiry_ts_ms=1_776_758_400_000,
            atm_iv=55.0,
            points=[
                sweep.SurfacePoint(strike=74_000.0, mark_iv=54.0),
                sweep.SurfacePoint(strike=77_000.0, mark_iv=55.0),
                sweep.SurfacePoint(strike=80_000.0, mark_iv=57.0),
            ],
            latest_received_at_ms=4_990,
        )

        row, reason = sweep.build_research_feature_row(
            params=params,
            market=market,
            claim_side="no",
            claim_book=claim_book,
            cex_book=cex_book,
            market_tape=market_tape,
            asset_cex_tape=asset_cex_tape,
            fair_prob_yes=0.45,
            anchor=anchor,
            now_ms=5_020,
        )

        self.assertIsNone(reason)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.signal_mode, "flow-first")
        self.assertTrue(row.flow_shape_valid)
        self.assertAlmostEqual(row.window_high_price, 0.40)
        self.assertAlmostEqual(row.window_low_price, 0.35)
        self.assertAlmostEqual(row.flow_refill_ratio, 0.2, places=3)
        self.assertGreater(row.flow_residual_gap_bps, 900.0)
        self.assertAlmostEqual(row.residual_basis_bps, ((0.55 - row.entry_price) / row.entry_price) * 10_000.0, places=3)
        self.assertAlmostEqual(row.anchor_follow_ratio, row.fair_claim_move_bps / row.window_displacement_bps, places=6)
        self.assertEqual(row.market_rule_kind, "above")
        self.assertEqual(row.market_family, "directional")
        self.assertEqual(row.entry_price_bucket, "mid")
        self.assertEqual(row.time_to_settlement_bucket, "gt72h")

    def test_build_research_feature_row_includes_confirmation_fields(self) -> None:
        params = sweep.replace(
            self.make_runtime_params(),
            poly_open_max_quote_age_ms=4_000,
            cex_open_max_quote_age_ms=4_000,
            max_anchor_age_ms=4_000,
            max_anchor_latency_delta_ms=4_000,
        )
        market = sweep.MarketSpec(
            symbol="bitcoin-above-74k-on-april-19",
            asset="btc",
            settlement_ts_ms=1_776_758_400_000,
            min_tick_size=0.001,
            cex_price_tick=0.1,
            cex_qty_step=0.001,
            rule=sweep.MarketRule(kind="above", lower_strike=74_000.0, upper_strike=None),
        )
        claim_book = sweep.OrderBook(
            bids={0.3595: 2_000.0, 0.3590: 2_000.0},
            asks={0.3600: 400.0, 0.3610: 400.0, 0.3620: 800.0},
            ts_ms=5_000,
            received_at_ms=5_010,
            sequence=20,
        )
        cex_book = sweep.OrderBook(
            bids={76_999.5: 1.0},
            asks={77_000.5: 1.0},
            ts_ms=5_000,
            received_at_ms=5_015,
            sequence=30,
        )
        market_tape = sweep.SignalTape()
        market_tape.push_decimal("no_ask", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.40, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_500, received_at_ms=4_510, sequence=11, value=0.35, retention_ms=20_000)
        market_tape.push_decimal("no_ask", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.351, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=0.4500, retention_ms=20_000)
        market_tape.push_float("fair_prob_yes", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=0.4502, retention_ms=20_000)
        asset_cex_tape = sweep.SignalTape()
        asset_cex_tape.push_decimal("cex_mid", ts_ms=1_000, received_at_ms=1_010, sequence=10, value=77_000.0, retention_ms=20_000)
        asset_cex_tape.push_decimal("cex_mid", ts_ms=4_900, received_at_ms=4_910, sequence=12, value=77_001.0, retention_ms=20_000)
        anchor = sweep.AnchorSnapshot(
            ts_ms=4_980,
            index_price=77_000.0,
            expiry_ts_ms=1_776_758_400_000,
            atm_iv=55.0,
            points=[
                sweep.SurfacePoint(strike=74_000.0, mark_iv=54.0),
                sweep.SurfacePoint(strike=77_000.0, mark_iv=55.0),
                sweep.SurfacePoint(strike=80_000.0, mark_iv=57.0),
            ],
            latest_received_at_ms=4_990,
        )
        confirmation_samples = [
            sweep.ConfirmationSample(ts_ms=1_000, price=0.40, spread_bps=200.0, top3_bid_notional=100.0),
            sweep.ConfirmationSample(ts_ms=4_500, price=0.35, spread_bps=500.0, top3_bid_notional=80.0),
            sweep.ConfirmationSample(ts_ms=5_500, price=0.37, spread_bps=460.0, top3_bid_notional=90.0),
            sweep.ConfirmationSample(ts_ms=7_500, price=0.39, spread_bps=420.0, top3_bid_notional=120.0),
        ]

        row, reason = sweep.build_research_feature_row(
            params=params,
            market=market,
            claim_side="no",
            claim_book=claim_book,
            cex_book=cex_book,
            market_tape=market_tape,
            asset_cex_tape=asset_cex_tape,
            fair_prob_yes=0.45,
            anchor=anchor,
            now_ms=8_000,
            confirmation_samples=confirmation_samples,
        )

        self.assertIsNone(reason)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertTrue(row.confirm_window_complete_1s)
        self.assertTrue(row.confirm_window_complete_3s)
        self.assertAlmostEqual(row.post_pulse_refill_ratio_1s, 0.4, places=3)
        self.assertAlmostEqual(row.post_pulse_refill_ratio_3s, 0.8, places=3)
        self.assertAlmostEqual(row.spread_snapback_bps, 80.0)
        self.assertAlmostEqual(row.bid_depth_rebuild_ratio, 1.5)

    def test_build_aggressive_rule_masks_adds_directional_dislocation_filters(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.08,
                    "fair_claim_price": 0.10,
                    "current_bid": 0.079,
                    "current_ask": 0.08,
                    "current_mid": 0.0795,
                    "window_high_price": 0.10,
                    "min_tick_size": 0.01,
                    "window_displacement_bps": 2000.0,
                    "flow_refill_ratio": 0.20,
                    "flow_extreme_age_ms": 500,
                    "fair_claim_move_bps": 100.0,
                    "cex_mid_move_bps": 20.0,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "low",
                    "time_to_settlement_bucket": "24h_to_72h",
                    "time_to_settlement_hours": 30.0,
                    "residual_basis_bps": 2500.0,
                    "anchor_follow_ratio": 0.05,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                },
                {
                    "asset": "eth",
                    "claim_side": "yes",
                    "entry_price": 0.42,
                    "fair_claim_price": 0.43,
                    "current_bid": 0.41,
                    "current_ask": 0.42,
                    "current_mid": 0.415,
                    "window_high_price": 0.44,
                    "min_tick_size": 0.01,
                    "window_displacement_bps": 1500.0,
                    "flow_refill_ratio": 0.20,
                    "flow_extreme_age_ms": 500,
                    "fair_claim_move_bps": 1200.0,
                    "cex_mid_move_bps": 30.0,
                    "market_rule_kind": "between",
                    "market_family": "range",
                    "entry_price_bucket": "mid",
                    "time_to_settlement_bucket": "lt6h",
                    "time_to_settlement_hours": 2.0,
                    "residual_basis_bps": 238.1,
                    "anchor_follow_ratio": 0.80,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                },
            ]
        )

        masks = dict(sweep.build_aggressive_rule_masks(frame))

        self.assertIn("rule=directional", masks)
        self.assertIn("entry<=0.15", masks)
        self.assertIn("tts>=24h", masks)
        self.assertIn("anchor_follow<=0.10", masks)
        self.assertIn("setup=directional_dislocation", masks)
        self.assertEqual(masks["rule=directional"].tolist(), [True, False])
        self.assertEqual(masks["entry<=0.15"].tolist(), [True, False])
        self.assertEqual(masks["tts>=24h"].tolist(), [True, False])
        self.assertEqual(masks["anchor_follow<=0.10"].tolist(), [True, False])
        self.assertEqual(masks["setup=directional_dislocation"].tolist(), [True, False])

    def test_enrich_frame_with_market_context_backfills_structural_columns(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 1_776_600_000_000,
                    "symbol": "bitcoin-above-74k-on-april-19",
                    "entry_price": 0.08,
                    "fair_claim_price": 0.10,
                    "window_displacement_bps": 2000.0,
                    "fair_claim_move_bps": 100.0,
                }
            ]
        )
        catalog = {
            "bitcoin-above-74k-on-april-19": sweep.MarketSpec(
                symbol="bitcoin-above-74k-on-april-19",
                asset="btc",
                settlement_ts_ms=1_776_758_400_000,
                min_tick_size=0.01,
                cex_price_tick=0.1,
                cex_qty_step=0.001,
                rule=sweep.MarketRule(kind="above", lower_strike=74_000.0, upper_strike=None),
            )
        }

        enriched = sweep.enrich_frame_with_market_context(frame, catalog)

        row = enriched.iloc[0]
        self.assertAlmostEqual(row["residual_basis_bps"], 2500.0)
        self.assertAlmostEqual(row["anchor_follow_ratio"], 0.05)
        self.assertEqual(row["market_rule_kind"], "above")
        self.assertEqual(row["market_family"], "directional")
        self.assertEqual(row["entry_price_bucket"], "low")
        self.assertEqual(row["time_to_settlement_bucket"], "24h_to_72h")

    def test_rank_cross_day_rules_requires_every_day_positive(self) -> None:
        day1 = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 1_000,
                    "symbol": "btc-a",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.08,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.12,
                    "window_high_price": 0.14,
                    "window_displacement_bps": 2000.0,
                    "flow_residual_gap_bps": 1000.0,
                    "flow_refill_ratio": 0.20,
                    "fair_claim_move_bps": 80.0,
                    "cex_mid_move_bps": 5.0,
                    "flow_extreme_age_ms": 500,
                    "current_bid": 0.07,
                    "current_ask": 0.08,
                    "current_mid": 0.075,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "low",
                    "time_to_settlement_hours": 30.0,
                    "time_to_settlement_bucket": "24h_to_72h",
                    "residual_basis_bps": 5000.0,
                    "anchor_follow_ratio": 0.04,
                    "outcome_future_max_bid_60m": 0.20,
                    "outcome_future_max_touch_60m": 0.21,
                    "outcome_deadline_bid_60m": 0.09,
                },
                {
                    "ts_ms": 2_000,
                    "symbol": "eth-a",
                    "asset": "eth",
                    "claim_side": "yes",
                    "entry_price": 0.45,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.47,
                    "window_high_price": 0.46,
                    "window_displacement_bps": 800.0,
                    "flow_residual_gap_bps": 200.0,
                    "flow_refill_ratio": 0.80,
                    "fair_claim_move_bps": 500.0,
                    "cex_mid_move_bps": 30.0,
                    "flow_extreme_age_ms": 3500,
                    "current_bid": 0.44,
                    "current_ask": 0.45,
                    "current_mid": 0.445,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "market_rule_kind": "between",
                    "market_family": "range",
                    "entry_price_bucket": "mid",
                    "time_to_settlement_hours": 4.0,
                    "time_to_settlement_bucket": "lt6h",
                    "residual_basis_bps": 444.4,
                    "anchor_follow_ratio": 0.60,
                    "outcome_future_max_bid_60m": 0.44,
                    "outcome_future_max_touch_60m": 0.44,
                    "outcome_deadline_bid_60m": 0.40,
                },
            ]
        )
        day2 = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 3_000,
                    "symbol": "btc-b",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.09,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.13,
                    "window_high_price": 0.15,
                    "window_displacement_bps": 2200.0,
                    "flow_residual_gap_bps": 1100.0,
                    "flow_refill_ratio": 0.18,
                    "fair_claim_move_bps": 90.0,
                    "cex_mid_move_bps": 6.0,
                    "flow_extreme_age_ms": 400,
                    "current_bid": 0.08,
                    "current_ask": 0.09,
                    "current_mid": 0.085,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "low",
                    "time_to_settlement_hours": 28.0,
                    "time_to_settlement_bucket": "24h_to_72h",
                    "residual_basis_bps": 4444.4,
                    "anchor_follow_ratio": 0.0409,
                    "outcome_future_max_bid_60m": 0.19,
                    "outcome_future_max_touch_60m": 0.20,
                    "outcome_deadline_bid_60m": 0.10,
                },
                {
                    "ts_ms": 4_000,
                    "symbol": "eth-b",
                    "asset": "eth",
                    "claim_side": "yes",
                    "entry_price": 0.46,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.49,
                    "window_high_price": 0.47,
                    "window_displacement_bps": 900.0,
                    "flow_residual_gap_bps": 220.0,
                    "flow_refill_ratio": 0.70,
                    "fair_claim_move_bps": 550.0,
                    "cex_mid_move_bps": 35.0,
                    "flow_extreme_age_ms": 3600,
                    "current_bid": 0.45,
                    "current_ask": 0.46,
                    "current_mid": 0.455,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "market_rule_kind": "between",
                    "market_family": "range",
                    "entry_price_bucket": "mid",
                    "time_to_settlement_hours": 5.0,
                    "time_to_settlement_bucket": "lt6h",
                    "residual_basis_bps": 652.1,
                    "anchor_follow_ratio": 0.6111,
                    "outcome_future_max_bid_60m": 0.50,
                    "outcome_future_max_touch_60m": 0.51,
                    "outcome_deadline_bid_60m": 0.30,
                },
            ]
        )

        ranked = sweep.rank_cross_day_rules(
            frames_by_label={"day1": day1, "day2": day2},
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=sweep.AggressiveExitConfig("60m", 5, "any_touch", "raw_tick"),
            min_trades_per_day=1,
            top_k=5,
        )

        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["rule_name"], "setup=directional_dislocation")
        self.assertEqual(ranked[0]["day_stats"]["day1"]["trade_count"], 1)
        self.assertEqual(ranked[0]["day_stats"]["day2"]["trade_count"], 1)
        self.assertGreater(ranked[0]["day_stats"]["day1"]["total_realized_pnl_usd"], 0.0)
        self.assertGreater(ranked[0]["day_stats"]["day2"]["total_realized_pnl_usd"], 0.0)

    def test_scan_feature_grid_prefers_stricter_refill_when_loose_combo_loses_money(self) -> None:
        rows = [
            sweep.ResearchFeatureRow(
                ts_ms=5_000,
                asset="btc",
                symbol="good-btc",
                signal_mode="flow-first",
                claim_side="no",
                entry_price=0.361,
                current_bid=0.3595,
                current_ask=0.3600,
                current_mid=0.35975,
                filled_notional_usd=450.0,
                filled_shares=1246.5373961218836,
                levels_consumed=3,
                depth_fill_ratio=1.0,
                min_tick_size=0.001,
                fair_prob_yes=0.45,
                fair_claim_price=0.55,
                anchor_age_ms=30,
                anchor_latency_delta_ms=40,
                window_first_price=0.40,
                window_last_price=0.351,
                window_high_price=0.40,
                window_low_price=0.35,
                window_high_ts_ms=1_000,
                window_low_ts_ms=4_500,
                claim_move_bps=975.0,
                window_displacement_bps=1250.0,
                flow_refill_ratio=0.20,
                flow_rebound_bps=285.7,
                flow_residual_gap_bps=1000.0,
                flow_extreme_age_ms=500,
                flow_shape_valid=True,
                fair_claim_move_bps=2.0,
                cex_mid_move_bps=8.0,
                timeout_exit_price=0.3595,
            ),
            sweep.ResearchFeatureRow(
                ts_ms=5_100,
                asset="btc",
                symbol="bad-btc",
                signal_mode="flow-first",
                claim_side="no",
                entry_price=0.361,
                current_bid=0.3450,
                current_ask=0.3600,
                current_mid=0.3525,
                filled_notional_usd=450.0,
                filled_shares=1246.5373961218836,
                levels_consumed=1,
                depth_fill_ratio=1.0,
                min_tick_size=0.001,
                fair_prob_yes=0.45,
                fair_claim_price=0.55,
                anchor_age_ms=30,
                anchor_latency_delta_ms=40,
                window_first_price=0.40,
                window_last_price=0.355,
                window_high_price=0.40,
                window_low_price=0.35,
                window_high_ts_ms=1_000,
                window_low_ts_ms=4_500,
                claim_move_bps=975.0,
                window_displacement_bps=1250.0,
                flow_refill_ratio=0.75,
                flow_rebound_bps=857.1,
                flow_residual_gap_bps=1000.0,
                flow_extreme_age_ms=500,
                flow_shape_valid=True,
                fair_claim_move_bps=2.0,
                cex_mid_move_bps=8.0,
                timeout_exit_price=0.3450,
            ),
        ]

        result = sweep.scan_feature_grid(
            rows,
            min_expected_net_pnl_usd=0.1,
            min_net_session_edge_bps=5.0,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            param_grid={
                "min_displacement_bps": [500.0],
                "min_residual_gap_bps": [500.0],
                "max_refill_ratio": [0.30, 0.90],
                "max_fair_move_bps": [20.0],
                "max_cex_move_bps": [20.0],
                "target_ticks": [1],
                "max_timeout_loss_usd": [20.0],
                "max_required_hit_rate": [0.95],
            },
        )

        self.assertIsNotNone(result["best_combo"])
        self.assertEqual(result["best_combo"]["max_refill_ratio"], 0.30)
        self.assertEqual(result["best_combo"]["candidate_count"], 1)
        self.assertGreater(result["best_combo"]["total_realizable_ev_usd"], 0.0)

    def test_compute_horizon_price_outcomes_tracks_future_touch_and_deadline_bid(self) -> None:
        outcome = sweep.compute_horizon_price_outcomes(
            times_ms=[1_000, 2_000, 3_000, 5_000],
            best_bids=[0.30, 0.35, 0.25, 0.40],
            last_trades=[0.00, 0.36, 0.00, 0.38],
            horizon_ms=1_500,
        )

        self.assertEqual(outcome.future_max_bid, [0.35, 0.35, 0.25, 0.4])
        self.assertEqual(outcome.future_max_touch, [0.36, 0.36, 0.25, 0.4])
        self.assertEqual(outcome.deadline_bid, [0.35, 0.25, 0.25, 0.4])

    def test_rank_aggressive_rules_finds_profitable_subset(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 1_000,
                    "symbol": "btc-a",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.40,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.55,
                    "window_high_price": 0.48,
                    "window_displacement_bps": 1500.0,
                    "flow_residual_gap_bps": 700.0,
                    "flow_refill_ratio": 0.10,
                    "fair_claim_move_bps": 3.0,
                    "cex_mid_move_bps": 4.0,
                    "flow_extreme_age_ms": 300,
                    "current_bid": 0.39,
                    "current_ask": 0.40,
                    "current_mid": 0.395,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.48,
                    "outcome_deadline_bid_15m": 0.38,
                },
                {
                    "ts_ms": 2_000,
                    "symbol": "btc-b",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.42,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.56,
                    "window_high_price": 0.49,
                    "window_displacement_bps": 1400.0,
                    "flow_residual_gap_bps": 650.0,
                    "flow_refill_ratio": 0.12,
                    "fair_claim_move_bps": 4.0,
                    "cex_mid_move_bps": 5.0,
                    "flow_extreme_age_ms": 500,
                    "current_bid": 0.41,
                    "current_ask": 0.42,
                    "current_mid": 0.415,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.50,
                    "outcome_deadline_bid_15m": 0.40,
                },
                {
                    "ts_ms": 3_000,
                    "symbol": "eth-a",
                    "asset": "eth",
                    "claim_side": "yes",
                    "entry_price": 0.60,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.62,
                    "window_high_price": 0.64,
                    "window_displacement_bps": 200.0,
                    "flow_residual_gap_bps": 80.0,
                    "flow_refill_ratio": 0.80,
                    "fair_claim_move_bps": 40.0,
                    "cex_mid_move_bps": 50.0,
                    "flow_extreme_age_ms": 3_000,
                    "current_bid": 0.55,
                    "current_ask": 0.60,
                    "current_mid": 0.575,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.58,
                    "outcome_future_max_touch_15m": 0.59,
                    "outcome_deadline_bid_15m": 0.52,
                },
            ]
        )
        exit_config = sweep.AggressiveExitConfig(
            horizon_label="15m",
            target_ticks=3,
            touch_mode="any_touch",
            target_mode="raw_tick",
        )
        derived = sweep.derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=exit_config,
        )

        ranked = sweep.rank_aggressive_signal_rules(
            frame=derived,
            min_trades=1,
            top_k=5,
        )

        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["rule_name"], "asset=btc")
        self.assertEqual(ranked[0]["trade_count"], 2)
        self.assertGreater(ranked[0]["avg_realized_pnl_usd"], 0.0)

    def test_rank_aggressive_rules_applies_symbol_side_cooldown(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 1_000,
                    "symbol": "btc-a",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.40,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.55,
                    "window_high_price": 0.48,
                    "window_displacement_bps": 1500.0,
                    "flow_residual_gap_bps": 700.0,
                    "flow_refill_ratio": 0.10,
                    "fair_claim_move_bps": 3.0,
                    "cex_mid_move_bps": 4.0,
                    "flow_extreme_age_ms": 300,
                    "current_bid": 0.39,
                    "current_ask": 0.40,
                    "current_mid": 0.395,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.48,
                    "outcome_deadline_bid_15m": 0.38,
                },
                {
                    "ts_ms": 1_100,
                    "symbol": "btc-a",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.41,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.56,
                    "window_high_price": 0.49,
                    "window_displacement_bps": 1400.0,
                    "flow_residual_gap_bps": 650.0,
                    "flow_refill_ratio": 0.12,
                    "fair_claim_move_bps": 4.0,
                    "cex_mid_move_bps": 5.0,
                    "flow_extreme_age_ms": 500,
                    "current_bid": 0.40,
                    "current_ask": 0.41,
                    "current_mid": 0.405,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.50,
                    "outcome_deadline_bid_15m": 0.40,
                },
            ]
        )
        derived = sweep.derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=sweep.AggressiveExitConfig(
                horizon_label="15m",
                target_ticks=3,
                touch_mode="any_touch",
                target_mode="raw_tick",
            ),
        )

        ranked = sweep.rank_aggressive_signal_rules(
            frame=derived,
            min_trades=1,
            top_k=5,
            cooldown_ms=15 * 60 * 1000,
        )

        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["trade_count"], 1)

    def test_rank_exit_config_baselines_prefers_higher_dedup_realized_pnl(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "ts_ms": 1_000,
                    "symbol": "btc-a",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.40,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.55,
                    "window_high_price": 0.48,
                    "window_displacement_bps": 1500.0,
                    "flow_residual_gap_bps": 700.0,
                    "flow_refill_ratio": 0.10,
                    "fair_claim_move_bps": 3.0,
                    "cex_mid_move_bps": 4.0,
                    "flow_extreme_age_ms": 300,
                    "current_bid": 0.39,
                    "current_ask": 0.40,
                    "current_mid": 0.395,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.48,
                    "outcome_deadline_bid_15m": 0.38,
                    "outcome_future_max_bid_60m": 0.60,
                    "outcome_future_max_touch_60m": 0.62,
                    "outcome_deadline_bid_60m": 0.39,
                },
                {
                    "ts_ms": 2_000,
                    "symbol": "btc-b",
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.42,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.56,
                    "window_high_price": 0.49,
                    "window_displacement_bps": 1400.0,
                    "flow_residual_gap_bps": 650.0,
                    "flow_refill_ratio": 0.12,
                    "fair_claim_move_bps": 4.0,
                    "cex_mid_move_bps": 5.0,
                    "flow_extreme_age_ms": 500,
                    "current_bid": 0.41,
                    "current_ask": 0.42,
                    "current_mid": 0.415,
                    "filled_notional_usd": 100.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "outcome_future_max_bid_15m": 0.47,
                    "outcome_future_max_touch_15m": 0.50,
                    "outcome_deadline_bid_15m": 0.40,
                    "outcome_future_max_bid_60m": 0.60,
                    "outcome_future_max_touch_60m": 0.63,
                    "outcome_deadline_bid_60m": 0.41,
                },
            ]
        )

        ranked = sweep.rank_exit_config_baselines(
            frame=frame,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_configs=[
                sweep.AggressiveExitConfig("15m", 3, "any_touch", "raw_tick"),
                sweep.AggressiveExitConfig("60m", 5, "any_touch", "raw_tick"),
            ],
            top_k=2,
        )

        self.assertEqual(ranked[0]["exit_config_name"], "60m:any_touch:raw_tick:ticks5")
        self.assertGreater(ranked[0]["total_realized_pnl_usd"], ranked[1]["total_realized_pnl_usd"])

    def test_compute_post_pulse_confirmation_metrics_uses_low_anchor_and_windows(self) -> None:
        samples = [
            sweep.ConfirmationSample(ts_ms=1_000, price=0.40, spread_bps=200.0, top3_bid_notional=100.0),
            sweep.ConfirmationSample(ts_ms=2_000, price=0.30, spread_bps=500.0, top3_bid_notional=80.0),
            sweep.ConfirmationSample(ts_ms=3_000, price=0.34, spread_bps=450.0, top3_bid_notional=90.0),
            sweep.ConfirmationSample(ts_ms=5_000, price=0.37, spread_bps=430.0, top3_bid_notional=120.0),
        ]

        metrics = sweep.compute_post_pulse_confirmation_metrics(
            samples=samples,
            window_high_price=0.40,
            window_low_price=0.30,
            window_low_ts_ms=2_000,
            now_ts_ms=5_000,
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertTrue(metrics.confirm_window_complete_1s)
        self.assertTrue(metrics.confirm_window_complete_3s)
        self.assertAlmostEqual(metrics.post_pulse_refill_ratio_1s, 0.4)
        self.assertAlmostEqual(metrics.post_pulse_refill_ratio_3s, 0.7)
        self.assertAlmostEqual(metrics.spread_snapback_bps, 70.0)
        self.assertAlmostEqual(metrics.bid_depth_rebuild_ratio, 1.5)

    def test_compute_post_pulse_confirmation_metrics_marks_incomplete_windows(self) -> None:
        samples = [
            sweep.ConfirmationSample(ts_ms=1_000, price=0.40, spread_bps=200.0, top3_bid_notional=100.0),
            sweep.ConfirmationSample(ts_ms=2_000, price=0.30, spread_bps=500.0, top3_bid_notional=80.0),
            sweep.ConfirmationSample(ts_ms=2_500, price=0.31, spread_bps=480.0, top3_bid_notional=82.0),
        ]

        metrics = sweep.compute_post_pulse_confirmation_metrics(
            samples=samples,
            window_high_price=0.40,
            window_low_price=0.30,
            window_low_ts_ms=2_000,
            now_ts_ms=2_500,
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertFalse(metrics.confirm_window_complete_1s)
        self.assertFalse(metrics.confirm_window_complete_3s)
        self.assertIsNone(metrics.post_pulse_refill_ratio_1s)
        self.assertIsNone(metrics.post_pulse_refill_ratio_3s)
        self.assertIsNone(metrics.spread_snapback_bps)
        self.assertIsNone(metrics.bid_depth_rebuild_ratio)

    def test_rank_dislocation_strategy_matrix_filters_by_positive_days_and_delta(self) -> None:
        rows = [
            {
                "symbol": "btc-strong",
                "claim_side": "no",
                "ts_ms": 10_000,
                "asset": "btc",
                "entry_price": 0.10,
                "min_tick_size": 0.01,
                "fair_claim_price": 0.20,
                "window_high_price": 0.18,
                "current_mid": 0.10,
                "filled_notional_usd": 100.0,
                "current_bid": 0.09,
                "current_ask": 0.10,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "window_displacement_bps": 1800.0,
                "flow_residual_gap_bps": 500.0,
                "flow_refill_ratio": 0.20,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "flow_extreme_age_ms": 500,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "time_to_settlement_hours": 48.0,
                "post_pulse_refill_ratio_1s": 0.30,
                "post_pulse_refill_ratio_3s": 0.40,
                "spread_snapback_bps": 80.0,
                "bid_depth_rebuild_ratio": 1.50,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
                "outcome_future_max_touch_15m": 0.11,
                "outcome_deadline_bid_15m": 0.09,
            },
            {
                "symbol": "btc-weak",
                "claim_side": "no",
                "ts_ms": 1_000_000,
                "asset": "btc",
                "entry_price": 0.10,
                "min_tick_size": 0.01,
                "fair_claim_price": 0.20,
                "window_high_price": 0.18,
                "current_mid": 0.10,
                "filled_notional_usd": 100.0,
                "current_bid": 0.08,
                "current_ask": 0.10,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "window_displacement_bps": 1800.0,
                "flow_residual_gap_bps": 500.0,
                "flow_refill_ratio": 0.20,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "flow_extreme_age_ms": 500,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "time_to_settlement_hours": 48.0,
                "post_pulse_refill_ratio_1s": 0.10,
                "post_pulse_refill_ratio_3s": 0.20,
                "spread_snapback_bps": 10.0,
                "bid_depth_rebuild_ratio": 1.0,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
                "outcome_future_max_touch_15m": 0.10,
                "outcome_deadline_bid_15m": 0.098,
            },
        ]
        day1 = sweep.pd.DataFrame(rows)
        day2 = sweep.pd.DataFrame(rows)

        ranked = sweep.rank_dislocation_strategy_matrix(
            frames_by_label={"day1": day1, "day2": day2},
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_trades_per_day=1,
        )

        self.assertTrue(ranked)
        baseline = next(item for item in ranked if item["strategy_name"] == "structure_core")
        strong = next(item for item in ranked if item["strategy_name"] == "structure_core + refill3s_fast")
        self.assertEqual(baseline["day_stats"]["day1"]["trade_count"], 2)
        self.assertEqual(strong["day_stats"]["day1"]["trade_count"], 1)
        self.assertAlmostEqual(baseline["total_realized_pnl_usd"], 16.0)
        self.assertAlmostEqual(strong["total_realized_pnl_usd"], 20.0)
        self.assertAlmostEqual(strong["delta_vs_structure_core"], 4.0)

    def test_run_dislocation_strategy_matrix_writes_checkpoint(self) -> None:
        rows = [
            {
                "symbol": "btc-strong",
                "claim_side": "no",
                "ts_ms": 10_000,
                "asset": "btc",
                "entry_price": 0.10,
                "min_tick_size": 0.01,
                "fair_claim_price": 0.20,
                "window_high_price": 0.18,
                "current_mid": 0.10,
                "filled_notional_usd": 100.0,
                "current_bid": 0.09,
                "current_ask": 0.10,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "window_displacement_bps": 1800.0,
                "flow_residual_gap_bps": 500.0,
                "flow_refill_ratio": 0.20,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "flow_extreme_age_ms": 500,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "time_to_settlement_hours": 48.0,
                "post_pulse_refill_ratio_1s": 0.30,
                "post_pulse_refill_ratio_3s": 0.40,
                "spread_snapback_bps": 80.0,
                "bid_depth_rebuild_ratio": 1.50,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
                "outcome_future_max_touch_15m": 0.11,
                "outcome_deadline_bid_15m": 0.09,
            }
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            checkpoint_path = sweep.run_dislocation_strategy_matrix(
                frames_by_label={"day1": sweep.pd.DataFrame(rows), "day2": sweep.pd.DataFrame(rows)},
                output_dir=Path(tmp_dir),
                label_prefix="unit_test_matrix",
                poly_fee_bps=0.0,
                cex_taker_fee_bps=0.0,
                cex_slippage_bps=0.0,
            )

            payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))["payload"]
            self.assertEqual(payload["exit_config_name"], "15m:any_touch:raw_tick:ticks1")
            self.assertTrue(payload["ranked_strategies"])
            self.assertEqual(payload["ranked_strategies"][0]["strategy_name"], "structure_core")

    def test_derive_dislocation_reachability_frame_aligns_break_even_to_tick_floor(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "symbol": "btc-reach",
                    "claim_side": "no",
                    "ts_ms": 10_000,
                    "asset": "btc",
                    "entry_price": 0.10,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.20,
                    "window_high_price": 0.18,
                    "current_mid": 0.095,
                    "filled_notional_usd": 100.0,
                    "current_bid": 0.09,
                    "current_ask": 0.10,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "window_displacement_bps": 1800.0,
                    "flow_residual_gap_bps": 500.0,
                    "flow_refill_ratio": 0.20,
                    "fair_claim_move_bps": 2.0,
                    "cex_mid_move_bps": 2.0,
                    "flow_extreme_age_ms": 500,
                    "residual_basis_bps": 500.0,
                    "anchor_follow_ratio": 0.10,
                    "market_rule_kind": "above",
                    "time_to_settlement_hours": 48.0,
                    "timeout_exit_price": 0.09,
                    "outcome_future_max_touch_15m": 0.11,
                    "outcome_deadline_bid_15m": 0.09,
                }
            ]
        )

        derived = sweep.derive_dislocation_reachability_frame(
            frame=frame,
            poly_fee_bps=20.0,
            cex_taker_fee_bps=5.0,
            cex_slippage_bps=2.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_expected_net_pnl_usd=1.0,
        )

        row = derived.iloc[0]
        self.assertAlmostEqual(row.min_profitable_target_distance_bps, 42.0)
        self.assertAlmostEqual(row.min_realizable_target_distance_bps, 1000.0)
        self.assertAlmostEqual(row.fixed_target_distance_to_mid_bps, 1578.9473684210527)
        self.assertAlmostEqual(row.fixed_target_required_hit_rate, 0.571, places=3)
        self.assertEqual(row.fixed_target_predicted_hit_rate, 0.0)
        self.assertTrue(bool(row.hit_target))

    def test_evaluate_dislocation_reachability_matrix_reports_reachability_dead_zone(self) -> None:
        rows = [
            {
                "symbol": "btc-reach",
                "claim_side": "no",
                "ts_ms": 10_000,
                "asset": "btc",
                "entry_price": 0.10,
                "min_tick_size": 0.01,
                "fair_claim_price": 0.20,
                "window_high_price": 0.18,
                "current_mid": 0.095,
                "filled_notional_usd": 100.0,
                "current_bid": 0.09,
                "current_ask": 0.10,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "window_displacement_bps": 1800.0,
                "flow_residual_gap_bps": 500.0,
                "flow_refill_ratio": 0.20,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "flow_extreme_age_ms": 500,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "time_to_settlement_hours": 48.0,
                "timeout_exit_price": 0.09,
                "outcome_future_max_touch_15m": 0.11,
                "outcome_deadline_bid_15m": 0.09,
            }
        ]
        result = sweep.evaluate_dislocation_reachability_matrix(
            frames_by_label={"day1": sweep.pd.DataFrame(rows), "day2": sweep.pd.DataFrame(rows)},
            poly_fee_bps=20.0,
            cex_taker_fee_bps=5.0,
            cex_slippage_bps=2.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_expected_net_pnl_usd=1.0,
            min_trades_per_day=1,
        )

        self.assertEqual(result["positive_strategy_count"], 1)
        baseline = next(item for item in result["strategy_stats"] if item["strategy_name"] == "structure_core")
        reachable_core = next(item for item in result["strategy_stats"] if item["strategy_name"] == "reachable_core_150")
        snapback = next(item for item in result["strategy_stats"] if item["strategy_name"] == "reachable_snapback_120")
        self.assertTrue(baseline["passes_cross_day"])
        self.assertAlmostEqual(baseline["day_stats"]["day1"]["fixed_target_distance_to_mid_bps_p50"], 1578.9473684210527)
        self.assertEqual(reachable_core["day_stats"]["day1"]["trade_count"], 0)
        self.assertFalse(reachable_core["passes_cross_day"])
        self.assertEqual(snapback["day_stats"]["day1"]["trade_count"], 0)
        self.assertFalse(snapback["passes_cross_day"])

    def test_run_dislocation_reachability_matrix_writes_checkpoint(self) -> None:
        rows = [
            {
                "symbol": "btc-reach",
                "claim_side": "no",
                "ts_ms": 10_000,
                "asset": "btc",
                "entry_price": 0.10,
                "min_tick_size": 0.01,
                "fair_claim_price": 0.20,
                "window_high_price": 0.18,
                "current_mid": 0.095,
                "filled_notional_usd": 100.0,
                "current_bid": 0.09,
                "current_ask": 0.10,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "window_displacement_bps": 1800.0,
                "flow_residual_gap_bps": 500.0,
                "flow_refill_ratio": 0.20,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "flow_extreme_age_ms": 500,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "time_to_settlement_hours": 48.0,
                "timeout_exit_price": 0.09,
                "outcome_future_max_touch_15m": 0.11,
                "outcome_deadline_bid_15m": 0.09,
            }
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            checkpoint_path = sweep.run_dislocation_reachability_matrix(
                frames_by_label={"day1": sweep.pd.DataFrame(rows), "day2": sweep.pd.DataFrame(rows)},
                output_dir=Path(tmp_dir),
                label_prefix="unit_test_reachability",
                poly_fee_bps=20.0,
                cex_taker_fee_bps=5.0,
                cex_slippage_bps=2.0,
                min_expected_net_pnl_usd=1.0,
                min_trades_per_day=1,
            )

            payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))["payload"]
            self.assertEqual(payload["exit_config_name"], "15m:any_touch:raw_tick:ticks1")
            self.assertEqual(payload["positive_strategy_count"], 1)
            self.assertEqual(payload["strategy_stats"][0]["strategy_name"], "structure_core")

    def test_build_geometry_signal_strategy_masks_uses_boundary_headroom_and_mode_split(self) -> None:
        elastic_row = {
            "symbol": "bitcoin-above-70k-on-april-21",
            "claim_side": "yes",
            "ts_ms": 10_000,
            "asset": "btc",
            "entry_price": 0.75,
            "min_tick_size": 0.01,
            "fair_claim_price": 0.77,
            "window_high_price": 0.77,
            "current_mid": 0.748,
            "filled_notional_usd": 100.0,
            "current_bid": 0.74,
            "current_ask": 0.75,
            "levels_consumed": 2,
            "depth_fill_ratio": 1.0,
            "window_displacement_bps": 60.0,
            "flow_residual_gap_bps": 50.0,
            "flow_refill_ratio": 0.20,
            "fair_claim_move_bps": 3.0,
            "cex_mid_move_bps": 2.0,
            "flow_extreme_age_ms": 500,
            "residual_basis_bps": 100.0,
            "anchor_follow_ratio": 0.80,
            "market_rule_kind": "above",
            "time_to_settlement_hours": 48.0,
            "timeout_exit_price": 0.74,
            "outcome_future_max_touch_15m": 0.77,
            "outcome_deadline_bid_15m": 0.75,
        }
        deep_row = {
            **elastic_row,
            "ts_ms": 1_000_000,
            "anchor_follow_ratio": 0.05,
        }
        headroom10_only_row = {
            **elastic_row,
            "ts_ms": 2_000_000,
            "entry_price": 0.88,
            "fair_claim_price": 0.90,
            "window_high_price": 0.90,
            "current_mid": 0.881,
            "current_bid": 0.87,
            "current_ask": 0.88,
            "anchor_follow_ratio": 0.30,
        }
        terminal_row = {
            **elastic_row,
            "ts_ms": 3_000_000,
            "entry_price": 0.95,
            "fair_claim_price": 0.97,
            "window_high_price": 0.97,
            "current_mid": 0.945,
            "current_bid": 0.94,
            "current_ask": 0.95,
            "levels_consumed": 1,
            "anchor_follow_ratio": 0.05,
        }

        derived = sweep.derive_dislocation_reachability_frame(
            frame=sweep.pd.DataFrame([elastic_row, deep_row, headroom10_only_row, terminal_row]),
            poly_fee_bps=20.0,
            cex_taker_fee_bps=5.0,
            cex_slippage_bps=2.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_expected_net_pnl_usd=0.5,
        )

        self.assertIn("boundary_headroom_ticks", derived.columns)
        self.assertAlmostEqual(float(derived["boundary_headroom_ticks"].iloc[0]), 25.0)
        self.assertAlmostEqual(float(derived["boundary_headroom_ticks"].iloc[2]), 12.0)
        self.assertAlmostEqual(float(derived["boundary_headroom_ticks"].iloc[3]), 5.0)

        masks = sweep.build_geometry_signal_strategy_masks(
            derived,
            symbol_filters={"bitcoin-above-70k-on-april-21"},
        )
        names = [name for name, _, _ in masks]
        self.assertEqual(
            names,
            [
                "geom_headroom10_core",
                "geom_headroom20_core",
                "geom_elastic_snapback",
                "geom_deep_reversion",
                "geom_terminal_zone_control",
            ],
        )
        counts = {name: int(mask.sum()) for name, _, mask in masks}
        self.assertEqual(counts["geom_headroom10_core"], 3)
        self.assertEqual(counts["geom_headroom20_core"], 2)
        self.assertEqual(counts["geom_elastic_snapback"], 1)
        self.assertEqual(counts["geom_deep_reversion"], 1)
        self.assertEqual(counts["geom_terminal_zone_control"], 1)

    def test_evaluate_geometry_signal_matrix_uses_symbol_filter_and_strategy_masks(self) -> None:
        snapback_row = {
            "symbol": "bitcoin-above-70k-on-april-21",
            "claim_side": "yes",
            "ts_ms": 10_000,
            "asset": "btc",
            "entry_price": 0.75,
            "min_tick_size": 0.01,
            "fair_claim_price": 0.97,
            "window_high_price": 0.97,
            "current_mid": 0.748,
            "filled_notional_usd": 100.0,
            "current_bid": 0.74,
            "current_ask": 0.75,
            "levels_consumed": 2,
            "depth_fill_ratio": 1.0,
            "window_displacement_bps": 60.0,
            "flow_residual_gap_bps": 50.0,
            "flow_refill_ratio": 0.20,
            "fair_claim_move_bps": 2.0,
            "cex_mid_move_bps": 1.0,
            "flow_extreme_age_ms": 500,
            "residual_basis_bps": 100.0,
            "anchor_follow_ratio": 0.80,
            "market_rule_kind": "above",
            "time_to_settlement_hours": 48.0,
            "timeout_exit_price": 0.94,
            "post_pulse_refill_ratio_1s": 0.20,
            "post_pulse_refill_ratio_3s": 0.20,
            "spread_snapback_bps": 5.0,
            "bid_depth_rebuild_ratio": 1.05,
            "confirm_window_complete_1s": True,
            "confirm_window_complete_3s": True,
            "outcome_future_max_touch_15m": 0.97,
            "outcome_deadline_bid_15m": 0.95,
        }
        reversion_row = {
            **snapback_row,
            "ts_ms": 1_000_000,
            "anchor_follow_ratio": 0.05,
        }
        headroom10_only_row = {
            **snapback_row,
            "ts_ms": 2_000_000,
            "entry_price": 0.88,
            "fair_claim_price": 0.90,
            "window_high_price": 0.90,
            "current_mid": 0.881,
            "current_bid": 0.87,
            "current_ask": 0.88,
            "levels_consumed": 1,
            "anchor_follow_ratio": 0.30,
        }
        terminal_row = {
            **snapback_row,
            "ts_ms": 3_000_000,
            "entry_price": 0.95,
            "fair_claim_price": 0.97,
            "window_high_price": 0.97,
            "current_mid": 0.945,
            "current_bid": 0.94,
            "current_ask": 0.95,
            "levels_consumed": 1,
            "anchor_follow_ratio": 0.05,
        }
        other_symbol_row = {
            **snapback_row,
            "symbol": "bitcoin-above-70k-on-april-22",
            "ts_ms": 4_000_000,
        }

        day1 = sweep.pd.DataFrame(
            [snapback_row, reversion_row, headroom10_only_row, terminal_row, other_symbol_row]
        )
        day2 = sweep.pd.DataFrame(
            [snapback_row, reversion_row, headroom10_only_row, terminal_row, other_symbol_row]
        )

        result = sweep.evaluate_geometry_signal_matrix(
            frames_by_label={"day1": day1, "day2": day2},
            poly_fee_bps=20.0,
            cex_taker_fee_bps=5.0,
            cex_slippage_bps=2.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_expected_net_pnl_usd=0.5,
            min_trades_per_day=1,
            symbol_filters={"bitcoin-above-70k-on-april-21"},
        )

        base = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_headroom10_core")
        strong = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_headroom20_core")
        snapback = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_elastic_snapback")
        deep = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_deep_reversion")
        terminal = next(
            item for item in result["strategy_stats"] if item["strategy_name"] == "geom_terminal_zone_control"
        )
        self.assertEqual(base["day_stats"]["day1"]["trade_count"], 3)
        self.assertEqual(strong["day_stats"]["day1"]["trade_count"], 2)
        self.assertEqual(snapback["day_stats"]["day1"]["trade_count"], 1)
        self.assertEqual(deep["day_stats"]["day1"]["trade_count"], 1)
        self.assertEqual(terminal["day_stats"]["day1"]["trade_count"], 1)
        self.assertEqual(strong["day_stats"]["day1"]["boundary_headroom_ticks_p50"], 25.0)
        self.assertEqual(result["symbol_filters"], ["bitcoin-above-70k-on-april-21"])

    def test_run_geometry_signal_matrix_writes_checkpoint(self) -> None:
        row = {
            "symbol": "bitcoin-above-70k-on-april-21",
            "claim_side": "yes",
            "ts_ms": 10_000,
            "asset": "btc",
            "entry_price": 0.95,
            "min_tick_size": 0.01,
            "fair_claim_price": 0.97,
            "window_high_price": 0.97,
            "current_mid": 0.945,
            "filled_notional_usd": 100.0,
            "current_bid": 0.94,
            "current_ask": 0.95,
            "levels_consumed": 2,
            "depth_fill_ratio": 1.0,
            "window_displacement_bps": 60.0,
            "flow_residual_gap_bps": 50.0,
            "flow_refill_ratio": 0.20,
            "fair_claim_move_bps": 2.0,
            "cex_mid_move_bps": 1.0,
            "flow_extreme_age_ms": 500,
            "residual_basis_bps": 100.0,
            "anchor_follow_ratio": 0.05,
            "market_rule_kind": "above",
            "time_to_settlement_hours": 48.0,
            "timeout_exit_price": 0.94,
            "post_pulse_refill_ratio_1s": 0.20,
            "post_pulse_refill_ratio_3s": 0.20,
            "spread_snapback_bps": 5.0,
            "bid_depth_rebuild_ratio": 1.05,
            "confirm_window_complete_1s": True,
            "confirm_window_complete_3s": True,
            "outcome_future_max_touch_15m": 0.97,
            "outcome_deadline_bid_15m": 0.95,
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            checkpoint_path = sweep.run_geometry_signal_matrix(
                frames_by_label={"day1": sweep.pd.DataFrame([row]), "day2": sweep.pd.DataFrame([row])},
                output_dir=Path(tmp_dir),
                label_prefix="unit_test_geometry",
                poly_fee_bps=20.0,
                cex_taker_fee_bps=5.0,
                cex_slippage_bps=2.0,
                min_expected_net_pnl_usd=0.5,
                min_trades_per_day=1,
                symbol_filters={"bitcoin-above-70k-on-april-21"},
            )

            payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))["payload"]
            self.assertEqual(payload["exit_config_name"], "15m:any_touch:raw_tick:ticks1")
            self.assertEqual(payload["symbol_filters"], ["bitcoin-above-70k-on-april-21"])
            self.assertTrue(payload["strategy_stats"])
            self.assertEqual(payload["strategy_stats"][0]["strategy_name"], "geom_headroom10_core")

    def test_build_dislocation_continuation_candidates_uses_opposite_side_execution(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "symbol": "btc-a",
                    "ts_ms": 10_000,
                    "asset": "btc",
                    "claim_side": "no",
                    "entry_price": 0.10,
                    "current_bid": 0.09,
                    "current_ask": 0.10,
                    "current_mid": 0.095,
                    "filled_notional_usd": 100.0,
                    "filled_shares": 1_000.0,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "min_tick_size": 0.01,
                    "fair_prob_yes": 0.90,
                    "fair_claim_price": 0.20,
                    "anchor_age_ms": 10,
                    "anchor_latency_delta_ms": 10,
                    "window_first_price": 0.18,
                    "window_last_price": 0.10,
                    "window_high_price": 0.18,
                    "window_low_price": 0.10,
                    "window_high_ts_ms": 9_000,
                    "window_low_ts_ms": 10_000,
                    "claim_move_bps": 4_000.0,
                    "window_displacement_bps": 1_800.0,
                    "flow_refill_ratio": 0.20,
                    "flow_rebound_bps": 100.0,
                    "flow_residual_gap_bps": 500.0,
                    "flow_extreme_age_ms": 500,
                    "flow_shape_valid": True,
                    "fair_claim_move_bps": 2.0,
                    "cex_mid_move_bps": 2.0,
                    "timeout_exit_price": 0.09,
                    "residual_basis_bps": 500.0,
                    "anchor_follow_ratio": 0.10,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "low",
                    "time_to_settlement_hours": 48.0,
                    "time_to_settlement_bucket": "24h_to_72h",
                    "post_pulse_refill_ratio_1s": 0.05,
                    "post_pulse_refill_ratio_3s": 0.10,
                    "spread_snapback_bps": -40.0,
                    "bid_depth_rebuild_ratio": 0.80,
                    "confirm_window_complete_1s": True,
                    "confirm_window_complete_3s": True,
                },
                {
                    "symbol": "btc-a",
                    "ts_ms": 10_000,
                    "asset": "btc",
                    "claim_side": "yes",
                    "entry_price": 0.90,
                    "current_bid": 0.89,
                    "current_ask": 0.90,
                    "current_mid": 0.895,
                    "filled_notional_usd": 300.0,
                    "filled_shares": 333.3333,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "min_tick_size": 0.01,
                    "fair_prob_yes": 0.90,
                    "fair_claim_price": 0.80,
                    "anchor_age_ms": 10,
                    "anchor_latency_delta_ms": 10,
                    "window_first_price": 0.82,
                    "window_last_price": 0.90,
                    "window_high_price": 0.90,
                    "window_low_price": 0.82,
                    "window_high_ts_ms": 10_000,
                    "window_low_ts_ms": 9_000,
                    "claim_move_bps": 1_000.0,
                    "window_displacement_bps": 800.0,
                    "flow_refill_ratio": 0.80,
                    "flow_rebound_bps": 700.0,
                    "flow_residual_gap_bps": 50.0,
                    "flow_extreme_age_ms": 500,
                    "flow_shape_valid": False,
                    "fair_claim_move_bps": 2.0,
                    "cex_mid_move_bps": 2.0,
                    "timeout_exit_price": 0.89,
                    "residual_basis_bps": -50.0,
                    "anchor_follow_ratio": 0.80,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "high",
                    "time_to_settlement_hours": 48.0,
                    "time_to_settlement_bucket": "24h_to_72h",
                    "post_pulse_refill_ratio_1s": 0.90,
                    "post_pulse_refill_ratio_3s": 0.90,
                    "spread_snapback_bps": 10.0,
                    "bid_depth_rebuild_ratio": 1.20,
                    "confirm_window_complete_1s": True,
                    "confirm_window_complete_3s": True,
                },
                {
                    "symbol": "btc-a",
                    "ts_ms": 10_000,
                    "asset": "btc",
                    "claim_side": "yes",
                    "entry_price": 0.84,
                    "current_bid": 0.83,
                    "current_ask": 0.84,
                    "current_mid": 0.835,
                    "filled_notional_usd": 300.0,
                    "filled_shares": 357.1428,
                    "levels_consumed": 1,
                    "depth_fill_ratio": 1.0,
                    "min_tick_size": 0.01,
                    "fair_prob_yes": 0.90,
                    "fair_claim_price": 0.80,
                    "anchor_age_ms": 10,
                    "anchor_latency_delta_ms": 10,
                    "window_first_price": 0.82,
                    "window_last_price": 0.84,
                    "window_high_price": 0.84,
                    "window_low_price": 0.82,
                    "window_high_ts_ms": 10_000,
                    "window_low_ts_ms": 9_000,
                    "claim_move_bps": 300.0,
                    "window_displacement_bps": 200.0,
                    "flow_refill_ratio": 0.80,
                    "flow_rebound_bps": 200.0,
                    "flow_residual_gap_bps": 20.0,
                    "flow_extreme_age_ms": 500,
                    "flow_shape_valid": False,
                    "fair_claim_move_bps": 2.0,
                    "cex_mid_move_bps": 2.0,
                    "timeout_exit_price": 0.83,
                    "residual_basis_bps": -50.0,
                    "anchor_follow_ratio": 0.80,
                    "market_rule_kind": "above",
                    "market_family": "directional",
                    "entry_price_bucket": "high",
                    "time_to_settlement_hours": 48.0,
                    "time_to_settlement_bucket": "24h_to_72h",
                    "post_pulse_refill_ratio_1s": 0.90,
                    "post_pulse_refill_ratio_3s": 0.90,
                    "spread_snapback_bps": 10.0,
                    "bid_depth_rebuild_ratio": 1.20,
                    "confirm_window_complete_1s": True,
                    "confirm_window_complete_3s": True,
                },
            ]
        )

        paired = sweep.build_dislocation_continuation_candidates(frame)

        self.assertEqual(len(paired), 1)
        row = paired.iloc[0]
        self.assertEqual(row["claim_side"], "no")
        self.assertEqual(row["execution_claim_side"], "yes")
        self.assertAlmostEqual(row["execution_entry_price"], 0.90)
        self.assertAlmostEqual(row["execution_filled_notional_usd"], 300.0)

    def test_derive_dislocation_continuation_exit_frame_uses_execution_side(self) -> None:
        frame = sweep.pd.DataFrame(
            [
                {
                    "symbol": "btc-a",
                    "claim_side": "no",
                    "ts_ms": 10_000,
                    "execution_claim_side": "yes",
                    "execution_entry_price": 0.90,
                    "execution_min_tick_size": 0.01,
                    "execution_filled_notional_usd": 300.0,
                    "execution_outcome_future_max_touch_15m": 0.92,
                    "execution_outcome_deadline_bid_15m": 0.88,
                }
            ]
        )

        derived = sweep.derive_dislocation_continuation_exit_frame(
            frame=frame,
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
        )

        self.assertTrue(bool(derived.iloc[0]["hit_target"]))
        self.assertAlmostEqual(float(derived.iloc[0]["target_exit_price"]), 0.91)
        self.assertAlmostEqual(float(derived.iloc[0]["realized_exit_price"]), 0.91)
        self.assertAlmostEqual(float(derived.iloc[0]["realized_net_pnl_usd"]), 3.333333333333333, places=6)

    def test_run_dislocation_continuation_strategy_matrix_writes_checkpoint(self) -> None:
        rows = [
            {
                "symbol": "btc-strong",
                "claim_side": "no",
                "ts_ms": 10_000,
                "asset": "btc",
                "entry_price": 0.10,
                "post_pulse_refill_ratio_1s": 0.05,
                "post_pulse_refill_ratio_3s": 0.10,
                "spread_snapback_bps": -40.0,
                "bid_depth_rebuild_ratio": 0.80,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
                "execution_claim_side": "yes",
                "execution_entry_price": 0.90,
                "execution_min_tick_size": 0.01,
                "execution_filled_notional_usd": 500.0,
                "execution_outcome_future_max_touch_15m": 0.92,
                "execution_outcome_deadline_bid_15m": 0.88,
            },
            {
                "symbol": "btc-weak",
                "claim_side": "no",
                "ts_ms": 1_000_000,
                "asset": "btc",
                "entry_price": 0.11,
                "post_pulse_refill_ratio_1s": 0.20,
                "post_pulse_refill_ratio_3s": 0.30,
                "spread_snapback_bps": 20.0,
                "bid_depth_rebuild_ratio": 1.20,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
                "execution_claim_side": "yes",
                "execution_entry_price": 0.80,
                "execution_min_tick_size": 0.01,
                "execution_filled_notional_usd": 100.0,
                "execution_outcome_future_max_touch_15m": 0.80,
                "execution_outcome_deadline_bid_15m": 0.79,
            },
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            checkpoint_path = sweep.run_dislocation_continuation_strategy_matrix(
                frames_by_label={"day1": sweep.pd.DataFrame(rows), "day2": sweep.pd.DataFrame(rows)},
                output_dir=Path(tmp_dir),
                label_prefix="unit_test_continuation",
                poly_fee_bps=0.0,
                cex_taker_fee_bps=0.0,
                cex_slippage_bps=0.0,
            )

            payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))["payload"]
            self.assertEqual(payload["exit_config_name"], "15m:any_touch:raw_tick:ticks1")
            self.assertEqual(payload["execution_mode"], "complementary_claim_long")
            self.assertTrue(payload["ranked_strategies"])
            self.assertEqual(payload["ranked_strategies"][0]["strategy_name"], "structure_core + refill1s_weak")

    def test_extract_dislocation_continuation_pairs_writes_pair_file_and_checkpoint(self) -> None:
        feature_rows = [
            {
                "symbol": "btc-a",
                "ts_ms": 10_000,
                "asset": "btc",
                "claim_side": "no",
                "entry_price": 0.10,
                "current_bid": 0.09,
                "current_ask": 0.10,
                "current_mid": 0.095,
                "filled_notional_usd": 100.0,
                "filled_shares": 1_000.0,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "min_tick_size": 0.01,
                "fair_prob_yes": 0.90,
                "fair_claim_price": 0.20,
                "anchor_age_ms": 10,
                "anchor_latency_delta_ms": 10,
                "window_first_price": 0.18,
                "window_last_price": 0.10,
                "window_high_price": 0.18,
                "window_low_price": 0.10,
                "window_high_ts_ms": 9_000,
                "window_low_ts_ms": 10_000,
                "claim_move_bps": 4_000.0,
                "window_displacement_bps": 1_800.0,
                "flow_refill_ratio": 0.20,
                "flow_rebound_bps": 100.0,
                "flow_residual_gap_bps": 500.0,
                "flow_extreme_age_ms": 500,
                "flow_shape_valid": True,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "timeout_exit_price": 0.09,
                "residual_basis_bps": 500.0,
                "anchor_follow_ratio": 0.10,
                "market_rule_kind": "above",
                "market_family": "directional",
                "entry_price_bucket": "low",
                "time_to_settlement_hours": 48.0,
                "time_to_settlement_bucket": "24h_to_72h",
                "post_pulse_refill_ratio_1s": 0.05,
                "post_pulse_refill_ratio_3s": 0.10,
                "spread_snapback_bps": -40.0,
                "bid_depth_rebuild_ratio": 0.80,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
            },
            {
                "symbol": "btc-a",
                "ts_ms": 10_000,
                "asset": "btc",
                "claim_side": "yes",
                "entry_price": 0.90,
                "current_bid": 0.89,
                "current_ask": 0.90,
                "current_mid": 0.895,
                "filled_notional_usd": 300.0,
                "filled_shares": 333.3333,
                "levels_consumed": 1,
                "depth_fill_ratio": 1.0,
                "min_tick_size": 0.01,
                "fair_prob_yes": 0.90,
                "fair_claim_price": 0.80,
                "anchor_age_ms": 10,
                "anchor_latency_delta_ms": 10,
                "window_first_price": 0.82,
                "window_last_price": 0.90,
                "window_high_price": 0.90,
                "window_low_price": 0.82,
                "window_high_ts_ms": 10_000,
                "window_low_ts_ms": 9_000,
                "claim_move_bps": 1_000.0,
                "window_displacement_bps": 800.0,
                "flow_refill_ratio": 0.80,
                "flow_rebound_bps": 700.0,
                "flow_residual_gap_bps": 50.0,
                "flow_extreme_age_ms": 500,
                "flow_shape_valid": False,
                "fair_claim_move_bps": 2.0,
                "cex_mid_move_bps": 2.0,
                "timeout_exit_price": 0.89,
                "residual_basis_bps": -50.0,
                "anchor_follow_ratio": 0.80,
                "market_rule_kind": "above",
                "market_family": "directional",
                "entry_price_bucket": "high",
                "time_to_settlement_hours": 48.0,
                "time_to_settlement_bucket": "24h_to_72h",
                "post_pulse_refill_ratio_1s": 0.90,
                "post_pulse_refill_ratio_3s": 0.90,
                "spread_snapback_bps": 10.0,
                "bid_depth_rebuild_ratio": 1.20,
                "confirm_window_complete_1s": True,
                "confirm_window_complete_3s": True,
            },
        ]
        tape_rows = [
            {
                "stream": "poly_book",
                "symbol": "btc-a",
                "token_side": "yes",
                "ts_exchange_ms": 10_000,
                "ts_recv_ms": 10_000,
                "sequence": 1,
                "record_kind": "snapshot",
                "payload": {
                    "bids": [["0.88", "100"]],
                    "asks": [["0.90", "100"]],
                    "last_trade_price": "0.89",
                },
            },
            {
                "stream": "poly_book",
                "symbol": "btc-a",
                "token_side": "yes",
                "ts_exchange_ms": 11_000,
                "ts_recv_ms": 11_000,
                "sequence": 2,
                "record_kind": None,
                "payload": {
                    "bids": [["0.89", "100"]],
                    "asks": [["0.91", "100"]],
                    "last_trade_price": "0.92",
                },
            },
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            feature_plain = tmp / "feature.jsonl"
            feature_plain.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in feature_rows),
                encoding="utf-8",
            )
            feature_zst = tmp / "feature.jsonl.zst"
            subprocess.run(["zstd", "-q", "-f", str(feature_plain), "-o", str(feature_zst)], check=True)

            tape_dir = tmp / "tapes"
            tape_dir.mkdir()
            tape_plain = tape_dir / "poly_book-000001.jsonl"
            tape_plain.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in tape_rows),
                encoding="utf-8",
            )
            tape_zst = tape_dir / "poly_book-000001.jsonl.zst"
            subprocess.run(["zstd", "-q", "-f", str(tape_plain), "-o", str(tape_zst)], check=True)

            checkpoint_path = sweep.extract_dislocation_continuation_pairs(
                feature_path=feature_zst,
                tape_dir=tape_dir,
                output_dir=tmp,
                label_prefix="unit_test_pairs",
            )

            payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))["payload"]
            self.assertEqual(payload["rows_written"], 1)
            pair_path = Path(payload["pair_path"])
            self.assertTrue(pair_path.exists())
            rows = list(sweep.iter_jsonl_zst(pair_path))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["execution_claim_side"], "yes")
            self.assertAlmostEqual(rows[0]["execution_outcome_future_max_touch_15m"], 0.92)


if __name__ == "__main__":
    unittest.main()
