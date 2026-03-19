use std::fs;
use std::path::PathBuf;

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;

use polyalpha_core::{
    AlphaEngine, ArbSignalAction, CexBaseQty, Exchange, InstrumentKind, MarketConfig,
    MarketDataEvent, MarketRule, MarketRuleKind, OrderBookSnapshot, OrderSide, PolyShares,
    PolymarketIds, Price, PriceLevel, SignalStrength, Symbol, TokenSide, UsdNotional,
    VenueQuantity,
};
use polyalpha_engine::{BasisStrategySnapshot, SimpleAlphaEngine, SimpleEngineConfig};

const EPSILON: f64 = 1e-6;

#[derive(Debug, Deserialize)]
struct FixtureRoot {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    symbol: String,
    market_question: String,
    market_rule: FixtureMarketRule,
    settlement_ts_ms: u64,
    config: FixtureConfig,
    cex_closes: Vec<f64>,
    poly_observations: Vec<FixturePolyObservation>,
    expected_steps: Vec<FixtureExpectedStep>,
}

#[derive(Debug, Deserialize)]
struct FixtureMarketRule {
    kind: MarketRuleKind,
    lower_strike: Option<f64>,
    upper_strike: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FixtureConfig {
    min_signal_samples: usize,
    rolling_window_minutes: usize,
    entry_z: f64,
    exit_z: f64,
    position_notional_usd: f64,
    cex_hedge_ratio: f64,
}

#[derive(Debug, Deserialize)]
struct FixturePolyObservation {
    ts_ms: u64,
    yes_price: f64,
    no_price: f64,
}

#[derive(Debug, Deserialize)]
struct FixtureExpectedStep {
    ts_ms: u64,
    preferred_snapshot: Option<FixtureSnapshot>,
    signal: Option<FixtureSignal>,
}

#[derive(Debug, Deserialize)]
struct FixtureSnapshot {
    token_side: String,
    poly_price: f64,
    fair_value: f64,
    signal_basis: f64,
    z_score: Option<f64>,
    delta: f64,
    cex_reference_price: f64,
    sigma: Option<f64>,
    minutes_to_expiry: f64,
}

#[derive(Debug, Deserialize)]
struct FixtureSignal {
    kind: String,
    token_side: Option<String>,
    poly_side: Option<String>,
    cex_side: Option<String>,
    signal_basis: Option<f64>,
    z_score: Option<f64>,
    delta: Option<f64>,
    reason: Option<String>,
}

#[tokio::test]
async fn rust_engine_matches_python_fixture_cases() {
    let fixture = load_fixture();
    assert!(!fixture.cases.is_empty(), "fixture should contain cases");

    for case in fixture.cases {
        run_case(case).await;
    }
}

async fn run_case(case: FixtureCase) {
    let symbol = Symbol::new(case.symbol.clone());
    let market = MarketConfig {
        symbol: symbol.clone(),
        poly_ids: PolymarketIds {
            condition_id: format!("condition-{}", case.name),
            yes_token_id: format!("{}-yes", case.name),
            no_token_id: format!("{}-no", case.name),
        },
        market_question: Some(case.market_question.clone()),
        market_rule: Some(MarketRule {
            kind: case.market_rule.kind,
            lower_strike: case.market_rule.lower_strike.map(price_from_f64),
            upper_strike: case.market_rule.upper_strike.map(price_from_f64),
        }),
        cex_symbol: "BTCUSDT".to_owned(),
        hedge_exchange: Exchange::Binance,
        strike_price: case
            .market_rule
            .lower_strike
            .or(case.market_rule.upper_strike)
            .map(price_from_f64),
        settlement_timestamp: case.settlement_ts_ms / 1000,
        min_tick_size: Price(Decimal::new(1, 2)),
        neg_risk: false,
        cex_price_tick: Decimal::new(1, 1),
        cex_qty_step: Decimal::new(1, 3),
        cex_contract_multiplier: Decimal::ONE,
    };
    let engine_config = SimpleEngineConfig {
        min_signal_samples: case.config.min_signal_samples,
        rolling_window_minutes: case.config.rolling_window_minutes,
        entry_z: case.config.entry_z,
        exit_z: case.config.exit_z,
        position_notional_usd: UsdNotional(
            Decimal::from_f64(case.config.position_notional_usd).unwrap_or_default(),
        ),
        cex_hedge_ratio: case.config.cex_hedge_ratio,
        dmm_half_spread: Decimal::new(1, 2),
        dmm_quote_size: PolyShares::ZERO,
    };
    let mut engine = SimpleAlphaEngine::with_markets(engine_config, vec![market]);

    let initial_close = case
        .cex_closes
        .first()
        .copied()
        .expect("fixture must include initial cex close");
    let _ = engine
        .on_market_data(&cex_event(&symbol, initial_close, 0))
        .await;

    assert_eq!(
        case.poly_observations.len(),
        case.expected_steps.len(),
        "case {} observation/expectation length mismatch",
        case.name
    );
    assert_eq!(
        case.cex_closes.len() >= case.poly_observations.len() + 1,
        true,
        "case {} should provide at least one extra cex close for bootstrap",
        case.name
    );

    for (idx, observation) in case.poly_observations.iter().enumerate() {
        let expected_step = &case.expected_steps[idx];
        assert_eq!(observation.ts_ms, expected_step.ts_ms);

        let _ = engine
            .on_market_data(&poly_event(
                &symbol,
                InstrumentKind::PolyYes,
                observation.yes_price,
                observation.ts_ms,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_event(
                &symbol,
                InstrumentKind::PolyNo,
                observation.no_price,
                observation.ts_ms,
            ))
            .await;
        let output = engine
            .on_market_data(&cex_event(
                &symbol,
                case.cex_closes[idx + 1],
                observation.ts_ms + 1,
            ))
            .await;

        assert_snapshot(
            case.name.as_str(),
            &engine.basis_snapshot(&symbol),
            expected_step,
        );
        assert_signal(case.name.as_str(), &output.arb_signals, expected_step);
    }
}

fn assert_snapshot(
    case_name: &str,
    actual: &Option<BasisStrategySnapshot>,
    expected_step: &FixtureExpectedStep,
) {
    match (&expected_step.preferred_snapshot, actual) {
        (None, None) => {}
        (Some(expected), Some(actual)) => {
            assert_eq!(actual.token_side, parse_token_side(&expected.token_side));
            assert_close(
                case_name,
                "poly_price",
                actual.poly_price,
                expected.poly_price,
            );
            assert_close(
                case_name,
                "fair_value",
                actual.fair_value,
                expected.fair_value,
            );
            assert_close(
                case_name,
                "signal_basis",
                actual.signal_basis,
                expected.signal_basis,
            );
            assert_optional_close(case_name, "z_score", actual.z_score, expected.z_score);
            assert_close(case_name, "delta", actual.delta, expected.delta);
            assert_close(
                case_name,
                "cex_reference_price",
                actual.cex_reference_price,
                expected.cex_reference_price,
            );
            assert_optional_close(case_name, "sigma", actual.sigma, expected.sigma);
            assert_close(
                case_name,
                "minutes_to_expiry",
                actual.minutes_to_expiry,
                expected.minutes_to_expiry,
            );
        }
        (expected, actual) => {
            panic!(
                "case {case_name} snapshot mismatch: expected {:?}, got {:?}",
                expected, actual
            );
        }
    }
}

fn assert_signal(
    case_name: &str,
    actual_signals: &[polyalpha_core::ArbSignalEvent],
    expected_step: &FixtureExpectedStep,
) {
    match &expected_step.signal {
        None => assert!(
            actual_signals.is_empty(),
            "case {case_name} expected no signal, got {actual_signals:?}"
        ),
        Some(expected) => {
            assert_eq!(
                actual_signals.len(),
                1,
                "case {case_name} expected exactly one signal"
            );
            let actual = &actual_signals[0];
            match expected.kind.as_str() {
                "open" => match &actual.action {
                    ArbSignalAction::BasisLong {
                        token_side,
                        poly_side,
                        cex_side,
                        delta,
                        ..
                    } => {
                        assert_eq!(
                            *token_side,
                            parse_token_side(expected.token_side.as_deref().unwrap_or("yes"))
                        );
                        assert_eq!(
                            *poly_side,
                            parse_order_side(expected.poly_side.as_deref().unwrap_or("buy"))
                        );
                        assert_eq!(
                            *cex_side,
                            parse_order_side(expected.cex_side.as_deref().unwrap_or("sell"))
                        );
                        assert_close(
                            case_name,
                            "signal_delta",
                            *delta,
                            expected.delta.unwrap_or_default(),
                        );
                        assert_optional_decimal_close(
                            case_name,
                            "signal_basis",
                            actual.basis_value.as_ref().and_then(|value| value.to_f64()),
                            expected.signal_basis,
                        );
                        assert_optional_decimal_close(
                            case_name,
                            "signal_z_score",
                            actual.z_score.as_ref().and_then(|value| value.to_f64()),
                            expected.z_score,
                        );
                        assert!(matches!(
                            actual.strength,
                            SignalStrength::Strong | SignalStrength::Normal | SignalStrength::Weak
                        ));
                    }
                    other => panic!("case {case_name} expected open signal, got {other:?}"),
                },
                "close" => match &actual.action {
                    ArbSignalAction::ClosePosition { reason } => {
                        assert_eq!(reason, expected.reason.as_deref().unwrap_or_default());
                    }
                    other => panic!("case {case_name} expected close signal, got {other:?}"),
                },
                other => panic!("unsupported expected signal kind {other}"),
            }
        }
    }
}

fn assert_close(case_name: &str, field: &str, actual: f64, expected: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff <= EPSILON,
        "case {case_name} field {field} mismatch: actual={actual}, expected={expected}, diff={diff}"
    );
}

fn assert_optional_close(case_name: &str, field: &str, actual: Option<f64>, expected: Option<f64>) {
    match (actual, expected) {
        (None, None) => {}
        (Some(actual), Some(expected)) => assert_close(case_name, field, actual, expected),
        _ => panic!(
            "case {case_name} field {field} mismatch: actual={actual:?}, expected={expected:?}"
        ),
    }
}

fn assert_optional_decimal_close(
    case_name: &str,
    field: &str,
    actual: Option<f64>,
    expected: Option<f64>,
) {
    assert_optional_close(case_name, field, actual, expected);
}

fn parse_token_side(value: &str) -> TokenSide {
    match value.to_ascii_lowercase().as_str() {
        "yes" => TokenSide::Yes,
        "no" => TokenSide::No,
        other => panic!("unsupported token side {other}"),
    }
}

fn parse_order_side(value: &str) -> OrderSide {
    match value.to_ascii_lowercase().as_str() {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => panic!("unsupported order side {other}"),
    }
}

fn price_from_f64(value: f64) -> Price {
    Price(Decimal::from_f64(value).unwrap_or_default())
}

fn poly_event(
    symbol: &Symbol,
    instrument: InstrumentKind,
    price: f64,
    ts_ms: u64,
) -> MarketDataEvent {
    let price = Price(Decimal::from_f64(price).unwrap_or_default());
    MarketDataEvent::OrderBookUpdate {
        snapshot: OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: symbol.clone(),
            instrument,
            bids: vec![PriceLevel {
                price,
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10, 0))),
            }],
            asks: vec![PriceLevel {
                price,
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10, 0))),
            }],
            exchange_timestamp_ms: ts_ms,
            received_at_ms: ts_ms,
            sequence: ts_ms,
        },
    }
}

fn cex_event(symbol: &Symbol, close: f64, ts_ms: u64) -> MarketDataEvent {
    let price = Price(Decimal::from_f64(close).unwrap_or_default());
    MarketDataEvent::OrderBookUpdate {
        snapshot: OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: symbol.clone(),
            instrument: InstrumentKind::CexPerp,
            bids: vec![PriceLevel {
                price,
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
            }],
            asks: vec![PriceLevel {
                price,
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
            }],
            exchange_timestamp_ms: ts_ms,
            received_at_ms: ts_ms,
            sequence: ts_ms,
        },
    }
}

fn load_fixture() -> FixtureRoot {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../tests/fixtures/btc_price_only_parity_cases.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read fixture {}: {err}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse fixture {}: {err}", path.display()))
}
