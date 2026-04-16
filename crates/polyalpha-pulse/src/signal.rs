use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use polyalpha_core::{OrderBookSnapshot, VenueQuantity};

use crate::model::{PulseMode, ReachabilityEnvelope, RecoveryObservationQuality};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutableQuoteSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutableQuote {
    pub requested_notional_usd: Decimal,
    pub filled_shares: Decimal,
    pub filled_notional_usd: Decimal,
    pub avg_price: Decimal,
    pub top_price: Option<Decimal>,
    pub depth_shortfall_usd: Decimal,
    pub depth_fill_ratio: Decimal,
    pub slippage_bps: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReversionPocket {
    pub reference_price: Decimal,
    pub target_price: Decimal,
    pub available_ticks: f64,
    pub pocket_notional_usd: Decimal,
    pub vacuum_ratio: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionEdgeEstimate {
    pub target_exit_price: Decimal,
    pub timeout_exit_price: Decimal,
    pub maker_gross_edge_bps: f64,
    pub timeout_exit_haircut_bps: f64,
    pub expected_net_edge_bps: f64,
    pub expected_net_pnl_usd: Decimal,
    pub timeout_net_edge_bps: f64,
    pub timeout_net_pnl_usd: Decimal,
    pub timeout_loss_estimate_usd: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecoveryObservation {
    pub max_recovery_ratio_within_2s: f64,
    pub max_recovery_ratio_within_5s: f64,
    pub time_to_first_50pct_refill_ms: Option<u64>,
    pub time_to_first_80pct_refill_ms: Option<u64>,
    pub quality: RecoveryObservationQuality,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SignalSample<T> {
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
    pub value: T,
}

impl SignalSample<Decimal> {
    pub fn new_decimal(
        exchange_timestamp_ms: u64,
        received_at_ms: u64,
        sequence: u64,
        value: Decimal,
    ) -> Self {
        Self {
            exchange_timestamp_ms,
            received_at_ms,
            sequence,
            value,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TargetCandidate {
    pub mode: PulseMode,
    pub target_exit_price: Decimal,
    pub target_distance_to_mid_bps: f64,
    pub timeout_secs: u64,
    pub economics_evaluated: bool,
    pub predicted_hit_rate: f64,
    pub maker_net_pnl_usd: Decimal,
    pub timeout_net_pnl_usd: Decimal,
    pub realizable_ev_usd: Decimal,
}

const CONTINUATION_CONFIRMATION_NOTIONAL_USD: i64 = 300;
const CONTINUATION_CONFIRMATION_LEVELS: usize = 2;
const FAIR_MOVE_CONFIRMATION_BPS: f64 = 20.0;
const DEEP_REVERSION_EXCESS_BPS: f64 = 30.0;
const ELASTIC_TIMEOUT_SECS: u64 = 60;
const DEEP_TIMEOUT_SECS: u64 = 900;
const ELASTIC_DISTANCE_CAP_BPS: f64 = 150.0;
const DEEP_DISTANCE_CAP_BPS: f64 = 200.0;
const BPS_EPSILON: f64 = 1e-6;

pub fn build_reachability_envelope(
    entry_notional_usd: Decimal,
    entry_price: Decimal,
    tick_size: Decimal,
    hedge_cost_bps: f64,
    fee_bps: f64,
    reserve_bps: f64,
    reachability_cap_bps: f64,
) -> ReachabilityEnvelope {
    let gross_bps = hedge_cost_bps + fee_bps + reserve_bps;
    let min_profitable_target_distance_bps = gross_bps.max(0.0);
    let min_realizable_target_distance_bps =
        aligned_tick_floor_bps(entry_price, tick_size, min_profitable_target_distance_bps);
    ReachabilityEnvelope {
        min_profitable_target_distance_bps,
        min_realizable_target_distance_bps,
        reachability_cap_bps,
        in_gray_zone: min_realizable_target_distance_bps > ELASTIC_DISTANCE_CAP_BPS,
        reachable: min_realizable_target_distance_bps <= reachability_cap_bps
            && entry_notional_usd > Decimal::ZERO
            && entry_price > Decimal::ZERO
            && tick_size > Decimal::ZERO,
    }
}

pub fn classify_pulse_mode(
    claim_price_move_bps: f64,
    fair_claim_move_bps: f64,
    cex_mid_move_bps: f64,
    swept_notional_usd: Decimal,
    swept_levels_count: usize,
) -> PulseMode {
    let continuation_confirmed = swept_notional_usd
        >= Decimal::new(CONTINUATION_CONFIRMATION_NOTIONAL_USD, 0)
        && swept_levels_count >= CONTINUATION_CONFIRMATION_LEVELS;
    if continuation_confirmed
        && (fair_claim_move_bps >= FAIR_MOVE_CONFIRMATION_BPS
            || cex_mid_move_bps >= FAIR_MOVE_CONFIRMATION_BPS)
    {
        PulseMode::ElasticSnapback
    } else if claim_price_move_bps
        > fair_claim_move_bps + cex_mid_move_bps + DEEP_REVERSION_EXCESS_BPS
    {
        PulseMode::DeepReversion
    } else {
        PulseMode::ElasticSnapback
    }
}

pub fn generate_target_candidates(
    mode: PulseMode,
    pulse_price: Decimal,
    pre_pulse_price: Decimal,
    tick_size: Decimal,
    min_realizable_target_distance_bps: f64,
    reachability_cap_bps: f64,
) -> Vec<TargetCandidate> {
    if pulse_price <= Decimal::ZERO || tick_size <= Decimal::ZERO || reachability_cap_bps <= 0.0 {
        return Vec::new();
    }

    let pulse_span = (pulse_price - pre_pulse_price).max(Decimal::ZERO);
    let min_distance_bps = min_realizable_target_distance_bps.max(0.0);
    // Reachability provides the floor; mode caps remain the second-stage shape constraint.
    let max_distance_bps = mode_distance_cap_bps(mode).min(reachability_cap_bps);
    if min_distance_bps > max_distance_bps + BPS_EPSILON {
        return Vec::new();
    }

    let mut candidates = Vec::new();
    for ratio in mode_recoil_steps(mode) {
        let raw_target = pulse_price - pulse_span * ratio;
        let rounded = round_to_tick(raw_target, tick_size);
        if rounded <= Decimal::ZERO || rounded >= pulse_price {
            continue;
        }
        let distance_bps = distance_to_mid_bps(pulse_price, rounded);
        if distance_bps + BPS_EPSILON < min_distance_bps
            || distance_bps - BPS_EPSILON > max_distance_bps
        {
            continue;
        }
        if candidates
            .iter()
            .any(|candidate: &TargetCandidate| candidate.target_exit_price == rounded)
        {
            continue;
        }
        candidates.push(TargetCandidate {
            mode,
            target_exit_price: rounded,
            target_distance_to_mid_bps: distance_bps,
            timeout_secs: mode_timeout_secs(mode),
            economics_evaluated: false,
            predicted_hit_rate: 0.0,
            maker_net_pnl_usd: Decimal::ZERO,
            timeout_net_pnl_usd: Decimal::ZERO,
            realizable_ev_usd: Decimal::ZERO,
        });
    }

    candidates
}

pub fn select_best_target(candidates: Vec<TargetCandidate>) -> Option<TargetCandidate> {
    candidates
        .into_iter()
        .filter(candidate_is_evaluated)
        .max_by(|left, right| {
            left.realizable_ev_usd
                .cmp(&right.realizable_ev_usd)
                .then_with(|| left.predicted_hit_rate.total_cmp(&right.predicted_hit_rate))
                .then_with(|| {
                    right
                        .target_distance_to_mid_bps
                        .total_cmp(&left.target_distance_to_mid_bps)
                })
        })
}

pub fn executable_poly_quote(
    book: &OrderBookSnapshot,
    side: ExecutableQuoteSide,
    requested_notional_usd: Decimal,
) -> Option<ExecutableQuote> {
    if requested_notional_usd <= Decimal::ZERO {
        return None;
    }

    let levels = match side {
        ExecutableQuoteSide::Buy => &book.asks,
        ExecutableQuoteSide::Sell => &book.bids,
    };
    let top_price = levels.first().map(|level| level.price.0);
    let mut remaining_notional = requested_notional_usd;
    let mut filled_notional = Decimal::ZERO;
    let mut filled_shares = Decimal::ZERO;

    for level in levels {
        if remaining_notional <= Decimal::ZERO {
            break;
        }
        let price = level.price.0;
        if price <= Decimal::ZERO {
            continue;
        }
        let VenueQuantity::PolyShares(shares) = level.quantity else {
            continue;
        };
        if shares.0 <= Decimal::ZERO {
            continue;
        }

        let level_notional = shares.0 * price;
        let take_notional = level_notional.min(remaining_notional);
        if take_notional <= Decimal::ZERO {
            continue;
        }
        let take_shares = take_notional / price;
        filled_notional += take_notional;
        filled_shares += take_shares;
        remaining_notional -= take_notional;
    }

    let avg_price = if filled_shares > Decimal::ZERO {
        filled_notional / filled_shares
    } else {
        Decimal::ZERO
    };
    let depth_shortfall_usd = remaining_notional.max(Decimal::ZERO);
    let depth_fill_ratio = if requested_notional_usd > Decimal::ZERO {
        (filled_notional / requested_notional_usd).clamp(Decimal::ZERO, Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    let slippage_bps = top_price
        .and_then(|top| {
            if top <= Decimal::ZERO || avg_price <= Decimal::ZERO {
                None
            } else {
                let direction = match side {
                    ExecutableQuoteSide::Buy => avg_price - top,
                    ExecutableQuoteSide::Sell => top - avg_price,
                };
                ((direction / top) * Decimal::from(10_000)).to_f64()
            }
        })
        .unwrap_or(0.0);

    Some(ExecutableQuote {
        requested_notional_usd,
        filled_shares,
        filled_notional_usd: filled_notional,
        avg_price,
        top_price,
        depth_shortfall_usd,
        depth_fill_ratio,
        slippage_bps,
    })
}

pub fn timeout_exit_sell_proxy(
    book: &OrderBookSnapshot,
    exit_notional_usd: Decimal,
) -> Option<ExecutableQuote> {
    executable_poly_quote(book, ExecutableQuoteSide::Sell, exit_notional_usd)
}

pub fn timeout_exit_price_for_shares(
    book: &OrderBookSnapshot,
    requested_shares: Decimal,
) -> Option<Decimal> {
    if requested_shares <= Decimal::ZERO {
        return None;
    }

    let mut remaining_shares = requested_shares;
    let mut filled_shares = Decimal::ZERO;
    let mut filled_notional = Decimal::ZERO;

    for level in &book.bids {
        if remaining_shares <= Decimal::ZERO {
            break;
        }
        let VenueQuantity::PolyShares(shares) = level.quantity else {
            continue;
        };
        if shares.0 <= Decimal::ZERO || level.price.0 <= Decimal::ZERO {
            continue;
        }

        let take_shares = shares.0.min(remaining_shares);
        filled_shares += take_shares;
        filled_notional += take_shares * level.price.0;
        remaining_shares -= take_shares;
    }

    if filled_shares <= Decimal::ZERO {
        return None;
    }

    Some(filled_notional / filled_shares)
}

pub fn estimate_session_edge(
    entry_notional_usd: Decimal,
    entry_price: Decimal,
    target_exit_price: Decimal,
    timeout_exit_price: Decimal,
    hedge_cost_bps: f64,
    fee_bps: f64,
    reserve_bps: f64,
) -> SessionEdgeEstimate {
    let maker_gross_edge_bps = price_edge_bps(entry_price, target_exit_price);
    let timeout_exit_haircut_bps = price_edge_bps(entry_price, timeout_exit_price).abs();
    let expected_net_edge_bps = maker_gross_edge_bps - hedge_cost_bps - fee_bps - reserve_bps;
    let expected_net_pnl_usd = edge_pnl_usd(entry_notional_usd, expected_net_edge_bps);
    let timeout_net_edge_bps =
        price_edge_bps(entry_price, timeout_exit_price) - hedge_cost_bps - fee_bps - reserve_bps;
    let timeout_net_pnl_usd = edge_pnl_usd(entry_notional_usd, timeout_net_edge_bps);
    let timeout_loss_estimate_usd = if timeout_net_pnl_usd < Decimal::ZERO {
        -timeout_net_pnl_usd
    } else {
        Decimal::ZERO
    };

    SessionEdgeEstimate {
        target_exit_price,
        timeout_exit_price,
        maker_gross_edge_bps,
        timeout_exit_haircut_bps,
        expected_net_edge_bps,
        expected_net_pnl_usd,
        timeout_net_edge_bps,
        timeout_net_pnl_usd,
        timeout_loss_estimate_usd,
    }
}

pub fn summarize_recovery_observation(
    samples: &[SignalSample<Decimal>],
    pulse_exchange_ts_ms: u64,
    pulse_price: Decimal,
) -> RecoveryObservation {
    let mut max_recovery_ratio_within_2s = 0.0_f64;
    let mut max_recovery_ratio_within_5s = 0.0_f64;
    let mut previous_ts = pulse_exchange_ts_ms;
    let mut max_interarrival_gap_ms_5s = 0_u64;
    let mut post_sweep_update_count_5s = 0_usize;
    let mut native_sequence_present = true;
    let mut used_exchange_ts = false;

    for sample in samples {
        let ts_ms = signal_sample_ts_ms(sample);
        if ts_ms < pulse_exchange_ts_ms || ts_ms.saturating_sub(pulse_exchange_ts_ms) > 5_000 {
            continue;
        }
        post_sweep_update_count_5s += 1;
        used_exchange_ts |= sample.exchange_timestamp_ms > 0;
        native_sequence_present &= sample.sequence > 0;
        max_interarrival_gap_ms_5s =
            max_interarrival_gap_ms_5s.max(ts_ms.saturating_sub(previous_ts));
        previous_ts = ts_ms;

        let recovery_ratio: f64 = if pulse_price > Decimal::ZERO {
            ((pulse_price - sample.value) / pulse_price)
                .to_f64()
                .unwrap_or(0.0)
                .max(0.0)
        } else {
            0.0
        };
        if ts_ms.saturating_sub(pulse_exchange_ts_ms) <= 2_000 {
            max_recovery_ratio_within_2s = max_recovery_ratio_within_2s.max(recovery_ratio);
        }
        max_recovery_ratio_within_5s = max_recovery_ratio_within_5s.max(recovery_ratio);
    }

    if post_sweep_update_count_5s == 0 {
        native_sequence_present = false;
    }

    let observation_quality_score = if post_sweep_update_count_5s >= 3
        && max_interarrival_gap_ms_5s <= 700
        && native_sequence_present
    {
        1.0
    } else {
        0.0
    };

    RecoveryObservation {
        max_recovery_ratio_within_2s,
        max_recovery_ratio_within_5s,
        time_to_first_50pct_refill_ms: None,
        time_to_first_80pct_refill_ms: None,
        quality: RecoveryObservationQuality {
            used_exchange_ts,
            native_sequence_present,
            post_sweep_update_count_5s,
            max_interarrival_gap_ms_5s,
            observation_quality_score,
            admission_eligible: observation_quality_score >= 1.0,
        },
    }
}

pub fn reversion_pocket(
    entry_price: Decimal,
    reference_price: Decimal,
    tick_size: Decimal,
    entry_notional_usd: Decimal,
) -> ReversionPocket {
    if entry_price <= Decimal::ZERO
        || reference_price <= entry_price
        || tick_size <= Decimal::ZERO
        || entry_notional_usd <= Decimal::ZERO
    {
        return ReversionPocket {
            reference_price,
            target_price: entry_price,
            available_ticks: 0.0,
            pocket_notional_usd: Decimal::ZERO,
            vacuum_ratio: Decimal::ZERO,
        };
    }

    let tick_count_decimal = ((reference_price - entry_price) / tick_size).max(Decimal::ZERO);
    let whole_ticks = tick_count_decimal.floor();
    let available_ticks = whole_ticks.to_f64().unwrap_or(0.0);
    let target_price = entry_price + whole_ticks * tick_size;
    let implied_shares = entry_notional_usd / entry_price;
    let pocket_notional_usd = implied_shares * (target_price - entry_price);

    ReversionPocket {
        reference_price,
        target_price,
        available_ticks,
        pocket_notional_usd,
        vacuum_ratio: Decimal::ONE,
    }
}

fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price.max(Decimal::ZERO);
    }
    // Pulse exits must sit on the venue grid and not cross through the discrete tick.
    ((price / tick_size).floor() * tick_size).max(Decimal::ZERO)
}

fn distance_to_mid_bps(current_mid_price: Decimal, target_price: Decimal) -> f64 {
    if current_mid_price <= Decimal::ZERO {
        return 0.0;
    }
    (((target_price - current_mid_price).abs() / current_mid_price) * Decimal::from(10_000))
        .to_f64()
        .unwrap_or(0.0)
}

fn one_tick_distance_bps(current_mid_price: Decimal, tick_size: Decimal) -> f64 {
    if current_mid_price <= Decimal::ZERO || tick_size <= Decimal::ZERO {
        return 0.0;
    }
    ((tick_size / current_mid_price) * Decimal::from(10_000))
        .to_f64()
        .unwrap_or(0.0)
}

fn aligned_tick_floor_bps(
    current_mid_price: Decimal,
    tick_size: Decimal,
    min_profitable_target_distance_bps: f64,
) -> f64 {
    let one_tick_bps = one_tick_distance_bps(current_mid_price, tick_size);
    if one_tick_bps <= 0.0 {
        return min_profitable_target_distance_bps.max(0.0);
    }

    let min_distance_bps = min_profitable_target_distance_bps.max(one_tick_bps);
    let tick_steps = ((min_distance_bps / one_tick_bps) - BPS_EPSILON)
        .ceil()
        .max(1.0);
    tick_steps * one_tick_bps
}

fn mode_distance_cap_bps(mode: PulseMode) -> f64 {
    match mode {
        PulseMode::ElasticSnapback => ELASTIC_DISTANCE_CAP_BPS,
        PulseMode::DeepReversion => DEEP_DISTANCE_CAP_BPS,
    }
}

fn mode_timeout_secs(mode: PulseMode) -> u64 {
    match mode {
        PulseMode::ElasticSnapback => ELASTIC_TIMEOUT_SECS,
        PulseMode::DeepReversion => DEEP_TIMEOUT_SECS,
    }
}

fn mode_recoil_steps(mode: PulseMode) -> [Decimal; 3] {
    match mode {
        PulseMode::ElasticSnapback => {
            [Decimal::new(9, 2), Decimal::new(11, 2), Decimal::new(13, 2)]
        }
        PulseMode::DeepReversion => [
            Decimal::new(14, 2),
            Decimal::new(16, 2),
            Decimal::new(18, 2),
        ],
    }
}

fn candidate_is_evaluated(candidate: &TargetCandidate) -> bool {
    candidate.economics_evaluated
}

fn price_edge_bps(entry_price: Decimal, exit_price: Decimal) -> f64 {
    if entry_price <= Decimal::ZERO {
        return 0.0;
    }
    (((exit_price - entry_price) / entry_price) * Decimal::from(10_000))
        .to_f64()
        .unwrap_or(0.0)
}

fn signal_sample_ts_ms<T>(sample: &SignalSample<T>) -> u64 {
    if sample.exchange_timestamp_ms > 0 {
        sample.exchange_timestamp_ms
    } else {
        sample.received_at_ms
    }
}

fn edge_pnl_usd(entry_notional_usd: Decimal, edge_bps: f64) -> Decimal {
    entry_notional_usd * Decimal::from_f64_retain(edge_bps / 10_000.0).unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{
        Exchange, InstrumentKind, OrderBookSnapshot, PolyShares, Price, PriceLevel, Symbol,
        VenueQuantity,
    };

    use super::{
        build_reachability_envelope, classify_pulse_mode, distance_to_mid_bps,
        estimate_session_edge, executable_poly_quote, generate_target_candidates, reversion_pocket,
        select_best_target, summarize_recovery_observation, ExecutableQuoteSide, SignalSample,
        TargetCandidate,
    };
    use crate::model::{PulseMode, ReachabilityEnvelope};

    fn level(price: i64, price_scale: u32, qty: i64, qty_scale: u32) -> PriceLevel {
        PriceLevel {
            price: Price(Decimal::new(price, price_scale)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(qty, qty_scale))),
        }
    }

    fn poly_book(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("test-market"),
            instrument: InstrumentKind::PolyNo,
            bids,
            asks,
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        }
    }

    fn reachability_envelope(
        reference_price: Decimal,
        tick_size: Decimal,
        min_profitable_target_distance_bps: f64,
        reachability_cap_bps: f64,
    ) -> ReachabilityEnvelope {
        build_reachability_envelope(
            Decimal::new(250, 0),
            reference_price,
            tick_size,
            min_profitable_target_distance_bps,
            0.0,
            0.0,
            reachability_cap_bps,
        )
    }

    #[test]
    fn executable_vwap_uses_multiple_price_levels_to_fill_requested_notional() {
        let book = poly_book(
            vec![],
            vec![
                level(35, 2, 300, 0),
                level(36, 2, 400, 0),
                level(37, 2, 500, 0),
            ],
        );

        let quote = executable_poly_quote(&book, ExecutableQuoteSide::Buy, Decimal::new(250, 0))
            .expect("quote");

        assert_eq!(quote.filled_notional_usd, Decimal::new(250, 0));
        assert_eq!(quote.depth_shortfall_usd, Decimal::ZERO);
        assert_eq!(quote.filled_shares.round_dp(6), Decimal::new(702702703, 6));
        assert_eq!(quote.avg_price.round_dp(6), Decimal::new(355769, 6));
    }

    #[test]
    fn executable_vwap_returns_shortfall_when_book_depth_is_too_thin() {
        let book = poly_book(vec![], vec![level(35, 2, 100, 0), level(36, 2, 100, 0)]);

        let quote = executable_poly_quote(&book, ExecutableQuoteSide::Buy, Decimal::new(250, 0))
            .expect("quote");

        assert_eq!(quote.filled_shares, Decimal::new(200, 0));
        assert_eq!(quote.filled_notional_usd, Decimal::new(71, 0));
        assert_eq!(quote.depth_shortfall_usd, Decimal::new(179, 0));
        assert_eq!(quote.depth_fill_ratio.round_dp(3), Decimal::new(284, 3));
    }

    #[test]
    fn reversion_pocket_counts_ticks_between_entry_and_reference_price() {
        let pocket = reversion_pocket(
            Decimal::new(35, 2),
            Decimal::new(39, 2),
            Decimal::new(1, 2),
            Decimal::new(250, 0),
        );

        assert_eq!(pocket.reference_price, Decimal::new(39, 2));
        assert_eq!(pocket.target_price, Decimal::new(39, 2));
        assert_eq!(pocket.available_ticks, 4.0);
        assert_eq!(
            pocket.pocket_notional_usd.round_dp(6),
            Decimal::new(28571429, 6)
        );
        assert_eq!(pocket.vacuum_ratio, Decimal::ONE);
    }

    #[test]
    fn session_edge_estimate_exposes_timeout_loss_proxy() {
        let estimate = estimate_session_edge(
            Decimal::new(250, 0),
            Decimal::new(20, 2),
            Decimal::new(21, 2),
            Decimal::new(18, 2),
            20.0,
            20.0,
            10.0,
        );

        assert_eq!(
            estimate.expected_net_pnl_usd.round_dp(2),
            Decimal::new(1125, 2)
        );
        assert_eq!(
            estimate.timeout_net_pnl_usd.round_dp(2),
            Decimal::new(-2625, 2)
        );
        assert_eq!(
            estimate.timeout_loss_estimate_usd.round_dp(2),
            Decimal::new(2625, 2)
        );
    }

    #[test]
    fn recovery_observation_prefers_exchange_timestamp_over_received_time() {
        let samples = vec![
            SignalSample::new_decimal(1_000, 1_180, 10, Decimal::new(45, 2)),
            SignalSample::new_decimal(1_600, 1_950, 11, Decimal::new(443, 3)),
            SignalSample::new_decimal(2_200, 2_700, 12, Decimal::new(441, 3)),
        ];

        let recovery = summarize_recovery_observation(&samples, 1_000, Decimal::new(45, 2));
        assert!(recovery.quality.used_exchange_ts);
        assert_eq!(recovery.quality.max_interarrival_gap_ms_5s, 600);
    }

    #[test]
    fn recovery_observation_marks_metrics_audit_only_when_quality_gate_fails() {
        let samples = vec![
            SignalSample::new_decimal(1_000, 1_000, 0, Decimal::new(45, 2)),
            SignalSample::new_decimal(2_600, 2_700, 0, Decimal::new(444, 3)),
        ];

        let recovery = summarize_recovery_observation(&samples, 1_000, Decimal::new(45, 2));
        assert!(!recovery.quality.native_sequence_present);
        assert!(!recovery.quality.admission_eligible);
    }

    #[test]
    fn recovery_observation_quality_ignores_samples_outside_post_pulse_window() {
        let samples = vec![
            SignalSample::new_decimal(0, 900, 0, Decimal::new(46, 2)),
            SignalSample::new_decimal(1_000, 1_000, 10, Decimal::new(45, 2)),
            SignalSample::new_decimal(1_600, 1_950, 11, Decimal::new(443, 3)),
            SignalSample::new_decimal(2_200, 2_700, 12, Decimal::new(441, 3)),
        ];

        let recovery = summarize_recovery_observation(&samples, 1_000, Decimal::new(45, 2));

        assert!(recovery.quality.used_exchange_ts);
        assert!(recovery.quality.native_sequence_present);
        assert_eq!(recovery.quality.post_sweep_update_count_5s, 3);
        assert!(recovery.quality.admission_eligible);
    }

    #[test]
    fn reachability_envelope_rejects_when_min_profitable_distance_exceeds_cap() {
        let envelope = build_reachability_envelope(
            Decimal::new(250, 0),
            Decimal::new(35, 2),
            Decimal::new(1, 2),
            120.0,
            60.0,
            40.0,
            200.0,
        );

        assert!(!envelope.reachable);
        assert!(envelope.min_profitable_target_distance_bps > 200.0);
        assert!(envelope.min_realizable_target_distance_bps > 200.0);
    }

    #[test]
    fn reachability_envelope_rejects_when_one_tick_exceeds_cap() {
        let envelope = build_reachability_envelope(
            Decimal::new(250, 0),
            Decimal::new(45, 2),
            Decimal::new(1, 2),
            40.0,
            30.0,
            25.0,
            200.0,
        );

        assert!(!envelope.reachable);
        assert_eq!(envelope.min_profitable_target_distance_bps, 95.0);
        assert!(envelope.min_realizable_target_distance_bps > 200.0);
    }

    #[test]
    fn reachability_envelope_uses_next_realizable_tick_when_gross_floor_sits_between_ticks() {
        let envelope = build_reachability_envelope(
            Decimal::new(250, 0),
            Decimal::new(80, 2),
            Decimal::new(1, 2),
            150.0,
            0.0,
            0.0,
            200.0,
        );

        assert!(!envelope.reachable);
        assert_eq!(envelope.min_profitable_target_distance_bps, 150.0);
        assert!((envelope.min_realizable_target_distance_bps - 250.0).abs() < 0.1);
    }

    #[test]
    fn elastic_snapback_targets_stay_inside_shallow_recoil_band() {
        let envelope = reachability_envelope(Decimal::new(45, 2), Decimal::new(1, 3), 95.0, 150.0);
        let targets = generate_target_candidates(
            PulseMode::ElasticSnapback,
            Decimal::new(45, 2),
            Decimal::new(40, 2),
            Decimal::new(1, 3),
            envelope.min_realizable_target_distance_bps,
            envelope.reachability_cap_bps,
        );

        assert!(!targets.is_empty());
        assert!(targets
            .iter()
            .all(|item| item.target_distance_to_mid_bps <= 150.0));
        assert!(targets.iter().all(|item| item.timeout_secs <= 120));
    }

    #[test]
    fn deep_reversion_targets_respect_break_even_floor_and_reachability_cap() {
        let envelope = reachability_envelope(Decimal::new(45, 2), Decimal::new(1, 3), 120.0, 200.0);
        let targets = generate_target_candidates(
            PulseMode::DeepReversion,
            Decimal::new(45, 2),
            Decimal::new(40, 2),
            Decimal::new(1, 3),
            envelope.min_realizable_target_distance_bps,
            envelope.reachability_cap_bps,
        );

        assert!(!targets.is_empty());
        assert!(targets
            .iter()
            .all(|item| item.target_distance_to_mid_bps >= 120.0));
        assert!(targets
            .iter()
            .all(|item| item.target_distance_to_mid_bps <= 200.0));
    }

    #[test]
    fn elastic_snapback_cent_tick_deduplicates_single_tick_candidate() {
        let envelope = reachability_envelope(Decimal::new(80, 2), Decimal::new(1, 2), 95.0, 150.0);
        let targets = generate_target_candidates(
            PulseMode::ElasticSnapback,
            Decimal::new(80, 2),
            Decimal::new(75, 2),
            Decimal::new(1, 2),
            envelope.min_realizable_target_distance_bps,
            envelope.reachability_cap_bps,
        );

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_exit_price, Decimal::new(79, 2));
        assert!((targets[0].target_distance_to_mid_bps - 125.0).abs() < 0.1);
    }

    #[test]
    fn generate_target_candidates_respects_explicit_reachability_cap() {
        let envelope = reachability_envelope(Decimal::new(80, 2), Decimal::new(1, 2), 95.0, 120.0);
        let targets = generate_target_candidates(
            PulseMode::ElasticSnapback,
            Decimal::new(80, 2),
            Decimal::new(75, 2),
            Decimal::new(1, 2),
            envelope.min_realizable_target_distance_bps,
            envelope.reachability_cap_bps,
        );

        assert!(targets.is_empty());
    }

    #[test]
    fn envelope_and_generator_share_aligned_tick_floor_contract() {
        let envelope = reachability_envelope(Decimal::new(80, 2), Decimal::new(1, 2), 150.0, 200.0);
        let targets = generate_target_candidates(
            PulseMode::ElasticSnapback,
            Decimal::new(80, 2),
            Decimal::new(75, 2),
            Decimal::new(1, 2),
            envelope.min_realizable_target_distance_bps,
            envelope.reachability_cap_bps,
        );

        assert!(!envelope.reachable);
        assert!((envelope.min_realizable_target_distance_bps - 250.0).abs() < 0.1);
        assert!(targets.is_empty());
    }

    #[test]
    fn distance_to_mid_bps_uses_mid_normalized_denominator() {
        let distance = distance_to_mid_bps(Decimal::new(45, 2), Decimal::new(44, 2));

        assert!((distance - 222.22).abs() < 0.1);
    }

    #[test]
    fn classify_pulse_mode_detects_deep_reversion_without_anchor_confirmation() {
        let mode = classify_pulse_mode(160.0, 20.0, 10.0, Decimal::new(100, 0), 1);

        assert_eq!(mode, PulseMode::DeepReversion);
    }

    #[test]
    fn realizable_ev_prefers_near_target_with_lower_paper_edge_but_higher_hit_rate() {
        let winner = select_best_target(vec![
            TargetCandidate {
                mode: PulseMode::DeepReversion,
                target_exit_price: Decimal::new(2512, 4),
                target_distance_to_mid_bps: 922.0,
                timeout_secs: 900,
                economics_evaluated: true,
                predicted_hit_rate: 0.18,
                maker_net_pnl_usd: Decimal::new(912, 2),
                timeout_net_pnl_usd: Decimal::new(-2753, 2),
                realizable_ev_usd: Decimal::new(120, 2),
            },
            TargetCandidate {
                mode: PulseMode::ElasticSnapback,
                target_exit_price: Decimal::new(2444, 4),
                target_distance_to_mid_bps: 118.0,
                timeout_secs: 60,
                economics_evaluated: true,
                predicted_hit_rate: 0.63,
                maker_net_pnl_usd: Decimal::new(472, 2),
                timeout_net_pnl_usd: Decimal::new(-310, 2),
                realizable_ev_usd: Decimal::new(162, 2),
            },
        ])
        .expect("best target");

        assert_eq!(winner.mode, PulseMode::ElasticSnapback);
        assert_eq!(winner.target_distance_to_mid_bps, 118.0);
    }

    #[test]
    fn select_best_target_prefers_higher_hit_rate_when_realizable_ev_ties() {
        let winner = select_best_target(vec![
            TargetCandidate {
                mode: PulseMode::DeepReversion,
                target_exit_price: Decimal::new(2512, 4),
                target_distance_to_mid_bps: 160.0,
                timeout_secs: 900,
                economics_evaluated: true,
                predicted_hit_rate: 0.41,
                maker_net_pnl_usd: Decimal::new(500, 2),
                timeout_net_pnl_usd: Decimal::new(-176, 2),
                realizable_ev_usd: Decimal::new(162, 2),
            },
            TargetCandidate {
                mode: PulseMode::ElasticSnapback,
                target_exit_price: Decimal::new(2444, 4),
                target_distance_to_mid_bps: 118.0,
                timeout_secs: 60,
                economics_evaluated: true,
                predicted_hit_rate: 0.63,
                maker_net_pnl_usd: Decimal::new(472, 2),
                timeout_net_pnl_usd: Decimal::new(-310, 2),
                realizable_ev_usd: Decimal::new(162, 2),
            },
        ])
        .expect("best target");

        assert_eq!(winner.mode, PulseMode::ElasticSnapback);
        assert_eq!(winner.predicted_hit_rate, 0.63);
    }

    #[test]
    fn select_best_target_rejects_unevaluated_candidates() {
        let winner = select_best_target(vec![TargetCandidate {
            mode: PulseMode::ElasticSnapback,
            target_exit_price: Decimal::new(2444, 4),
            target_distance_to_mid_bps: 118.0,
            timeout_secs: 60,
            economics_evaluated: false,
            predicted_hit_rate: 0.0,
            maker_net_pnl_usd: Decimal::ZERO,
            timeout_net_pnl_usd: Decimal::ZERO,
            realizable_ev_usd: Decimal::ZERO,
        }]);

        assert!(winner.is_none());
    }

    #[test]
    fn select_best_target_keeps_explicitly_evaluated_break_even_candidate() {
        let winner = select_best_target(vec![TargetCandidate {
            mode: PulseMode::ElasticSnapback,
            target_exit_price: Decimal::new(2444, 4),
            target_distance_to_mid_bps: 118.0,
            timeout_secs: 60,
            economics_evaluated: true,
            predicted_hit_rate: 0.0,
            maker_net_pnl_usd: Decimal::ZERO,
            timeout_net_pnl_usd: Decimal::ZERO,
            realizable_ev_usd: Decimal::ZERO,
        }])
        .expect("explicitly evaluated break-even candidate");

        assert_eq!(winner.mode, PulseMode::ElasticSnapback);
    }
}
