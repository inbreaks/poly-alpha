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
pub struct TargetCandidate {
    pub mode: PulseMode,
    pub target_exit_price: Decimal,
    pub target_distance_to_mid_bps: f64,
    pub timeout_secs: u64,
    pub predicted_hit_rate: f64,
    pub maker_net_pnl_usd: Decimal,
    pub timeout_net_pnl_usd: Decimal,
    pub realizable_ev_usd: Decimal,
}

pub fn build_reachability_envelope(
    entry_notional_usd: Decimal,
    entry_price: Decimal,
    hedge_cost_bps: f64,
    fee_bps: f64,
    reserve_bps: f64,
    reachability_cap_bps: f64,
) -> ReachabilityEnvelope {
    let gross_bps = hedge_cost_bps + fee_bps + reserve_bps;
    let min_profitable_target_distance_bps = gross_bps.max(0.0);
    ReachabilityEnvelope {
        min_profitable_target_distance_bps,
        reachability_cap_bps,
        in_gray_zone: min_profitable_target_distance_bps > 150.0,
        reachable: min_profitable_target_distance_bps <= reachability_cap_bps
            && entry_notional_usd > Decimal::ZERO
            && entry_price > Decimal::ZERO,
    }
}

pub fn classify_pulse_mode(
    claim_price_move_bps: f64,
    fair_claim_move_bps: f64,
    cex_mid_move_bps: f64,
    swept_notional_usd: Decimal,
    swept_levels_count: usize,
) -> PulseMode {
    let continuation_confirmed =
        swept_notional_usd >= Decimal::new(300, 0) && swept_levels_count >= 2;
    if continuation_confirmed && (fair_claim_move_bps >= 20.0 || cex_mid_move_bps >= 20.0) {
        PulseMode::ElasticSnapback
    } else if claim_price_move_bps > fair_claim_move_bps + cex_mid_move_bps + 30.0 {
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
    min_profitable_target_distance_bps: f64,
) -> Vec<TargetCandidate> {
    let pulse_span = (pulse_price - pre_pulse_price).max(Decimal::ZERO);
    let max_distance_bps = match mode {
        PulseMode::ElasticSnapback => 150.0,
        PulseMode::DeepReversion => 200.0,
    };
    let recoil_steps = match mode {
        PulseMode::ElasticSnapback => [
            Decimal::new(20, 2),
            Decimal::new(28, 2),
            Decimal::new(35, 2),
        ],
        PulseMode::DeepReversion => [
            Decimal::new(35, 2),
            Decimal::new(50, 2),
            Decimal::new(65, 2),
        ],
    };

    recoil_steps
        .into_iter()
        .filter_map(|ratio| {
            let raw_target = pulse_price - pulse_span * ratio;
            let rounded = round_to_tick(raw_target, tick_size);
            let distance_bps = distance_to_mid_bps(pulse_price, rounded);
            if distance_bps < min_profitable_target_distance_bps || distance_bps > max_distance_bps
            {
                return None;
            }
            Some(TargetCandidate {
                mode,
                target_exit_price: rounded,
                target_distance_to_mid_bps: distance_bps,
                timeout_secs: match mode {
                    PulseMode::ElasticSnapback => 60,
                    PulseMode::DeepReversion => 900,
                },
                predicted_hit_rate: 0.0,
                maker_net_pnl_usd: Decimal::ZERO,
                timeout_net_pnl_usd: Decimal::ZERO,
                realizable_ev_usd: Decimal::ZERO,
            })
        })
        .collect()
}

pub fn select_best_target(candidates: Vec<TargetCandidate>) -> Option<TargetCandidate> {
    candidates.into_iter().max_by(|left, right| {
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
    ((price / tick_size).floor() * tick_size).max(Decimal::ZERO)
}

fn distance_to_mid_bps(entry_price: Decimal, target_price: Decimal) -> f64 {
    if entry_price <= Decimal::ZERO {
        return 0.0;
    }
    ((target_price - entry_price).abs() * Decimal::from(10_000))
        .to_f64()
        .unwrap_or(0.0)
}

fn price_edge_bps(entry_price: Decimal, exit_price: Decimal) -> f64 {
    if entry_price <= Decimal::ZERO {
        return 0.0;
    }
    (((exit_price - entry_price) / entry_price) * Decimal::from(10_000))
        .to_f64()
        .unwrap_or(0.0)
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
        build_reachability_envelope, estimate_session_edge, executable_poly_quote,
        generate_target_candidates, reversion_pocket, select_best_target, ExecutableQuoteSide,
        TargetCandidate,
    };
    use crate::model::PulseMode;

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
    fn reachability_envelope_rejects_when_min_profitable_distance_exceeds_cap() {
        let envelope = build_reachability_envelope(
            Decimal::new(250, 0),
            Decimal::new(35, 2),
            120.0,
            60.0,
            40.0,
            200.0,
        );

        assert!(!envelope.reachable);
        assert!(envelope.min_profitable_target_distance_bps > 200.0);
    }

    #[test]
    fn elastic_snapback_targets_stay_inside_shallow_recoil_band() {
        let targets = generate_target_candidates(
            PulseMode::ElasticSnapback,
            Decimal::new(45, 2),
            Decimal::new(40, 2),
            Decimal::new(1, 3),
            95.0,
        );

        assert!(!targets.is_empty());
        assert!(targets
            .iter()
            .all(|item| item.target_distance_to_mid_bps <= 150.0));
        assert!(targets.iter().all(|item| item.timeout_secs <= 120));
    }

    #[test]
    fn deep_reversion_targets_respect_break_even_floor_and_reachability_cap() {
        let targets = generate_target_candidates(
            PulseMode::DeepReversion,
            Decimal::new(45, 2),
            Decimal::new(40, 2),
            Decimal::new(1, 3),
            120.0,
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
    fn realizable_ev_prefers_near_target_with_lower_paper_edge_but_higher_hit_rate() {
        let winner = select_best_target(vec![
            TargetCandidate {
                mode: PulseMode::DeepReversion,
                target_exit_price: Decimal::new(2512, 4),
                target_distance_to_mid_bps: 922.0,
                timeout_secs: 900,
                predicted_hit_rate: 0.0,
                maker_net_pnl_usd: Decimal::new(912, 2),
                timeout_net_pnl_usd: Decimal::new(-2753, 2),
                realizable_ev_usd: Decimal::new(-2753, 2),
            },
            TargetCandidate {
                mode: PulseMode::ElasticSnapback,
                target_exit_price: Decimal::new(2444, 4),
                target_distance_to_mid_bps: 118.0,
                timeout_secs: 60,
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
}
