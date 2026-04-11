use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use thiserror::Error;

use polyalpha_core::{MarketRule, MarketRuleKind};

use crate::model::{AnchorSnapshot, EventPriceOutput, PricingQuality, PulseFailureCode};

const HOURS_PER_YEAR: f64 = 365.0 * 24.0;
const MILLIS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0 * 1000.0;

pub type Result<T> = std::result::Result<T, PricingError>;

#[derive(Clone, Debug, PartialEq)]
pub struct EventPricerConfig {
    pub delta_bump_ratio_bps: u64,
    pub min_abs_bump: Decimal,
    pub max_abs_bump: Decimal,
    pub delta_stability_warn_ratio: f64,
    pub expiry_gap_adjustment: ExpiryGapAdjustment,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExpiryGapAdjustment {
    pub grace_gap_hours: u64,
    pub soft_window_hours: u64,
    pub hard_cap_hours: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExpiryGapAdjustedPrice {
    pub fair_prob_yes: f64,
    pub adjustment_applied: bool,
    pub gap_hours: f64,
}

#[derive(Clone, Debug)]
pub struct EventPricer {
    config: EventPricerConfig,
}

#[derive(Debug, Error, PartialEq)]
pub enum PricingError {
    #[error("anchor surface is empty")]
    EmptyAnchorSurface,
    #[error("anchor index price is not finite")]
    InvalidIndexPrice,
    #[error("market rule is incomplete")]
    IncompleteMarketRule,
    #[error("between market rule must have upper strike above lower strike")]
    InvalidBetweenRule,
    #[error(
        "expiry gap exceeds hard cap: gap_hours={gap_hours:.2}, hard_cap_hours={hard_cap_hours}"
    )]
    HardExpiryGapExceeded {
        gap_hours: f64,
        hard_cap_hours: u64,
        failure_code: PulseFailureCode,
    },
}

impl EventPricer {
    pub fn new(config: EventPricerConfig) -> Self {
        Self { config }
    }

    pub fn price(
        &self,
        anchor: &AnchorSnapshot,
        market_rule: &MarketRule,
        event_expiry_ts_ms: u64,
    ) -> Result<EventPriceOutput> {
        let index_price = anchor
            .index_price
            .to_f64()
            .ok_or(PricingError::InvalidIndexPrice)?;
        if !index_price.is_finite() || index_price <= 0.0 {
            return Err(PricingError::InvalidIndexPrice);
        }
        let delta_bump = compute_delta_bump(anchor.index_price, &self.config);
        let delta_bump_f64 = delta_bump.to_f64().unwrap_or(0.0);

        let base_adjusted = self.adjusted_probability_for_index(
            anchor,
            market_rule,
            event_expiry_ts_ms,
            index_price,
        )?;
        let fair_prob_yes = base_adjusted.fair_prob_yes;
        let fair_prob_yes_plus = self
            .adjusted_probability_for_index(
                anchor,
                market_rule,
                event_expiry_ts_ms,
                index_price + delta_bump_f64,
            )?
            .fair_prob_yes;
        let fair_prob_yes_minus = self
            .adjusted_probability_for_index(
                anchor,
                market_rule,
                event_expiry_ts_ms,
                (index_price - delta_bump_f64).max(f64::EPSILON),
            )?
            .fair_prob_yes;

        let double_bump = (delta_bump + delta_bump)
            .to_f64()
            .unwrap_or(delta_bump_f64 * 2.0);
        let fair_prob_yes_plus_double = self
            .adjusted_probability_for_index(
                anchor,
                market_rule,
                event_expiry_ts_ms,
                index_price + double_bump,
            )?
            .fair_prob_yes;
        let fair_prob_yes_minus_double = self
            .adjusted_probability_for_index(
                anchor,
                market_rule,
                event_expiry_ts_ms,
                (index_price - double_bump).max(f64::EPSILON),
            )?
            .fair_prob_yes;

        let event_delta_yes =
            central_difference(fair_prob_yes_plus, fair_prob_yes_minus, delta_bump_f64);
        let event_delta_yes_double = central_difference(
            fair_prob_yes_plus_double,
            fair_prob_yes_minus_double,
            double_bump,
        );
        let gamma_estimate = Some(second_derivative(
            fair_prob_yes_plus,
            fair_prob_yes,
            fair_prob_yes_minus,
            delta_bump_f64,
        ));
        let delta_stability_ratio = relative_difference(event_delta_yes, event_delta_yes_double);

        Ok(EventPriceOutput {
            fair_prob_yes,
            fair_prob_no: clamp_probability(1.0 - fair_prob_yes),
            event_delta_yes,
            event_delta_no: -event_delta_yes,
            gamma_estimate,
            delta_bump_used: delta_bump,
            expiry_gap_adjustment_applied: base_adjusted.adjustment_applied,
            pricing_quality: PricingQuality {
                delta_stable: delta_stability_ratio
                    .map(|ratio| ratio <= self.config.delta_stability_warn_ratio)
                    .unwrap_or(true),
                delta_stability_ratio,
            },
        })
    }

    fn adjusted_probability_for_index(
        &self,
        anchor: &AnchorSnapshot,
        market_rule: &MarketRule,
        event_expiry_ts_ms: u64,
        index_price: f64,
    ) -> Result<ExpiryGapAdjustedPrice> {
        if anchor.local_surface_points.is_empty() {
            return Err(PricingError::EmptyAnchorSurface);
        }
        let option_t_years = time_to_expiry_years(anchor.ts_ms, anchor.expiry_ts_ms);
        let event_t_years = time_to_expiry_years(anchor.ts_ms, event_expiry_ts_ms);
        let unadjusted = probability_for_rule(anchor, market_rule, index_price, option_t_years)?;
        let bridged_target = probability_for_rule(anchor, market_rule, index_price, event_t_years)?;
        self.config.expiry_gap_adjustment.apply(
            unadjusted,
            bridged_target,
            option_t_years,
            event_t_years,
        )
    }
}

impl ExpiryGapAdjustment {
    pub fn new(grace_gap_hours: u64, soft_window_hours: u64, hard_cap_hours: u64) -> Self {
        Self {
            grace_gap_hours,
            soft_window_hours,
            hard_cap_hours,
        }
    }

    pub fn apply(
        &self,
        unadjusted_prob: f64,
        bridged_target_prob: f64,
        option_t_years: f64,
        event_t_years: f64,
    ) -> Result<ExpiryGapAdjustedPrice> {
        let gap_hours = (event_t_years - option_t_years).abs() * HOURS_PER_YEAR;
        if gap_hours > self.hard_cap_hours as f64 {
            return Err(PricingError::HardExpiryGapExceeded {
                gap_hours,
                hard_cap_hours: self.hard_cap_hours,
                failure_code: PulseFailureCode::HardExpiryGapExceeded,
            });
        }
        if gap_hours <= self.grace_gap_hours as f64 {
            return Ok(ExpiryGapAdjustedPrice {
                fair_prob_yes: clamp_probability(unadjusted_prob),
                adjustment_applied: false,
                gap_hours,
            });
        }

        let interpolation_base = self
            .soft_window_hours
            .saturating_sub(self.grace_gap_hours)
            .max(1) as f64;
        let effective_gap_hours = gap_hours.min(self.soft_window_hours as f64);
        let blend = ((effective_gap_hours - self.grace_gap_hours as f64) / interpolation_base)
            .clamp(0.0, 1.0);

        Ok(ExpiryGapAdjustedPrice {
            fair_prob_yes: clamp_probability(
                unadjusted_prob + (bridged_target_prob - unadjusted_prob) * blend,
            ),
            adjustment_applied: true,
            gap_hours,
        })
    }
}

pub fn compute_delta_bump(index_price: Decimal, config: &EventPricerConfig) -> Decimal {
    let index_price = index_price.abs();
    let ratio = Decimal::new(config.delta_bump_ratio_bps as i64, 4);
    let raw_bump = index_price * ratio;
    raw_bump
        .max(config.min_abs_bump.abs())
        .min(config.max_abs_bump.abs())
}

fn probability_for_rule(
    anchor: &AnchorSnapshot,
    market_rule: &MarketRule,
    index_price: f64,
    time_to_expiry_years: f64,
) -> Result<f64> {
    match market_rule.kind {
        MarketRuleKind::Above => {
            let strike = market_rule
                .lower_strike
                .map(|value| value.to_f64())
                .ok_or(PricingError::IncompleteMarketRule)?;
            Ok(probability_above(
                anchor,
                index_price,
                strike,
                time_to_expiry_years,
            ))
        }
        MarketRuleKind::Below => {
            let strike = market_rule
                .upper_strike
                .map(|value| value.to_f64())
                .ok_or(PricingError::IncompleteMarketRule)?;
            Ok(clamp_probability(
                1.0 - probability_above(anchor, index_price, strike, time_to_expiry_years),
            ))
        }
        MarketRuleKind::Between => {
            let lower = market_rule
                .lower_strike
                .map(|value| value.to_f64())
                .ok_or(PricingError::IncompleteMarketRule)?;
            let upper = market_rule
                .upper_strike
                .map(|value| value.to_f64())
                .ok_or(PricingError::IncompleteMarketRule)?;
            if upper <= lower {
                return Err(PricingError::InvalidBetweenRule);
            }
            Ok(clamp_probability(
                probability_above(anchor, index_price, lower, time_to_expiry_years)
                    - probability_above(anchor, index_price, upper, time_to_expiry_years),
            ))
        }
    }
}

fn probability_above(
    anchor: &AnchorSnapshot,
    index_price: f64,
    strike: f64,
    time_to_expiry_years: f64,
) -> f64 {
    if !index_price.is_finite() || index_price <= 0.0 || !strike.is_finite() || strike <= 0.0 {
        return 0.0;
    }
    let iv = interpolated_mark_iv(anchor, strike)
        .unwrap_or(anchor.atm_iv)
        .max(0.0)
        / 100.0;
    if time_to_expiry_years <= 0.0 || iv <= 0.0 {
        return match index_price.partial_cmp(&strike) {
            Some(std::cmp::Ordering::Greater) => 1.0,
            Some(std::cmp::Ordering::Less) => 0.0,
            _ => 0.5,
        };
    }
    let sigma_sqrt_t = iv * time_to_expiry_years.sqrt();
    if sigma_sqrt_t <= f64::EPSILON {
        return match index_price.partial_cmp(&strike) {
            Some(std::cmp::Ordering::Greater) => 1.0,
            Some(std::cmp::Ordering::Less) => 0.0,
            _ => 0.5,
        };
    }
    let d2 = ((index_price / strike).ln() - 0.5 * iv * iv * time_to_expiry_years) / sigma_sqrt_t;
    clamp_probability(standard_normal_cdf(d2))
}

fn interpolated_mark_iv(anchor: &AnchorSnapshot, target_strike: f64) -> Option<f64> {
    let mut surface = anchor
        .local_surface_points
        .iter()
        .filter_map(|point| point.strike.to_f64().map(|strike| (strike, point.mark_iv)))
        .collect::<Vec<_>>();
    if surface.is_empty() {
        return None;
    }
    surface.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut unique_surface: Vec<(f64, f64)> = Vec::new();
    for (strike, iv) in surface {
        if let Some(last) = unique_surface.last_mut() {
            if (last.0 - strike).abs() <= f64::EPSILON {
                last.1 = (last.1 + iv) / 2.0;
                continue;
            }
        }
        unique_surface.push((strike, iv));
    }
    if target_strike <= unique_surface[0].0 {
        return Some(unique_surface[0].1);
    }
    if target_strike >= unique_surface[unique_surface.len() - 1].0 {
        return Some(unique_surface[unique_surface.len() - 1].1);
    }
    for window in unique_surface.windows(2) {
        let (left_strike, left_iv) = window[0];
        let (right_strike, right_iv) = window[1];
        if target_strike >= left_strike && target_strike <= right_strike {
            let span = right_strike - left_strike;
            if span.abs() <= f64::EPSILON {
                return Some((left_iv + right_iv) / 2.0);
            }
            let weight = (target_strike - left_strike) / span;
            return Some(left_iv + (right_iv - left_iv) * weight);
        }
    }
    unique_surface.last().map(|(_, iv)| *iv)
}

fn standard_normal_cdf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs() / 2.0_f64.sqrt();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let erf = 1.0 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * (-x * x).exp());
    0.5 * (1.0 + sign * erf)
}

fn time_to_expiry_years(reference_ts_ms: u64, expiry_ts_ms: u64) -> f64 {
    expiry_ts_ms.saturating_sub(reference_ts_ms) as f64 / MILLIS_PER_YEAR
}

fn central_difference(plus: f64, minus: f64, bump: f64) -> f64 {
    if bump <= f64::EPSILON {
        0.0
    } else {
        (plus - minus) / (2.0 * bump)
    }
}

fn second_derivative(plus: f64, mid: f64, minus: f64, bump: f64) -> f64 {
    if bump <= f64::EPSILON {
        0.0
    } else {
        (plus - 2.0 * mid + minus) / (bump * bump)
    }
}

fn relative_difference(first: f64, second: f64) -> Option<f64> {
    if !first.is_finite() || !second.is_finite() {
        return None;
    }
    let denom = first.abs().max(1e-9);
    Some(((first - second).abs()) / denom)
}

fn clamp_probability(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn pricing_test_config(
        delta_bump_ratio_bps: u64,
        min_abs_bump: Decimal,
        max_abs_bump: Decimal,
    ) -> EventPricerConfig {
        EventPricerConfig {
            delta_bump_ratio_bps,
            min_abs_bump,
            max_abs_bump,
            delta_stability_warn_ratio: 0.25,
            expiry_gap_adjustment: ExpiryGapAdjustment::new(60, 360, 720),
        }
    }

    #[test]
    fn event_delta_uses_relative_bump_with_floor_and_cap() {
        let config = pricing_test_config(1, Decimal::new(5, 0), Decimal::new(25, 0));

        let bump_eth = compute_delta_bump(Decimal::new(2_000, 0), &config);
        let bump_btc = compute_delta_bump(Decimal::new(100_000, 0), &config);

        assert_eq!(bump_eth, Decimal::new(5, 0));
        assert_eq!(bump_btc, Decimal::new(10, 0));
    }

    #[test]
    fn expiry_gap_adjustment_bridges_small_positive_gap_before_hard_cap() {
        let adjustment = ExpiryGapAdjustment::new(60, 360, 720);

        let adjusted = adjustment
            .apply(0.61, 0.64, 2.0 / 365.0, 6.0 / 365.0)
            .expect("soft mismatch should still price");

        assert!(adjusted.fair_prob_yes > 0.61);
        assert!(adjusted.fair_prob_yes < 0.64);
    }
}
