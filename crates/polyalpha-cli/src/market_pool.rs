#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarketPoolState {
    pub last_focus_at_ms: Option<u64>,
    pub last_tradeable_at_ms: Option<u64>,
    pub open_cooldown_until_ms: Option<u64>,
    pub open_cooldown_reason: Option<String>,
    cooldown_family: Option<CooldownFamily>,
    cooldown_streak: u8,
    cooldown_last_started_at_ms: Option<u64>,
}

const COOLDOWN_REASON_PREFIX: &str = "cooldown_";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CooldownFamily {
    Structural,
    Edge,
    MarketData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CooldownPolicy {
    family: CooldownFamily,
    base_duration_ms: u64,
    repeat_window_ms: Option<u64>,
    max_duration_ms: u64,
}

pub fn note_focus_activity(state: &mut MarketPoolState, now_ms: u64) {
    state.last_focus_at_ms = Some(now_ms);
}

pub fn note_tradeable_activity(state: &mut MarketPoolState, now_ms: u64) {
    state.last_focus_at_ms = Some(now_ms);
    state.last_tradeable_at_ms = Some(now_ms);
    clear_open_cooldown(state);
    reset_open_cooldown_tracking(state);
}

pub fn clear_expired_open_cooldown(state: &mut MarketPoolState, now_ms: u64) {
    if state
        .open_cooldown_until_ms
        .is_some_and(|until_ms| until_ms <= now_ms)
    {
        clear_open_cooldown(state);
    }
}

pub fn active_open_cooldown_reason(state: &MarketPoolState, now_ms: u64) -> Option<&str> {
    match (
        state.open_cooldown_until_ms,
        state.open_cooldown_reason.as_deref(),
    ) {
        (Some(until_ms), Some(reason)) if until_ms > now_ms => Some(reason),
        _ => None,
    }
}

pub fn active_open_cooldown_remaining_ms(state: &MarketPoolState, now_ms: u64) -> Option<u64> {
    state.open_cooldown_until_ms.and_then(|until_ms| {
        (until_ms > now_ms)
            .then_some(until_ms.saturating_sub(now_ms))
            .filter(|remaining_ms| *remaining_ms > 0)
    })
}

pub fn cooldown_gate_reason(reason: &str) -> String {
    format!("{COOLDOWN_REASON_PREFIX}{reason}")
}

pub fn cooldown_reason_from_gate(reason: &str) -> Option<&str> {
    reason.strip_prefix(COOLDOWN_REASON_PREFIX)
}

pub fn extract_plan_rejection_reason_code(message: &str) -> Option<String> {
    extract_reason_code(message, "plan rejected [")
        .or_else(|| extract_reason_code(message, "plan revalidation failed ["))
}

pub fn maybe_start_open_cooldown(
    state: &mut MarketPoolState,
    base_stale_ms: u64,
    reason: &str,
    now_ms: u64,
) -> bool {
    clear_expired_open_cooldown(state, now_ms);

    let Some(policy) = open_cooldown_policy(base_stale_ms, reason) else {
        return false;
    };
    let streak = next_cooldown_streak(state, policy, now_ms);
    let duration_ms = cooldown_duration_ms(policy, streak);

    let new_until_ms = now_ms.saturating_add(duration_ms);
    let old_until_ms = state.open_cooldown_until_ms.unwrap_or(0);
    if new_until_ms >= old_until_ms {
        state.open_cooldown_until_ms = Some(new_until_ms);
        state.open_cooldown_reason = Some(reason.to_owned());
    } else {
        state.open_cooldown_until_ms = Some(old_until_ms);
    }
    state.last_focus_at_ms = Some(now_ms);
    state.cooldown_family = Some(policy.family);
    state.cooldown_streak = streak;
    state.cooldown_last_started_at_ms = Some(now_ms);
    true
}

fn open_cooldown_policy(base_stale_ms: u64, reason: &str) -> Option<CooldownPolicy> {
    let base_ms = base_stale_ms.max(5_000);
    match reason {
        "zero_cex_hedge_qty"
        | "residual_delta_too_large"
        | "shock_loss_too_large"
        | "insufficient_poly_depth"
        | "insufficient_cex_depth"
        | "poly_max_price_exceeded"
        | "adapter_cannot_preserve_constraints" => Some(CooldownPolicy {
            family: CooldownFamily::Structural,
            base_duration_ms: base_ms.saturating_mul(12).max(15 * 60 * 1_000),
            repeat_window_ms: Some(30 * 60 * 1_000),
            max_duration_ms: 60 * 60 * 1_000,
        }),
        "non_positive_planned_edge"
        | "open_instant_loss_too_large"
        | "poly_price_impact_too_large"
        | "planned_edge_deteriorated"
        | "residual_risk_deteriorated" => Some(CooldownPolicy {
            family: CooldownFamily::Edge,
            base_duration_ms: base_ms.saturating_mul(6).max(3 * 60 * 1_000),
            repeat_window_ms: Some(10 * 60 * 1_000),
            max_duration_ms: 15 * 60 * 1_000,
        }),
        "below_min_poly_price" | "above_or_equal_max_poly_price" | "missing_poly_quote" => {
            Some(CooldownPolicy {
                family: CooldownFamily::Edge,
                base_duration_ms: base_ms.saturating_mul(6).max(3 * 60 * 1_000),
                repeat_window_ms: Some(10 * 60 * 1_000),
                max_duration_ms: 15 * 60 * 1_000,
            })
        }
        "missing_poly"
        | "missing_cex"
        | "missing_observed_state"
        | "poly_stale"
        | "cex_stale"
        | "misaligned"
        | "connection_impaired"
        | "poly_lag_borderline"
        | "missing_poly_book"
        | "missing_cex_book"
        | "one_sided_poly_book"
        | "one_sided_cex_book"
        | "stale_book_sequence"
        | "book_conflict_same_sequence"
        | "plan_ttl_expired"
        | "poly_sequence_drift_exceeded"
        | "cex_sequence_drift_exceeded"
        | "poly_price_move_exceeded"
        | "cex_price_move_exceeded"
        | "book_missing_on_revalidate"
        | "book_conflict_on_revalidate" => Some(CooldownPolicy {
            family: CooldownFamily::MarketData,
            base_duration_ms: base_ms.saturating_mul(3),
            repeat_window_ms: None,
            max_duration_ms: base_ms.saturating_mul(3),
        }),
        _ => None,
    }
}

fn clear_open_cooldown(state: &mut MarketPoolState) {
    state.open_cooldown_until_ms = None;
    state.open_cooldown_reason = None;
}

fn reset_open_cooldown_tracking(state: &mut MarketPoolState) {
    state.cooldown_family = None;
    state.cooldown_streak = 0;
    state.cooldown_last_started_at_ms = None;
}

fn next_cooldown_streak(state: &MarketPoolState, policy: CooldownPolicy, now_ms: u64) -> u8 {
    let Some(repeat_window_ms) = policy.repeat_window_ms else {
        return 1;
    };

    match (
        state.cooldown_family,
        state.cooldown_last_started_at_ms,
        state.cooldown_streak,
    ) {
        (Some(family), Some(last_started_at_ms), streak)
            if family == policy.family
                && now_ms.saturating_sub(last_started_at_ms) <= repeat_window_ms =>
        {
            streak.saturating_add(1)
        }
        _ => 1,
    }
}

fn cooldown_duration_ms(policy: CooldownPolicy, streak: u8) -> u64 {
    let mut duration_ms = policy.base_duration_ms;
    for _ in 1..streak {
        duration_ms = duration_ms.saturating_mul(2).min(policy.max_duration_ms);
        if duration_ms == policy.max_duration_ms {
            break;
        }
    }
    duration_ms
}

fn extract_reason_code(message: &str, prefix: &str) -> Option<String> {
    let rest = message.strip_prefix(prefix)?;
    let (code, _) = rest.split_once(']')?;
    (!code.is_empty()).then(|| code.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maybe_start_open_cooldown_tracks_reason_and_remaining_time() {
        let mut state = MarketPoolState::default();

        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            1_000,
        ));
        assert_eq!(
            active_open_cooldown_reason(&state, 1_001),
            Some("zero_cex_hedge_qty")
        );
        assert_eq!(
            active_open_cooldown_remaining_ms(&state, 1_001),
            Some(899_999)
        );
    }

    #[test]
    fn note_tradeable_activity_clears_open_cooldown() {
        let mut state = MarketPoolState::default();
        maybe_start_open_cooldown(&mut state, 5_000, "non_positive_planned_edge", 1_000);

        note_tradeable_activity(&mut state, 2_000);

        assert_eq!(state.last_tradeable_at_ms, Some(2_000));
        assert_eq!(state.open_cooldown_until_ms, None);
        assert_eq!(state.open_cooldown_reason, None);
    }

    #[test]
    fn maybe_start_open_cooldown_accepts_ws_stale_and_price_band_reasons() {
        let mut stale_state = MarketPoolState::default();
        assert!(maybe_start_open_cooldown(
            &mut stale_state,
            5_000,
            "cex_stale",
            1_000,
        ));
        assert_eq!(
            active_open_cooldown_reason(&stale_state, 1_001),
            Some("cex_stale")
        );

        let mut band_state = MarketPoolState::default();
        assert!(maybe_start_open_cooldown(
            &mut band_state,
            5_000,
            "above_or_equal_max_poly_price",
            2_000,
        ));
        assert_eq!(
            active_open_cooldown_reason(&band_state, 2_001),
            Some("above_or_equal_max_poly_price")
        );
    }

    #[test]
    fn repeated_structural_rejections_extend_cooldown_duration() {
        let mut state = MarketPoolState::default();

        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            1_000,
        ));
        let first_until_ms = state.open_cooldown_until_ms.expect("first cooldown");

        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            first_until_ms.saturating_add(1),
        ));
        let second_started_at_ms = first_until_ms.saturating_add(1);
        let second_until_ms = state.open_cooldown_until_ms.expect("second cooldown");

        assert_eq!(first_until_ms.saturating_sub(1_000), 900_000);
        assert_eq!(
            second_until_ms.saturating_sub(second_started_at_ms),
            1_800_000,
            "repeat structural failures should escalate the cooldown instead of reusing the base duration",
        );
    }

    #[test]
    fn tradeable_activity_resets_structural_cooldown_escalation() {
        let mut state = MarketPoolState::default();

        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            1_000,
        ));
        let first_until_ms = state.open_cooldown_until_ms.expect("first cooldown");
        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            first_until_ms.saturating_add(1),
        ));

        note_tradeable_activity(&mut state, 200_000);

        assert!(maybe_start_open_cooldown(
            &mut state,
            5_000,
            "zero_cex_hedge_qty",
            201_000,
        ));
        assert_eq!(
            state
                .open_cooldown_until_ms
                .expect("cooldown after reset")
                .saturating_sub(201_000),
            900_000,
            "a successful tradeable activity should reset the structural cooldown escalation",
        );
    }
}
