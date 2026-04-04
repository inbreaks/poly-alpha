use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use super::{
    CexBaseQty, OrderSide, PolyShares, Price, SignalStrength, Symbol, TokenSide, UsdNotional,
};

pub const PLANNING_SCHEMA_VERSION: u32 = 1;

fn deserialize_planning_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let schema_version = u32::deserialize(deserializer)?;
    if schema_version == PLANNING_SCHEMA_VERSION {
        Ok(schema_version)
    } else {
        Err(D::Error::custom(format!(
            "incompatible planning schema_version: expected {}, got {}",
            PLANNING_SCHEMA_VERSION, schema_version
        )))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OpenCandidate {
    #[serde(deserialize_with = "deserialize_planning_schema_version")]
    pub schema_version: u32,
    pub candidate_id: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub direction: String,
    pub fair_value: f64,
    pub raw_mispricing: f64,
    pub delta_estimate: f64,
    pub risk_budget_usd: f64,
    pub strength: SignalStrength,
    pub z_score: Option<f64>,
    #[serde(default)]
    pub raw_sigma: Option<f64>,
    #[serde(default)]
    pub effective_sigma: Option<f64>,
    #[serde(default)]
    pub sigma_source: Option<String>,
    #[serde(default)]
    pub returns_window_len: usize,
    pub timestamp_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanRejectionReason {
    MissingPolyBook,
    MissingCexBook,
    OneSidedPolyBook,
    OneSidedCexBook,
    StaleBookSequence,
    BookConflictSameSequence,
    NonPositivePlannedEdge,
    InsufficientPolyDepth,
    InsufficientCexDepth,
    OpenInstantLossTooLarge,
    PolyMaxPriceExceeded,
    PolyMinProceedsNotMet,
    ZeroCexHedgeQty,
    ResidualDeltaTooLarge,
    ShockLossTooLarge,
    MarketPhaseDisallowsIntent,
    HigherPriorityPlanActive,
    AdapterCannotPreserveConstraints,
}

impl PlanRejectionReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::MissingPolyBook => "missing_poly_book",
            Self::MissingCexBook => "missing_cex_book",
            Self::OneSidedPolyBook => "one_sided_poly_book",
            Self::OneSidedCexBook => "one_sided_cex_book",
            Self::StaleBookSequence => "stale_book_sequence",
            Self::BookConflictSameSequence => "book_conflict_same_sequence",
            Self::NonPositivePlannedEdge => "non_positive_planned_edge",
            Self::InsufficientPolyDepth => "insufficient_poly_depth",
            Self::InsufficientCexDepth => "insufficient_cex_depth",
            Self::OpenInstantLossTooLarge => "open_instant_loss_too_large",
            Self::PolyMaxPriceExceeded => "poly_max_price_exceeded",
            Self::PolyMinProceedsNotMet => "poly_min_proceeds_not_met",
            Self::ZeroCexHedgeQty => "zero_cex_hedge_qty",
            Self::ResidualDeltaTooLarge => "residual_delta_too_large",
            Self::ShockLossTooLarge => "shock_loss_too_large",
            Self::MarketPhaseDisallowsIntent => "market_phase_disallows_intent",
            Self::HigherPriorityPlanActive => "higher_priority_plan_active",
            Self::AdapterCannotPreserveConstraints => "adapter_cannot_preserve_constraints",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RevalidationFailureReason {
    PlanTtlExpired,
    PolySequenceDriftExceeded,
    CexSequenceDriftExceeded,
    PolyPriceMoveExceeded,
    CexPriceMoveExceeded,
    PlannedEdgeDeteriorated,
    ResidualRiskDeteriorated,
    PlanSuperseded,
    BookMissingOnRevalidate,
    BookConflictOnRevalidate,
}

impl RevalidationFailureReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::PlanTtlExpired => "plan_ttl_expired",
            Self::PolySequenceDriftExceeded => "poly_sequence_drift_exceeded",
            Self::CexSequenceDriftExceeded => "cex_sequence_drift_exceeded",
            Self::PolyPriceMoveExceeded => "poly_price_move_exceeded",
            Self::CexPriceMoveExceeded => "cex_price_move_exceeded",
            Self::PlannedEdgeDeteriorated => "planned_edge_deteriorated",
            Self::ResidualRiskDeteriorated => "residual_risk_deteriorated",
            Self::PlanSuperseded => "plan_superseded",
            Self::BookMissingOnRevalidate => "book_missing_on_revalidate",
            Self::BookConflictOnRevalidate => "book_conflict_on_revalidate",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDecisionReason {
    CexTopUpCheaper,
    PolyFlattenCheaper,
    CexTopUpFaster,
    PolyFlattenOnlySafeRoute,
    GhostOrderGuardBlocksFlatten,
    MarketPhaseBlocksPoly,
    CexDepthInsufficient,
    PolyDepthInsufficient,
    ForceExitRequired,
}

impl RecoveryDecisionReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::CexTopUpCheaper => "cex_top_up_cheaper",
            Self::PolyFlattenCheaper => "poly_flatten_cheaper",
            Self::CexTopUpFaster => "cex_top_up_faster",
            Self::PolyFlattenOnlySafeRoute => "poly_flatten_only_safe_route",
            Self::GhostOrderGuardBlocksFlatten => "ghost_order_guard_blocks_flatten",
            Self::MarketPhaseBlocksPoly => "market_phase_blocks_poly",
            Self::CexDepthInsufficient => "cex_depth_insufficient",
            Self::PolyDepthInsufficient => "poly_depth_insufficient",
            Self::ForceExitRequired => "force_exit_required",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResidualSnapshot {
    #[serde(deserialize_with = "deserialize_planning_schema_version")]
    pub schema_version: u32,
    pub residual_delta: f64,
    pub planned_cex_qty: CexBaseQty,
    pub current_poly_yes_shares: PolyShares,
    pub current_poly_no_shares: PolyShares,
    pub preferred_cex_side: OrderSide,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "intent_type", rename_all = "snake_case")]
pub enum PlanningIntent {
    OpenPosition {
        #[serde(deserialize_with = "deserialize_planning_schema_version")]
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        candidate: OpenCandidate,
        max_budget_usd: f64,
        max_residual_delta: f64,
        max_shock_loss_usd: f64,
    },
    ClosePosition {
        #[serde(deserialize_with = "deserialize_planning_schema_version")]
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        close_reason: String,
        target_close_ratio: f64,
    },
    DeltaRebalance {
        #[serde(deserialize_with = "deserialize_planning_schema_version")]
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        residual_snapshot: ResidualSnapshot,
        target_residual_delta_max: f64,
        target_shock_loss_max: f64,
    },
    ResidualRecovery {
        #[serde(deserialize_with = "deserialize_planning_schema_version")]
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        residual_snapshot: ResidualSnapshot,
        recovery_reason: RecoveryDecisionReason,
    },
    ForceExit {
        #[serde(deserialize_with = "deserialize_planning_schema_version")]
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        force_reason: String,
        allow_negative_edge: bool,
    },
}

impl PlanningIntent {
    pub fn schema_version(&self) -> u32 {
        match self {
            Self::OpenPosition { schema_version, .. }
            | Self::ClosePosition { schema_version, .. }
            | Self::DeltaRebalance { schema_version, .. }
            | Self::ResidualRecovery { schema_version, .. }
            | Self::ForceExit { schema_version, .. } => *schema_version,
        }
    }

    pub fn intent_id(&self) -> &str {
        match self {
            Self::OpenPosition { intent_id, .. }
            | Self::ClosePosition { intent_id, .. }
            | Self::DeltaRebalance { intent_id, .. }
            | Self::ResidualRecovery { intent_id, .. }
            | Self::ForceExit { intent_id, .. } => intent_id,
        }
    }

    pub fn correlation_id(&self) -> &str {
        match self {
            Self::OpenPosition { correlation_id, .. }
            | Self::ClosePosition { correlation_id, .. }
            | Self::DeltaRebalance { correlation_id, .. }
            | Self::ResidualRecovery { correlation_id, .. }
            | Self::ForceExit { correlation_id, .. } => correlation_id,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::OpenPosition { symbol, .. }
            | Self::ClosePosition { symbol, .. }
            | Self::DeltaRebalance { symbol, .. }
            | Self::ResidualRecovery { symbol, .. }
            | Self::ForceExit { symbol, .. } => symbol,
        }
    }

    pub fn intent_type_code(&self) -> &'static str {
        match self {
            Self::OpenPosition { .. } => "open_position",
            Self::ClosePosition { .. } => "close_position",
            Self::DeltaRebalance { .. } => "delta_rebalance",
            Self::ResidualRecovery { .. } => "residual_recovery",
            Self::ForceExit { .. } => "force_exit",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OrderLedgerEntry {
    pub order_id: String,
    pub client_order_id: String,
    pub requested_qty: String,
    pub filled_qty: String,
    pub cancelled_qty: String,
    pub avg_price: String,
    pub fee_usd: f64,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TradePlan {
    #[serde(deserialize_with = "deserialize_planning_schema_version")]
    pub schema_version: u32,
    pub plan_id: String,
    pub parent_plan_id: Option<String>,
    pub supersedes_plan_id: Option<String>,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub intent_type: String,
    pub priority: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_decision_reason: Option<RecoveryDecisionReason>,
    pub created_at_ms: u64,
    pub poly_exchange_timestamp_ms: u64,
    pub poly_received_at_ms: u64,
    pub poly_sequence: u64,
    pub cex_exchange_timestamp_ms: u64,
    pub cex_received_at_ms: u64,
    pub cex_sequence: u64,
    pub plan_hash: String,
    pub poly_side: OrderSide,
    pub poly_token_side: TokenSide,
    pub poly_sizing_mode: String,
    pub poly_requested_shares: PolyShares,
    pub poly_planned_shares: PolyShares,
    pub poly_max_cost_usd: UsdNotional,
    pub poly_max_avg_price: Price,
    pub poly_max_shares: PolyShares,
    pub poly_min_avg_price: Price,
    pub poly_min_proceeds_usd: UsdNotional,
    pub poly_book_avg_price: Price,
    pub poly_executable_price: Price,
    pub poly_friction_cost_usd: UsdNotional,
    pub poly_fee_usd: UsdNotional,
    pub cex_side: OrderSide,
    pub cex_planned_qty: CexBaseQty,
    pub cex_book_avg_price: Price,
    pub cex_executable_price: Price,
    pub cex_friction_cost_usd: UsdNotional,
    pub cex_fee_usd: UsdNotional,
    pub raw_edge_usd: UsdNotional,
    pub planned_edge_usd: UsdNotional,
    pub expected_funding_cost_usd: UsdNotional,
    pub residual_risk_penalty_usd: UsdNotional,
    pub post_rounding_residual_delta: f64,
    pub shock_loss_up_1pct: UsdNotional,
    pub shock_loss_down_1pct: UsdNotional,
    pub shock_loss_up_2pct: UsdNotional,
    pub shock_loss_down_2pct: UsdNotional,
    pub plan_ttl_ms: u64,
    pub max_poly_sequence_drift: u64,
    pub max_cex_sequence_drift: u64,
    pub max_poly_price_move: Price,
    pub max_cex_price_move: Price,
    pub min_planned_edge_usd: UsdNotional,
    pub max_residual_delta: f64,
    pub max_shock_loss_usd: UsdNotional,
    pub max_plan_vs_fill_deviation_usd: UsdNotional,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionResult {
    #[serde(deserialize_with = "deserialize_planning_schema_version")]
    pub schema_version: u32,
    pub plan_id: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub status: String,
    pub poly_order_ledger: Vec<OrderLedgerEntry>,
    pub cex_order_ledger: Vec<OrderLedgerEntry>,
    pub actual_poly_cost_usd: UsdNotional,
    pub actual_cex_cost_usd: UsdNotional,
    pub actual_poly_fee_usd: UsdNotional,
    pub actual_cex_fee_usd: UsdNotional,
    pub actual_funding_cost_usd: UsdNotional,
    pub realized_edge_usd: UsdNotional,
    pub plan_vs_fill_deviation_usd: UsdNotional,
    pub actual_residual_delta: f64,
    pub actual_shock_loss_up_1pct: UsdNotional,
    pub actual_shock_loss_down_1pct: UsdNotional,
    pub actual_shock_loss_up_2pct: UsdNotional,
    pub actual_shock_loss_down_2pct: UsdNotional,
    pub recovery_required: bool,
    pub timestamp_ms: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        AlphaEngineOutput, OpenCandidate, PlanRejectionReason, PlanningIntent,
        RecoveryDecisionReason, ResidualSnapshot, SignalStrength, Symbol, TokenSide,
        PLANNING_SCHEMA_VERSION,
    };

    #[test]
    fn planning_objects_roundtrip_with_schema_version() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.47,
            raw_mispricing: 0.06,
            delta_estimate: 0.18,
            risk_budget_usd: 200.0,
            strength: SignalStrength::Normal,
            z_score: Some(2.1),
            raw_sigma: Some(0.001),
            effective_sigma: Some(0.002),
            sigma_source: Some("default".to_owned()),
            returns_window_len: 12,
            timestamp_ms: 1_716_000_000_000,
        };

        let encoded = serde_json::to_value(&candidate).unwrap();
        assert_eq!(encoded["schema_version"], json!(1));
        let decoded: OpenCandidate = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.schema_version, 1);
    }

    #[test]
    fn planning_intent_roundtrips_with_schema_version() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-2".to_owned(),
            correlation_id: "corr-2".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::No,
            direction: "short".to_owned(),
            fair_value: 0.58,
            raw_mispricing: -0.04,
            delta_estimate: -0.12,
            risk_budget_usd: 150.0,
            strength: SignalStrength::Weak,
            z_score: Some(-1.8),
            raw_sigma: None,
            effective_sigma: Some(0.002),
            sigma_source: Some("default".to_owned()),
            returns_window_len: 0,
            timestamp_ms: 1_716_000_000_100,
        };
        let intent = PlanningIntent::OpenPosition {
            schema_version: 1,
            intent_id: "intent-1".to_owned(),
            correlation_id: "corr-2".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            candidate,
            max_budget_usd: 150.0,
            max_residual_delta: 0.05,
            max_shock_loss_usd: 12.5,
        };

        let encoded = serde_json::to_value(&intent).unwrap();
        let decoded: PlanningIntent = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.schema_version(), 1);
        assert_eq!(decoded.intent_type_code(), "open_position");
    }

    #[test]
    fn plan_rejection_codes_are_stable() {
        assert_eq!(
            PlanRejectionReason::MissingPolyBook.code(),
            "missing_poly_book"
        );
        assert_eq!(
            PlanRejectionReason::ZeroCexHedgeQty.code(),
            "zero_cex_hedge_qty"
        );
    }

    #[test]
    fn plan_rejection_reason_exposes_open_instant_loss_too_large_code() {
        assert_eq!(
            PlanRejectionReason::OpenInstantLossTooLarge.code(),
            "open_instant_loss_too_large"
        );
    }

    #[test]
    fn alpha_engine_output_collects_open_candidates() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-3".to_owned(),
            correlation_id: "corr-3".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::No,
            direction: "short".to_owned(),
            fair_value: 0.58,
            raw_mispricing: -0.04,
            delta_estimate: -0.12,
            risk_budget_usd: 150.0,
            strength: SignalStrength::Weak,
            z_score: Some(-1.8),
            raw_sigma: None,
            effective_sigma: None,
            sigma_source: None,
            returns_window_len: 0,
            timestamp_ms: 1_716_000_000_100,
        };

        let mut output = AlphaEngineOutput::default();
        output.push_open_candidate(candidate);
        assert_eq!(output.open_candidates.len(), 1);
        assert!(!output.is_empty());
    }

    #[test]
    fn planning_objects_reject_incompatible_schema_version() {
        let raw = json!({
            "schema_version": PLANNING_SCHEMA_VERSION + 1,
            "candidate_id": "cand-1",
            "correlation_id": "corr-1",
            "symbol": "btc-price-only",
            "token_side": "yes",
            "direction": "long",
            "fair_value": 0.47,
            "raw_mispricing": 0.06,
            "delta_estimate": 0.18,
            "risk_budget_usd": 200.0,
            "strength": "normal",
            "z_score": 2.1,
            "timestamp_ms": 1_716_000_000_000u64
        });

        let err = serde_json::from_value::<OpenCandidate>(raw).expect_err("schema mismatch");
        assert!(err
            .to_string()
            .contains("incompatible planning schema_version"));
    }

    #[test]
    fn residual_recovery_requires_compatible_nested_snapshot_schema_version() {
        let intent = PlanningIntent::ResidualRecovery {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            residual_snapshot: ResidualSnapshot {
                schema_version: PLANNING_SCHEMA_VERSION,
                residual_delta: 0.12,
                planned_cex_qty: crate::CexBaseQty(rust_decimal::Decimal::new(1, 0)),
                current_poly_yes_shares: crate::PolyShares(rust_decimal::Decimal::new(3, 0)),
                current_poly_no_shares: crate::PolyShares(rust_decimal::Decimal::new(1, 0)),
                preferred_cex_side: crate::OrderSide::Buy,
            },
            recovery_reason: RecoveryDecisionReason::CexTopUpFaster,
        };
        let mut raw = serde_json::to_value(intent).unwrap();
        raw["residual_snapshot"]["schema_version"] = json!(PLANNING_SCHEMA_VERSION + 1);

        let err = serde_json::from_value::<PlanningIntent>(raw).expect_err("schema mismatch");
        assert!(err
            .to_string()
            .contains("incompatible planning schema_version"));
    }

    #[test]
    fn delta_rebalance_requires_compatible_nested_snapshot_schema_version() {
        let intent = PlanningIntent::DeltaRebalance {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            residual_snapshot: ResidualSnapshot {
                schema_version: PLANNING_SCHEMA_VERSION,
                residual_delta: 0.12,
                planned_cex_qty: crate::CexBaseQty(rust_decimal::Decimal::new(1, 0)),
                current_poly_yes_shares: crate::PolyShares(rust_decimal::Decimal::new(3, 0)),
                current_poly_no_shares: crate::PolyShares(rust_decimal::Decimal::new(1, 0)),
                preferred_cex_side: crate::OrderSide::Buy,
            },
            target_residual_delta_max: 0.05,
            target_shock_loss_max: 10.0,
        };
        let mut raw = serde_json::to_value(intent).unwrap();
        raw["residual_snapshot"]["schema_version"] = json!(PLANNING_SCHEMA_VERSION + 1);

        let err = serde_json::from_value::<PlanningIntent>(raw).expect_err("schema mismatch");
        assert!(err
            .to_string()
            .contains("incompatible planning schema_version"));
    }
}
