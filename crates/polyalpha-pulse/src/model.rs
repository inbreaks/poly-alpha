use rust_decimal::Decimal;

use polyalpha_data::DeribitAsset;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PulseAsset {
    Btc,
    Eth,
    Sol,
    Xrp,
}

impl PulseAsset {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "btc",
            Self::Eth => "eth",
            Self::Sol => "sol",
            Self::Xrp => "xrp",
        }
    }

    pub fn from_routing_key(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "btc" => Some(Self::Btc),
            "eth" => Some(Self::Eth),
            "sol" => Some(Self::Sol),
            "xrp" => Some(Self::Xrp),
            _ => None,
        }
    }

    pub fn as_deribit_asset(self) -> Option<DeribitAsset> {
        match self {
            Self::Btc => Some(DeribitAsset::Btc),
            Self::Eth => Some(DeribitAsset::Eth),
            Self::Sol | Self::Xrp => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnchorSnapshot {
    pub asset: PulseAsset,
    pub provider_id: String,
    pub ts_ms: u64,
    pub index_price: Decimal,
    pub expiry_ts_ms: u64,
    pub atm_iv: f64,
    pub local_surface_points: Vec<LocalSurfacePoint>,
    pub quality: AnchorQualityMetrics,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LocalSurfacePoint {
    pub instrument_name: String,
    pub strike: Decimal,
    pub expiry_ts_ms: u64,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub mark_iv: f64,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnchorQualityMetrics {
    pub anchor_age_ms: u64,
    pub max_quote_spread_bps: Option<Decimal>,
    pub has_strike_coverage: bool,
    pub has_liquidity: bool,
    pub expiry_mismatch_minutes: i64,
    pub greeks_complete: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PricingQuality {
    pub delta_stable: bool,
    pub delta_stability_ratio: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventPriceOutput {
    pub fair_prob_yes: f64,
    pub fair_prob_no: f64,
    pub event_delta_yes: f64,
    pub event_delta_no: f64,
    pub gamma_estimate: Option<f64>,
    pub delta_bump_used: Decimal,
    pub expiry_gap_adjustment_applied: bool,
    pub pricing_quality: PricingQuality,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PulseOpportunityInput {
    pub instant_basis_bps: f64,
    pub poly_vwap_slippage_bps: f64,
    pub hedge_slippage_bps: f64,
    pub fee_bps: f64,
    pub perp_basis_penalty_bps: f64,
    pub rehedge_reserve_bps: f64,
    pub timeout_exit_reserve_bps: f64,
    pub expected_net_edge_bps: f64,
    pub expected_net_pnl_usd: Decimal,
    pub timeout_loss_estimate_usd: Decimal,
    pub reversion_pocket_ticks: f64,
    pub vacuum_ratio: Decimal,
    pub anchor_quality_ok: bool,
    pub pricing_quality_ok: bool,
    pub data_fresh: bool,
    pub has_pulse_history: bool,
    pub claim_price_move_bps: f64,
    pub fair_claim_move_bps: f64,
    pub cex_mid_move_bps: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DetectorDecision {
    pub should_trade: bool,
    pub net_session_edge_bps: f64,
    pub expected_net_pnl_usd: Decimal,
    pub pulse_score_bps: f64,
    pub required_hit_rate: f64,
    pub rejection_code: Option<PulseFailureCode>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PulseFailureCode {
    NetSessionEdgeBelowThreshold,
    AnchorQualityRejected,
    AnchorLatencyDeltaRejected,
    DataFreshnessRejected,
    OpeningRejectCooldownActive,
    PulseConfirmationRejected,
    TimeoutRiskRejected,
    HardExpiryGapExceeded,
}

impl PulseFailureCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NetSessionEdgeBelowThreshold => "net_session_edge_below_threshold",
            Self::AnchorQualityRejected => "anchor_quality_rejected",
            Self::AnchorLatencyDeltaRejected => "anchor_latency_delta_rejected",
            Self::DataFreshnessRejected => "data_freshness_rejected",
            Self::OpeningRejectCooldownActive => "opening_reject_cooldown_active",
            Self::PulseConfirmationRejected => "pulse_confirmation_rejected",
            Self::TimeoutRiskRejected => "timeout_risk_rejected",
            Self::HardExpiryGapExceeded => "hard_expiry_gap_exceeded",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PulseSessionState {
    PreTradeAudit,
    PolyOpening,
    HedgeOpening,
    MakerExitWorking,
    Pegging,
    Rehedging,
    EmergencyHedge,
    EmergencyFlatten,
    ChasingExit,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GammaCapMode {
    DeltaClamp,
    ProtectiveOnly,
    Freeze,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SessionCommand {
    SubmitPolyOpen { max_price: Decimal, qty: Decimal },
    SubmitMakerExit { price: Decimal, qty: Decimal },
    ReplaceMakerExit { price: Decimal, qty: Decimal },
    RequestHedgeReconcile { hedge_reference_qty: Decimal },
    EmergencyFlatten { reason: PulseFailureCode },
}

impl SessionCommand {
    pub fn hedge_reference_qty(&self) -> Decimal {
        match self {
            Self::RequestHedgeReconcile {
                hedge_reference_qty,
            } => *hedge_reference_qty,
            _ => Decimal::ZERO,
        }
    }
}
