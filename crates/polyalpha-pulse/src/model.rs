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
