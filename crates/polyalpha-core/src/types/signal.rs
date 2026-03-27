use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{
    CexBaseQty, OpenCandidate, OrderSide, PolyShares, Price, Symbol, TokenSide, UsdNotional,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignalStrength {
    Strong,
    Normal,
    Weak,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbLeg {
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub quantity: PolyShares,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DmmQuoteState {
    pub symbol: Symbol,
    pub bid: Price,
    pub ask: Price,
    pub bid_qty: PolyShares,
    pub ask_qty: PolyShares,
    pub updated_at_ms: u64,
}

pub type DmmQuoteSlot = Option<DmmQuoteState>;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DmmQuoteUpdate {
    pub symbol: Symbol,
    pub next_state: DmmQuoteSlot,
}

impl DmmQuoteUpdate {
    pub fn set(state: DmmQuoteState) -> Self {
        let symbol = state.symbol.clone();
        Self {
            symbol,
            next_state: Some(state),
        }
    }

    pub fn clear(symbol: Symbol) -> Self {
        Self {
            symbol,
            next_state: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ArbSignalAction {
    BasisLong {
        token_side: TokenSide,
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,
        delta: f64,
    },
    BasisShort {
        token_side: TokenSide,
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,
        delta: f64,
    },
    DeltaRebalance {
        cex_side: OrderSide,
        cex_qty_adjust: CexBaseQty,
        new_delta: f64,
    },
    NegRiskArb {
        legs: Vec<ArbLeg>,
    },
    ClosePosition {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArbSignalEvent {
    pub signal_id: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub action: ArbSignalAction,
    pub strength: SignalStrength,
    pub basis_value: Option<Decimal>,
    pub z_score: Option<Decimal>,
    pub expected_pnl: UsdNotional,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EngineWarning {
    ConnectionLost {
        symbol: Symbol,
        poly_connected: bool,
        cex_connected: bool,
    },
    NoCexData {
        symbol: Symbol,
    },
    NoPolyData {
        symbol: Symbol,
    },
    CexPriceStale {
        symbol: Symbol,
        cex_age_ms: u64,
        max_age_ms: u64,
    },
    PolyPriceStale {
        symbol: Symbol,
        poly_age_ms: u64,
        max_age_ms: u64,
    },
    DataMisaligned {
        symbol: Symbol,
        poly_time_ms: u64,
        cex_time_ms: u64,
        diff_ms: u64,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AlphaEngineOutput {
    pub dmm_updates: Vec<DmmQuoteUpdate>,
    #[serde(default)]
    pub open_candidates: Vec<OpenCandidate>,
    pub arb_signals: Vec<ArbSignalEvent>,
    #[serde(default)]
    pub warnings: Vec<EngineWarning>,
}

impl AlphaEngineOutput {
    pub fn is_empty(&self) -> bool {
        self.dmm_updates.is_empty()
            && self.open_candidates.is_empty()
            && self.arb_signals.is_empty()
            && self.warnings.is_empty()
    }

    pub fn push_dmm_update(&mut self, update: DmmQuoteUpdate) {
        self.dmm_updates.push(update);
    }

    pub fn push_quote_state(&mut self, state: DmmQuoteState) {
        self.push_dmm_update(DmmQuoteUpdate::set(state));
    }

    pub fn clear_quote(&mut self, symbol: Symbol) {
        self.push_dmm_update(DmmQuoteUpdate::clear(symbol));
    }

    pub fn push_open_candidate(&mut self, candidate: OpenCandidate) {
        self.open_candidates.push(candidate);
    }

    pub fn push_arb_signal(&mut self, signal: ArbSignalEvent) {
        self.arb_signals.push(signal);
    }

    pub fn push_warning(&mut self, warning: EngineWarning) {
        self.warnings.push(warning);
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn dmm_quote_updates_support_set_and_clear_semantics() {
        let state = DmmQuoteState {
            symbol: Symbol::new("btc-100k-mar-2026"),
            bid: Price(Decimal::new(48, 2)),
            ask: Price(Decimal::new(52, 2)),
            bid_qty: PolyShares(Decimal::new(25, 0)),
            ask_qty: PolyShares(Decimal::new(20, 0)),
            updated_at_ms: 1_716_000_000_000,
        };

        let set = DmmQuoteUpdate::set(state.clone());
        assert_eq!(set.symbol, state.symbol);
        assert_eq!(set.next_state, Some(state));

        let clear = DmmQuoteUpdate::clear(Symbol::new("btc-100k-mar-2026"));
        assert!(clear.next_state.is_none());
    }

    #[test]
    fn alpha_engine_output_tracks_both_signal_paths() {
        let mut output = AlphaEngineOutput::default();
        assert!(output.is_empty());

        output.push_quote_state(DmmQuoteState {
            symbol: Symbol::new("btc-100k-mar-2026"),
            bid: Price(Decimal::new(49, 2)),
            ask: Price(Decimal::new(51, 2)),
            bid_qty: PolyShares(Decimal::new(10, 0)),
            ask_qty: PolyShares(Decimal::new(10, 0)),
            updated_at_ms: 1_716_000_000_000,
        });
        output.push_arb_signal(ArbSignalEvent {
            signal_id: "sig-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-100k-mar-2026"),
            action: ArbSignalAction::ClosePosition {
                reason: "basis normalized".to_owned(),
            },
            strength: SignalStrength::Normal,
            basis_value: None,
            z_score: None,
            expected_pnl: UsdNotional(Decimal::ZERO),
            timestamp_ms: 1_716_000_000_000,
        });

        assert_eq!(output.dmm_updates.len(), 1);
        assert_eq!(output.arb_signals.len(), 1);
        assert!(!output.is_empty());
    }

    #[test]
    fn alpha_engine_output_treats_warnings_as_non_empty() {
        let mut output = AlphaEngineOutput::default();
        output.push_warning(EngineWarning::NoCexData {
            symbol: Symbol::new("btc-100k-mar-2026"),
        });

        assert_eq!(output.warnings.len(), 1);
        assert!(!output.is_empty());
    }
}
