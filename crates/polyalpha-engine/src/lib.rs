use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use polyalpha_core::{
    AlphaEngine, AlphaEngineOutput, ArbSignalAction, ArbSignalEvent, DmmQuoteState, EngineParams,
    InstrumentKind, MarketDataEvent, MarketPhase, OrderBookSnapshot, OrderSide, PolyShares, Price,
    SignalStrength, Symbol, UsdNotional,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArbBias {
    LongPoly,
    ShortPoly,
}

#[derive(Clone, Debug, PartialEq)]
struct SymbolState {
    poly_yes_mid: Option<Decimal>,
    cex_mid: Option<Decimal>,
    previous_cex_mid: Option<Decimal>,
    funding_rate: Option<f64>,
    market_phase: MarketPhase,
    warmup_samples_seen: usize,
    active_arb: Option<ArbBias>,
    has_live_dmm_quote: bool,
    last_update_ms: u64,
}

impl Default for SymbolState {
    fn default() -> Self {
        Self {
            poly_yes_mid: None,
            cex_mid: None,
            previous_cex_mid: None,
            funding_rate: None,
            market_phase: MarketPhase::Trading,
            warmup_samples_seen: 0,
            active_arb: None,
            has_live_dmm_quote: false,
            last_update_ms: 0,
        }
    }
}

impl SymbolState {
    fn has_required_inputs(&self) -> bool {
        self.poly_yes_mid.is_some() && self.cex_mid.is_some()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimpleEngineConfig {
    pub warmup_samples: usize,
    pub basis_entry_threshold: f64,
    pub basis_exit_threshold: f64,
    pub target_shares: PolyShares,
    pub dmm_half_spread: Decimal,
    pub dmm_quote_size: PolyShares,
    pub base_delta: f64,
    pub delta_sensitivity: f64,
    pub cex_anchor_price: f64,
    pub cex_price_scale: f64,
    pub funding_rate_weight: f64,
    pub momentum_weight: f64,
}

impl Default for SimpleEngineConfig {
    fn default() -> Self {
        Self {
            warmup_samples: 2,
            basis_entry_threshold: 0.08,
            basis_exit_threshold: 0.03,
            target_shares: PolyShares(Decimal::new(25, 0)),
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: PolyShares(Decimal::new(20, 0)),
            base_delta: 0.0001,
            delta_sensitivity: 0.0005,
            cex_anchor_price: 100_000.0,
            cex_price_scale: 20_000.0,
            funding_rate_weight: 40.0,
            momentum_weight: 2.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimpleAlphaEngine {
    config: SimpleEngineConfig,
    states: HashMap<Symbol, SymbolState>,
    max_position_usd: Option<UsdNotional>,
    next_signal_seq: u64,
}

impl Default for SimpleAlphaEngine {
    fn default() -> Self {
        Self::new(SimpleEngineConfig::default())
    }
}

impl SimpleAlphaEngine {
    pub fn new(config: SimpleEngineConfig) -> Self {
        Self {
            config,
            states: HashMap::new(),
            max_position_usd: None,
            next_signal_seq: 0,
        }
    }

    pub fn config(&self) -> &SimpleEngineConfig {
        &self.config
    }

    fn apply_market_event(&mut self, event: &MarketDataEvent) -> Option<Symbol> {
        match event {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                let symbol = snapshot.symbol.clone();
                let mut state = self.states.remove(&symbol).unwrap_or_default();
                state.last_update_ms = snapshot.received_at_ms;
                if let Some(mid) = mid_from_orderbook(snapshot) {
                    match snapshot.instrument {
                        InstrumentKind::PolyYes => {
                            state.poly_yes_mid = Some(clamp_probability(mid));
                        }
                        InstrumentKind::PolyNo => {
                            state.poly_yes_mid = Some(clamp_probability(Decimal::ONE - mid));
                        }
                        InstrumentKind::CexPerp => {
                            state.previous_cex_mid = state.cex_mid;
                            state.cex_mid = Some(mid.max(Decimal::ZERO));
                        }
                    }
                }
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::TradeUpdate {
                symbol,
                instrument,
                price,
                timestamp_ms,
                ..
            } => {
                let symbol = symbol.clone();
                let mut state = self.states.remove(&symbol).unwrap_or_default();
                state.last_update_ms = *timestamp_ms;
                match instrument {
                    InstrumentKind::PolyYes => {
                        state.poly_yes_mid = Some(clamp_probability(price.0));
                    }
                    InstrumentKind::PolyNo => {
                        state.poly_yes_mid = Some(clamp_probability(Decimal::ONE - price.0));
                    }
                    InstrumentKind::CexPerp => {
                        state.previous_cex_mid = state.cex_mid;
                        state.cex_mid = Some(price.0.max(Decimal::ZERO));
                    }
                }
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::FundingRate {
                symbol,
                rate,
                next_funding_time_ms,
                ..
            } => {
                let symbol = symbol.clone();
                let mut state = self.states.remove(&symbol).unwrap_or_default();
                state.last_update_ms = *next_funding_time_ms;
                state.funding_rate = rate.to_f64();
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::MarketLifecycle {
                symbol,
                phase,
                timestamp_ms,
            } => {
                let symbol = symbol.clone();
                let mut state = self.states.remove(&symbol).unwrap_or_default();
                state.last_update_ms = *timestamp_ms;
                state.market_phase = phase.clone();
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::ConnectionEvent { .. } => None,
        }
    }

    fn generate_output(&mut self, symbol: &Symbol) -> AlphaEngineOutput {
        let mut state = self.states.remove(symbol).unwrap_or_default();
        let mut output = AlphaEngineOutput::default();

        if !state.has_required_inputs() {
            self.states.insert(symbol.clone(), state);
            return output;
        }

        if state.warmup_samples_seen < self.config.warmup_samples {
            state.warmup_samples_seen += 1;
            self.states.insert(symbol.clone(), state);
            return output;
        }

        if state.market_phase.allows_dmm() {
            if let Some(quote_state) = self.build_dmm_quote(symbol.clone(), &state) {
                output.push_quote_state(quote_state);
                state.has_live_dmm_quote = true;
            }
        } else if state.has_live_dmm_quote {
            output.clear_quote(symbol.clone());
            state.has_live_dmm_quote = false;
        }

        if !state.market_phase.allows_new_positions() {
            if state.active_arb.take().is_some() {
                output
                    .push_arb_signal(self.close_signal(symbol, "market phase blocks new exposure"));
            }
            self.states.insert(symbol.clone(), state);
            return output;
        }

        if let Some(basis) = self.current_basis(&state) {
            let abs_basis = basis.abs();
            let entry = self.config.basis_entry_threshold;
            let exit = self.config.basis_exit_threshold.min(entry);

            if abs_basis >= entry {
                let bias = if basis > 0.0 {
                    ArbBias::ShortPoly
                } else {
                    ArbBias::LongPoly
                };

                if state.active_arb != Some(bias) {
                    if let Some(signal) = self.build_basis_signal(symbol, &state, basis, bias) {
                        output.push_arb_signal(signal);
                        state.active_arb = Some(bias);
                    }
                }
            } else if abs_basis <= exit {
                if state.active_arb.take().is_some() {
                    output.push_arb_signal(
                        self.close_signal(symbol, "basis reverted inside exit band"),
                    );
                }
            }
        }

        self.states.insert(symbol.clone(), state);
        output
    }

    fn build_dmm_quote(&self, symbol: Symbol, state: &SymbolState) -> Option<DmmQuoteState> {
        if self.config.dmm_quote_size.0 <= Decimal::ZERO {
            return None;
        }

        let mid = state.poly_yes_mid?;
        let half = self.config.dmm_half_spread.max(Decimal::ZERO);
        let mut bid = (mid - half).max(Decimal::ZERO);
        let mut ask = (mid + half).min(Decimal::ONE);
        if ask <= bid {
            ask = (bid + Decimal::new(1, 3)).min(Decimal::ONE);
        }
        if ask <= bid {
            bid = Decimal::ZERO;
            ask = Decimal::new(1, 3);
        }

        Some(DmmQuoteState {
            symbol,
            bid: Price(bid),
            ask: Price(ask),
            bid_qty: self.config.dmm_quote_size,
            ask_qty: self.config.dmm_quote_size,
            updated_at_ms: state.last_update_ms,
        })
    }

    fn current_basis(&self, state: &SymbolState) -> Option<f64> {
        let poly = state.poly_yes_mid?.to_f64()?;
        let cex_proxy = self.cex_proxy_probability(state)?;
        Some((poly - cex_proxy).clamp(-1.0, 1.0))
    }

    fn cex_proxy_probability(&self, state: &SymbolState) -> Option<f64> {
        let cex_mid = state.cex_mid?.to_f64()?;
        let mut proxy =
            0.5 + (cex_mid - self.config.cex_anchor_price) / self.config.cex_price_scale.max(1.0);

        if let Some(rate) = state.funding_rate {
            proxy += rate * self.config.funding_rate_weight;
        }

        if let Some(prev) = state.previous_cex_mid.and_then(|value| value.to_f64()) {
            if prev.abs() > f64::EPSILON {
                proxy += ((cex_mid - prev) / prev) * self.config.momentum_weight;
            }
        }

        Some(proxy.clamp(0.0, 1.0))
    }

    fn build_basis_signal(
        &mut self,
        symbol: &Symbol,
        state: &SymbolState,
        basis: f64,
        bias: ArbBias,
    ) -> Option<ArbSignalEvent> {
        let poly_mid = state.poly_yes_mid?;
        if poly_mid <= Decimal::ZERO {
            return None;
        }

        let mut target_shares = self.config.target_shares;
        if let Some(max_position_usd) = self.max_position_usd {
            let cap_shares = max_position_usd.0 / poly_mid;
            if cap_shares < target_shares.0 {
                target_shares = PolyShares(cap_shares.max(Decimal::ZERO));
            }
        }
        if target_shares.0 <= Decimal::ZERO {
            return None;
        }

        let poly_price = Price(poly_mid);
        let poly_target_notional = UsdNotional::from_poly(target_shares, poly_price);
        let delta =
            (self.config.base_delta + basis.abs() * self.config.delta_sensitivity).clamp(0.0, 1.0);
        let cex_hedge_qty = target_shares.to_cex_base_qty(delta);

        let (poly_side, cex_side, action) = match bias {
            ArbBias::LongPoly => {
                let action = ArbSignalAction::BasisLong {
                    poly_side: OrderSide::Buy,
                    poly_target_shares: target_shares,
                    poly_target_notional,
                    cex_side: OrderSide::Sell,
                    cex_hedge_qty,
                    delta,
                };
                (OrderSide::Buy, OrderSide::Sell, action)
            }
            ArbBias::ShortPoly => {
                let action = ArbSignalAction::BasisShort {
                    poly_side: OrderSide::Sell,
                    poly_target_shares: target_shares,
                    poly_target_notional,
                    cex_side: OrderSide::Buy,
                    cex_hedge_qty,
                    delta,
                };
                (OrderSide::Sell, OrderSide::Buy, action)
            }
        };

        let expected_pnl = UsdNotional(
            poly_target_notional.0 * Decimal::from_f64(basis.abs()).unwrap_or(Decimal::ZERO),
        );
        let z = if self.config.basis_entry_threshold.abs() <= f64::EPSILON {
            0.0
        } else {
            basis / self.config.basis_entry_threshold
        };

        Some(ArbSignalEvent {
            signal_id: self.next_signal_id(symbol, poly_side, cex_side),
            symbol: symbol.clone(),
            action,
            strength: self.signal_strength(basis.abs()),
            basis_value: Some(Decimal::from_f64(basis).unwrap_or(Decimal::ZERO)),
            z_score: Some(Decimal::from_f64(z).unwrap_or(Decimal::ZERO)),
            expected_pnl,
            timestamp_ms: state.last_update_ms,
        })
    }

    fn close_signal(&mut self, symbol: &Symbol, reason: &str) -> ArbSignalEvent {
        ArbSignalEvent {
            signal_id: self.next_signal_id(symbol, OrderSide::Sell, OrderSide::Buy),
            symbol: symbol.clone(),
            action: ArbSignalAction::ClosePosition {
                reason: reason.to_owned(),
            },
            strength: SignalStrength::Normal,
            basis_value: None,
            z_score: None,
            expected_pnl: UsdNotional::ZERO,
            timestamp_ms: self
                .states
                .get(symbol)
                .map(|state| state.last_update_ms)
                .unwrap_or_default(),
        }
    }

    fn signal_strength(&self, abs_basis: f64) -> SignalStrength {
        if abs_basis >= self.config.basis_entry_threshold * 2.0 {
            SignalStrength::Strong
        } else if abs_basis >= self.config.basis_entry_threshold * 1.2 {
            SignalStrength::Normal
        } else {
            SignalStrength::Weak
        }
    }

    fn next_signal_id(
        &mut self,
        symbol: &Symbol,
        poly_side: OrderSide,
        cex_side: OrderSide,
    ) -> String {
        self.next_signal_seq += 1;
        format!(
            "sig-{}-{:?}-{:?}-{}",
            symbol.0, poly_side, cex_side, self.next_signal_seq
        )
    }
}

#[async_trait]
impl AlphaEngine for SimpleAlphaEngine {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> AlphaEngineOutput {
        let Some(symbol) = self.apply_market_event(event) else {
            return AlphaEngineOutput::default();
        };

        self.generate_output(&symbol)
    }

    fn update_params(&mut self, params: EngineParams) {
        if let Some(entry) = params.basis_entry_zscore {
            self.config.basis_entry_threshold = entry.abs();
            if self.config.basis_exit_threshold > self.config.basis_entry_threshold {
                self.config.basis_exit_threshold = self.config.basis_entry_threshold * 0.5;
            }
        }

        if let Some(exit) = params.basis_exit_zscore {
            self.config.basis_exit_threshold =
                exit.abs().min(self.config.basis_entry_threshold.max(0.0));
        }

        self.max_position_usd = params.max_position_usd;
    }
}

fn mid_from_orderbook(snapshot: &OrderBookSnapshot) -> Option<Decimal> {
    let best_bid = snapshot.bids.first().map(|level| level.price.0);
    let best_ask = snapshot.asks.first().map(|level| level.price.0);
    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::new(2, 0)),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
        (None, None) => None,
    }
}

fn clamp_probability(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

pub fn crate_status() -> &'static str {
    "polyalpha-engine mvp ready"
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{
        AlphaEngine, CexBaseQty, Exchange, MarketDataEvent, OrderBookSnapshot, PriceLevel,
        VenueQuantity,
    };

    use super::*;

    fn poly_orderbook_event(symbol: &str, bid: Decimal, ask: Decimal, ts: u64) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new(symbol),
                instrument: InstrumentKind::PolyYes,
                bids: vec![PriceLevel {
                    price: Price(bid),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10, 0))),
                }],
                asks: vec![PriceLevel {
                    price: Price(ask),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10, 0))),
                }],
                exchange_timestamp_ms: ts,
                received_at_ms: ts,
                sequence: ts,
            },
        }
    }

    fn cex_orderbook_event(symbol: &str, bid: Decimal, ask: Decimal, ts: u64) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new(symbol),
                instrument: InstrumentKind::CexPerp,
                bids: vec![PriceLevel {
                    price: Price(bid),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
                }],
                asks: vec![PriceLevel {
                    price: Price(ask),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
                }],
                exchange_timestamp_ms: ts,
                received_at_ms: ts,
                sequence: ts,
            },
        }
    }

    fn lifecycle_event(symbol: &str, phase: MarketPhase, ts: u64) -> MarketDataEvent {
        MarketDataEvent::MarketLifecycle {
            symbol: Symbol::new(symbol),
            phase,
            timestamp_ms: ts,
        }
    }

    #[tokio::test]
    async fn warmup_blocks_signal_generation_until_threshold() {
        let mut engine = SimpleAlphaEngine::new(SimpleEngineConfig {
            warmup_samples: 2,
            ..SimpleEngineConfig::default()
        });
        let symbol = "btc-100k-mar-2026";

        let out1 = engine
            .on_market_data(&cex_orderbook_event(
                symbol,
                Decimal::new(100_000, 0),
                Decimal::new(100_010, 0),
                1,
            ))
            .await;
        assert!(out1.is_empty());

        let out2 = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(49, 2),
                Decimal::new(51, 2),
                2,
            ))
            .await;
        assert!(out2.is_empty());

        let out3 = engine
            .on_market_data(&cex_orderbook_event(
                symbol,
                Decimal::new(100_000, 0),
                Decimal::new(100_010, 0),
                3,
            ))
            .await;
        assert!(out3.is_empty());
    }

    #[tokio::test]
    async fn emits_dmm_quote_once_warmup_completed() {
        let mut engine = SimpleAlphaEngine::new(SimpleEngineConfig {
            warmup_samples: 1,
            basis_entry_threshold: 0.2,
            ..SimpleEngineConfig::default()
        });
        let symbol = "btc-100k-mar-2026";

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                symbol,
                Decimal::new(100_000, 0),
                Decimal::new(100_010, 0),
                1,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(49, 2),
                Decimal::new(51, 2),
                2,
            ))
            .await;

        let out = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(499, 3),
                Decimal::new(501, 3),
                3,
            ))
            .await;

        assert_eq!(out.dmm_updates.len(), 1);
        assert!(out.dmm_updates[0].next_state.is_some());
    }

    #[tokio::test]
    async fn emits_arb_signal_when_basis_crosses_entry() {
        let mut engine = SimpleAlphaEngine::new(SimpleEngineConfig {
            warmup_samples: 1,
            basis_entry_threshold: 0.05,
            basis_exit_threshold: 0.02,
            ..SimpleEngineConfig::default()
        });
        let symbol = "btc-100k-mar-2026";

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                symbol,
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                1,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(70, 2),
                Decimal::new(72, 2),
                2,
            ))
            .await;

        let out = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(71, 2),
                Decimal::new(73, 2),
                3,
            ))
            .await;

        assert_eq!(out.arb_signals.len(), 1);
        assert!(matches!(
            out.arb_signals[0].action,
            ArbSignalAction::BasisShort { .. }
        ));
    }

    #[tokio::test]
    async fn lifecycle_close_only_clears_dmm_and_closes_active_arb() {
        let mut engine = SimpleAlphaEngine::new(SimpleEngineConfig {
            warmup_samples: 0,
            basis_entry_threshold: 0.05,
            basis_exit_threshold: 0.02,
            ..SimpleEngineConfig::default()
        });
        let symbol = "btc-100k-mar-2026";

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                symbol,
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                1,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                symbol,
                Decimal::new(72, 2),
                Decimal::new(74, 2),
                2,
            ))
            .await;

        let out = engine
            .on_market_data(&lifecycle_event(
                symbol,
                MarketPhase::CloseOnly {
                    hours_remaining: 1.0,
                },
                3,
            ))
            .await;

        assert_eq!(out.dmm_updates.len(), 1);
        assert!(out.dmm_updates[0].next_state.is_none());
        assert_eq!(out.arb_signals.len(), 1);
        assert!(matches!(
            out.arb_signals[0].action,
            ArbSignalAction::ClosePosition { .. }
        ));
    }
}
