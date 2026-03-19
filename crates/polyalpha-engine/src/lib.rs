use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use libm::erf;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use polyalpha_core::{
    AlphaEngine, AlphaEngineOutput, ArbSignalAction, ArbSignalEvent, DmmQuoteState, EngineParams,
    InstrumentKind, MarketConfig, MarketDataEvent, MarketPhase, MarketRule, MarketRuleKind,
    OrderBookSnapshot, OrderSide, PolyShares, Price, SignalStrength, Symbol, TokenSide,
    UsdNotional,
};

const ONE_MINUTE_MS: u64 = 60_000;
const MIN_MINUTES_TO_EXPIRY: f64 = 1.0;
const MIN_VOLATILITY: f64 = 1e-6;
const DELTA_BUMP_PCT: f64 = 1e-4;
const SQRT_TWO: f64 = std::f64::consts::SQRT_2;

#[derive(Clone, Debug, PartialEq)]
pub struct BasisStrategySnapshot {
    pub token_side: TokenSide,
    pub poly_price: f64,
    pub fair_value: f64,
    pub signal_basis: f64,
    pub z_score: Option<f64>,
    pub delta: f64,
    pub cex_reference_price: f64,
    pub sigma: Option<f64>,
    pub minutes_to_expiry: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct PendingPolyObservation {
    ts_ms: u64,
    minute_bucket_ms: u64,
    price: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct TokenMarketState {
    max_history_len: usize,
    current_mid: Option<Decimal>,
    pending_observation: Option<PendingPolyObservation>,
    last_processed_minute_ms: Option<u64>,
    history: VecDeque<f64>,
    last_snapshot: Option<BasisStrategySnapshot>,
}

impl TokenMarketState {
    fn new(history_capacity: usize) -> Self {
        Self {
            max_history_len: history_capacity.max(2),
            current_mid: None,
            pending_observation: None,
            last_processed_minute_ms: None,
            history: VecDeque::with_capacity(history_capacity.max(2)),
            last_snapshot: None,
        }
    }

    fn update_mid(&mut self, ts_ms: u64, mid: Decimal) {
        self.current_mid = Some(clamp_probability(mid));

        let Some(price) = self.current_mid.and_then(|value| value.to_f64()) else {
            return;
        };
        let minute_bucket_ms = minute_bucket(ts_ms);
        if self.last_processed_minute_ms == Some(minute_bucket_ms) {
            return;
        }

        match self.pending_observation.as_mut() {
            Some(pending) if pending.minute_bucket_ms == minute_bucket_ms => {
                pending.ts_ms = ts_ms;
                pending.price = price;
            }
            _ => {
                self.pending_observation = Some(PendingPolyObservation {
                    ts_ms,
                    minute_bucket_ms,
                    price,
                });
            }
        }
    }

    fn mark_processed(&mut self, minute_bucket_ms: u64) {
        self.last_processed_minute_ms = Some(minute_bucket_ms);
        self.pending_observation = None;
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CexMinuteState {
    current_minute_ms: Option<u64>,
    current_minute_last_close: Option<f64>,
    previous_completed_close: Option<f64>,
    returns_window: VecDeque<f64>,
}

impl CexMinuteState {
    fn new() -> Self {
        Self {
            current_minute_ms: None,
            current_minute_last_close: None,
            previous_completed_close: None,
            returns_window: VecDeque::new(),
        }
    }

    fn ingest_price(&mut self, ts_ms: u64, price: f64, max_returns: usize) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }

        let minute_ms = minute_bucket(ts_ms);
        match self.current_minute_ms {
            None => {
                self.current_minute_ms = Some(minute_ms);
                self.current_minute_last_close = Some(price);
            }
            Some(current_minute_ms) if minute_ms < current_minute_ms => {}
            Some(current_minute_ms) if minute_ms == current_minute_ms => {
                self.current_minute_last_close = Some(price);
            }
            Some(_) => {
                let finalized_close = self.current_minute_last_close.unwrap_or(price);
                if let Some(previous_completed_close) = self.previous_completed_close {
                    if previous_completed_close > 0.0 && finalized_close > 0.0 {
                        self.returns_window
                            .push_back((finalized_close / previous_completed_close).ln());
                        while self.returns_window.len() > max_returns.max(2) {
                            self.returns_window.pop_front();
                        }
                    }
                }
                self.previous_completed_close = Some(finalized_close);
                self.current_minute_ms = Some(minute_ms);
                self.current_minute_last_close = Some(price);
            }
        }
    }

    fn reference_close(&self) -> Option<f64> {
        self.previous_completed_close
    }

    fn sigma(&self) -> Option<f64> {
        if self.returns_window.len() < 2 {
            return None;
        }

        let n = self.returns_window.len() as f64;
        let mean = self.returns_window.iter().sum::<f64>() / n;
        let variance = self
            .returns_window
            .iter()
            .map(|value| {
                let centered = value - mean;
                centered * centered
            })
            .sum::<f64>()
            / (n - 1.0);
        Some(variance.max(0.0).sqrt())
    }
}

#[derive(Clone, Debug, PartialEq)]
struct SymbolState {
    yes: TokenMarketState,
    no: TokenMarketState,
    cex_mid: Option<Decimal>,
    cex_minutes: CexMinuteState,
    market_phase: MarketPhase,
    active_token_side: Option<TokenSide>,
    has_live_dmm_quote: bool,
    last_update_ms: u64,
    last_basis_snapshot: Option<BasisStrategySnapshot>,
}

impl SymbolState {
    fn new(history_capacity: usize) -> Self {
        Self {
            yes: TokenMarketState::new(history_capacity),
            no: TokenMarketState::new(history_capacity),
            cex_mid: None,
            cex_minutes: CexMinuteState::new(),
            market_phase: MarketPhase::Trading,
            active_token_side: None,
            has_live_dmm_quote: false,
            last_update_ms: 0,
            last_basis_snapshot: None,
        }
    }

    fn set_basis_snapshot(&mut self, snapshot: Option<BasisStrategySnapshot>) {
        self.last_basis_snapshot = snapshot;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimpleEngineConfig {
    pub min_signal_samples: usize,
    pub rolling_window_minutes: usize,
    pub entry_z: f64,
    pub exit_z: f64,
    pub position_notional_usd: UsdNotional,
    pub cex_hedge_ratio: f64,
    pub dmm_half_spread: Decimal,
    pub dmm_quote_size: PolyShares,
}

impl Default for SimpleEngineConfig {
    fn default() -> Self {
        Self {
            min_signal_samples: 2,
            rolling_window_minutes: 360,
            entry_z: 2.0,
            exit_z: 0.5,
            position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
            cex_hedge_ratio: 1.0,
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: PolyShares::ZERO,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimpleAlphaEngine {
    config: SimpleEngineConfig,
    markets: HashMap<Symbol, MarketConfig>,
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
        Self::with_markets(config, Vec::new())
    }

    pub fn with_markets(config: SimpleEngineConfig, markets: Vec<MarketConfig>) -> Self {
        Self {
            config,
            markets: markets
                .into_iter()
                .map(|market| (market.symbol.clone(), market))
                .collect(),
            states: HashMap::new(),
            max_position_usd: None,
            next_signal_seq: 0,
        }
    }

    pub fn config(&self) -> &SimpleEngineConfig {
        &self.config
    }

    pub fn replace_markets(&mut self, markets: Vec<MarketConfig>) {
        self.markets = markets
            .into_iter()
            .map(|market| (market.symbol.clone(), market))
            .collect();
    }

    pub fn basis_snapshot(&self, symbol: &Symbol) -> Option<BasisStrategySnapshot> {
        self.states
            .get(symbol)
            .and_then(|state| state.last_basis_snapshot.clone())
    }

    pub fn sync_position_state(&mut self, symbol: &Symbol, token_side: Option<TokenSide>) {
        let default_state = self.state_for_symbol();
        let state = self.states.entry(symbol.clone()).or_insert(default_state);
        state.active_token_side = token_side;
    }

    fn history_capacity(&self) -> usize {
        self.config
            .rolling_window_minutes
            .max(self.config.min_signal_samples)
            .max(2)
    }

    fn state_for_symbol(&self) -> SymbolState {
        SymbolState::new(self.history_capacity())
    }

    fn apply_market_event(&mut self, event: &MarketDataEvent) -> Option<Symbol> {
        match event {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                let symbol = snapshot.symbol.clone();
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol());
                state.last_update_ms = snapshot.received_at_ms;
                if let Some(mid) = mid_from_orderbook(snapshot) {
                    match snapshot.instrument {
                        InstrumentKind::PolyYes => {
                            state.yes.update_mid(snapshot.received_at_ms, mid)
                        }
                        InstrumentKind::PolyNo => state.no.update_mid(snapshot.received_at_ms, mid),
                        InstrumentKind::CexPerp => {
                            state.cex_mid = Some(mid.max(Decimal::ZERO));
                            if let Some(price) = state.cex_mid.and_then(|value| value.to_f64()) {
                                state.cex_minutes.ingest_price(
                                    snapshot.received_at_ms,
                                    price,
                                    self.history_capacity(),
                                );
                            }
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
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol());
                state.last_update_ms = *timestamp_ms;
                match instrument {
                    InstrumentKind::PolyYes => state.yes.update_mid(*timestamp_ms, price.0),
                    InstrumentKind::PolyNo => state.no.update_mid(*timestamp_ms, price.0),
                    InstrumentKind::CexPerp => {
                        state.cex_mid = Some(price.0.max(Decimal::ZERO));
                        if let Some(value) = state.cex_mid.and_then(|item| item.to_f64()) {
                            state.cex_minutes.ingest_price(
                                *timestamp_ms,
                                value,
                                self.history_capacity(),
                            );
                        }
                    }
                }
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::FundingRate {
                symbol,
                next_funding_time_ms,
                ..
            } => {
                let symbol = symbol.clone();
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol());
                state.last_update_ms = *next_funding_time_ms;
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::MarketLifecycle {
                symbol,
                phase,
                timestamp_ms,
            } => {
                let symbol = symbol.clone();
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol());
                state.last_update_ms = *timestamp_ms;
                state.market_phase = phase.clone();
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::ConnectionEvent { .. } => None,
        }
    }

    fn generate_output(
        &mut self,
        symbol: &Symbol,
        flush_pending: bool,
        force_phase_check: bool,
    ) -> AlphaEngineOutput {
        let mut state = self
            .states
            .remove(symbol)
            .unwrap_or_else(|| self.state_for_symbol());
        let mut output = AlphaEngineOutput::default();

        self.update_dmm_state(symbol, &mut state, &mut output);

        if force_phase_check && !state.market_phase.allows_new_positions() {
            if state.active_token_side.take().is_some() {
                output.push_arb_signal(self.close_signal_at(
                    symbol,
                    "market phase blocks new exposure",
                    state.last_update_ms,
                ));
            }
            self.states.insert(symbol.clone(), state);
            return output;
        }

        if flush_pending {
            self.flush_pending_observations(symbol, &mut state, &mut output);
        }

        self.states.insert(symbol.clone(), state);
        output
    }

    fn update_dmm_state(
        &self,
        symbol: &Symbol,
        state: &mut SymbolState,
        output: &mut AlphaEngineOutput,
    ) {
        if !state.market_phase.allows_dmm() || self.config.dmm_quote_size.0 <= Decimal::ZERO {
            if state.has_live_dmm_quote {
                output.clear_quote(symbol.clone());
                state.has_live_dmm_quote = false;
            }
            return;
        }

        let Some(mid) = state.yes.current_mid else {
            return;
        };
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

        output.push_quote_state(DmmQuoteState {
            symbol: symbol.clone(),
            bid: Price(bid),
            ask: Price(ask),
            bid_qty: self.config.dmm_quote_size,
            ask_qty: self.config.dmm_quote_size,
            updated_at_ms: state.last_update_ms,
        });
        state.has_live_dmm_quote = true;
    }

    fn flush_pending_observations(
        &mut self,
        symbol: &Symbol,
        state: &mut SymbolState,
        output: &mut AlphaEngineOutput,
    ) {
        let Some(market) = self.markets.get(symbol).cloned() else {
            state.yes.pending_observation = None;
            state.no.pending_observation = None;
            state.set_basis_snapshot(None);
            return;
        };
        let Some(rule) = market.resolved_market_rule() else {
            state.yes.pending_observation = None;
            state.no.pending_observation = None;
            state.set_basis_snapshot(None);
            return;
        };
        let Some(cex_reference_price) = state.cex_minutes.reference_close() else {
            if let Some(pending) = state.yes.pending_observation.take() {
                state.yes.mark_processed(pending.minute_bucket_ms);
            }
            if let Some(pending) = state.no.pending_observation.take() {
                state.no.mark_processed(pending.minute_bucket_ms);
            }
            state.set_basis_snapshot(None);
            return;
        };

        let sigma = state.cex_minutes.sigma();
        let yes_evaluation = self.evaluate_token_pending(
            &rule,
            &market,
            TokenSide::Yes,
            &state.yes,
            cex_reference_price,
            sigma,
        );
        let no_evaluation = self.evaluate_token_pending(
            &rule,
            &market,
            TokenSide::No,
            &state.no,
            cex_reference_price,
            sigma,
        );

        let preferred_snapshot = match state.active_token_side {
            Some(token_side) => match token_side {
                TokenSide::Yes => yes_evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.snapshot.clone())
                    .or_else(|| state.yes.last_snapshot.clone()),
                TokenSide::No => no_evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.snapshot.clone())
                    .or_else(|| state.no.last_snapshot.clone()),
            },
            None => choose_preferred_snapshot([
                yes_evaluation
                    .as_ref()
                    .and_then(|item| item.snapshot.clone()),
                no_evaluation
                    .as_ref()
                    .and_then(|item| item.snapshot.clone()),
            ]),
        };
        state.set_basis_snapshot(preferred_snapshot.clone());

        if !state.market_phase.allows_new_positions() {
            if state.active_token_side.take().is_some() {
                output.push_arb_signal(self.close_signal_at(
                    symbol,
                    "market phase blocks new exposure",
                    state.last_update_ms,
                ));
            }
        } else if let Some(active_side) = state.active_token_side {
            let maybe_snapshot = match active_side {
                TokenSide::Yes => yes_evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.snapshot.as_ref()),
                TokenSide::No => no_evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.snapshot.as_ref()),
            };

            if let Some(snapshot) = maybe_snapshot {
                if let Some(z_score) = snapshot.z_score {
                    if z_score.abs() <= self.config.exit_z || z_score >= self.config.entry_z {
                        state.active_token_side = None;
                        output.push_arb_signal(self.close_signal_at(
                            symbol,
                            "signal basis reverted inside exit band",
                            state.last_update_ms,
                        ));
                    }
                }
            }
        } else if let Some(snapshot) = choose_entry_snapshot(
            [
                yes_evaluation
                    .as_ref()
                    .and_then(|item| item.snapshot.clone()),
                no_evaluation
                    .as_ref()
                    .and_then(|item| item.snapshot.clone()),
            ],
            self.config.entry_z,
        ) {
            if let Some(signal) = self.build_basis_signal(symbol, &snapshot) {
                state.active_token_side = Some(snapshot.token_side);
                output.push_arb_signal(signal);
            }
        }

        if let Some(evaluation) = yes_evaluation {
            apply_token_evaluation(&mut state.yes, evaluation);
        }
        if let Some(evaluation) = no_evaluation {
            apply_token_evaluation(&mut state.no, evaluation);
        }
    }

    fn evaluate_token_pending(
        &self,
        rule: &MarketRule,
        market: &MarketConfig,
        token_side: TokenSide,
        token_state: &TokenMarketState,
        cex_reference_price: f64,
        sigma: Option<f64>,
    ) -> Option<EvaluatedToken> {
        let pending = token_state.pending_observation.clone()?;
        let settlement_ts_ms = market.settlement_timestamp.saturating_mul(1000);
        let minutes_to_expiry =
            (settlement_ts_ms.saturating_sub(pending.ts_ms) as f64) / ONE_MINUTE_MS as f64;
        if pending.price <= 0.0 || cex_reference_price <= 0.0 {
            return Some(EvaluatedToken {
                minute_bucket_ms: pending.minute_bucket_ms,
                snapshot: None,
            });
        }

        let fair_value = token_fair_value(
            rule,
            token_side,
            cex_reference_price,
            sigma,
            minutes_to_expiry,
        );
        let delta = token_delta(
            rule,
            token_side,
            cex_reference_price,
            sigma,
            minutes_to_expiry,
        );
        let signal_basis = pending.price - fair_value;
        let z_score = rolling_zscore(
            &token_state.history,
            signal_basis,
            self.config.min_signal_samples,
        );

        Some(EvaluatedToken {
            minute_bucket_ms: pending.minute_bucket_ms,
            snapshot: Some(BasisStrategySnapshot {
                token_side,
                poly_price: pending.price,
                fair_value,
                signal_basis,
                z_score,
                delta,
                cex_reference_price,
                sigma,
                minutes_to_expiry,
            }),
        })
    }

    fn build_basis_signal(
        &mut self,
        symbol: &Symbol,
        snapshot: &BasisStrategySnapshot,
    ) -> Option<ArbSignalEvent> {
        let poly_price = Decimal::from_f64(snapshot.poly_price)?;
        if poly_price <= Decimal::ZERO {
            return None;
        }

        let mut target_notional = self.config.position_notional_usd;
        if let Some(max_position_usd) = self.max_position_usd {
            target_notional = UsdNotional(target_notional.0.min(max_position_usd.0));
        }
        if target_notional.0 <= Decimal::ZERO {
            return None;
        }

        let target_shares = PolyShares(target_notional.0 / poly_price);
        if target_shares.0 <= Decimal::ZERO {
            return None;
        }

        let hedge_qty_signed = -(target_shares.0.to_f64().unwrap_or(0.0))
            * snapshot.delta
            * self.config.cex_hedge_ratio.max(0.0);
        let cex_side = if hedge_qty_signed >= 0.0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let cex_hedge_qty = polyalpha_core::CexBaseQty(
            Decimal::from_f64(hedge_qty_signed.abs()).unwrap_or_default(),
        );
        let poly_target_notional = UsdNotional::from_poly(target_shares, Price(poly_price));
        let expected_pnl = UsdNotional(
            poly_target_notional.0
                * Decimal::from_f64(snapshot.signal_basis.abs()).unwrap_or(Decimal::ZERO),
        );

        Some(ArbSignalEvent {
            signal_id: self.next_signal_id(symbol, snapshot.token_side),
            symbol: symbol.clone(),
            action: ArbSignalAction::BasisLong {
                token_side: snapshot.token_side,
                poly_side: OrderSide::Buy,
                poly_target_shares: target_shares,
                poly_target_notional,
                cex_side,
                cex_hedge_qty,
                delta: snapshot.delta,
            },
            strength: self.signal_strength(snapshot.z_score),
            basis_value: Some(Decimal::from_f64(snapshot.signal_basis).unwrap_or(Decimal::ZERO)),
            z_score: snapshot
                .z_score
                .and_then(Decimal::from_f64)
                .or(Some(Decimal::ZERO)),
            expected_pnl,
            timestamp_ms: self
                .states
                .get(symbol)
                .map(|state| state.last_update_ms)
                .unwrap_or_default(),
        })
    }

    fn close_signal_at(
        &mut self,
        symbol: &Symbol,
        reason: &str,
        timestamp_ms: u64,
    ) -> ArbSignalEvent {
        ArbSignalEvent {
            signal_id: self.next_signal_id(symbol, TokenSide::Yes),
            symbol: symbol.clone(),
            action: ArbSignalAction::ClosePosition {
                reason: reason.to_owned(),
            },
            strength: SignalStrength::Normal,
            basis_value: None,
            z_score: None,
            expected_pnl: UsdNotional::ZERO,
            timestamp_ms,
        }
    }

    fn signal_strength(&self, z_score: Option<f64>) -> SignalStrength {
        let abs_z = z_score.unwrap_or_default().abs();
        if abs_z >= self.config.entry_z + 1.0 {
            SignalStrength::Strong
        } else if abs_z >= self.config.entry_z {
            SignalStrength::Normal
        } else {
            SignalStrength::Weak
        }
    }

    fn next_signal_id(&mut self, symbol: &Symbol, token_side: TokenSide) -> String {
        self.next_signal_seq += 1;
        format!("sig-{}-{:?}-{}", symbol.0, token_side, self.next_signal_seq)
    }
}

#[async_trait]
impl AlphaEngine for SimpleAlphaEngine {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> AlphaEngineOutput {
        let Some(symbol) = self.apply_market_event(event) else {
            return AlphaEngineOutput::default();
        };

        let flush_pending = matches!(
            event,
            MarketDataEvent::OrderBookUpdate { snapshot }
                if matches!(snapshot.instrument, InstrumentKind::CexPerp)
        ) || matches!(
            event,
            MarketDataEvent::TradeUpdate { instrument, .. }
                if matches!(instrument, InstrumentKind::CexPerp)
        );
        let force_phase_check = matches!(event, MarketDataEvent::MarketLifecycle { .. });

        self.generate_output(&symbol, flush_pending, force_phase_check)
    }

    fn update_params(&mut self, params: EngineParams) {
        if let Some(entry) = params.basis_entry_zscore {
            self.config.entry_z = entry.abs().max(0.0);
            if self.config.exit_z > self.config.entry_z {
                self.config.exit_z = self.config.entry_z * 0.5;
            }
        }

        if let Some(exit) = params.basis_exit_zscore {
            self.config.exit_z = exit.abs().min(self.config.entry_z.max(0.0));
        }

        self.max_position_usd = params.max_position_usd;
    }
}

#[derive(Clone, Debug, PartialEq)]
struct EvaluatedToken {
    minute_bucket_ms: u64,
    snapshot: Option<BasisStrategySnapshot>,
}

fn apply_token_evaluation(token_state: &mut TokenMarketState, evaluation: EvaluatedToken) {
    if let Some(snapshot) = evaluation.snapshot {
        token_state.history.push_back(snapshot.signal_basis);
        while token_state.history.len() > token_state.max_history_len {
            token_state.history.pop_front();
        }
        token_state.last_snapshot = Some(snapshot);
    }
    token_state.mark_processed(evaluation.minute_bucket_ms);
}

fn choose_entry_snapshot(
    candidates: [Option<BasisStrategySnapshot>; 2],
    entry_z: f64,
) -> Option<BasisStrategySnapshot> {
    let mut eligible = candidates
        .into_iter()
        .flatten()
        .filter(|snapshot| {
            snapshot.minutes_to_expiry > MIN_MINUTES_TO_EXPIRY
                && snapshot.poly_price > 0.0
                && snapshot.cex_reference_price > 0.0
                && snapshot
                    .z_score
                    .map(|z_score| z_score <= -entry_z)
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        let left_z = left.z_score.unwrap_or(f64::INFINITY);
        let right_z = right.z_score.unwrap_or(f64::INFINITY);
        left_z
            .partial_cmp(&right_z)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .signal_basis
                    .abs()
                    .partial_cmp(&left.signal_basis.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    eligible.into_iter().next()
}

fn choose_preferred_snapshot(
    candidates: [Option<BasisStrategySnapshot>; 2],
) -> Option<BasisStrategySnapshot> {
    let mut snapshots = candidates.into_iter().flatten().collect::<Vec<_>>();
    snapshots.sort_by(|left, right| {
        let left_rank = left.z_score.unwrap_or(left.signal_basis);
        let right_rank = right.z_score.unwrap_or(right.signal_basis);
        left_rank
            .partial_cmp(&right_rank)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .signal_basis
                    .abs()
                    .partial_cmp(&left.signal_basis.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    snapshots.into_iter().next()
}

fn minute_bucket(ts_ms: u64) -> u64 {
    (ts_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS
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

fn normal_cdf(value: f64) -> f64 {
    0.5 * (1.0 + erf(value / SQRT_TWO))
}

fn token_fair_value(
    rule: &MarketRule,
    token_side: TokenSide,
    spot_price: f64,
    sigma: Option<f64>,
    minutes_to_expiry: f64,
) -> f64 {
    let yes_probability = yes_probability_from_rule(rule, spot_price, sigma, minutes_to_expiry);
    match token_side {
        TokenSide::Yes => yes_probability.clamp(0.0, 1.0),
        TokenSide::No => (1.0 - yes_probability).clamp(0.0, 1.0),
    }
}

fn token_delta(
    rule: &MarketRule,
    token_side: TokenSide,
    spot_price: f64,
    sigma: Option<f64>,
    minutes_to_expiry: f64,
) -> f64 {
    let bump = (spot_price.abs() * DELTA_BUMP_PCT).max(1.0);
    let lower_spot = (spot_price - bump).max(1e-9);
    let upper_spot = spot_price + bump;
    let down = token_fair_value(rule, token_side, lower_spot, sigma, minutes_to_expiry);
    let up = token_fair_value(rule, token_side, upper_spot, sigma, minutes_to_expiry);
    (up - down) / (upper_spot - lower_spot)
}

fn yes_probability_from_rule(
    rule: &MarketRule,
    spot_price: f64,
    sigma: Option<f64>,
    minutes_to_expiry: f64,
) -> f64 {
    let spot = spot_price.max(1e-9);
    if sigma.unwrap_or_default() < MIN_VOLATILITY || minutes_to_expiry <= MIN_MINUTES_TO_EXPIRY {
        return realized_yes_payout(rule, spot);
    }

    let sigma = sigma.unwrap_or_default();
    let variance = (sigma * sigma * minutes_to_expiry).max(MIN_VOLATILITY * MIN_VOLATILITY);
    let stdev = variance.sqrt();
    let mu = spot.ln() - 0.5 * variance;

    let above_probability = |strike: f64| -> f64 {
        let z_value = ((strike.max(1e-9)).ln() - mu) / stdev;
        1.0 - normal_cdf(z_value)
    };

    match rule.kind {
        MarketRuleKind::Above => {
            let strike = rule.lower_strike.map(Price::to_f64).unwrap_or_default();
            above_probability(strike)
        }
        MarketRuleKind::Below => {
            let strike = rule.upper_strike.map(Price::to_f64).unwrap_or_default();
            1.0 - above_probability(strike)
        }
        MarketRuleKind::Between => {
            let lower = rule.lower_strike.map(Price::to_f64).unwrap_or_default();
            let upper = rule.upper_strike.map(Price::to_f64).unwrap_or_default();
            (above_probability(lower) - above_probability(upper)).clamp(0.0, 1.0)
        }
    }
}

fn realized_yes_payout(rule: &MarketRule, terminal_price: f64) -> f64 {
    match rule.kind {
        MarketRuleKind::Above => {
            if terminal_price > rule.lower_strike.map(Price::to_f64).unwrap_or_default() {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Below => {
            if terminal_price < rule.upper_strike.map(Price::to_f64).unwrap_or_default() {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Between => {
            let lower = rule.lower_strike.map(Price::to_f64).unwrap_or_default();
            let upper = rule.upper_strike.map(Price::to_f64).unwrap_or_default();
            if terminal_price >= lower && terminal_price < upper {
                1.0
            } else {
                0.0
            }
        }
    }
}

fn rolling_zscore(values: &VecDeque<f64>, latest: f64, min_signal_samples: usize) -> Option<f64> {
    let n = values.len();
    if n < min_signal_samples.max(2) {
        return None;
    }

    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = values
        .iter()
        .map(|value| {
            let centered = value - mean;
            centered * centered
        })
        .sum::<f64>()
        / n as f64;
    let std = variance.max(0.0).sqrt();
    if std <= 1e-12 {
        return None;
    }
    Some((latest - mean) / std)
}

pub fn crate_status() -> &'static str {
    "polyalpha-engine price-only basis parity ready"
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{
        AlphaEngine, CexBaseQty, Exchange, MarketDataEvent, PolymarketIds, PriceLevel,
        VenueQuantity,
    };

    use super::*;

    fn sample_market() -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new("btc-price-only"),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some(
                "Will the price of Bitcoin be above $99,000 on March 31, 2026?".to_owned(),
            ),
            market_rule: Some(MarketRule {
                kind: MarketRuleKind::Above,
                lower_strike: Some(Price(Decimal::new(99_000, 0))),
                upper_strike: None,
            }),
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(99_000, 0))),
            settlement_timestamp: 1_775_001_600,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn poly_orderbook_event(
        instrument: InstrumentKind,
        bid: Decimal,
        ask: Decimal,
        ts: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new("btc-price-only"),
                instrument,
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

    fn cex_orderbook_event(bid: Decimal, ask: Decimal, ts: u64) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-price-only"),
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

    fn lifecycle_event(phase: MarketPhase, ts: u64) -> MarketDataEvent {
        MarketDataEvent::MarketLifecycle {
            symbol: Symbol::new("btc-price-only"),
            phase,
            timestamp_ms: ts,
        }
    }

    fn test_engine() -> SimpleAlphaEngine {
        SimpleAlphaEngine::with_markets(
            SimpleEngineConfig {
                min_signal_samples: 2,
                rolling_window_minutes: 4,
                entry_z: 2.0,
                exit_z: 0.5,
                position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
                cex_hedge_ratio: 1.0,
                dmm_half_spread: Decimal::new(1, 2),
                dmm_quote_size: PolyShares::ZERO,
            },
            vec![sample_market()],
        )
    }

    #[tokio::test]
    async fn uses_previous_completed_cex_close_for_snapshot() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(52, 2),
                Decimal::new(54, 2),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(101_000, 0),
                Decimal::new(101_000, 0),
                60_200,
            ))
            .await;

        let snapshot = engine
            .basis_snapshot(&Symbol::new("btc-price-only"))
            .expect("snapshot should exist");
        assert_eq!(snapshot.cex_reference_price, 100_000.0);
    }

    #[tokio::test]
    async fn emits_basis_long_yes_when_yes_token_is_more_underpriced() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(51, 2),
                Decimal::new(53, 2),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyNo,
                Decimal::new(47, 2),
                Decimal::new(49, 2),
                60_120,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_100, 0),
                Decimal::new(100_100, 0),
                60_200,
            ))
            .await;

        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(50, 2),
                Decimal::new(52, 2),
                120_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyNo,
                Decimal::new(48, 2),
                Decimal::new(50, 2),
                120_120,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_200, 0),
                Decimal::new(100_200, 0),
                120_200,
            ))
            .await;

        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(35, 2),
                Decimal::new(37, 2),
                180_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyNo,
                Decimal::new(63, 2),
                Decimal::new(65, 2),
                180_120,
            ))
            .await;
        let out = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_300, 0),
                Decimal::new(100_300, 0),
                180_200,
            ))
            .await;

        assert_eq!(out.arb_signals.len(), 1);
        match &out.arb_signals[0].action {
            ArbSignalAction::BasisLong { token_side, .. } => {
                assert_eq!(*token_side, TokenSide::Yes);
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[tokio::test]
    async fn close_only_phase_closes_active_position() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

        for (offset, yes_bid, yes_ask, no_bid, no_ask, cex_price) in [
            (60_100, 51, 53, 47, 49, 100_100),
            (120_100, 50, 52, 48, 50, 100_200),
        ] {
            let _ = engine
                .on_market_data(&poly_orderbook_event(
                    InstrumentKind::PolyYes,
                    Decimal::new(yes_bid, 2),
                    Decimal::new(yes_ask, 2),
                    offset,
                ))
                .await;
            let _ = engine
                .on_market_data(&poly_orderbook_event(
                    InstrumentKind::PolyNo,
                    Decimal::new(no_bid, 2),
                    Decimal::new(no_ask, 2),
                    offset + 20,
                ))
                .await;
            let _ = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(cex_price, 0),
                    Decimal::new(cex_price, 0),
                    offset + 100,
                ))
                .await;
        }

        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(35, 2),
                Decimal::new(37, 2),
                180_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyNo,
                Decimal::new(63, 2),
                Decimal::new(65, 2),
                180_120,
            ))
            .await;
        let open = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_300, 0),
                Decimal::new(100_300, 0),
                180_200,
            ))
            .await;
        assert_eq!(open.arb_signals.len(), 1);

        let close = engine
            .on_market_data(&lifecycle_event(
                MarketPhase::CloseOnly {
                    hours_remaining: 0.5,
                },
                180_300,
            ))
            .await;
        assert_eq!(close.arb_signals.len(), 1);
        assert!(matches!(
            close.arb_signals[0].action,
            ArbSignalAction::ClosePosition { .. }
        ));
    }
}
