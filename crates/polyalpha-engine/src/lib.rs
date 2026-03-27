use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use libm::erf;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use polyalpha_core::{
    AlphaEngine, AlphaEngineOutput, ArbSignalAction, ArbSignalEvent, ConnectionStatus,
    DmmQuoteState, EngineParams, EngineWarning, Exchange, InstrumentKind, MarketConfig,
    MarketDataEvent, MarketPhase, MarketRule, MarketRuleKind, OrderBookSnapshot, OrderSide,
    PolyShares, Price, SignalStrength, Symbol, TokenSide, UsdNotional,
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
    previous_completed_minute_ms: Option<u64>,
    returns_window: VecDeque<f64>,
}

impl CexMinuteState {
    fn new() -> Self {
        Self {
            current_minute_ms: None,
            current_minute_last_close: None,
            previous_completed_close: None,
            previous_completed_minute_ms: None,
            returns_window: VecDeque::new(),
        }
    }

    fn ingest_price(&mut self, ts_ms: u64, price: f64, max_returns: usize) -> Result<(), String> {
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("Invalid price: {}", price));
        }

        let minute_ms = minute_bucket(ts_ms);
        match self.current_minute_ms {
            None => {
                // First data point only seeds the in-progress minute.
                // The first completed close becomes available on the next minute rollover,
                // which keeps reference closes and sigma causal.
                self.current_minute_ms = Some(minute_ms);
                self.current_minute_last_close = Some(price);
                Ok(())
            }
            Some(current_minute_ms) if minute_ms < current_minute_ms => Ok(()),
            Some(current_minute_ms) if minute_ms == current_minute_ms => {
                self.current_minute_last_close = Some(price);
                Ok(())
            }
            Some(current_minute_ms) => {
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
                // If the feed resumes after skipping multiple whole minutes, carry the finalized
                // close forward to the latest completed minute. This avoids reporting an
                // artificially large minute skew immediately after reconnect while still keeping
                // the reference close causal.
                self.previous_completed_minute_ms = Some(
                    if minute_ms > current_minute_ms.saturating_add(ONE_MINUTE_MS) {
                        minute_ms.saturating_sub(ONE_MINUTE_MS)
                    } else {
                        current_minute_ms
                    },
                );
                self.current_minute_ms = Some(minute_ms);
                self.current_minute_last_close = Some(price);
                Ok(())
            }
        }
    }

    fn reference_close_with_time(&self) -> Option<(f64, u64)> {
        Some((
            self.previous_completed_close?,
            self.previous_completed_minute_ms?,
        ))
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
    poly_connected: bool,
    cex_connected: bool,
    last_poly_update_ms: u64,
    last_cex_update_ms: u64,
    last_basis_snapshot: Option<BasisStrategySnapshot>,
}

impl SymbolState {
    fn new(history_capacity: usize, poly_connected: bool, cex_connected: bool) -> Self {
        Self {
            yes: TokenMarketState::new(history_capacity),
            no: TokenMarketState::new(history_capacity),
            cex_mid: None,
            cex_minutes: CexMinuteState::new(),
            market_phase: MarketPhase::Trading,
            active_token_side: None,
            has_live_dmm_quote: false,
            last_update_ms: 0,
            poly_connected,
            cex_connected,
            last_poly_update_ms: 0,
            last_cex_update_ms: 0,
            last_basis_snapshot: None,
        }
    }

    fn set_basis_snapshot(&mut self, snapshot: Option<BasisStrategySnapshot>) {
        self.last_basis_snapshot = snapshot;
    }

    fn mark_poly_update(&mut self, ts_ms: u64) {
        self.poly_connected = true;
        self.last_poly_update_ms = ts_ms;
    }

    fn mark_cex_update(&mut self, ts_ms: u64) {
        self.cex_connected = true;
        self.last_cex_update_ms = ts_ms;
    }

    fn has_pending_observations(&self) -> bool {
        self.yes.pending_observation.is_some() || self.no.pending_observation.is_some()
    }

    fn discard_pending_observations(&mut self) {
        if let Some(pending) = self.yes.pending_observation.take() {
            self.yes.mark_processed(pending.minute_bucket_ms);
        }
        if let Some(pending) = self.no.pending_observation.take() {
            self.no.mark_processed(pending.minute_bucket_ms);
        }
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
    pub enable_freshness_check: bool,
    pub reject_on_disconnect: bool,
    pub max_poly_data_age_ms: u64,
    pub max_cex_data_age_ms: u64,
    pub max_time_diff_ms: u64,
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
            enable_freshness_check: true,
            reject_on_disconnect: true,
            max_poly_data_age_ms: ONE_MINUTE_MS,
            max_cex_data_age_ms: ONE_MINUTE_MS,
            max_time_diff_ms: ONE_MINUTE_MS,
        }
    }
}

/// Per-market parameter overrides for strategy configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MarketOverrideConfig {
    pub entry_z: Option<f64>,
    pub exit_z: Option<f64>,
    pub rolling_window_minutes: Option<usize>,
    pub min_warmup_samples: Option<usize>,
    pub min_basis_bps: Option<f64>,
    pub position_notional_usd: Option<UsdNotional>,
}

#[derive(Clone, Debug)]
pub struct SimpleAlphaEngine {
    config: SimpleEngineConfig,
    markets: HashMap<Symbol, MarketConfig>,
    market_overrides: HashMap<Symbol, MarketOverrideConfig>,
    states: HashMap<Symbol, SymbolState>,
    poly_connected: bool,
    cex_connections: HashMap<Exchange, bool>,
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
            market_overrides: HashMap::new(),
            states: HashMap::new(),
            poly_connected: true,
            cex_connections: HashMap::new(),
            max_position_usd: None,
            next_signal_seq: 0,
        }
    }

    pub fn config(&self) -> &SimpleEngineConfig {
        &self.config
    }

    /// Set per-market parameter overrides
    pub fn set_market_overrides(&mut self, overrides: HashMap<Symbol, MarketOverrideConfig>) {
        self.market_overrides = overrides;
    }

    /// Get entry Z-score threshold for a specific market (with override support)
    fn get_entry_z(&self, symbol: &Symbol) -> f64 {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.entry_z)
            .unwrap_or(self.config.entry_z)
    }

    /// Get exit Z-score threshold for a specific market (with override support)
    fn get_exit_z(&self, symbol: &Symbol) -> f64 {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.exit_z)
            .unwrap_or(self.config.exit_z)
    }

    /// Get rolling window minutes for a specific market (with override support)
    pub fn get_rolling_window_minutes(&self, symbol: &Symbol) -> usize {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.rolling_window_minutes)
            .unwrap_or(self.config.rolling_window_minutes)
    }

    /// Get minimum warmup samples for a specific market (with override support)
    pub fn get_min_warmup_samples(&self, symbol: &Symbol) -> usize {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.min_warmup_samples)
            .unwrap_or(self.config.min_signal_samples)
    }

    /// Get position notional USD for a specific market (with override support)
    pub fn get_position_notional_usd(&self, symbol: &Symbol) -> UsdNotional {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.position_notional_usd)
            .unwrap_or(self.config.position_notional_usd)
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

    /// Manually flush pending observations for a specific symbol.
    /// This is useful when you want to process pending data even without a CEX event.
    pub fn flush_symbol(&mut self, symbol: &Symbol) -> AlphaEngineOutput {
        self.generate_output(symbol, true, false)
    }

    pub fn set_poly_connected(&mut self, _symbol: &Symbol, connected: bool) {
        self.poly_connected = connected;
        for state in self.states.values_mut() {
            state.poly_connected = connected;
        }
    }

    pub fn set_cex_connected(&mut self, symbol: &Symbol, connected: bool) {
        let exchange = self.markets.get(symbol).map(|market| market.hedge_exchange);
        if let Some(exchange) = exchange {
            self.cex_connections.insert(exchange, connected);
            for (candidate_symbol, state) in self.states.iter_mut() {
                let matches_exchange = self
                    .markets
                    .get(candidate_symbol)
                    .map(|market| market.hedge_exchange == exchange)
                    .unwrap_or(false);
                if matches_exchange {
                    state.cex_connected = connected;
                }
            }
        } else if let Some(state) = self.states.get_mut(symbol) {
            state.cex_connected = connected;
        }
    }

    pub fn sync_position_state(&mut self, symbol: &Symbol, token_side: Option<TokenSide>) {
        let default_state = self.state_for_symbol(symbol);
        let state = self.states.entry(symbol.clone()).or_insert(default_state);
        state.active_token_side = token_side;
    }

    /// Warmup engine with historical CEX kline data
    /// This pre-populates the returns_window to reduce warmup time
    pub fn warmup_cex_prices(&mut self, symbol: &Symbol, klines: &[(u64, f64)]) {
        let capacity = self.history_capacity();
        let default_state = self.state_for_symbol(symbol);
        let state = self.states.entry(symbol.clone()).or_insert(default_state);

        // Ingest klines in chronological order
        for (ts_ms, close) in klines {
            let _ = state.cex_minutes.ingest_price(*ts_ms, *close, capacity);
        }

        // After ingesting, the last close is in current_minute_last_close.
        // We need to finalize it to previous_completed_close for reference_close() to work.
        if let Some(last_close) = state.cex_minutes.current_minute_last_close {
            state.cex_minutes.previous_completed_close = Some(last_close);
            state.cex_minutes.previous_completed_minute_ms = state.cex_minutes.current_minute_ms;
        }

        // Set last known price as current mid
        if let Some((ts_ms, last_close)) = klines.last() {
            state.cex_mid = Some(Decimal::from_f64(*last_close).unwrap_or(Decimal::ZERO));
            state.mark_cex_update(*ts_ms);
        }
    }

    /// Warmup engine with historical Polymarket prices and corresponding CEX prices
    /// This pre-populates the basis history to reduce warmup time
    /// cex_prices: map from timestamp_ms -> close price (from klines)
    /// poly_prices: list of (timestamp_ms, price) from price history
    pub fn warmup_poly_prices(
        &mut self,
        symbol: &Symbol,
        token_side: TokenSide,
        market: &MarketConfig,
        cex_prices: &std::collections::HashMap<u64, f64>,
        poly_prices: &[(u64, f64)],
    ) {
        let capacity = self.history_capacity();
        let default_state = self.state_for_symbol(symbol);
        let state = self.states.entry(symbol.clone()).or_insert(default_state);

        // Get the market rule
        let Some(rule) = market.resolved_market_rule() else {
            return;
        };

        // Calculate sigma from CEX returns if available
        let sigma = if state.cex_minutes.returns_window.len() >= 30 {
            let returns: Vec<f64> = state.cex_minutes.returns_window.iter().copied().collect();
            calculate_sigma(&returns)
        } else {
            // Default annualized volatility ~50%
            Some(0.5 / (252.0_f64).sqrt() / (1440.0_f64).sqrt())
        };

        let settlement_ts_ms = market.settlement_timestamp.saturating_mul(1000);
        // Get sorted CEX timestamps for finding closest match
        let mut cex_ts_list: Vec<u64> = cex_prices.keys().copied().collect();
        cex_ts_list.sort();

        // Process poly prices in chronological order
        for (ts_ms, poly_price) in poly_prices {
            // Find the closest CEX price within 5 minutes
            let cex_price = Self::find_closest_cex_price(*ts_ms, &cex_ts_list, cex_prices);

            if let Some(cex_price) = cex_price {
                let minutes_to_expiry =
                    (settlement_ts_ms.saturating_sub(*ts_ms) as f64) / ONE_MINUTE_MS as f64;

                let fair_value =
                    token_fair_value(&rule, token_side, cex_price, sigma, minutes_to_expiry);
                let signal_basis = poly_price - fair_value;

                // Add to appropriate token history
                match token_side {
                    TokenSide::Yes => {
                        state.yes.history.push_back(signal_basis);
                        while state.yes.history.len() > capacity {
                            state.yes.history.pop_front();
                        }
                        state.yes.current_mid =
                            Some(Decimal::from_f64(*poly_price).unwrap_or(Decimal::ZERO));
                    }
                    TokenSide::No => {
                        state.no.history.push_back(signal_basis);
                        while state.no.history.len() > capacity {
                            state.no.history.pop_front();
                        }
                        state.no.current_mid =
                            Some(Decimal::from_f64(*poly_price).unwrap_or(Decimal::ZERO));
                    }
                }
            }
        }

        // Update last_processed_minute_ms to prevent reprocessing
        if let Some((last_ts, _)) = poly_prices.last() {
            let last_minute = (*last_ts / ONE_MINUTE_MS) * ONE_MINUTE_MS;
            match token_side {
                TokenSide::Yes => state.yes.last_processed_minute_ms = Some(last_minute),
                TokenSide::No => state.no.last_processed_minute_ms = Some(last_minute),
            }

            // Generate a basis snapshot for the last data point if we have history
            let history = match token_side {
                TokenSide::Yes => &state.yes.history,
                TokenSide::No => &state.no.history,
            };

            if history.len() >= self.config.min_signal_samples {
                // Get the last matched poly price and its CEX reference
                // We need to iterate backwards to find the last matched point
                for (ts_ms, poly_price) in poly_prices.iter().rev() {
                    if let Some(cex_price) =
                        Self::find_closest_cex_price(*ts_ms, &cex_ts_list, cex_prices)
                    {
                        let minutes_to_expiry =
                            (settlement_ts_ms.saturating_sub(*ts_ms) as f64) / ONE_MINUTE_MS as f64;
                        let fair_value = token_fair_value(
                            &rule,
                            token_side,
                            cex_price,
                            sigma,
                            minutes_to_expiry,
                        );
                        let signal_basis = poly_price - fair_value;
                        let z_score =
                            rolling_zscore(history, signal_basis, self.config.min_signal_samples);
                        let delta =
                            token_delta(&rule, token_side, cex_price, sigma, minutes_to_expiry);

                        let snapshot = BasisStrategySnapshot {
                            token_side,
                            poly_price: *poly_price,
                            fair_value,
                            signal_basis,
                            z_score,
                            delta,
                            cex_reference_price: cex_price,
                            sigma,
                            minutes_to_expiry,
                        };

                        match token_side {
                            TokenSide::Yes => state.yes.last_snapshot = Some(snapshot.clone()),
                            TokenSide::No => state.no.last_snapshot = Some(snapshot.clone()),
                        }

                        // Set the overall basis snapshot (prefer Yes, or whichever has data)
                        state.set_basis_snapshot(Some(snapshot));
                        break;
                    }
                }
            }
        }
    }

    /// Find the closest CEX price within 5 minutes of the target timestamp
    fn find_closest_cex_price(
        target_ts_ms: u64,
        sorted_ts: &[u64],
        cex_prices: &std::collections::HashMap<u64, f64>,
    ) -> Option<f64> {
        const MAX_DIFF_MS: u64 = 5 * ONE_MINUTE_MS; // 5 minutes

        // Binary search for closest timestamp
        let pos = sorted_ts.binary_search(&target_ts_ms).unwrap_or_else(|x| x);

        // Check nearby timestamps
        let mut best_match: Option<(u64, f64)> = None;
        let mut best_diff = u64::MAX;

        for &ts in sorted_ts.iter().take(pos + 1).skip(pos.saturating_sub(5)) {
            let diff = if ts > target_ts_ms {
                ts - target_ts_ms
            } else {
                target_ts_ms - ts
            };
            if diff < best_diff && diff <= MAX_DIFF_MS {
                if let Some(&price) = cex_prices.get(&ts) {
                    best_diff = diff;
                    best_match = Some((ts, price));
                }
            }
        }

        best_match.map(|(_, price)| price)
    }

    fn history_capacity(&self) -> usize {
        self.config
            .rolling_window_minutes
            .max(self.config.min_signal_samples)
            .max(2)
    }

    fn state_for_symbol(&self, symbol: &Symbol) -> SymbolState {
        SymbolState::new(
            self.history_capacity(),
            self.poly_connected,
            self.cex_connected_for_symbol(symbol),
        )
    }

    fn cex_connected_for_symbol(&self, symbol: &Symbol) -> bool {
        self.markets
            .get(symbol)
            .and_then(|market| self.cex_connections.get(&market.hedge_exchange).copied())
            .unwrap_or(true)
    }

    fn apply_exchange_connection_status(&mut self, exchange: Exchange, connected: bool) {
        if exchange == Exchange::Polymarket {
            self.poly_connected = connected;
            for state in self.states.values_mut() {
                state.poly_connected = connected;
            }
            return;
        }

        self.cex_connections.insert(exchange, connected);
        for (symbol, state) in self.states.iter_mut() {
            let matches_exchange = self
                .markets
                .get(symbol)
                .map(|market| market.hedge_exchange == exchange)
                .unwrap_or(false);
            if matches_exchange {
                state.cex_connected = connected;
            }
        }
    }

    fn apply_market_event(&mut self, event: &MarketDataEvent) -> Option<Symbol> {
        match event {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                let symbol = snapshot.symbol.clone();
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol(&symbol));
                state.last_update_ms = snapshot.received_at_ms;
                if let Some(mid) = mid_from_orderbook(snapshot) {
                    match snapshot.instrument {
                        InstrumentKind::PolyYes => {
                            state.mark_poly_update(snapshot.received_at_ms);
                            self.poly_connected = true;
                            state.yes.update_mid(snapshot.received_at_ms, mid);
                        }
                        InstrumentKind::PolyNo => {
                            state.mark_poly_update(snapshot.received_at_ms);
                            self.poly_connected = true;
                            state.no.update_mid(snapshot.received_at_ms, mid);
                        }
                        InstrumentKind::CexPerp => {
                            state.mark_cex_update(snapshot.received_at_ms);
                            self.cex_connections.insert(snapshot.exchange, true);
                            state.cex_mid = Some(mid.max(Decimal::ZERO));
                            if let Some(price) = state.cex_mid.and_then(|value| value.to_f64()) {
                                let _ = state.cex_minutes.ingest_price(
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
                exchange,
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
                    .unwrap_or_else(|| self.state_for_symbol(&symbol));
                state.last_update_ms = *timestamp_ms;
                match instrument {
                    InstrumentKind::PolyYes => {
                        state.mark_poly_update(*timestamp_ms);
                        self.poly_connected = true;
                        state.yes.update_mid(*timestamp_ms, price.0);
                    }
                    InstrumentKind::PolyNo => {
                        state.mark_poly_update(*timestamp_ms);
                        self.poly_connected = true;
                        state.no.update_mid(*timestamp_ms, price.0);
                    }
                    InstrumentKind::CexPerp => {
                        state.mark_cex_update(*timestamp_ms);
                        self.cex_connections.insert(*exchange, true);
                        state.cex_mid = Some(price.0.max(Decimal::ZERO));
                        if let Some(value) = state.cex_mid.and_then(|item| item.to_f64()) {
                            let _ = state.cex_minutes.ingest_price(
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
                exchange,
                symbol,
                next_funding_time_ms,
                ..
            } => {
                let symbol = symbol.clone();
                let mut state = self
                    .states
                    .remove(&symbol)
                    .unwrap_or_else(|| self.state_for_symbol(&symbol));
                state.last_update_ms = *next_funding_time_ms;
                state.cex_connected = true;
                self.cex_connections.insert(*exchange, true);
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
                    .unwrap_or_else(|| self.state_for_symbol(&symbol));
                state.last_update_ms = *timestamp_ms;
                state.market_phase = phase.clone();
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::ConnectionEvent { exchange, status } => {
                self.apply_exchange_connection_status(
                    *exchange,
                    matches!(status, ConnectionStatus::Connected),
                );
                None
            }
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
            .unwrap_or_else(|| self.state_for_symbol(symbol));
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
        if !state.has_pending_observations() {
            if state.last_basis_snapshot.is_none() {
                output.push_warning(EngineWarning::NoPolyData {
                    symbol: symbol.clone(),
                });
            }
            return;
        }

        if self.config.enable_freshness_check
            && self.config.reject_on_disconnect
            && (!state.poly_connected || !state.cex_connected)
        {
            output.push_warning(EngineWarning::ConnectionLost {
                symbol: symbol.clone(),
                poly_connected: state.poly_connected,
                cex_connected: state.cex_connected,
            });
            state.discard_pending_observations();
            state.set_basis_snapshot(None);
            return;
        }

        let Some((cex_reference_price, cex_reference_minute_ms)) =
            state.cex_minutes.reference_close_with_time()
        else {
            output.push_warning(EngineWarning::NoCexData {
                symbol: symbol.clone(),
            });
            state.discard_pending_observations();
            state.set_basis_snapshot(None);
            return;
        };

        if self.config.enable_freshness_check {
            let cex_age_ms = state
                .last_update_ms
                .saturating_sub(state.last_cex_update_ms);
            if cex_age_ms > self.config.max_cex_data_age_ms {
                output.push_warning(EngineWarning::CexPriceStale {
                    symbol: symbol.clone(),
                    cex_age_ms,
                    max_age_ms: self.config.max_cex_data_age_ms,
                });
                state.discard_pending_observations();
                state.set_basis_snapshot(None);
                return;
            }

            let poly_age_ms = state
                .last_update_ms
                .saturating_sub(state.last_poly_update_ms);
            if poly_age_ms > self.config.max_poly_data_age_ms {
                output.push_warning(EngineWarning::PolyPriceStale {
                    symbol: symbol.clone(),
                    poly_age_ms,
                    max_age_ms: self.config.max_poly_data_age_ms,
                });
                state.discard_pending_observations();
                state.set_basis_snapshot(None);
                return;
            }
        };

        let sigma = state.cex_minutes.sigma();
        let yes_evaluation = self.evaluate_token_pending(
            symbol,
            &rule,
            &market,
            TokenSide::Yes,
            &state.yes,
            cex_reference_price,
            cex_reference_minute_ms,
            sigma,
        );
        let no_evaluation = self.evaluate_token_pending(
            symbol,
            &rule,
            &market,
            TokenSide::No,
            &state.no,
            cex_reference_price,
            cex_reference_minute_ms,
            sigma,
        );

        let mut data_quality_issue = false;
        if let Some(warning) = yes_evaluation
            .as_ref()
            .and_then(|evaluation| evaluation.warning.clone())
        {
            output.push_warning(warning);
            data_quality_issue = true;
        }
        if let Some(warning) = no_evaluation
            .as_ref()
            .and_then(|evaluation| evaluation.warning.clone())
        {
            output.push_warning(warning);
            data_quality_issue = true;
        }

        let has_current_snapshot = yes_evaluation
            .as_ref()
            .and_then(|item| item.snapshot.as_ref())
            .is_some()
            || no_evaluation
                .as_ref()
                .and_then(|item| item.snapshot.as_ref())
                .is_some();

        let preferred_snapshot = if data_quality_issue && !has_current_snapshot {
            None
        } else {
            match state.active_token_side {
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
                        .and_then(|item| item.snapshot.clone())
                        .or_else(|| state.yes.last_snapshot.clone()),
                    no_evaluation
                        .as_ref()
                        .and_then(|item| item.snapshot.clone())
                        .or_else(|| state.no.last_snapshot.clone()),
                ]),
            }
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
                    let entry_z = self.get_entry_z(symbol);
                    let exit_z = self.get_exit_z(symbol);
                    if z_score.abs() <= exit_z || z_score >= entry_z {
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
            self.get_entry_z(symbol),
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
        symbol: &Symbol,
        rule: &MarketRule,
        market: &MarketConfig,
        token_side: TokenSide,
        token_state: &TokenMarketState,
        cex_reference_price: f64,
        cex_reference_minute_ms: u64,
        sigma: Option<f64>,
    ) -> Option<EvaluatedToken> {
        let pending = token_state.pending_observation.clone()?;
        if self.config.enable_freshness_check {
            let time_diff_ms = if pending.minute_bucket_ms >= cex_reference_minute_ms {
                pending.minute_bucket_ms - cex_reference_minute_ms
            } else {
                cex_reference_minute_ms - pending.minute_bucket_ms
            };
            if time_diff_ms > self.config.max_time_diff_ms {
                return Some(EvaluatedToken {
                    minute_bucket_ms: pending.minute_bucket_ms,
                    snapshot: None,
                    warning: Some(EngineWarning::DataMisaligned {
                        symbol: symbol.clone(),
                        poly_time_ms: pending.minute_bucket_ms,
                        cex_time_ms: cex_reference_minute_ms,
                        diff_ms: time_diff_ms,
                    }),
                });
            }
        }
        let settlement_ts_ms = market.settlement_timestamp.saturating_mul(1000);
        let minutes_to_expiry =
            (settlement_ts_ms.saturating_sub(pending.ts_ms) as f64) / ONE_MINUTE_MS as f64;
        if pending.price <= 0.0 || cex_reference_price <= 0.0 {
            return Some(EvaluatedToken {
                minute_bucket_ms: pending.minute_bucket_ms,
                snapshot: None,
                warning: None,
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
            warning: None,
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
        let mut cex_hedge_qty = polyalpha_core::CexBaseQty(
            Decimal::from_f64(hedge_qty_signed.abs()).unwrap_or_default(),
        );
        if let Some(market) = self.markets.get(symbol) {
            cex_hedge_qty = cex_hedge_qty.floor_to_step(market.cex_qty_step);
        }
        if cex_hedge_qty.0 <= Decimal::ZERO {
            return None;
        }
        let poly_target_notional = UsdNotional::from_poly(target_shares, Price(poly_price));
        let expected_pnl = UsdNotional(
            poly_target_notional.0
                * Decimal::from_f64(snapshot.signal_basis.abs()).unwrap_or(Decimal::ZERO),
        );

        Some(ArbSignalEvent {
            signal_id: self.next_signal_id(symbol, snapshot.token_side),
            correlation_id: self.generate_correlation_id(),
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
            strength: self.signal_strength(symbol, snapshot.z_score),
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
            correlation_id: self.generate_correlation_id(),
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

    fn signal_strength(&self, symbol: &Symbol, z_score: Option<f64>) -> SignalStrength {
        let abs_z = z_score.unwrap_or_default().abs();
        let entry_z = self.get_entry_z(symbol);
        if abs_z >= entry_z + 1.0 {
            SignalStrength::Strong
        } else if abs_z >= entry_z {
            SignalStrength::Normal
        } else {
            SignalStrength::Weak
        }
    }

    fn next_signal_id(&mut self, symbol: &Symbol, token_side: TokenSide) -> String {
        self.next_signal_seq += 1;
        format!("sig-{}-{:?}-{}", symbol.0, token_side, self.next_signal_seq)
    }

    fn generate_correlation_id(&mut self) -> String {
        use uuid::Uuid;
        format!("corr-{}", Uuid::new_v4())
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

        if let Some(window_secs) = params.rolling_window_secs {
            self.config.rolling_window_minutes = ((window_secs / 60) as usize).max(2);
        }

        self.max_position_usd = params.max_position_usd;
    }
}

#[derive(Clone, Debug, PartialEq)]
struct EvaluatedToken {
    minute_bucket_ms: u64,
    snapshot: Option<BasisStrategySnapshot>,
    warning: Option<EngineWarning>,
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
    let best_bid = snapshot
        .bids
        .iter()
        .max_by(|left, right| left.price.cmp(&right.price))
        .map(|level| level.price.0);
    let best_ask = snapshot
        .asks
        .iter()
        .min_by(|left, right| left.price.cmp(&right.price))
        .map(|level| level.price.0);

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

/// Calculate standard deviation from a list of returns
fn calculate_sigma(returns: &[f64]) -> Option<f64> {
    if returns.len() < 2 {
        return None;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns
        .iter()
        .map(|value| {
            let centered = value - mean;
            centered * centered
        })
        .sum::<f64>()
        / (n - 1.0);
    Some(variance.max(0.0).sqrt())
}

pub fn crate_status() -> &'static str {
    "polyalpha-engine price-only basis parity ready"
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{
        AlphaEngine, CexBaseQty, ConnectionStatus, Exchange, MarketDataEvent, PolymarketIds,
        PriceLevel, VenueQuantity,
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
                last_trade_price: None,
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
                last_trade_price: None,
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

    fn connection_event(exchange: Exchange, status: ConnectionStatus) -> MarketDataEvent {
        MarketDataEvent::ConnectionEvent { exchange, status }
    }

    fn test_engine_with_config(config: SimpleEngineConfig) -> SimpleAlphaEngine {
        SimpleAlphaEngine::with_markets(config, vec![sample_market()])
    }

    fn test_engine() -> SimpleAlphaEngine {
        test_engine_with_config(SimpleEngineConfig {
            min_signal_samples: 2,
            rolling_window_minutes: 4,
            entry_z: 2.0,
            exit_z: 0.5,
            position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
            cex_hedge_ratio: 1.0,
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: PolyShares::ZERO,
            ..SimpleEngineConfig::default()
        })
    }

    fn relaxed_freshness_engine(max_data_age_ms: u64) -> SimpleAlphaEngine {
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
                max_poly_data_age_ms: max_data_age_ms,
                max_cex_data_age_ms: max_data_age_ms,
                ..SimpleEngineConfig::default()
            },
            vec![sample_market()],
        )
    }

    async fn seed_basis_long_yes_signal(engine: &mut SimpleAlphaEngine) -> ArbSignalEvent {
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

        // Keep the warmup path slightly volatile so sigma stays above the minimum
        // and the resulting basis candidate remains hedgeable under current rules.
        for (offset, yes_bid, yes_ask, no_bid, no_ask, cex_price) in [
            (60_100, 51, 53, 47, 49, 100_150),
            (120_100, 50, 52, 48, 50, 100_300),
            (180_100, 35, 37, 63, 65, 100_450),
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
            let output = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(cex_price, 0),
                    Decimal::new(cex_price, 0),
                    offset + 100,
                ))
                .await;
            if let Some(signal) = output.arb_signals.into_iter().next() {
                return signal;
            }
        }

        panic!("expected a BasisLong signal during warmup sequence");
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
    async fn preserves_basis_snapshot_when_follow_up_cex_event_has_no_new_poly_data() {
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
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyNo,
                Decimal::new(46, 2),
                Decimal::new(48, 2),
                60_120,
            ))
            .await;
        let first_output = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(101_000, 0),
                Decimal::new(101_000, 0),
                60_200,
            ))
            .await;
        assert!(first_output.warnings.is_empty());

        let initial_snapshot = engine
            .basis_snapshot(&Symbol::new("btc-price-only"))
            .expect("snapshot should exist after aligned poly/cex data");

        let follow_up_output = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(101_050, 0),
                Decimal::new(101_050, 0),
                60_300,
            ))
            .await;

        assert!(follow_up_output.warnings.is_empty());
        let preserved_snapshot = engine
            .basis_snapshot(&Symbol::new("btc-price-only"))
            .expect("snapshot should be preserved until fresh poly data arrives");
        assert_eq!(preserved_snapshot, initial_snapshot);
    }

    #[tokio::test]
    async fn emits_basis_long_yes_when_yes_token_is_more_underpriced() {
        let mut engine = test_engine();
        let signal = seed_basis_long_yes_signal(&mut engine).await;

        match signal.action {
            ArbSignalAction::BasisLong { token_side, .. } => {
                assert_eq!(token_side, TokenSide::Yes);
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[tokio::test]
    async fn does_not_emit_basis_signal_when_cex_hedge_qty_is_zero() {
        let mut engine = test_engine_with_config(SimpleEngineConfig {
            min_signal_samples: 2,
            rolling_window_minutes: 4,
            entry_z: 2.0,
            exit_z: 0.5,
            position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
            cex_hedge_ratio: 0.0,
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: PolyShares::ZERO,
            ..SimpleEngineConfig::default()
        });

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
            (180_100, 35, 37, 63, 65, 100_300),
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
            let output = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(cex_price, 0),
                    Decimal::new(cex_price, 0),
                    offset + 100,
                ))
                .await;

            assert!(
                output.arb_signals.is_empty(),
                "zero-hedge open signal must be suppressed"
            );
        }
    }

    #[tokio::test]
    async fn close_only_phase_closes_active_position() {
        let mut engine = test_engine();
        let open = seed_basis_long_yes_signal(&mut engine).await;
        assert!(matches!(open.action, ArbSignalAction::BasisLong { .. }));

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

    #[tokio::test]
    async fn blocks_signal_when_cex_price_is_stale() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_100, 0),
                Decimal::new(100_100, 0),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(52, 2),
                Decimal::new(54, 2),
                180_100,
            ))
            .await;

        let output = engine.flush_symbol(&Symbol::new("btc-price-only"));

        assert!(output.arb_signals.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::CexPriceStale { .. }
        ));
    }

    #[tokio::test]
    async fn blocks_signal_when_connection_is_lost() {
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
            .on_market_data(&connection_event(
                Exchange::Polymarket,
                ConnectionStatus::Disconnected,
            ))
            .await;

        let output = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(101_000, 0),
                Decimal::new(101_000, 0),
                60_200,
            ))
            .await;

        assert!(output.arb_signals.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::ConnectionLost {
                poly_connected: false,
                cex_connected: true,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn blocks_signal_when_poly_and_cex_minutes_are_misaligned() {
        let mut engine = relaxed_freshness_engine(5 * ONE_MINUTE_MS);

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_100, 0),
                Decimal::new(100_100, 0),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(52, 2),
                Decimal::new(54, 2),
                180_100,
            ))
            .await;

        let output = engine.flush_symbol(&Symbol::new("btc-price-only"));

        assert!(output.arb_signals.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::DataMisaligned { .. }
        ));
    }

    #[tokio::test]
    async fn reconnect_gap_carries_cex_reference_to_latest_completed_minute() {
        let mut engine = relaxed_freshness_engine(10 * ONE_MINUTE_MS);

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_100, 0),
                Decimal::new(100_100, 0),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_200, 0),
                Decimal::new(100_200, 0),
                300_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(52, 2),
                Decimal::new(54, 2),
                300_100,
            ))
            .await;

        let output = engine.flush_symbol(&Symbol::new("btc-price-only"));

        assert!(
            !output
                .warnings
                .iter()
                .any(|warning| matches!(warning, EngineWarning::DataMisaligned { .. })),
            "恢复连接后的分钟参考价不应继续落后多个整分钟"
        );
    }
}
