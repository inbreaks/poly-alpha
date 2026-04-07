use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use libm::erf;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use polyalpha_core::{
    cex_venue_symbol, AlphaEngine, AlphaEngineOutput, ConnectionStatus, DmmQuoteState,
    EngineParams, EngineWarning, Exchange, InstrumentKind, MarketConfig, MarketDataEvent,
    MarketPhase, MarketRule, MarketRuleKind, OpenCandidate, OrderBookSnapshot, PolyShares, Price,
    SignalStrength, Symbol, TokenSide, UsdNotional, PLANNING_SCHEMA_VERSION,
};

const ONE_MINUTE_MS: u64 = 60_000;
const MIN_MINUTES_TO_EXPIRY: f64 = 1.0;
const MIN_VOLATILITY: f64 = 1e-6;
const MIN_REALIZED_SIGMA_SAMPLES: usize = 30;
const DELTA_BUMP_PCT: f64 = 1e-4;
const DELTA_BUMP_MIN_TICKS: f64 = 10.0;
const DELTA_BUMP_BETWEEN_WIDTH_FRACTION: f64 = 0.25;
const SQRT_TWO: f64 = std::f64::consts::SQRT_2;
const DEFAULT_ANNUALIZED_VOLATILITY: f64 = 0.5;

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
    pub raw_sigma: Option<f64>,
    pub sigma_source: String,
    pub returns_window_len: usize,
    pub minutes_to_expiry: f64,
    pub basis_history_len: usize,
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

    fn warmup_from_klines(klines: &[(u64, f64)], max_returns: usize) -> Self {
        let mut state = Self::new();
        for (ts_ms, close) in klines {
            let _ = state.ingest_price(*ts_ms, *close, max_returns);
        }

        // Keep the latest kline available as the last completed close so reference_close()
        // can work immediately after warmup, matching the pre-existing warmup behavior.
        if let Some(last_close) = state.current_minute_last_close {
            state.previous_completed_close = Some(last_close);
            state.previous_completed_minute_ms = state.current_minute_ms;
        }

        state
    }

    fn absorb_warmup(&mut self, warmed: Self) {
        if warmed.returns_window.len() > self.returns_window.len() {
            self.returns_window = warmed.returns_window;
        }

        let warmed_previous_minute_ms = warmed.previous_completed_minute_ms.unwrap_or_default();
        let current_previous_minute_ms = self.previous_completed_minute_ms.unwrap_or_default();
        if self.previous_completed_minute_ms.is_none()
            || warmed_previous_minute_ms > current_previous_minute_ms
        {
            self.previous_completed_close = warmed.previous_completed_close;
            self.previous_completed_minute_ms = warmed.previous_completed_minute_ms;
        }

        let warmed_current_minute_ms = warmed.current_minute_ms.unwrap_or_default();
        let current_minute_ms = self.current_minute_ms.unwrap_or_default();
        if self.current_minute_ms.is_none() || warmed_current_minute_ms > current_minute_ms {
            self.current_minute_ms = warmed.current_minute_ms;
            self.current_minute_last_close = warmed.current_minute_last_close;
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
        self.last_poly_update_ms = ts_ms;
    }

    fn mark_cex_update(&mut self, ts_ms: u64) {
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
    pub enable_freshness_check: Option<bool>,
    pub reject_on_disconnect: Option<bool>,
    pub max_poly_data_age_ms: Option<u64>,
    pub max_cex_data_age_ms: Option<u64>,
    pub max_time_diff_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct SimpleAlphaEngine {
    config: SimpleEngineConfig,
    markets: HashMap<Symbol, MarketConfig>,
    cex_symbols_by_venue: HashMap<Exchange, HashMap<String, Arc<[Symbol]>>>,
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
        let (markets, cex_symbols_by_venue) = Self::index_markets(markets);
        Self {
            config,
            markets,
            cex_symbols_by_venue,
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

    fn get_enable_freshness_check(&self, symbol: &Symbol) -> bool {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.enable_freshness_check)
            .unwrap_or(self.config.enable_freshness_check)
    }

    fn get_reject_on_disconnect(&self, symbol: &Symbol) -> bool {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.reject_on_disconnect)
            .unwrap_or(self.config.reject_on_disconnect)
    }

    fn get_max_poly_data_age_ms(&self, symbol: &Symbol) -> u64 {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.max_poly_data_age_ms)
            .unwrap_or(self.config.max_poly_data_age_ms)
    }

    fn get_max_cex_data_age_ms(&self, symbol: &Symbol) -> u64 {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.max_cex_data_age_ms)
            .unwrap_or(self.config.max_cex_data_age_ms)
    }

    fn get_max_time_diff_ms(&self, symbol: &Symbol) -> u64 {
        self.market_overrides
            .get(symbol)
            .and_then(|o| o.max_time_diff_ms)
            .unwrap_or(self.config.max_time_diff_ms)
    }

    pub fn replace_markets(&mut self, markets: Vec<MarketConfig>) {
        let (markets, cex_symbols_by_venue) = Self::index_markets(markets);
        self.markets = markets;
        self.cex_symbols_by_venue = cex_symbols_by_venue;
    }

    pub fn basis_snapshot(&self, symbol: &Symbol) -> Option<BasisStrategySnapshot> {
        self.states
            .get(symbol)
            .and_then(|state| state.last_basis_snapshot.clone())
    }

    pub fn basis_history_lengths(&self, symbol: &Symbol) -> (usize, usize) {
        self.states
            .get(symbol)
            .map(|state| (state.yes.history.len(), state.no.history.len()))
            .unwrap_or_default()
    }

    fn index_markets(
        markets: Vec<MarketConfig>,
    ) -> (
        HashMap<Symbol, MarketConfig>,
        HashMap<Exchange, HashMap<String, Arc<[Symbol]>>>,
    ) {
        let mut market_map = HashMap::new();
        let mut venue_index: HashMap<Exchange, HashMap<String, Vec<Symbol>>> = HashMap::new();

        for market in markets {
            let symbol = market.symbol.clone();
            let exchange = market.hedge_exchange;
            let venue_symbol = cex_venue_symbol(exchange, &market.cex_symbol);
            venue_index
                .entry(exchange)
                .or_default()
                .entry(venue_symbol)
                .or_default()
                .push(symbol.clone());
            market_map.insert(symbol, market);
        }

        let cex_symbols_by_venue = venue_index
            .into_iter()
            .map(|(exchange, symbols)| {
                (
                    exchange,
                    symbols
                        .into_iter()
                        .map(|(venue_symbol, symbols)| {
                            (venue_symbol, Arc::<[Symbol]>::from(symbols))
                        })
                        .collect(),
                )
            })
            .collect();

        (market_map, cex_symbols_by_venue)
    }

    fn cex_symbols_for_venue(
        &self,
        exchange: Exchange,
        venue_symbol: &str,
    ) -> Option<Arc<[Symbol]>> {
        self.cex_symbols_by_venue
            .get(&exchange)
            .and_then(|symbols| symbols.get(venue_symbol))
            .cloned()
    }

    fn sigma_context_from_raw(raw_sigma: Option<f64>, returns_window_len: usize) -> SigmaContext {
        let realized_sigma = if returns_window_len >= MIN_REALIZED_SIGMA_SAMPLES {
            raw_sigma.filter(|value| *value >= MIN_VOLATILITY)
        } else {
            None
        };
        let effective_sigma = effective_sigma(realized_sigma);
        let sigma_source = if realized_sigma.is_some() {
            "realized"
        } else {
            "default"
        };
        SigmaContext {
            raw_sigma,
            effective_sigma,
            sigma_source,
            returns_window_len,
        }
    }

    pub fn close_reason(&self, symbol: &Symbol) -> Option<String> {
        let state = self.states.get(symbol)?;
        state.active_token_side?;

        if matches!(
            state.market_phase,
            MarketPhase::ForceReduce { .. }
                | MarketPhase::CloseOnly { .. }
                | MarketPhase::SettlementPending
                | MarketPhase::Disputed
                | MarketPhase::Resolved
        ) {
            return Some("market phase blocks new exposure".to_owned());
        }

        let snapshot = state.last_basis_snapshot.as_ref()?;
        let z_score = snapshot.z_score?;
        let entry_z = self.get_entry_z(symbol);
        let exit_z = self.get_exit_z(symbol);
        if z_score.abs() <= exit_z || z_score >= entry_z {
            return Some("signal basis reverted inside exit band".to_owned());
        }

        None
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
        let capacity = self.history_capacity_for_symbol(symbol);
        let default_state = self.state_for_symbol(symbol);
        let state = self.states.entry(symbol.clone()).or_insert(default_state);
        let warmed = CexMinuteState::warmup_from_klines(klines, capacity);

        state.cex_minutes.absorb_warmup(warmed);

        // Keep the freshest live quote if one already arrived before warmup finished.
        if let Some((ts_ms, last_close)) = klines.last() {
            if state.last_cex_update_ms <= *ts_ms || state.cex_mid.is_none() {
                state.cex_mid = Some(Decimal::from_f64(*last_close).unwrap_or(Decimal::ZERO));
                state.mark_cex_update(*ts_ms);
            }
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
        let capacity = self.history_capacity_for_symbol(symbol);
        let min_signal_samples = self.get_min_warmup_samples(symbol);
        let default_state = self.state_for_symbol(symbol);
        let state = self.states.entry(symbol.clone()).or_insert(default_state);

        // Get the market rule
        let Some(rule) = market.resolved_market_rule() else {
            return;
        };

        let returns: Vec<f64> = state.cex_minutes.returns_window.iter().copied().collect();
        let sigma_context = Self::sigma_context_from_raw(
            calculate_sigma(&returns),
            state.cex_minutes.returns_window.len(),
        );
        let sigma = sigma_context.effective_sigma;

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

            if history.len() >= min_signal_samples {
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
                        let z_score = rolling_zscore(history, signal_basis, min_signal_samples);
                        let delta = token_delta(
                            &rule,
                            token_side,
                            cex_price,
                            sigma,
                            minutes_to_expiry,
                            market.cex_price_tick.to_f64().unwrap_or_default(),
                        );

                        let snapshot = BasisStrategySnapshot {
                            token_side,
                            poly_price: *poly_price,
                            fair_value,
                            signal_basis,
                            z_score,
                            delta,
                            cex_reference_price: cex_price,
                            sigma,
                            raw_sigma: sigma_context.raw_sigma,
                            sigma_source: sigma_context.sigma_source.to_owned(),
                            returns_window_len: sigma_context.returns_window_len,
                            minutes_to_expiry,
                            basis_history_len: history.len(),
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

    fn history_capacity_for_symbol(&self, symbol: &Symbol) -> usize {
        self.get_rolling_window_minutes(symbol)
            .max(self.get_min_warmup_samples(symbol))
            .max(2)
    }

    fn state_for_symbol(&self, symbol: &Symbol) -> SymbolState {
        SymbolState::new(
            self.history_capacity_for_symbol(symbol),
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

    fn ingest_cex_mid_for_symbol(
        &self,
        symbol: &Symbol,
        state: &mut SymbolState,
        updated_at_ms: u64,
        mid: Decimal,
    ) {
        state.mark_cex_update(updated_at_ms);
        state.cex_mid = Some(mid.max(Decimal::ZERO));
        if let Some(price) = state.cex_mid.and_then(|value| value.to_f64()) {
            let _ = state.cex_minutes.ingest_price(
                updated_at_ms,
                price,
                self.history_capacity_for_symbol(symbol),
            );
        }
    }

    fn apply_cex_venue_event_to_symbol(&mut self, symbol: &Symbol, event: &MarketDataEvent) {
        let mut state = self
            .states
            .remove(symbol)
            .unwrap_or_else(|| self.state_for_symbol(symbol));

        match event {
            MarketDataEvent::CexVenueOrderBookUpdate {
                bids,
                asks,
                exchange_timestamp_ms: _,
                received_at_ms,
                ..
            } => {
                state.last_update_ms = *received_at_ms;
                if let Some(mid) = mid_from_price_levels(bids, asks) {
                    self.ingest_cex_mid_for_symbol(symbol, &mut state, *received_at_ms, mid);
                }
            }
            MarketDataEvent::CexVenueTradeUpdate {
                price,
                timestamp_ms,
                ..
            } => {
                state.last_update_ms = *timestamp_ms;
                self.ingest_cex_mid_for_symbol(symbol, &mut state, *timestamp_ms, price.0);
            }
            MarketDataEvent::CexVenueFundingRate {
                next_funding_time_ms,
                ..
            } => {
                state.last_update_ms = *next_funding_time_ms;
            }
            _ => return,
        }

        self.states.insert(symbol.clone(), state);
    }

    fn apply_cex_venue_market_event(
        &mut self,
        exchange: Exchange,
        venue_symbol: &str,
        event: &MarketDataEvent,
        flush_pending: bool,
    ) -> AlphaEngineOutput {
        let Some(symbols) = self.cex_symbols_for_venue(exchange, venue_symbol) else {
            return AlphaEngineOutput::default();
        };

        for symbol in symbols.iter() {
            self.apply_cex_venue_event_to_symbol(symbol, event);
        }

        let mut output = AlphaEngineOutput::default();
        for symbol in symbols.iter() {
            output.extend(self.generate_output(symbol, flush_pending, false));
        }
        output
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
                            state.yes.update_mid(snapshot.received_at_ms, mid);
                        }
                        InstrumentKind::PolyNo => {
                            state.mark_poly_update(snapshot.received_at_ms);
                            state.no.update_mid(snapshot.received_at_ms, mid);
                        }
                        InstrumentKind::CexPerp => {
                            self.ingest_cex_mid_for_symbol(
                                &symbol,
                                &mut state,
                                snapshot.received_at_ms,
                                mid,
                            );
                        }
                    }
                }
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::TradeUpdate {
                exchange: _,
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
                        state.yes.update_mid(*timestamp_ms, price.0);
                    }
                    InstrumentKind::PolyNo => {
                        state.mark_poly_update(*timestamp_ms);
                        state.no.update_mid(*timestamp_ms, price.0);
                    }
                    InstrumentKind::CexPerp => {
                        self.ingest_cex_mid_for_symbol(&symbol, &mut state, *timestamp_ms, price.0);
                    }
                }
                self.states.insert(symbol.clone(), state);
                Some(symbol)
            }
            MarketDataEvent::CexVenueOrderBookUpdate { .. }
            | MarketDataEvent::CexVenueTradeUpdate { .. }
            | MarketDataEvent::CexVenueFundingRate { .. } => None,
            MarketDataEvent::FundingRate {
                exchange: _,
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

        if self.get_enable_freshness_check(symbol)
            && self.get_reject_on_disconnect(symbol)
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

        if self.get_enable_freshness_check(symbol) {
            let cex_age_ms = state
                .last_update_ms
                .saturating_sub(state.last_cex_update_ms);
            let max_cex_data_age_ms = self.get_max_cex_data_age_ms(symbol);
            if cex_age_ms > max_cex_data_age_ms {
                output.push_warning(EngineWarning::CexPriceStale {
                    symbol: symbol.clone(),
                    cex_age_ms,
                    max_age_ms: max_cex_data_age_ms,
                });
                state.discard_pending_observations();
                state.set_basis_snapshot(None);
                return;
            }

            let poly_age_ms = state
                .last_update_ms
                .saturating_sub(state.last_poly_update_ms);
            let max_poly_data_age_ms = self.get_max_poly_data_age_ms(symbol);
            if poly_age_ms > max_poly_data_age_ms {
                output.push_warning(EngineWarning::PolyPriceStale {
                    symbol: symbol.clone(),
                    poly_age_ms,
                    max_age_ms: max_poly_data_age_ms,
                });
                state.discard_pending_observations();
                state.set_basis_snapshot(None);
                return;
            }
        };

        let sigma_context = Self::sigma_context_from_raw(
            state.cex_minutes.sigma(),
            state.cex_minutes.returns_window.len(),
        );
        let yes_evaluation = self.evaluate_token_pending(
            symbol,
            &rule,
            &market,
            TokenSide::Yes,
            &state.yes,
            cex_reference_price,
            cex_reference_minute_ms,
            &sigma_context,
        );
        let no_evaluation = self.evaluate_token_pending(
            symbol,
            &rule,
            &market,
            TokenSide::No,
            &state.no,
            cex_reference_price,
            cex_reference_minute_ms,
            &sigma_context,
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

        if state.market_phase.allows_new_positions() && state.active_token_side.is_none() {
            if let Some(snapshot) = choose_entry_snapshot(
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
                if let Some(candidate) =
                    self.build_open_candidate(symbol, &snapshot, state.last_update_ms)
                {
                    output.push_open_candidate(candidate);
                }
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
        sigma_context: &SigmaContext,
    ) -> Option<EvaluatedToken> {
        let pending = token_state.pending_observation.clone()?;
        if self.get_enable_freshness_check(symbol) {
            let time_diff_ms = if pending.minute_bucket_ms >= cex_reference_minute_ms {
                pending.minute_bucket_ms - cex_reference_minute_ms
            } else {
                cex_reference_minute_ms - pending.minute_bucket_ms
            };
            if time_diff_ms > self.get_max_time_diff_ms(symbol) {
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
            sigma_context.effective_sigma,
            minutes_to_expiry,
        );
        let delta = token_delta(
            rule,
            token_side,
            cex_reference_price,
            sigma_context.effective_sigma,
            minutes_to_expiry,
            market.cex_price_tick.to_f64().unwrap_or_default(),
        );
        let signal_basis = pending.price - fair_value;
        let z_score = rolling_zscore(
            &token_state.history,
            signal_basis,
            self.get_min_warmup_samples(symbol),
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
                sigma: sigma_context.effective_sigma,
                raw_sigma: sigma_context.raw_sigma,
                sigma_source: sigma_context.sigma_source.to_owned(),
                returns_window_len: sigma_context.returns_window_len,
                minutes_to_expiry,
                basis_history_len: token_state.history.len(),
            }),
            warning: None,
        })
    }

    fn build_open_candidate(
        &mut self,
        symbol: &Symbol,
        snapshot: &BasisStrategySnapshot,
        timestamp_ms: u64,
    ) -> Option<OpenCandidate> {
        let mut risk_budget = self.get_position_notional_usd(symbol);
        if let Some(max_position_usd) = self.max_position_usd {
            risk_budget = UsdNotional(risk_budget.0.min(max_position_usd.0));
        }
        if risk_budget.0 <= Decimal::ZERO {
            return None;
        }

        Some(OpenCandidate {
            schema_version: PLANNING_SCHEMA_VERSION,
            candidate_id: self.next_candidate_id(symbol, snapshot.token_side),
            correlation_id: self.generate_correlation_id(),
            symbol: symbol.clone(),
            token_side: snapshot.token_side,
            direction: "long".to_owned(),
            fair_value: snapshot.fair_value,
            raw_mispricing: snapshot.signal_basis,
            delta_estimate: snapshot.delta,
            risk_budget_usd: risk_budget.0.to_f64().unwrap_or_default(),
            strength: self.signal_strength(symbol, snapshot.z_score),
            z_score: snapshot.z_score,
            raw_sigma: snapshot.raw_sigma,
            effective_sigma: snapshot.sigma,
            sigma_source: Some(snapshot.sigma_source.clone()),
            returns_window_len: snapshot.returns_window_len,
            timestamp_ms,
        })
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

    fn next_candidate_id(&mut self, symbol: &Symbol, token_side: TokenSide) -> String {
        self.next_signal_seq += 1;
        format!(
            "cand-{}-{:?}-{}",
            symbol.0, token_side, self.next_signal_seq
        )
    }

    fn generate_correlation_id(&mut self) -> String {
        use uuid::Uuid;
        format!("corr-{}", Uuid::new_v4())
    }
}

#[async_trait]
impl AlphaEngine for SimpleAlphaEngine {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> AlphaEngineOutput {
        match event {
            MarketDataEvent::CexVenueOrderBookUpdate {
                exchange,
                venue_symbol,
                ..
            } => {
                return self.apply_cex_venue_market_event(*exchange, venue_symbol, event, true);
            }
            MarketDataEvent::CexVenueTradeUpdate {
                exchange,
                venue_symbol,
                ..
            } => {
                return self.apply_cex_venue_market_event(*exchange, venue_symbol, event, true);
            }
            MarketDataEvent::CexVenueFundingRate {
                exchange,
                venue_symbol,
                ..
            } => {
                return self.apply_cex_venue_market_event(*exchange, venue_symbol, event, false);
            }
            _ => {}
        }

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

#[derive(Clone, Debug)]
struct SigmaContext {
    raw_sigma: Option<f64>,
    effective_sigma: Option<f64>,
    sigma_source: &'static str,
    returns_window_len: usize,
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
    mid_from_price_levels(&snapshot.bids, &snapshot.asks)
}

fn mid_from_price_levels(
    bids: &[polyalpha_core::PriceLevel],
    asks: &[polyalpha_core::PriceLevel],
) -> Option<Decimal> {
    let best_bid = bids
        .iter()
        .max_by(|left, right| left.price.cmp(&right.price))
        .map(|level| level.price.0);
    let best_ask = asks
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

fn default_minute_sigma() -> f64 {
    DEFAULT_ANNUALIZED_VOLATILITY / (252.0_f64).sqrt() / (1440.0_f64).sqrt()
}

fn effective_sigma(sigma: Option<f64>) -> Option<f64> {
    sigma
        .filter(|value| *value >= MIN_VOLATILITY)
        .or(Some(default_minute_sigma()))
}

fn token_delta(
    rule: &MarketRule,
    token_side: TokenSide,
    spot_price: f64,
    sigma: Option<f64>,
    minutes_to_expiry: f64,
    cex_price_tick: f64,
) -> f64 {
    let bump = delta_bump(rule, spot_price, cex_price_tick);
    let lower_spot = (spot_price - bump).max(1e-9);
    let upper_spot = spot_price + bump;
    let down = token_fair_value(rule, token_side, lower_spot, sigma, minutes_to_expiry);
    let up = token_fair_value(rule, token_side, upper_spot, sigma, minutes_to_expiry);
    (up - down) / (upper_spot - lower_spot)
}

fn delta_bump(rule: &MarketRule, spot_price: f64, cex_price_tick: f64) -> f64 {
    let spot = spot_price.abs().max(1e-9);
    let tick = cex_price_tick.abs().max(1e-9);
    let base_bump = (spot * DELTA_BUMP_PCT).max(tick * DELTA_BUMP_MIN_TICKS);

    let capped_bump = match rule.kind {
        MarketRuleKind::Between => {
            let lower = rule.lower_strike.map(Price::to_f64).unwrap_or_default();
            let upper = rule.upper_strike.map(Price::to_f64).unwrap_or_default();
            let width = (upper - lower).abs();
            if width > 0.0 {
                let width_cap = (width * DELTA_BUMP_BETWEEN_WIDTH_FRACTION).max(tick);
                base_bump.min(width_cap)
            } else {
                base_bump
            }
        }
        _ => base_bump,
    };

    capped_bump.max(tick)
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

    fn shared_cex_markets() -> Vec<MarketConfig> {
        let mut first = sample_market();
        first.symbol = Symbol::new("btc-price-only-a");
        first.poly_ids = PolymarketIds {
            condition_id: "condition-a".to_owned(),
            yes_token_id: "yes-a".to_owned(),
            no_token_id: "no-a".to_owned(),
        };
        first.market_rule = Some(MarketRule {
            kind: MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(98_000, 0))),
            upper_strike: None,
        });
        first.strike_price = Some(Price(Decimal::new(98_000, 0)));

        let mut second = sample_market();
        second.symbol = Symbol::new("btc-price-only-b");
        second.poly_ids = PolymarketIds {
            condition_id: "condition-b".to_owned(),
            yes_token_id: "yes-b".to_owned(),
            no_token_id: "no-b".to_owned(),
        };
        second.market_rule = Some(MarketRule {
            kind: MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(102_000, 0))),
            upper_strike: None,
        });
        second.strike_price = Some(Price(Decimal::new(102_000, 0)));

        vec![first, second]
    }

    fn short_dated_sigma_warmup_market() -> MarketConfig {
        MarketConfig {
            settlement_timestamp: 7_200,
            market_question: Some(
                "Will the price of Bitcoin be above $101,000 in the next two hours?".to_owned(),
            ),
            market_rule: Some(MarketRule {
                kind: MarketRuleKind::Above,
                lower_strike: Some(Price(Decimal::new(101_000, 0))),
                upper_strike: None,
            }),
            strike_price: Some(Price(Decimal::new(101_000, 0))),
            ..sample_market()
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

    fn cex_venue_orderbook_event(
        bid: Decimal,
        ask: Decimal,
        exchange_timestamp_ms: u64,
        received_at_ms: u64,
        sequence: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: "BTCUSDT".to_owned(),
            bids: vec![PriceLevel {
                price: Price(bid),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
            }],
            asks: vec![PriceLevel {
                price: Price(ask),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 1))),
            }],
            exchange_timestamp_ms,
            received_at_ms,
            sequence,
        }
    }

    fn poly_orderbook_event_for(
        symbol: &str,
        instrument: InstrumentKind,
        bid: Decimal,
        ask: Decimal,
        ts: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new(symbol),
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

    async fn seed_basis_long_yes_output(engine: &mut SimpleAlphaEngine) -> AlphaEngineOutput {
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
            if !output.open_candidates.is_empty() {
                return output;
            }
        }

        panic!("expected an open candidate during warmup sequence");
    }

    async fn seed_basis_long_yes_candidate(engine: &mut SimpleAlphaEngine) -> OpenCandidate {
        let output = seed_basis_long_yes_output(engine).await;
        output
            .open_candidates
            .into_iter()
            .next()
            .expect("expected an open candidate during warmup sequence")
    }

    #[tokio::test]
    async fn cex_venue_orderbook_updates_all_markets_attached_to_same_symbol() {
        let mut engine = SimpleAlphaEngine::with_markets(
            SimpleEngineConfig {
                min_signal_samples: 2,
                rolling_window_minutes: 4,
                entry_z: 2.0,
                exit_z: 0.5,
                position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
                cex_hedge_ratio: 1.0,
                dmm_half_spread: Decimal::new(1, 2),
                dmm_quote_size: PolyShares::ZERO,
                ..SimpleEngineConfig::default()
            },
            shared_cex_markets(),
        );

        let _ = engine
            .on_market_data(&cex_venue_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
                0,
                0,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event_for(
                "btc-price-only-a",
                InstrumentKind::PolyYes,
                Decimal::new(52, 2),
                Decimal::new(54, 2),
                60_100,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event_for(
                "btc-price-only-a",
                InstrumentKind::PolyNo,
                Decimal::new(46, 2),
                Decimal::new(48, 2),
                60_120,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event_for(
                "btc-price-only-b",
                InstrumentKind::PolyYes,
                Decimal::new(32, 2),
                Decimal::new(34, 2),
                60_140,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event_for(
                "btc-price-only-b",
                InstrumentKind::PolyNo,
                Decimal::new(66, 2),
                Decimal::new(68, 2),
                60_160,
            ))
            .await;

        let output = engine
            .on_market_data(&cex_venue_orderbook_event(
                Decimal::new(100_100, 0),
                Decimal::new(100_100, 0),
                120_100,
                120_100,
                2,
            ))
            .await;

        assert!(
            output.warnings.is_empty(),
            "shared venue update should refresh both attached markets without freshness warnings"
        );

        let first_snapshot = engine
            .basis_snapshot(&Symbol::new("btc-price-only-a"))
            .expect("first market should get refreshed cex reference");
        let second_snapshot = engine
            .basis_snapshot(&Symbol::new("btc-price-only-b"))
            .expect("second market should get refreshed cex reference");

        assert_eq!(first_snapshot.cex_reference_price, 100_000.0);
        assert_eq!(second_snapshot.cex_reference_price, 100_000.0);
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
    async fn emits_open_candidate_for_yes_when_yes_token_is_more_underpriced() {
        let mut engine = test_engine();
        let candidate = seed_basis_long_yes_candidate(&mut engine).await;

        assert_eq!(candidate.token_side, TokenSide::Yes);
    }

    #[tokio::test]
    async fn emits_open_candidate_without_execution_fields() {
        let mut engine = test_engine();
        let output = seed_basis_long_yes_output(&mut engine).await;

        assert_eq!(output.open_candidates.len(), 1);
        let candidate = &output.open_candidates[0];
        assert_eq!(candidate.schema_version, PLANNING_SCHEMA_VERSION);
        assert_eq!(candidate.symbol, Symbol::new("btc-price-only"));
        assert_eq!(candidate.token_side, TokenSide::Yes);
        assert_eq!(candidate.direction, "long");
        assert_eq!(candidate.risk_budget_usd, 1_000.0);
        assert!(candidate.raw_mispricing.abs() > 0.0);
        assert!(candidate.delta_estimate.abs() > 0.0);
        assert!(candidate.effective_sigma.is_some());
        assert_eq!(candidate.sigma_source.as_deref(), Some("default"));
        assert!(candidate.returns_window_len >= 2);
    }

    #[tokio::test]
    async fn keeps_default_sigma_until_realized_window_is_fully_warmed_up() {
        let mut engine = SimpleAlphaEngine::with_markets(
            SimpleEngineConfig {
                min_signal_samples: 2,
                rolling_window_minutes: 4,
                entry_z: 0.5,
                exit_z: 0.25,
                position_notional_usd: UsdNotional(Decimal::new(1_000, 0)),
                cex_hedge_ratio: 1.0,
                dmm_half_spread: Decimal::new(1, 2),
                dmm_quote_size: PolyShares::ZERO,
                ..SimpleEngineConfig::default()
            },
            vec![short_dated_sigma_warmup_market()],
        );

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

        let mut final_output = AlphaEngineOutput::default();
        for (offset, yes_bid, yes_ask, no_bid, no_ask, cex_price) in [
            (60_100, 14, 16, 84, 86, 100_001),
            (120_100, 12, 14, 86, 88, 100_002),
            (180_100, 4, 6, 94, 96, 100_003),
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
            final_output = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(cex_price, 0),
                    Decimal::new(cex_price, 0),
                    offset + 100,
                ))
                .await;
        }

        let candidate = final_output
            .open_candidates
            .into_iter()
            .next()
            .expect("expected candidate after third aligned observation");
        assert_eq!(candidate.returns_window_len, 2);
        assert_eq!(candidate.sigma_source.as_deref(), Some("default"));
        assert_eq!(candidate.token_side, TokenSide::Yes);
        assert!(candidate.fair_value < 0.5);
        assert!(candidate.delta_estimate.abs() > 1e-6);
    }

    #[tokio::test]
    async fn late_cex_warmup_backfills_returns_window_without_regressing_live_minute() {
        let market = sample_market();
        let symbol = market.symbol.clone();
        let mut engine = SimpleAlphaEngine::with_markets(
            SimpleEngineConfig {
                min_signal_samples: 2,
                rolling_window_minutes: 600,
                ..SimpleEngineConfig::default()
            },
            vec![market],
        );

        for minute in 0..=10_u64 {
            let ts = (minute + 1_000) * ONE_MINUTE_MS;
            let _ = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(100_000 + minute as i64, 0),
                    Decimal::new(100_000 + minute as i64, 0),
                    ts,
                ))
                .await;
        }

        let live_state = engine
            .states
            .get(&symbol)
            .expect("state after live cex events");
        let live_current_minute_ms = live_state
            .cex_minutes
            .current_minute_ms
            .expect("current minute after live cex events");
        let live_returns_len = live_state.cex_minutes.returns_window.len();
        let live_cex_mid = live_state.cex_mid;
        let live_last_cex_update_ms = live_state.last_cex_update_ms;
        assert!(live_returns_len >= 9);

        let historical_klines = (0..750_u64)
            .map(|minute| (minute * ONE_MINUTE_MS, 90_000.0 + (minute as f64 * 0.5)))
            .collect::<Vec<_>>();

        engine.warmup_cex_prices(&symbol, &historical_klines);

        let warmed_state = engine
            .states
            .get(&symbol)
            .expect("state after late cex warmup");
        assert_eq!(
            warmed_state.cex_minutes.current_minute_ms,
            Some(live_current_minute_ms)
        );
        assert_eq!(warmed_state.cex_mid, live_cex_mid);
        assert_eq!(warmed_state.last_cex_update_ms, live_last_cex_update_ms);
        assert!(
            warmed_state.cex_minutes.returns_window.len() >= 100,
            "expected historical warmup to backfill returns window, got {}",
            warmed_state.cex_minutes.returns_window.len()
        );
    }

    #[tokio::test]
    async fn open_candidate_timestamp_matches_triggering_cex_event() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

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

            let cex_timestamp_ms = offset + 100;
            let output = engine
                .on_market_data(&cex_orderbook_event(
                    Decimal::new(cex_price, 0),
                    Decimal::new(cex_price, 0),
                    cex_timestamp_ms,
                ))
                .await;

            if let Some(candidate) = output.open_candidates.first() {
                assert_eq!(candidate.timestamp_ms, cex_timestamp_ms);
                return;
            }
        }

        panic!("expected an open candidate during warmup sequence");
    }

    #[tokio::test]
    async fn sigma_warmup_keeps_future_market_delta_non_zero() {
        let mut engine = test_engine();

        let _ = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(100_000, 0),
                Decimal::new(100_000, 0),
                0,
            ))
            .await;

        for (offset, yes_bid, yes_ask, no_bid, no_ask) in [
            (60_100, 51, 53, 47, 49),
            (120_100, 50, 52, 48, 50),
            (180_100, 35, 37, 63, 65),
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
                    Decimal::new(100_000, 0),
                    Decimal::new(100_000, 0),
                    offset + 100,
                ))
                .await;

            if let Some(candidate) = output.open_candidates.first() {
                assert!(
                    candidate.fair_value < 1.0,
                    "future market should not collapse to deterministic payout during sigma warmup"
                );
                assert!(
                    candidate.delta_estimate.abs() > 0.0,
                    "future market should remain hedgeable during sigma warmup"
                );
                return;
            }
        }

        panic!("expected an open candidate during sigma warmup sequence");
    }

    #[test]
    fn token_delta_keeps_low_price_between_market_hedgeable() {
        let rule = MarketRule {
            kind: MarketRuleKind::Between,
            lower_strike: Some(Price(Decimal::new(130, 2))),
            upper_strike: Some(Price(Decimal::new(140, 2))),
        };

        let delta = token_delta(
            &rule,
            TokenSide::Yes,
            1.28415,
            Some(0.0008300198675933288),
            4_480.0,
            0.0001,
        );

        assert!(
            delta.abs() > 1e-6,
            "low-price between market should remain hedgeable, got delta={delta}"
        );
    }

    #[tokio::test]
    async fn emitting_open_candidate_does_not_mark_position_active() {
        let mut engine = test_engine();
        let _ = seed_basis_long_yes_output(&mut engine).await;
        let symbol = Symbol::new("btc-price-only");

        let _ = engine
            .on_market_data(&lifecycle_event(
                MarketPhase::CloseOnly {
                    hours_remaining: 0.5,
                },
                180_300,
            ))
            .await;

        assert_eq!(engine.close_reason(&symbol), None);
    }

    #[tokio::test]
    async fn still_emits_open_candidate_when_cex_hedge_ratio_is_zero() {
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
        let mut saw_candidate = false;

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

            if !output.open_candidates.is_empty() {
                saw_candidate = true;
                break;
            }
        }

        assert!(
            saw_candidate,
            "zero-hedge configuration should still emit a candidate"
        );
    }

    #[tokio::test]
    async fn close_only_phase_exposes_close_reason_for_synced_position() {
        let mut engine = test_engine();
        let candidate = seed_basis_long_yes_candidate(&mut engine).await;
        let symbol = Symbol::new("btc-price-only");
        engine.sync_position_state(&symbol, Some(candidate.token_side));

        let close = engine
            .on_market_data(&lifecycle_event(
                MarketPhase::CloseOnly {
                    hours_remaining: 0.5,
                },
                180_300,
            ))
            .await;

        assert!(close.open_candidates.is_empty());
        assert_eq!(
            engine.close_reason(&symbol).as_deref(),
            Some("market phase blocks new exposure")
        );
    }

    #[tokio::test]
    async fn pre_settlement_phase_does_not_force_close_synced_position() {
        let mut engine = test_engine();
        let candidate = seed_basis_long_yes_candidate(&mut engine).await;
        let symbol = Symbol::new("btc-price-only");
        engine.sync_position_state(&symbol, Some(candidate.token_side));

        let close = engine
            .on_market_data(&lifecycle_event(
                MarketPhase::PreSettlement {
                    hours_remaining: 20.0,
                },
                180_300,
            ))
            .await;

        assert!(close.open_candidates.is_empty());
        assert_eq!(engine.close_reason(&symbol), None);
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

        assert!(output.open_candidates.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::CexPriceStale { .. }
        ));
        assert_eq!(engine.close_reason(&Symbol::new("btc-price-only")), None);
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

        assert!(output.open_candidates.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::ConnectionLost {
                poly_connected: false,
                cex_connected: true,
                ..
            }
        ));
        assert_eq!(engine.close_reason(&Symbol::new("btc-price-only")), None);
    }

    #[tokio::test]
    async fn quote_updates_do_not_restore_connection_after_reconnecting_event() {
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
                60_000,
            ))
            .await;
        let _ = engine
            .on_market_data(&connection_event(
                Exchange::Polymarket,
                ConnectionStatus::Reconnecting,
            ))
            .await;
        let _ = engine
            .on_market_data(&connection_event(
                Exchange::Binance,
                ConnectionStatus::Reconnecting,
            ))
            .await;
        let _ = engine
            .on_market_data(&poly_orderbook_event(
                InstrumentKind::PolyYes,
                Decimal::new(53, 2),
                Decimal::new(55, 2),
                60_100,
            ))
            .await;
        let output = engine
            .on_market_data(&cex_orderbook_event(
                Decimal::new(101_000, 0),
                Decimal::new(101_000, 0),
                60_200,
            ))
            .await;
        assert!(output.open_candidates.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::ConnectionLost {
                poly_connected: false,
                cex_connected: false,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn funding_updates_do_not_restore_cex_connection_after_reconnecting_event() {
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
                60_000,
            ))
            .await;
        let _ = engine
            .on_market_data(&connection_event(
                Exchange::Binance,
                ConnectionStatus::Reconnecting,
            ))
            .await;
        let _ = engine
            .on_market_data(&MarketDataEvent::FundingRate {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-price-only"),
                rate: Decimal::new(1, 4),
                next_funding_time_ms: 60_100,
            })
            .await;
        let output = engine.flush_symbol(&Symbol::new("btc-price-only"));
        assert!(output.open_candidates.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::ConnectionLost {
                poly_connected: true,
                cex_connected: false,
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

        assert!(output.open_candidates.is_empty());
        assert_eq!(output.warnings.len(), 1);
        assert!(matches!(
            output.warnings[0],
            EngineWarning::DataMisaligned { .. }
        ));
        assert_eq!(engine.close_reason(&Symbol::new("btc-price-only")), None);
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
