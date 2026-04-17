use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use polyalpha_audit::{
    AuditGateResult, PulseAssetHealthAuditEvent, PulseBookLevelAuditRow, PulseBookSnapshotAudit,
    PulseLifecycleAuditEvent, PulseMarketTapeAuditEvent, PulseSessionSummaryRow, PulseSignalMode,
    PulseSignalSnapshotAuditEvent,
};
use polyalpha_core::{
    asset_key_from_cex_symbol, CexBaseQty, CexOrderRequest, ClientOrderId, ConnectionStatus,
    EvaluationStats, Exchange, MarketConfig, MarketDataEvent, MarketRule, MarketView,
    OrderBookSnapshot, OrderExecutor, OrderRequest, OrderResponse, OrderSide, OrderType,
    PolyOrderRequest, PolyShares, PolySizingInstruction, Price, PulseAssetHealthRow,
    PulseMarketMonitorRow, PulseMonitorView, PulseSessionDetailView, PulseSessionMonitorRow,
    SignalStats, Symbol, TimeInForce, TokenSide, UsdNotional, VenueQuantity,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use thiserror::Error;

use polyalpha_core::Settings;

use crate::anchor::provider::AnchorProvider;
use crate::anchor::router::AnchorRouter;
use crate::audit::{PulseAuditEmitter, PulseAuditRecord, PulseAuditSink, PulseSessionAuditSummary};
use crate::detector::{PulseDetector, PulseDetectorConfig};
use crate::hedge::GlobalHedgeAggregator;
use crate::model::{AnchorSnapshot, GammaCapMode, PulseAsset, PulseMode, PulseSessionState};
use crate::monitor::build_pulse_monitor_view;
use crate::pricing::{EventPricer, EventPricerConfig, ExpiryGapAdjustment};
use crate::session::{PolyFillOutcome, PulseSession, PulseSessionConfig};
use crate::signal::{
    build_reachability_envelope, classify_pulse_mode, estimate_session_edge, executable_poly_quote,
    generate_target_candidates, reversion_pocket, select_best_target,
    summarize_recovery_observation, timeout_exit_price_for_shares, ExecutableQuoteSide,
    RecoveryObservation, SignalSample,
};

pub type Result<T> = std::result::Result<T, PulseRuntimeBuildError>;

const OPENING_OUTCOME_PENDING: &str = "pending";
const OPENING_OUTCOME_EFFECTIVE_OPEN: &str = "effective_open";
const OPENING_OUTCOME_REJECTED: &str = "rejected";
const OPENING_REJECTION_ZERO_FILL: &str = "zero_fill";
const OPENING_REJECTION_MIN_OPEN_NOTIONAL: &str = "min_open_notional";
const OPENING_REJECTION_MIN_OPEN_FILL_RATIO: &str = "min_open_fill_ratio";
const OPENING_REJECTION_ZERO_HEDGE_QTY: &str = "zero_hedge_qty";
const OPENING_REJECTION_MIN_EXPECTED_NET_PNL: &str = "min_expected_net_pnl";
const PULSE_WS_TRANSPORT_MAX_IDLE_MS: u64 = 2_000;
const DETECTOR_REACHABILITY_CAP_BPS: f64 = 500.0;
const DETECTOR_HIT_RATE_SAFETY_MARGIN_PP: f64 = 10.0;
const DETECTOR_MIN_REALIZABLE_EV_USD: i64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PulseExecutionMode {
    Paper,
    LiveMock,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulseBookFeedStub {
    pub asset: PulseAsset,
    pub depth: u16,
}

#[derive(Clone)]
pub struct PulseRuntimeFixture {
    pub settings: Settings,
    pub anchor_provider: Arc<dyn AnchorProvider>,
    pub poly_books: Vec<PulseBookFeedStub>,
    pub binance_books: Vec<PulseBookFeedStub>,
}

pub struct PulseRuntimeBuilder {
    settings: Settings,
    anchor_provider: Option<Arc<dyn AnchorProvider>>,
    poly_books: Vec<PulseBookFeedStub>,
    binance_books: Vec<PulseBookFeedStub>,
    execution_mode: PulseExecutionMode,
    executor: Option<Arc<dyn OrderExecutor>>,
    markets: Option<Vec<MarketConfig>>,
    position_sync: Option<Arc<ActualPositionSync>>,
    audit_emitter: Option<PulseAuditEmitter>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordedBinanceOrder {
    pub asset: PulseAsset,
    pub qty: Decimal,
    pub avg_price: Option<Decimal>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PulseRuntimeStats {
    pub signals_seen: u64,
    pub signals_rejected: u64,
    pub evaluation_attempts: u64,
    pub rejection_reasons: HashMap<String, u64>,
}

pub struct PulseRuntime {
    settings: Settings,
    execution_mode: PulseExecutionMode,
    enabled_assets: Vec<PulseAsset>,
    anchor_providers: HashMap<PulseAsset, Arc<dyn AnchorProvider>>,
    max_anchor_age_ms: HashMap<PulseAsset, u64>,
    max_anchor_latency_delta_ms: HashMap<PulseAsset, u64>,
    #[allow(dead_code)]
    poly_books: Vec<PulseBookFeedStub>,
    #[allow(dead_code)]
    binance_books: Vec<PulseBookFeedStub>,
    executor: Option<Arc<dyn OrderExecutor>>,
    markets_by_asset: HashMap<PulseAsset, Vec<MarketConfig>>,
    observed_books: HashMap<String, ObservedMarketBooks>,
    signal_tapes: HashMap<String, MarketSignalTape>,
    last_anchor_tape_ts_by_asset: HashMap<PulseAsset, u64>,
    connection_statuses: HashMap<Exchange, ConnectionStatus>,
    last_transport_activity_ms: HashMap<Exchange, u64>,
    opening_reject_cooldowns: HashMap<String, u64>,
    active_sessions: HashMap<String, ManagedSession>,
    next_session_seq: u64,
    detector: PulseDetector,
    pricer: EventPricer,
    actual_hedge_positions: HashMap<PulseAsset, Decimal>,
    asset_health: HashMap<PulseAsset, PulseAssetHealthRow>,
    hedge_aggregator: GlobalHedgeAggregator,
    audit_sink: PulseAuditSink,
    last_monitor_view: Option<PulseMonitorView>,
    recorded_binance_orders: Vec<RecordedBinanceOrder>,
    stats: PulseRuntimeStats,
    position_sync: Option<Arc<ActualPositionSync>>,
}

pub type ActualPositionSync =
    dyn Fn(PulseAsset, &MarketConfig) -> std::result::Result<Option<Decimal>, String> + Send + Sync;

#[derive(Clone, Debug, Default)]
struct ObservedMarketBooks {
    yes: Option<OrderBookSnapshot>,
    no: Option<OrderBookSnapshot>,
    cex: Option<OrderBookSnapshot>,
}

#[derive(Clone, Debug, Default)]
struct MarketSignalTape {
    yes_ask: VecDeque<SignalSample<Decimal>>,
    no_ask: VecDeque<SignalSample<Decimal>>,
    cex_mid: VecDeque<SignalSample<Decimal>>,
    fair_prob_yes: VecDeque<SignalSample<f64>>,
}

#[derive(Clone, Debug)]
struct ManagedSession {
    session: PulseSession,
    market: MarketConfig,
    opened_at_ms: u64,
    deadline_ms: u64,
    entry_price: Decimal,
    target_exit_price: Decimal,
    timeout_exit_price: Decimal,
    net_edge_bps: f64,
    pulse_score_bps: f64,
    available_reversion_ticks: f64,
    reversion_pocket_notional_usd: Decimal,
    vacuum_ratio: Decimal,
    entry_executable_notional_usd: Decimal,
    candidate_expected_net_pnl_usd: Decimal,
    timeout_loss_estimate_usd: Decimal,
    required_hit_rate: f64,
    last_allocated_hedge_qty: Decimal,
    opening_allocated_hedge_qty: Decimal,
    actual_fill_notional_usd: Decimal,
    expected_open_net_pnl_usd: Decimal,
    effective_open: bool,
    opening_outcome: String,
    opening_rejection_reason: Option<String>,
    anchor_latency_delta_ms: u64,
    maker_order_updated_at_ms: u64,
    poly_order_seq: u64,
    hedge_position_qty: Decimal,
    hedge_avg_entry_price: Option<Decimal>,
    hedge_realized_pnl_usd: Decimal,
}

#[derive(Clone, Debug)]
struct EntryCandidate {
    market: MarketConfig,
    claim_side: TokenSide,
    mode: PulseMode,
    entry_price: Decimal,
    target_exit_price: Decimal,
    timeout_exit_price: Decimal,
    timeout_secs: u64,
    net_edge_bps: f64,
    expected_net_pnl_usd: Decimal,
    timeout_loss_estimate_usd: Decimal,
    required_hit_rate: f64,
    realizable_ev_usd: Decimal,
    min_profitable_target_distance_bps: f64,
    target_distance_to_mid_bps: f64,
    pulse_score_bps: f64,
    available_reversion_ticks: f64,
    entry_executable_notional_usd: Decimal,
    reversion_pocket_notional_usd: Decimal,
    vacuum_ratio: Decimal,
    event_delta_yes: f64,
    anchor_latency_delta_ms: u64,
    fair_prob_yes: f64,
    base_hit_rate: f64,
    predicted_hit_rate: f64,
    observation_quality_score: f64,
}

#[derive(Clone, Debug)]
struct PolyExitIntent {
    client_order_id: ClientOrderId,
    symbol: Symbol,
    token_side: TokenSide,
    qty: Decimal,
    price: Decimal,
    time_in_force: TimeInForce,
    post_only: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct HedgeExecutionOutcome {
    filled_qty: Decimal,
    avg_price: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PulseMarketSignalSnapshot {
    claim_side: TokenSide,
    mode: PulseMode,
    pulse_score_bps: f64,
    net_edge_bps: f64,
    fair_prob_yes: f64,
    entry_price: Decimal,
    target_exit_price: Decimal,
    timeout_exit_price: Decimal,
    timeout_secs: u64,
    expected_net_pnl_usd: Decimal,
    timeout_loss_estimate_usd: Decimal,
    required_hit_rate: f64,
    predicted_hit_rate: f64,
    realizable_ev_usd: Decimal,
    min_profitable_target_distance_bps: f64,
    target_distance_to_mid_bps: f64,
    reversion_pocket_ticks: f64,
    reversion_pocket_notional_usd: Decimal,
    vacuum_ratio: Decimal,
}

#[derive(Clone, Copy)]
struct SignalAuditContext<'a> {
    asset: PulseAsset,
    market: &'a MarketConfig,
    anchor: &'a AnchorSnapshot,
    yes_book: &'a OrderBookSnapshot,
    no_book: &'a OrderBookSnapshot,
    cex_book: &'a OrderBookSnapshot,
    now_ms: u64,
    poly_transport_healthy: bool,
    hedge_transport_healthy: bool,
}

#[derive(Debug, Error)]
pub enum PulseRuntimeBuildError {
    #[error("pulse runtime has no enabled assets")]
    NoEnabledAssets,
    #[error("pulse runtime is missing an anchor provider")]
    MissingAnchorProvider,
    #[error("pulse runtime failed to build anchor router: {0}")]
    AnchorRouter(String),
}

impl PulseRuntimeBuilder {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            anchor_provider: None,
            poly_books: Vec::new(),
            binance_books: Vec::new(),
            execution_mode: PulseExecutionMode::Paper,
            executor: None,
            markets: None,
            position_sync: None,
            audit_emitter: None,
        }
    }

    pub fn with_anchor_provider(mut self, anchor_provider: Arc<dyn AnchorProvider>) -> Self {
        self.anchor_provider = Some(anchor_provider);
        self
    }

    pub fn with_poly_books(mut self, poly_books: Vec<PulseBookFeedStub>) -> Self {
        self.poly_books = poly_books;
        self
    }

    pub fn with_binance_books(mut self, binance_books: Vec<PulseBookFeedStub>) -> Self {
        self.binance_books = binance_books;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: PulseExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    pub fn with_executor(mut self, executor: Arc<dyn OrderExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn with_markets(mut self, markets: Vec<MarketConfig>) -> Self {
        self.markets = Some(markets);
        self
    }

    pub fn with_position_sync(mut self, position_sync: Arc<ActualPositionSync>) -> Self {
        self.position_sync = Some(position_sync);
        self
    }

    pub fn with_audit_emitter(mut self, audit_emitter: PulseAuditEmitter) -> Self {
        self.audit_emitter = Some(audit_emitter);
        self
    }

    pub async fn build(self) -> Result<PulseRuntime> {
        let enabled_assets = enabled_assets_from_settings(&self.settings);
        if enabled_assets.is_empty() {
            return Err(PulseRuntimeBuildError::NoEnabledAssets);
        }

        let anchor_providers = match self.anchor_provider {
            Some(provider) => enabled_assets
                .iter()
                .copied()
                .map(|asset| (asset, provider.clone()))
                .collect(),
            None => {
                let router = AnchorRouter::from_settings(&self.settings)
                    .map_err(|err| PulseRuntimeBuildError::AnchorRouter(err.to_string()))?;
                let mut providers = HashMap::new();
                for asset in &enabled_assets {
                    let provider = router
                        .provider_for_asset(*asset)
                        .map_err(|err| PulseRuntimeBuildError::AnchorRouter(err.to_string()))?;
                    providers.insert(*asset, provider);
                }
                providers
            }
        };
        let max_anchor_age_ms = max_anchor_age_from_settings(&self.settings, &enabled_assets);
        let max_anchor_latency_delta_ms =
            max_anchor_latency_delta_from_settings(&self.settings, &enabled_assets);
        let markets = self
            .markets
            .unwrap_or_else(|| self.settings.markets.clone());
        let markets_by_asset = filter_pulse_markets(&enabled_assets, markets);
        let detector = PulseDetector::new(PulseDetectorConfig {
            min_net_session_edge_bps: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .min_net_session_edge_bps
                .to_f64()
                .unwrap_or_default(),
            min_claim_price_move_bps: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .min_claim_price_move_bps
                .to_f64()
                .unwrap_or_default(),
            max_fair_claim_move_bps: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .max_fair_claim_move_bps
                .to_f64()
                .unwrap_or_default(),
            max_cex_mid_move_bps: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .max_cex_mid_move_bps
                .to_f64()
                .unwrap_or_default(),
            min_pulse_score_bps: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .min_pulse_score_bps
                .to_f64()
                .unwrap_or_default(),
            min_reversion_pocket_ticks: self
                .settings
                .strategy
                .pulse_arb
                .exit
                .base_target_ticks
                .max(1) as f64,
            max_timeout_loss_usd: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .max_timeout_loss_usd
                .0,
            max_required_hit_rate: self
                .settings
                .strategy
                .pulse_arb
                .entry
                .max_required_hit_rate
                .to_f64()
                .unwrap_or(1.0),
            min_expected_net_pnl_usd: self
                .settings
                .strategy
                .pulse_arb
                .session
                .effective_min_expected_net_pnl_usd()
                .0,
            reachability_cap_bps: DETECTOR_REACHABILITY_CAP_BPS,
            hit_rate_safety_margin_pp: DETECTOR_HIT_RATE_SAFETY_MARGIN_PP,
            min_realizable_ev_usd: Decimal::new(DETECTOR_MIN_REALIZABLE_EV_USD, 0),
        });
        let pricer = EventPricer::new(event_pricer_config(&self.settings));

        let audit_sink = match self.audit_emitter.clone() {
            Some(emitter) => PulseAuditSink::with_emitter(emitter),
            None => PulseAuditSink::default(),
        };

        Ok(PulseRuntime {
            settings: self.settings,
            execution_mode: self.execution_mode,
            enabled_assets,
            anchor_providers,
            max_anchor_age_ms,
            max_anchor_latency_delta_ms,
            poly_books: self.poly_books,
            binance_books: self.binance_books,
            executor: self.executor,
            markets_by_asset,
            observed_books: HashMap::new(),
            signal_tapes: HashMap::new(),
            last_anchor_tape_ts_by_asset: HashMap::new(),
            connection_statuses: HashMap::new(),
            last_transport_activity_ms: HashMap::new(),
            opening_reject_cooldowns: HashMap::new(),
            active_sessions: HashMap::new(),
            next_session_seq: 1,
            detector,
            pricer,
            actual_hedge_positions: HashMap::new(),
            asset_health: HashMap::new(),
            hedge_aggregator: GlobalHedgeAggregator::new(Decimal::new(1, 3), 100),
            last_monitor_view: None,
            recorded_binance_orders: Vec::new(),
            stats: PulseRuntimeStats::default(),
            position_sync: self.position_sync,
            audit_sink,
        })
    }
}

impl PulseRuntime {
    fn record_exchange_activity(&mut self, exchange: Exchange, ts_ms: u64) {
        self.connection_statuses
            .insert(exchange, ConnectionStatus::Connected);
        self.last_transport_activity_ms
            .entry(exchange)
            .and_modify(|current| *current = (*current).max(ts_ms))
            .or_insert(ts_ms);
    }

    fn update_connection_status(&mut self, exchange: Exchange, status: ConnectionStatus) {
        self.connection_statuses.insert(exchange, status);
        if status == ConnectionStatus::Connected {
            self.last_transport_activity_ms
                .insert(exchange, current_time_ms());
        }
    }

    fn selected_poly_leg_max_age_ms(&self) -> u64 {
        self.settings
            .strategy
            .market_data
            .resolved_poly_open_max_quote_age_ms()
            .max(self.settings.strategy.pulse_arb.entry.pulse_window_ms)
    }

    fn transport_is_healthy(&self, exchange: Exchange, now_ms: u64) -> bool {
        match self.connection_statuses.get(&exchange) {
            None => true,
            Some(ConnectionStatus::Connected) => self
                .last_transport_activity_ms
                .get(&exchange)
                .is_some_and(|ts_ms| {
                    now_ms.saturating_sub(*ts_ms) <= PULSE_WS_TRANSPORT_MAX_IDLE_MS
                }),
            Some(_) => false,
        }
    }

    pub fn execution_mode(&self) -> PulseExecutionMode {
        self.execution_mode
    }

    pub fn enabled_assets(&self) -> Vec<PulseAsset> {
        self.enabled_assets.clone()
    }

    pub fn uses_global_hedge_aggregator(&self) -> bool {
        true
    }

    pub fn session_summary(&self, session_id: &str) -> Option<&PulseSessionAuditSummary> {
        self.audit_sink.session_summary(session_id)
    }

    pub fn audit_records(&self) -> &[PulseAuditRecord] {
        self.audit_sink.records()
    }

    pub fn total_audit_record_count(&self) -> usize {
        self.audit_sink.total_record_count()
    }

    pub fn pulse_session_rows(&self) -> Vec<PulseSessionSummaryRow> {
        self.audit_sink.warehouse_rows()
    }

    pub fn last_monitor_view(&self) -> Option<&PulseMonitorView> {
        self.last_monitor_view.as_ref()
    }

    pub fn market_views(&self, now_ms: u64) -> Vec<MarketView> {
        let mut rows = self
            .enabled_assets
            .iter()
            .flat_map(|asset| {
                self.markets_by_asset
                    .get(asset)
                    .into_iter()
                    .flat_map(move |markets| markets.iter().map(move |market| (*asset, market)))
            })
            .map(|(asset, market)| self.monitor_market_view(asset, market, now_ms))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        rows
    }

    pub fn recorded_binance_orders(&self) -> &[RecordedBinanceOrder] {
        &self.recorded_binance_orders
    }

    pub fn active_session_count(&self) -> usize {
        self.active_sessions.len()
    }

    pub fn signal_stats(&self) -> SignalStats {
        SignalStats {
            seen: self.stats.signals_seen,
            rejected: self.stats.signals_rejected,
            rejection_reasons: self.stats.rejection_reasons.clone(),
        }
    }

    pub fn evaluation_stats(&self) -> EvaluationStats {
        EvaluationStats {
            attempts: self.stats.evaluation_attempts,
            ..EvaluationStats::default()
        }
    }

    pub fn actual_hedge_position(&self, asset: PulseAsset) -> Decimal {
        self.actual_hedge_positions
            .get(&asset)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    fn sync_actual_hedge_position(
        &mut self,
        asset: PulseAsset,
        market: &MarketConfig,
    ) -> std::result::Result<Decimal, String> {
        let actual_position = match self.position_sync.as_ref() {
            Some(sync) => sync(asset, market)?.unwrap_or_else(|| self.actual_hedge_position(asset)),
            None => self.actual_hedge_position(asset),
        };
        self.actual_hedge_positions.insert(asset, actual_position);
        self.hedge_aggregator
            .sync_actual_position(asset, actual_position);
        Ok(actual_position)
    }

    pub fn observe_market_event(&mut self, event: MarketDataEvent) {
        match event {
            MarketDataEvent::OrderBookUpdate { snapshot } => {
                let snapshot_for_signal = snapshot.clone();
                self.record_exchange_activity(snapshot.exchange, snapshot.received_at_ms);
                let books = self
                    .observed_books
                    .entry(snapshot.symbol.0.clone())
                    .or_default();
                match snapshot.instrument {
                    polyalpha_core::InstrumentKind::PolyYes => books.yes = Some(snapshot),
                    polyalpha_core::InstrumentKind::PolyNo => books.no = Some(snapshot),
                    polyalpha_core::InstrumentKind::CexPerp => books.cex = Some(snapshot),
                }
                self.record_signal_sample_for_snapshot(&snapshot_for_signal);
            }
            MarketDataEvent::CexVenueOrderBookUpdate {
                exchange,
                venue_symbol,
                bids,
                asks,
                exchange_timestamp_ms,
                received_at_ms,
                sequence,
            } => {
                self.record_exchange_activity(exchange, received_at_ms);
                let symbols = self
                    .markets_by_asset
                    .values()
                    .flat_map(|markets| markets.iter())
                    .filter(|market| {
                        market.hedge_exchange == exchange
                            && market.cex_symbol.eq_ignore_ascii_case(&venue_symbol)
                    })
                    .map(|market| market.symbol.clone())
                    .collect::<Vec<_>>();
                for symbol in symbols {
                    let snapshot = OrderBookSnapshot {
                        exchange,
                        symbol: symbol.clone(),
                        instrument: polyalpha_core::InstrumentKind::CexPerp,
                        bids: bids.clone(),
                        asks: asks.clone(),
                        exchange_timestamp_ms,
                        received_at_ms,
                        sequence,
                        last_trade_price: None,
                    };
                    let books = self.observed_books.entry(symbol.0.clone()).or_default();
                    books.cex = Some(snapshot.clone());
                    self.record_signal_sample_for_snapshot(&snapshot);
                }
            }
            MarketDataEvent::TradeUpdate {
                exchange,
                timestamp_ms,
                ..
            }
            | MarketDataEvent::CexVenueTradeUpdate {
                exchange,
                timestamp_ms,
                ..
            } => {
                self.record_exchange_activity(exchange, timestamp_ms);
            }
            MarketDataEvent::FundingRate { exchange, .. }
            | MarketDataEvent::CexVenueFundingRate { exchange, .. } => {
                self.record_exchange_activity(exchange, current_time_ms());
            }
            MarketDataEvent::ConnectionEvent { exchange, status } => {
                self.update_connection_status(exchange, status);
            }
            _ => {}
        }
    }

    pub async fn run_tick(&mut self, now_ms: u64) -> std::result::Result<(), String> {
        let enabled_assets = self.enabled_assets.clone();
        for asset in enabled_assets {
            self.manage_existing_sessions(asset, now_ms).await?;
            self.try_open_new_session(asset, now_ms).await?;
            self.refresh_asset_health(asset, now_ms);
        }
        self.refresh_monitor_view(now_ms);
        Ok(())
    }

    pub async fn run_timeout_acceptance_scenario(
        &mut self,
        session_id: &str,
    ) -> std::result::Result<(), String> {
        let mut session = PulseSession::new(PulseSessionConfig {
            session_id: session_id.to_owned(),
            asset: PulseAsset::Btc,
            claim_side: TokenSide::No,
            planned_poly_qty: Decimal::new(10_000, 0),
            min_opening_notional_usd: Decimal::new(250, 0),
            min_open_fill_ratio: Decimal::ZERO,
            gamma_cap_mode: GammaCapMode::DeltaClamp,
            max_abs_event_delta: 0.75,
        });
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });
        session.recompute_target_delta(0.00041);
        session.mark_hedge_opened();

        self.hedge_aggregator.upsert(
            session.session_id().to_owned(),
            session.asset(),
            session.target_delta_exposure(),
        );
        self.hedge_aggregator
            .sync_actual_position(PulseAsset::Btc, Decimal::new(11, 1));
        let reconcile = self
            .hedge_aggregator
            .reconcile(PulseAsset::Btc)
            .ok_or_else(|| "expected timeout scenario reconcile order".to_owned())?;

        self.audit_sink.record_lifecycle(lifecycle_event(
            &session,
            reconcile.order_qty,
            reconcile.net_target_delta,
            reconcile.actual_exchange_position,
        ));
        self.recorded_binance_orders.push(RecordedBinanceOrder {
            asset: reconcile.asset,
            qty: reconcile.order_qty,
            avg_price: None,
        });

        session.start_chasing_exit();
        self.audit_sink.record_lifecycle(lifecycle_event(
            &session,
            reconcile.order_qty,
            reconcile.net_target_delta,
            Decimal::ZERO,
        ));

        self.hedge_aggregator.remove(session.session_id());
        self.hedge_aggregator
            .sync_actual_position(PulseAsset::Btc, Decimal::ZERO);
        session.mark_closed();
        self.audit_sink.record_lifecycle(lifecycle_event(
            &session,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        ));
        self.audit_sink
            .record_asset_health_lazy(|| PulseAssetHealthAuditEvent {
                asset: session.asset().as_str().to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(21),
                cex_quote_age_ms: Some(9),
                open_sessions: 0,
                net_target_delta: Some(Decimal::ZERO.normalize().to_string()),
                actual_exchange_position: Some(Decimal::ZERO.normalize().to_string()),
                status: Some("enabled".to_owned()),
                disable_reason: None,
            });
        self.last_monitor_view = Some(build_pulse_monitor_view(
            Vec::new(),
            vec![PulseAssetHealthRow {
                asset: session.asset().as_str().to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(21),
                cex_quote_age_ms: Some(9),
                open_sessions: 0,
                net_target_delta: Some(0.0),
                actual_exchange_position: Some(0.0),
                status: Some("enabled".to_owned()),
                disable_reason: None,
            }],
            Vec::new(),
            Some(session_detail_view(&session, 0, 31.4, reconcile.order_qty)),
        ));

        let audit_event_count = self.audit_sink.audit_event_count_for_session(session_id) + 1;
        self.audit_sink.finalize_session(
            PulseSessionAuditSummary {
                session_id: session_id.to_owned(),
                final_state: "closed".to_owned(),
                deadline_exit_triggered: true,
                audit_event_count,
            },
            PulseSessionSummaryRow {
                pulse_session_id: session_id.to_owned(),
                asset: session.asset().as_str().to_owned(),
                state: "closed".to_owned(),
                opened_at_ms: 1_717_171_717_000,
                closed_at_ms: Some(1_717_171_718_000),
                planned_poly_qty: session.planned_poly_qty().normalize().to_string(),
                actual_poly_filled_qty: session.actual_poly_filled_qty().normalize().to_string(),
                actual_poly_fill_ratio: session.actual_poly_fill_ratio(),
                entry_price: Some("0.35".to_owned()),
                actual_fill_notional_usd: "1225".to_owned(),
                candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
                expected_open_net_pnl_usd: "3.85".to_owned(),
                timeout_loss_estimate_usd: Some("21.68".to_owned()),
                required_hit_rate: Some(0.768),
                pulse_score_bps: Some(182.5),
                effective_open: true,
                opening_outcome: OPENING_OUTCOME_EFFECTIVE_OPEN.to_owned(),
                opening_rejection_reason: None,
                opening_allocated_hedge_qty: reconcile.order_qty.normalize().to_string(),
                session_target_delta_exposure: session
                    .target_delta_exposure()
                    .normalize()
                    .to_string(),
                session_allocated_hedge_qty: reconcile.order_qty.normalize().to_string(),
                net_edge_bps: Some(31.4),
                realized_pnl_usd: Some(75.7),
                exit_path: Some("maker_proxy_hit".to_owned()),
                target_exit_price: Some("0.38".to_owned()),
                final_exit_price: Some("0.38".to_owned()),
                timeout_exit_price: Some("0.31".to_owned()),
                entry_executable_notional_usd: Some("250".to_owned()),
                reversion_pocket_ticks: Some(4.0),
                reversion_pocket_notional_usd: Some("28.57".to_owned()),
                vacuum_ratio: Some("1".to_owned()),
                anchor_latency_delta_ms: Some(18),
                distance_to_mid_bps: Some(8.0),
                relative_order_age_ms: Some(950),
            },
        );

        Ok(())
    }

    pub async fn run_opposing_sessions_acceptance_scenario(
        &mut self,
    ) -> std::result::Result<(), String> {
        self.hedge_aggregator
            .upsert("pulse-session-a", PulseAsset::Btc, Decimal::new(35, 2));
        self.hedge_aggregator
            .upsert("pulse-session-b", PulseAsset::Btc, Decimal::new(-20, 2));
        self.hedge_aggregator
            .sync_actual_position(PulseAsset::Btc, Decimal::ZERO);
        let reconcile = self
            .hedge_aggregator
            .reconcile(PulseAsset::Btc)
            .ok_or_else(|| "expected opposing session net order".to_owned())?;

        self.recorded_binance_orders.push(RecordedBinanceOrder {
            asset: reconcile.asset,
            qty: reconcile.order_qty,
            avg_price: None,
        });
        self.audit_sink
            .record_asset_health_lazy(|| PulseAssetHealthAuditEvent {
                asset: "btc".to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(35),
                anchor_latency_delta_ms: Some(12),
                poly_quote_age_ms: Some(18),
                cex_quote_age_ms: Some(7),
                open_sessions: reconcile.attributions.len(),
                net_target_delta: Some(reconcile.net_target_delta.normalize().to_string()),
                actual_exchange_position: Some(
                    reconcile.actual_exchange_position.normalize().to_string(),
                ),
                status: Some("degraded".to_owned()),
                disable_reason: Some("residual_hedge".to_owned()),
            });
        self.last_monitor_view = Some(build_pulse_monitor_view(
            vec![
                PulseSessionMonitorRow {
                    session_id: "pulse-session-a".to_owned(),
                    asset: "btc".to_owned(),
                    state: "rehedging".to_owned(),
                    remaining_secs: 600,
                    net_edge_bps: 28.0,
                },
                PulseSessionMonitorRow {
                    session_id: "pulse-session-b".to_owned(),
                    asset: "btc".to_owned(),
                    state: "rehedging".to_owned(),
                    remaining_secs: 420,
                    net_edge_bps: 22.0,
                },
            ],
            vec![PulseAssetHealthRow {
                asset: "btc".to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(35),
                anchor_latency_delta_ms: Some(12),
                poly_quote_age_ms: Some(18),
                cex_quote_age_ms: Some(7),
                open_sessions: reconcile.attributions.len(),
                net_target_delta: Some(0.15),
                actual_exchange_position: Some(0.0),
                status: Some("degraded".to_owned()),
                disable_reason: Some("residual_hedge".to_owned()),
            }],
            Vec::new(),
            None,
        ));

        Ok(())
    }

    async fn manage_existing_sessions(
        &mut self,
        asset: PulseAsset,
        now_ms: u64,
    ) -> std::result::Result<(), String> {
        let session_ids = self
            .active_sessions
            .iter()
            .filter_map(|(session_id, managed)| {
                (managed.session.asset() == asset).then_some(session_id.clone())
            })
            .collect::<Vec<_>>();

        for session_id in session_ids {
            let mut close_reason = None::<(Decimal, bool)>;
            let mut lifecycle_recorded = false;

            if let Some(runtime_inputs) = self.session_runtime_inputs(&session_id, now_ms)? {
                let mut pending_rehedge_market = None::<MarketConfig>;
                let mut pending_rehedge_record = false;
                let mut pending_pegging_record = false;
                let mut pending_timeout_audit = false;
                let mut pending_poly_cancel = None::<Symbol>;
                let mut pending_poly_submit = None::<PolyExitIntent>;

                {
                    let managed = self
                        .active_sessions
                        .get_mut(&session_id)
                        .ok_or_else(|| format!("missing managed session `{session_id}`"))?;
                    let current_target = managed.session.target_delta_exposure();
                    managed
                        .session
                        .set_pin_risk_active(runtime_inputs.pin_risk_active);
                    managed
                        .session
                        .recompute_target_delta(runtime_inputs.event_delta_yes);
                    managed.anchor_latency_delta_ms = runtime_inputs.anchor_latency_delta_ms;

                    let delta_drift =
                        (managed.session.target_delta_exposure() - current_target).abs();
                    if delta_drift
                        >= self
                            .settings
                            .strategy
                            .pulse_arb
                            .rehedge
                            .delta_drift_threshold
                    {
                        managed.session.mark_rehedging();
                        self.hedge_aggregator.upsert(
                            managed.session.session_id().to_owned(),
                            asset,
                            managed.session.target_delta_exposure(),
                        );
                        pending_rehedge_market = Some(managed.market.clone());
                        pending_rehedge_record = true;
                    }

                    if runtime_inputs.target_exit_price != managed.target_exit_price {
                        managed.target_exit_price = runtime_inputs.target_exit_price;
                        managed.maker_order_updated_at_ms = now_ms;
                        managed.session.mark_pegging();
                        pending_poly_cancel = Some(managed.market.symbol.clone());
                        pending_poly_submit = next_poly_exit_intent(
                            managed,
                            "maker",
                            managed.target_exit_price,
                            TimeInForce::Gtc,
                            true,
                        );
                        pending_pegging_record = true;
                    }

                    if runtime_inputs.current_sell_price >= managed.target_exit_price {
                        close_reason = Some((managed.target_exit_price, false));
                    } else if now_ms >= managed.deadline_ms {
                        managed.session.start_chasing_exit();
                        close_reason = Some((runtime_inputs.current_sell_price, true));
                        pending_poly_cancel = Some(managed.market.symbol.clone());
                        pending_poly_submit = next_poly_exit_intent(
                            managed,
                            "chase",
                            runtime_inputs.current_sell_price,
                            TimeInForce::Ioc,
                            false,
                        );
                        pending_timeout_audit = true;
                    }
                }

                if let Some(market) = pending_rehedge_market {
                    let hedge_outcome = self
                        .reconcile_and_execute_hedge_detailed(asset, &market)
                        .await?;
                    {
                        let managed = self
                            .active_sessions
                            .get_mut(&session_id)
                            .ok_or_else(|| format!("missing managed session `{session_id}`"))?;
                        apply_session_hedge_fill(managed, hedge_outcome);
                        managed.last_allocated_hedge_qty = hedge_outcome.filled_qty;
                    }
                    if pending_rehedge_record {
                        self.record_session_lifecycle(&session_id)?;
                        lifecycle_recorded = true;
                    }
                    if let Some(managed) = self.active_sessions.get_mut(&session_id) {
                        managed.session.mark_maker_exit_working();
                    }
                }

                if let Some(symbol) = pending_poly_cancel.as_ref() {
                    self.cancel_poly_working_orders(symbol).await?;
                }
                if let Some(intent) = pending_poly_submit {
                    self.submit_poly_exit_intent(intent).await?;
                }

                if pending_pegging_record {
                    self.record_session_lifecycle(&session_id)?;
                    lifecycle_recorded = true;
                    if let Some(managed) = self.active_sessions.get_mut(&session_id) {
                        managed.session.mark_maker_exit_working();
                    }
                }

                if pending_timeout_audit {
                    self.record_session_lifecycle(&session_id)?;
                    lifecycle_recorded = true;
                }
            } else if let Some(managed) = self.active_sessions.get_mut(&session_id) {
                managed.session.trigger_emergency_flatten(
                    crate::model::PulseFailureCode::DataFreshnessRejected,
                );
                close_reason = Some((managed.entry_price, false));
                let _ = managed;
                self.record_session_lifecycle(&session_id)?;
                lifecycle_recorded = true;
            }

            if let Some((exit_price, deadline_exit_triggered)) = close_reason {
                self.close_session(&session_id, now_ms, exit_price, deadline_exit_triggered)
                    .await?;
            } else if !lifecycle_recorded {
                self.refresh_asset_health(asset, now_ms);
            }
        }

        Ok(())
    }

    async fn try_open_new_session(
        &mut self,
        asset: PulseAsset,
        now_ms: u64,
    ) -> std::result::Result<(), String> {
        let open_sessions = self
            .active_sessions
            .values()
            .filter(|managed| managed.session.asset() == asset)
            .count();
        if open_sessions
            >= self
                .settings
                .strategy
                .pulse_arb
                .runtime
                .max_concurrent_sessions_per_asset
        {
            return Ok(());
        }

        let Some(candidate) = self.best_entry_candidate(asset, now_ms)? else {
            return Ok(());
        };
        let Some(executor) = self.executor.clone() else {
            return Ok(());
        };

        let session_id = format!("pulse-{}-{}", asset.as_str(), self.next_session_seq);
        self.next_session_seq += 1;

        let opening_request_notional = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_opening_request_notional_usd()
            .0;
        let min_notional = self
            .settings
            .strategy
            .pulse_arb
            .session
            .min_opening_notional_usd
            .0;
        let min_open_fill_ratio = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_min_open_fill_ratio();
        let requires_nonzero_hedge = self
            .settings
            .strategy
            .pulse_arb
            .session
            .require_nonzero_hedge;
        let min_expected_net_pnl_usd = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_min_expected_net_pnl_usd()
            .0;
        let planned_qty = (opening_request_notional / candidate.entry_price).max(Decimal::ZERO);
        let max_avg_price = poly_open_max_avg_price(&self.settings, candidate.entry_price);
        let max_cost_usd = poly_open_max_cost_usd(&self.settings, opening_request_notional);
        if planned_qty <= Decimal::ZERO {
            return Ok(());
        }

        let mut managed = ManagedSession {
            session: PulseSession::new(PulseSessionConfig {
                session_id: session_id.clone(),
                asset,
                claim_side: candidate.claim_side,
                planned_poly_qty: planned_qty,
                min_opening_notional_usd: min_notional,
                min_open_fill_ratio,
                gamma_cap_mode: gamma_cap_mode(&self.settings),
                max_abs_event_delta: self
                    .settings
                    .strategy
                    .pulse_arb
                    .pin_risk
                    .max_abs_event_delta
                    .to_f64()
                    .unwrap_or(0.75),
            }),
            market: candidate.market.clone(),
            opened_at_ms: now_ms,
            deadline_ms: now_ms + candidate.timeout_secs.saturating_mul(1_000),
            entry_price: candidate.entry_price,
            target_exit_price: candidate.target_exit_price,
            timeout_exit_price: candidate.timeout_exit_price,
            net_edge_bps: candidate.net_edge_bps,
            pulse_score_bps: candidate.pulse_score_bps,
            available_reversion_ticks: candidate.available_reversion_ticks,
            reversion_pocket_notional_usd: candidate.reversion_pocket_notional_usd,
            vacuum_ratio: candidate.vacuum_ratio,
            entry_executable_notional_usd: candidate.entry_executable_notional_usd,
            candidate_expected_net_pnl_usd: candidate.expected_net_pnl_usd,
            timeout_loss_estimate_usd: candidate.timeout_loss_estimate_usd,
            required_hit_rate: candidate.required_hit_rate,
            last_allocated_hedge_qty: Decimal::ZERO,
            opening_allocated_hedge_qty: Decimal::ZERO,
            actual_fill_notional_usd: Decimal::ZERO,
            expected_open_net_pnl_usd: Decimal::ZERO,
            effective_open: false,
            opening_outcome: OPENING_OUTCOME_PENDING.to_owned(),
            opening_rejection_reason: None,
            anchor_latency_delta_ms: candidate.anchor_latency_delta_ms,
            maker_order_updated_at_ms: now_ms,
            poly_order_seq: 0,
            hedge_position_qty: Decimal::ZERO,
            hedge_avg_entry_price: None,
            hedge_realized_pnl_usd: Decimal::ZERO,
        };
        self.record_managed_lifecycle(&managed);

        let poly_request = OrderRequest::Poly(PolyOrderRequest {
            client_order_id: ClientOrderId(format!("{session_id}-poly-open")),
            symbol: candidate.market.symbol.clone(),
            token_side: candidate.claim_side,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            sizing: PolySizingInstruction::BuyBudgetCap {
                max_cost_usd: UsdNotional(max_cost_usd),
                max_avg_price: Price(max_avg_price),
                max_shares: PolyShares(planned_qty),
            },
            time_in_force: TimeInForce::Fok,
            post_only: false,
        });
        let poly_response = executor
            .submit_order(poly_request)
            .await
            .map_err(|err| format!("submit pulse poly open failed: {err}"))?;
        let filled_qty = filled_poly_qty(&poly_response);
        let avg_price = poly_response
            .average_price
            .map(|price| price.0)
            .unwrap_or(candidate.entry_price);
        let actual_fill_notional_usd = avg_price * filled_qty;
        let expected_open_net_pnl_usd =
            expected_net_pnl_usd(actual_fill_notional_usd, candidate.net_edge_bps);
        managed.actual_fill_notional_usd = actual_fill_notional_usd;
        managed.expected_open_net_pnl_usd = expected_open_net_pnl_usd;
        managed.entry_price = avg_price;
        managed.target_exit_price = candidate.target_exit_price.max(avg_price);
        managed.timeout_exit_price = candidate.timeout_exit_price;
        managed.opening_rejection_reason = opening_fill_rejection_reason(
            planned_qty,
            filled_qty,
            avg_price,
            min_notional,
            min_open_fill_ratio,
        )
        .map(str::to_owned);
        managed.opening_outcome = if managed.opening_rejection_reason.is_some() {
            OPENING_OUTCOME_REJECTED.to_owned()
        } else {
            OPENING_OUTCOME_PENDING.to_owned()
        };
        managed.session.on_poly_fill(PolyFillOutcome {
            planned_qty,
            filled_qty,
            avg_price,
        });
        if managed.session.state() == PulseSessionState::Closed {
            if managed.opening_rejection_reason.as_deref()
                == Some(OPENING_REJECTION_MIN_OPEN_NOTIONAL)
            {
                self.maybe_start_min_open_notional_cooldown(&managed.market.symbol, now_ms);
            }
            self.record_managed_lifecycle(&managed);
            self.finalize_managed_session(managed, now_ms, avg_price, false, Decimal::ZERO, None);
            return Ok(());
        }
        managed.session.set_pin_risk_active(self.is_pin_risk(
            asset,
            &candidate.market,
            now_ms,
            candidate.fair_prob_yes,
        ));
        managed
            .session
            .recompute_target_delta(candidate.event_delta_yes);

        let rounded_hedge_qty = rounded_hedge_qty_for_target(
            managed.session.target_delta_exposure(),
            &candidate.market,
        );
        let expected_net_pnl_usd = expected_open_net_pnl_usd;
        if (requires_nonzero_hedge && rounded_hedge_qty <= Decimal::ZERO)
            || expected_net_pnl_usd < min_expected_net_pnl_usd
        {
            managed.opening_outcome = OPENING_OUTCOME_REJECTED.to_owned();
            managed.opening_rejection_reason = Some(
                if requires_nonzero_hedge && rounded_hedge_qty <= Decimal::ZERO {
                    OPENING_REJECTION_ZERO_HEDGE_QTY.to_owned()
                } else {
                    OPENING_REJECTION_MIN_EXPECTED_NET_PNL.to_owned()
                },
            );
            managed.session.mark_closed();
            self.record_managed_lifecycle(&managed);
            self.finalize_managed_session(managed, now_ms, avg_price, false, Decimal::ZERO, None);
            return Ok(());
        }

        self.hedge_aggregator.upsert(
            session_id.clone(),
            asset,
            managed.session.target_delta_exposure(),
        );
        let hedge_outcome = self
            .reconcile_and_execute_hedge_detailed(asset, &candidate.market)
            .await?;
        apply_session_hedge_fill(&mut managed, hedge_outcome);
        managed.last_allocated_hedge_qty = hedge_outcome.filled_qty;
        managed.opening_allocated_hedge_qty = hedge_outcome.filled_qty;
        managed.effective_open = true;
        managed.opening_outcome = OPENING_OUTCOME_EFFECTIVE_OPEN.to_owned();
        managed.opening_rejection_reason = None;
        managed.session.mark_hedge_opened();
        let maker_exit_price = managed.target_exit_price;
        if let Some(intent) = next_poly_exit_intent(
            &mut managed,
            "maker",
            maker_exit_price,
            TimeInForce::Gtc,
            true,
        ) {
            self.submit_poly_exit_intent(intent).await?;
        }
        self.record_managed_lifecycle(&managed);
        self.active_sessions.insert(session_id, managed);
        Ok(())
    }

    #[cfg(test)]
    async fn reconcile_and_execute_hedge(
        &mut self,
        asset: PulseAsset,
        market: &MarketConfig,
    ) -> std::result::Result<Decimal, String> {
        self.reconcile_and_execute_hedge_detailed(asset, market)
            .await
            .map(|outcome| outcome.filled_qty)
    }

    async fn reconcile_and_execute_hedge_detailed(
        &mut self,
        asset: PulseAsset,
        market: &MarketConfig,
    ) -> std::result::Result<HedgeExecutionOutcome, String> {
        let Some(executor) = self.executor.clone() else {
            return Ok(HedgeExecutionOutcome {
                filled_qty: Decimal::ZERO,
                avg_price: None,
            });
        };
        let actual_position = self.sync_actual_hedge_position(asset, market)?;
        let Some(order) = self.hedge_aggregator.reconcile(asset) else {
            return Ok(HedgeExecutionOutcome {
                filled_qty: Decimal::ZERO,
                avg_price: None,
            });
        };

        let side = if order.order_qty >= Decimal::ZERO {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let base_qty = CexBaseQty(order.order_qty.abs()).floor_to_step(market.cex_qty_step);
        if base_qty.0 <= Decimal::ZERO {
            return Ok(HedgeExecutionOutcome {
                filled_qty: Decimal::ZERO,
                avg_price: None,
            });
        }
        let limit_price = aggressive_hedge_limit_price(side, self.books_for_symbol(&market.symbol))
            .ok_or_else(|| format!("missing cex quote for pulse hedge `{}`", market.symbol.0))?;
        let request = OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId(format!(
                "pulse-hedge-{}-{}",
                asset.as_str(),
                self.recorded_binance_orders.len() + 1
            )),
            exchange: Exchange::Binance,
            symbol: market.symbol.clone(),
            venue_symbol: market.cex_symbol.clone(),
            side,
            order_type: OrderType::Limit,
            price: Some(Price(limit_price)),
            base_qty,
            time_in_force: TimeInForce::Ioc,
            reduce_only: false,
        });
        let response = executor
            .submit_order(request)
            .await
            .map_err(|err| format!("submit pulse hedge failed: {err}"))?;
        let filled_qty = filled_cex_qty(&response);
        let signed_filled_qty = if side == OrderSide::Buy {
            filled_qty
        } else {
            -filled_qty
        };
        let avg_price = response
            .average_price
            .map(|price| price.0)
            .or(Some(limit_price));
        let new_actual_position = actual_position + signed_filled_qty;
        self.actual_hedge_positions
            .insert(asset, new_actual_position);
        self.hedge_aggregator
            .sync_actual_position(asset, new_actual_position);
        self.recorded_binance_orders.push(RecordedBinanceOrder {
            asset,
            qty: signed_filled_qty,
            avg_price,
        });
        Ok(HedgeExecutionOutcome {
            filled_qty: signed_filled_qty,
            avg_price,
        })
    }

    async fn close_session(
        &mut self,
        session_id: &str,
        now_ms: u64,
        exit_price: Decimal,
        deadline_exit_triggered: bool,
    ) -> std::result::Result<(), String> {
        let mut managed = self
            .active_sessions
            .remove(session_id)
            .ok_or_else(|| format!("missing managed session `{session_id}`"))?;
        let asset = managed.session.asset();
        let exit_path =
            runtime_exit_path_for_close(managed.session.state(), deadline_exit_triggered);
        self.hedge_aggregator.remove(session_id);
        let hedge_release = self
            .reconcile_and_execute_hedge_detailed(asset, &managed.market)
            .await?;
        apply_session_hedge_fill(&mut managed, hedge_release);
        managed.session.mark_closed();
        let actual_after = self.actual_hedge_position(asset);
        self.audit_sink.record_lifecycle(runtime_lifecycle_event(
            &managed,
            actual_after,
            self.books_for_symbol(&managed.market.symbol),
        ));
        self.finalize_managed_session(
            managed,
            now_ms,
            exit_price,
            deadline_exit_triggered,
            hedge_release.filled_qty,
            Some(exit_path),
        );
        Ok(())
    }

    async fn submit_poly_exit_intent(
        &self,
        intent: PolyExitIntent,
    ) -> std::result::Result<(), String> {
        let Some(executor) = self.executor.clone() else {
            return Ok(());
        };
        if intent.qty <= Decimal::ZERO {
            return Ok(());
        }
        let request = OrderRequest::Poly(PolyOrderRequest {
            client_order_id: intent.client_order_id,
            symbol: intent.symbol,
            token_side: intent.token_side,
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            sizing: PolySizingInstruction::SellExactShares {
                shares: PolyShares(intent.qty),
                min_avg_price: Price(intent.price),
            },
            time_in_force: intent.time_in_force,
            post_only: intent.post_only,
        });
        executor
            .submit_order(request)
            .await
            .map_err(|err| format!("submit pulse poly exit failed: {err}"))?;
        Ok(())
    }

    async fn cancel_poly_working_orders(&self, symbol: &Symbol) -> std::result::Result<(), String> {
        let Some(executor) = self.executor.clone() else {
            return Ok(());
        };
        executor
            .cancel_all(Exchange::Polymarket, symbol)
            .await
            .map_err(|err| format!("cancel pulse poly orders failed: {err}"))?;
        Ok(())
    }

    fn session_runtime_inputs(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> std::result::Result<Option<SessionRuntimeInputs>, String> {
        let Some(managed) = self.active_sessions.get(session_id) else {
            return Ok(None);
        };
        let Some(anchor) = self.anchor_snapshot_for(
            managed.session.asset(),
            Some(managed.market.settlement_timestamp * 1_000),
        ) else {
            return Ok(None);
        };
        let books = self.books_for_symbol(&managed.market.symbol);
        let Some(current_sell_price) = best_bid_for_claim_side(books, managed.session.claim_side())
        else {
            return Ok(None);
        };
        let market_rule = managed
            .market
            .resolved_market_rule()
            .ok_or_else(|| format!("market `{}` missing rule", managed.market.symbol.0))?;
        let priced = self
            .pricer
            .price(
                &anchor,
                &market_rule,
                managed.market.settlement_timestamp * 1_000,
            )
            .map_err(|err| format!("price pulse session `{session_id}` failed: {err}"))?;
        Ok(Some(SessionRuntimeInputs {
            current_sell_price,
            target_exit_price: current_sell_price
                .max(recommended_exit_price_for_settings(
                    &self.settings,
                    managed.entry_price,
                    managed.pulse_score_bps,
                    managed.available_reversion_ticks,
                    managed.market.min_tick_size.0,
                ))
                .min(Decimal::ONE),
            pin_risk_active: self.is_pin_risk(
                managed.session.asset(),
                &managed.market,
                now_ms,
                priced.fair_prob_yes,
            ),
            anchor_latency_delta_ms: now_ms.saturating_sub(anchor.ts_ms),
            event_delta_yes: priced.event_delta_yes,
        }))
    }

    fn best_entry_candidate(
        &mut self,
        asset: PulseAsset,
        now_ms: u64,
    ) -> std::result::Result<Option<EntryCandidate>, String> {
        let Some(markets) = self.markets_by_asset.get(&asset).cloned() else {
            return Ok(None);
        };

        let mut best = None::<EntryCandidate>;
        for market in &markets {
            if self.market_in_opening_reject_cooldown(&market.symbol, now_ms) {
                self.record_rejection(crate::model::PulseFailureCode::OpeningRejectCooldownActive);
                continue;
            }
            let Some(candidate) = self.entry_candidate_for_market(asset, market, now_ms)? else {
                continue;
            };
            if best.as_ref().is_none_or(|current| {
                candidate.realizable_ev_usd > current.realizable_ev_usd
                    || (candidate.realizable_ev_usd == current.realizable_ev_usd
                        && (candidate.predicted_hit_rate > current.predicted_hit_rate
                            || (candidate.predicted_hit_rate == current.predicted_hit_rate
                                && candidate.pulse_score_bps > current.pulse_score_bps)))
            }) {
                best = Some(candidate);
            }
        }
        Ok(best)
    }

    fn market_in_opening_reject_cooldown(&mut self, symbol: &Symbol, now_ms: u64) -> bool {
        let Some(until_ms) = self.opening_reject_cooldowns.get(&symbol.0).copied() else {
            return false;
        };
        if now_ms < until_ms {
            return true;
        }
        self.opening_reject_cooldowns.remove(&symbol.0);
        false
    }

    fn maybe_start_min_open_notional_cooldown(&mut self, symbol: &Symbol, now_ms: u64) {
        let cooldown_secs = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_min_open_notional_reject_cooldown_secs();
        if cooldown_secs == 0 {
            return;
        }
        let until_ms = now_ms.saturating_add(cooldown_secs.saturating_mul(1_000));
        self.opening_reject_cooldowns
            .insert(symbol.0.clone(), until_ms);
    }

    fn entry_candidate_for_market(
        &mut self,
        asset: PulseAsset,
        market: &MarketConfig,
        now_ms: u64,
    ) -> std::result::Result<Option<EntryCandidate>, String> {
        let Some(anchor) =
            self.anchor_snapshot_for(asset, Some(market.settlement_timestamp * 1_000))
        else {
            return Ok(None);
        };
        let anchor_latency_delta_ms = now_ms.saturating_sub(anchor.ts_ms);
        let anchor_latency_delta_limit_ms = self
            .max_anchor_latency_delta_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        if anchor_latency_delta_ms > anchor_latency_delta_limit_ms {
            self.record_rejection(crate::model::PulseFailureCode::AnchorLatencyDeltaRejected);
            return Ok(None);
        }
        let books = self.books_for_symbol(&market.symbol).cloned();
        let (Some(_yes_ask), Some(_no_ask), Some(yes_book), Some(no_book), Some(cex_book)) = (
            best_ask(books.as_ref().and_then(|books| books.yes.as_ref())),
            best_ask(books.as_ref().and_then(|books| books.no.as_ref())),
            books.as_ref().and_then(|books| books.yes.as_ref()),
            books.as_ref().and_then(|books| books.no.as_ref()),
            books.as_ref().and_then(|books| books.cex.as_ref()),
        ) else {
            return Ok(None);
        };
        let market_rule = match market.resolved_market_rule() {
            Some(rule) => rule,
            None => return Ok(None),
        };
        let poly_transport_healthy = self.transport_is_healthy(Exchange::Polymarket, now_ms);
        if !poly_transport_healthy {
            self.record_rejection(crate::model::PulseFailureCode::PolyTransportRejected);
            return Ok(None);
        }
        let hedge_transport_healthy = self.transport_is_healthy(market.hedge_exchange, now_ms);
        if !hedge_transport_healthy {
            self.record_rejection(crate::model::PulseFailureCode::HedgeTransportRejected);
            return Ok(None);
        }
        self.stats.evaluation_attempts = self.stats.evaluation_attempts.saturating_add(1);
        self.record_anchor_market_tape_if_needed(asset, &anchor);
        let audit_ctx = SignalAuditContext {
            asset,
            market,
            anchor: &anchor,
            yes_book,
            no_book,
            cex_book,
            now_ms,
            poly_transport_healthy,
            hedge_transport_healthy,
        };
        let priced =
            match self
                .pricer
                .price(&anchor, &market_rule, market.settlement_timestamp * 1_000)
            {
                Ok(priced) => priced,
                Err(crate::pricing::PricingError::HardExpiryGapExceeded { .. }) => {
                    self.record_rejection(crate::model::PulseFailureCode::HardExpiryGapExceeded);
                    self.record_signal_snapshot_if_enabled(&audit_ctx, |event| {
                        event.admission_result = AuditGateResult::Rejected;
                        event.rejection_reason = Some(
                            crate::model::PulseFailureCode::HardExpiryGapExceeded
                                .as_str()
                                .to_owned(),
                        );
                    });
                    return Ok(None);
                }
                Err(err) => {
                    return Err(format!("price market `{}` failed: {err}", market.symbol.0))
                }
            };

        let pulse_window_ms = self.settings.strategy.pulse_arb.entry.pulse_window_ms;
        let current_cex_mid = mid_price_from_book(cex_book).unwrap_or(Decimal::ZERO);
        let prior_tape = self.signal_tapes.get(&market.symbol.0);
        let yes_book_ts_ms = book_signal_timestamp_ms(yes_book);
        let no_book_ts_ms = book_signal_timestamp_ms(no_book);
        let cex_book_ts_ms = book_signal_timestamp_ms(cex_book);
        let prior_yes_ask = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.yes_ask, yes_book_ts_ms, pulse_window_ms));
        let prior_no_ask = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.no_ask, no_book_ts_ms, pulse_window_ms));
        let prior_cex_mid = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.cex_mid, cex_book_ts_ms, pulse_window_ms));
        let prior_fair_prob_yes = prior_tape
            .and_then(|tape| latest_float_before(&tape.fair_prob_yes, now_ms, pulse_window_ms));
        self.record_fair_prob_sample(&market.symbol, now_ms, priced.fair_prob_yes);
        let selected_poly_leg_max_age_ms = self.selected_poly_leg_max_age_ms();
        let yes_fresh = quote_is_fresh(now_ms, yes_book, selected_poly_leg_max_age_ms);
        let no_fresh = quote_is_fresh(now_ms, no_book, selected_poly_leg_max_age_ms);
        let cex_fresh = quote_is_fresh(
            now_ms,
            cex_book,
            self.settings
                .strategy
                .market_data
                .resolved_cex_open_max_quote_age_ms(),
        );
        let anchor_age_limit = self
            .max_anchor_age_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        let anchor_quality_ok = anchor.quality.anchor_age_ms <= anchor_age_limit
            && anchor.quality.has_strike_coverage
            && anchor.quality.has_liquidity
            && anchor.quality.greeks_complete;
        if !cex_fresh || (!yes_fresh && !no_fresh) {
            self.record_rejection(crate::model::PulseFailureCode::DataFreshnessRejected);
            self.record_signal_snapshot_if_enabled(&audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.rejection_reason = Some(
                    crate::model::PulseFailureCode::DataFreshnessRejected
                        .as_str()
                        .to_owned(),
                );
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return Ok(None);
        }
        if !anchor_quality_ok || !priced.pricing_quality.delta_stable {
            self.record_rejection(crate::model::PulseFailureCode::AnchorQualityRejected);
            self.record_signal_snapshot_if_enabled(&audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.rejection_reason = Some(
                    crate::model::PulseFailureCode::AnchorQualityRejected
                        .as_str()
                        .to_owned(),
                );
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return Ok(None);
        }
        if prior_cex_mid.is_none()
            || prior_fair_prob_yes.is_none()
            || (prior_yes_ask.is_none() && prior_no_ask.is_none())
        {
            self.record_rejection(crate::model::PulseFailureCode::PulseConfirmationRejected);
            self.record_signal_snapshot_if_enabled(&audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.rejection_reason = Some(
                    crate::model::PulseFailureCode::PulseConfirmationRejected
                        .as_str()
                        .to_owned(),
                );
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return Ok(None);
        }
        let yes_candidate = yes_fresh
            .then(|| {
                self.evaluate_claim_candidate(
                    &audit_ctx,
                    market,
                    TokenSide::Yes,
                    yes_book,
                    cex_book,
                    now_ms,
                    current_cex_mid,
                    prior_yes_ask,
                    prior_cex_mid,
                    prior_fair_prob_yes,
                    &priced,
                    anchor_quality_ok,
                    cex_fresh,
                    anchor_latency_delta_ms,
                )
            })
            .flatten();
        let no_candidate = no_fresh
            .then(|| {
                self.evaluate_claim_candidate(
                    &audit_ctx,
                    market,
                    TokenSide::No,
                    no_book,
                    cex_book,
                    now_ms,
                    current_cex_mid,
                    prior_no_ask,
                    prior_cex_mid,
                    prior_fair_prob_yes,
                    &priced,
                    anchor_quality_ok,
                    cex_fresh,
                    anchor_latency_delta_ms,
                )
            })
            .flatten();

        let candidate = match (yes_candidate, no_candidate) {
            (Some(yes), Some(no)) => {
                if yes.realizable_ev_usd > no.realizable_ev_usd
                    || (yes.realizable_ev_usd == no.realizable_ev_usd
                        && (yes.predicted_hit_rate > no.predicted_hit_rate
                            || (yes.predicted_hit_rate == no.predicted_hit_rate
                                && yes.pulse_score_bps >= no.pulse_score_bps)))
                {
                    Some(yes)
                } else {
                    Some(no)
                }
            }
            (Some(candidate), None) | (None, Some(candidate)) => Some(candidate),
            (None, None) => None,
        };

        if candidate.is_some() {
            self.stats.signals_seen = self.stats.signals_seen.saturating_add(1);
        }

        Ok(candidate)
    }

    fn evaluate_claim_candidate(
        &mut self,
        audit_ctx: &SignalAuditContext<'_>,
        market: &MarketConfig,
        claim_side: TokenSide,
        claim_book: &OrderBookSnapshot,
        cex_book: &OrderBookSnapshot,
        now_ms: u64,
        current_cex_mid: Decimal,
        prior_claim_ask: Option<Decimal>,
        prior_cex_mid: Option<Decimal>,
        prior_fair_prob_yes: Option<f64>,
        priced: &crate::model::EventPriceOutput,
        anchor_quality_ok: bool,
        data_fresh: bool,
        anchor_latency_delta_ms: u64,
    ) -> Option<EntryCandidate> {
        if !quote_is_fresh(now_ms, claim_book, self.selected_poly_leg_max_age_ms())
            || !quote_is_fresh(
                now_ms,
                cex_book,
                self.settings
                    .strategy
                    .market_data
                    .resolved_cex_open_max_quote_age_ms(),
            )
        {
            self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.claim_side = Some(token_side_label(claim_side).to_owned());
                event.rejection_reason = Some("claim_quote_stale_during_eval".to_owned());
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return None;
        }
        let opening_request_notional = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_opening_request_notional_usd()
            .0;
        let Some(entry_quote) = executable_poly_quote(
            claim_book,
            ExecutableQuoteSide::Buy,
            opening_request_notional,
        ) else {
            self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.claim_side = Some(token_side_label(claim_side).to_owned());
                event.rejection_reason = Some("entry_quote_unavailable".to_owned());
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return None;
        };
        if entry_quote.filled_notional_usd <= Decimal::ZERO
            || entry_quote.avg_price <= Decimal::ZERO
        {
            self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.claim_side = Some(token_side_label(claim_side).to_owned());
                event.rejection_reason = Some("entry_quote_empty".to_owned());
                event.fair_prob_yes = Some(priced.fair_prob_yes);
            });
            return None;
        }

        let fair_claim_prob = match claim_side {
            TokenSide::Yes => priced.fair_prob_yes,
            TokenSide::No => priced.fair_prob_no,
        };
        let fair_claim_price = Decimal::from_f64_retain(fair_claim_prob).unwrap_or(Decimal::ZERO);
        let pulse_reference_price = prior_claim_ask.unwrap_or(entry_quote.avg_price);
        let pocket = reversion_pocket(
            entry_quote.avg_price,
            pulse_reference_price,
            market.min_tick_size.0,
            entry_quote.filled_notional_usd,
        );
        let claim_price_move_bps = prior_claim_ask
            .and_then(|prior| {
                if prior <= Decimal::ZERO || prior <= entry_quote.avg_price {
                    Some(0.0)
                } else {
                    (((prior - entry_quote.avg_price) / prior) * Decimal::from(10_000)).to_f64()
                }
            })
            .unwrap_or(0.0);
        let fair_claim_move_bps = prior_fair_prob_yes
            .map(|prior_yes| match claim_side {
                TokenSide::Yes => (priced.fair_prob_yes - prior_yes).abs() * 10_000.0,
                TokenSide::No => (priced.fair_prob_no - (1.0 - prior_yes)).abs() * 10_000.0,
            })
            .unwrap_or(0.0);
        let cex_mid_move_bps = prior_cex_mid
            .and_then(|prior| {
                if prior <= Decimal::ZERO {
                    Some(0.0)
                } else {
                    (((current_cex_mid - prior).abs() / prior) * Decimal::from(10_000)).to_f64()
                }
            })
            .unwrap_or(0.0);
        let timeout_exit_price =
            timeout_exit_price_for_shares(claim_book, entry_quote.filled_shares)
                .unwrap_or(Decimal::ZERO);
        let hedge_cost_bps = self.settings.paper_slippage.cex_slippage_bps as f64;
        let fee_bps = (self.settings.execution_costs.poly_fee_bps
            + self.settings.execution_costs.cex_taker_fee_bps) as f64;
        let reserve_bps = self.settings.execution_costs.cex_taker_fee_bps as f64
            + self.settings.execution_costs.poly_fee_bps as f64 / 2.0;
        let reachability = build_reachability_envelope(
            entry_quote.filled_notional_usd,
            pulse_reference_price,
            market.min_tick_size.0,
            hedge_cost_bps,
            fee_bps,
            reserve_bps,
            DETECTOR_REACHABILITY_CAP_BPS,
        );
        let swept_levels_count =
            consumed_ask_levels_for_notional(claim_book, entry_quote.requested_notional_usd);
        let mode = classify_pulse_mode(
            claim_price_move_bps,
            fair_claim_move_bps,
            cex_mid_move_bps,
            entry_quote.filled_notional_usd,
            swept_levels_count,
        );
        let recovery_observation =
            self.summarize_recovery_observation_for_claim(market, claim_side, claim_book);
        let post_pulse_depth_gap_bps = claim_price_move_bps;
        let target_candidates = generate_target_candidates(
            mode,
            pulse_reference_price,
            entry_quote.avg_price,
            market.min_tick_size.0,
            reachability.min_realizable_target_distance_bps,
            reachability.reachability_cap_bps,
        );
        let evaluated_targets = target_candidates
            .into_iter()
            .map(|target| {
                let session_edge = estimate_session_edge(
                    entry_quote.filled_notional_usd,
                    entry_quote.avg_price,
                    target
                        .target_exit_price
                        .min(fair_claim_price.max(entry_quote.avg_price)),
                    timeout_exit_price,
                    hedge_cost_bps,
                    fee_bps,
                    reserve_bps,
                );
                let base_hit_rate = estimate_base_hit_rate(
                    target.target_distance_to_mid_bps,
                    entry_quote.filled_notional_usd,
                    swept_levels_count,
                    post_pulse_depth_gap_bps,
                    entry_quote.depth_fill_ratio,
                );
                let predicted_hit_rate =
                    estimate_predicted_hit_rate(base_hit_rate, &recovery_observation);
                crate::signal::TargetCandidate {
                    economics_evaluated: true,
                    predicted_hit_rate,
                    maker_net_pnl_usd: session_edge.expected_net_pnl_usd,
                    timeout_net_pnl_usd: session_edge.timeout_net_pnl_usd,
                    realizable_ev_usd: realizable_ev_usd(
                        predicted_hit_rate,
                        session_edge.expected_net_pnl_usd,
                        session_edge.timeout_net_pnl_usd,
                    ),
                    ..target
                }
            })
            .collect::<Vec<_>>();
        let Some(selected_target) = select_best_target(evaluated_targets) else {
            self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                event.admission_result = AuditGateResult::Rejected;
                event.claim_side = Some(token_side_label(claim_side).to_owned());
                event.mode_candidate = audit_signal_mode(mode);
                event.rejection_reason = Some("no_reachable_target".to_owned());
                event.fair_prob_yes = Some(priced.fair_prob_yes);
                event.entry_price = Some(decimal_to_audit_string(entry_quote.avg_price));
                event.timeout_exit_price = Some(decimal_to_audit_string(timeout_exit_price));
                event.pulse_score_bps = (claim_price_move_bps - fair_claim_move_bps).max(0.0);
                event.claim_price_move_bps = claim_price_move_bps;
                event.fair_claim_move_bps = fair_claim_move_bps;
                event.cex_mid_move_bps = cex_mid_move_bps;
                event.swept_notional_usd = decimal_to_audit_string(entry_quote.filled_notional_usd);
                event.swept_levels_count = swept_levels_count;
                event.post_pulse_depth_gap_bps = post_pulse_depth_gap_bps;
                event.min_profitable_target_distance_bps =
                    reachability.min_profitable_target_distance_bps;
                event.reachability_cap_bps = reachability.reachability_cap_bps;
                event.in_gray_zone = reachability.in_gray_zone;
                event.reachable = reachability.reachable;
                event.used_exchange_ts = recovery_observation.quality.used_exchange_ts;
                event.native_sequence_present =
                    recovery_observation.quality.native_sequence_present;
                event.post_sweep_update_count_5s =
                    recovery_observation.quality.post_sweep_update_count_5s;
                event.max_interarrival_gap_ms_5s =
                    recovery_observation.quality.max_interarrival_gap_ms_5s;
                event.observation_quality_score =
                    Some(recovery_observation.quality.observation_quality_score);
                event.admission_eligible = recovery_observation.quality.admission_eligible;
            });
            return None;
        };
        let session_edge = estimate_session_edge(
            entry_quote.filled_notional_usd,
            entry_quote.avg_price,
            selected_target
                .target_exit_price
                .min(fair_claim_price.max(entry_quote.avg_price)),
            timeout_exit_price,
            self.settings.paper_slippage.cex_slippage_bps as f64,
            (self.settings.execution_costs.poly_fee_bps
                + self.settings.execution_costs.cex_taker_fee_bps) as f64,
            self.settings.execution_costs.cex_taker_fee_bps as f64
                + self.settings.execution_costs.poly_fee_bps as f64 / 2.0,
        );
        let has_pulse_history =
            prior_claim_ask.is_some() && prior_cex_mid.is_some() && prior_fair_prob_yes.is_some();
        let instant_basis_bps = ((fair_claim_price - entry_quote.avg_price)
            * Decimal::from(10_000))
        .to_f64()
        .unwrap_or_default();
        let decision = self.detector.evaluate(crate::model::PulseOpportunityInput {
            instant_basis_bps,
            poly_vwap_slippage_bps: entry_quote.slippage_bps,
            hedge_slippage_bps: self.settings.paper_slippage.cex_slippage_bps as f64,
            fee_bps: (self.settings.execution_costs.poly_fee_bps
                + self.settings.execution_costs.cex_taker_fee_bps) as f64,
            perp_basis_penalty_bps: 0.0,
            rehedge_reserve_bps: self.settings.execution_costs.cex_taker_fee_bps as f64,
            timeout_exit_reserve_bps: self.settings.execution_costs.poly_fee_bps as f64 / 2.0,
            expected_net_edge_bps: session_edge.expected_net_edge_bps,
            expected_net_pnl_usd: selected_target.maker_net_pnl_usd,
            timeout_loss_estimate_usd: session_edge.timeout_loss_estimate_usd,
            min_profitable_target_distance_bps: reachability.min_profitable_target_distance_bps,
            target_distance_to_mid_bps: Some(selected_target.target_distance_to_mid_bps),
            predicted_hit_rate: Some(selected_target.predicted_hit_rate),
            realizable_ev_usd: Some(selected_target.realizable_ev_usd),
            reversion_pocket_ticks: pocket.available_ticks,
            vacuum_ratio: pocket.vacuum_ratio,
            anchor_quality_ok,
            pricing_quality_ok: priced.pricing_quality.delta_stable,
            data_fresh,
            has_pulse_history,
            claim_price_move_bps,
            fair_claim_move_bps,
            cex_mid_move_bps,
        });
        if !decision.should_trade {
            if let Some(code) = decision.rejection_code {
                self.record_rejection(code);
                self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                    populate_signal_snapshot_event(
                        event,
                        claim_side,
                        mode,
                        priced.fair_prob_yes,
                        entry_quote.avg_price,
                        decision.net_session_edge_bps,
                        selected_target,
                        session_edge.timeout_exit_price,
                        session_edge.timeout_loss_estimate_usd,
                        decision.pulse_score_bps,
                        claim_price_move_bps,
                        fair_claim_move_bps,
                        cex_mid_move_bps,
                        entry_quote.filled_notional_usd,
                        swept_levels_count,
                        post_pulse_depth_gap_bps,
                        &reachability,
                        &recovery_observation.quality,
                    );
                    event.admission_result = AuditGateResult::Rejected;
                    event.rejection_reason = Some(code.as_str().to_owned());
                });
            } else {
                self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
                    populate_signal_snapshot_event(
                        event,
                        claim_side,
                        mode,
                        priced.fair_prob_yes,
                        entry_quote.avg_price,
                        decision.net_session_edge_bps,
                        selected_target,
                        session_edge.timeout_exit_price,
                        session_edge.timeout_loss_estimate_usd,
                        decision.pulse_score_bps,
                        claim_price_move_bps,
                        fair_claim_move_bps,
                        cex_mid_move_bps,
                        entry_quote.filled_notional_usd,
                        swept_levels_count,
                        post_pulse_depth_gap_bps,
                        &reachability,
                        &recovery_observation.quality,
                    );
                    event.admission_result = AuditGateResult::Rejected;
                    event.rejection_reason = Some("detector_rejected".to_owned());
                });
            }
            return None;
        }

        let base_hit_rate = estimate_base_hit_rate(
            selected_target.target_distance_to_mid_bps,
            entry_quote.filled_notional_usd,
            swept_levels_count,
            post_pulse_depth_gap_bps,
            entry_quote.depth_fill_ratio,
        );

        self.record_signal_snapshot_if_enabled(audit_ctx, |event| {
            populate_signal_snapshot_event(
                event,
                claim_side,
                mode,
                priced.fair_prob_yes,
                entry_quote.avg_price,
                decision.net_session_edge_bps,
                selected_target,
                session_edge.timeout_exit_price,
                session_edge.timeout_loss_estimate_usd,
                decision.pulse_score_bps,
                claim_price_move_bps,
                fair_claim_move_bps,
                cex_mid_move_bps,
                entry_quote.filled_notional_usd,
                swept_levels_count,
                post_pulse_depth_gap_bps,
                &reachability,
                &recovery_observation.quality,
            );
            event.admission_result = AuditGateResult::Passed;
        });

        Some(EntryCandidate {
            market: market.clone(),
            claim_side,
            mode: selected_target.mode,
            entry_price: entry_quote.avg_price,
            target_exit_price: selected_target.target_exit_price,
            timeout_exit_price: session_edge.timeout_exit_price,
            timeout_secs: selected_target.timeout_secs,
            net_edge_bps: decision.net_session_edge_bps,
            expected_net_pnl_usd: selected_target.maker_net_pnl_usd,
            timeout_loss_estimate_usd: session_edge.timeout_loss_estimate_usd,
            required_hit_rate: decision.required_hit_rate,
            realizable_ev_usd: selected_target.realizable_ev_usd,
            min_profitable_target_distance_bps: reachability.min_profitable_target_distance_bps,
            target_distance_to_mid_bps: selected_target.target_distance_to_mid_bps,
            pulse_score_bps: decision.pulse_score_bps,
            available_reversion_ticks: pocket.available_ticks,
            entry_executable_notional_usd: entry_quote.filled_notional_usd,
            reversion_pocket_notional_usd: pocket.pocket_notional_usd,
            vacuum_ratio: pocket.vacuum_ratio,
            event_delta_yes: priced.event_delta_yes,
            anchor_latency_delta_ms,
            fair_prob_yes: priced.fair_prob_yes,
            base_hit_rate,
            predicted_hit_rate: selected_target.predicted_hit_rate,
            observation_quality_score: recovery_observation.quality.observation_quality_score,
        })
    }

    fn summarize_recovery_observation_for_claim(
        &self,
        market: &MarketConfig,
        claim_side: TokenSide,
        claim_book: &OrderBookSnapshot,
    ) -> crate::signal::RecoveryObservation {
        let pulse_exchange_ts_ms = book_signal_timestamp_ms(claim_book);
        let pulse_price = best_ask(Some(claim_book)).unwrap_or(Decimal::ZERO);
        let Some(tape) = self.signal_tapes.get(&market.symbol.0) else {
            return summarize_recovery_observation(&[], pulse_exchange_ts_ms, pulse_price);
        };
        let claim_samples = match claim_side {
            TokenSide::Yes => tape.yes_ask.iter().copied().collect::<Vec<_>>(),
            TokenSide::No => tape.no_ask.iter().copied().collect::<Vec<_>>(),
        };
        summarize_recovery_observation(&claim_samples, pulse_exchange_ts_ms, pulse_price)
    }

    fn record_rejection(&mut self, code: crate::model::PulseFailureCode) {
        self.stats.signals_rejected = self.stats.signals_rejected.saturating_add(1);
        let entry = self
            .stats
            .rejection_reasons
            .entry(code.as_str().to_owned())
            .or_insert(0);
        *entry = entry.saturating_add(1);
    }

    fn refresh_asset_health(&mut self, asset: PulseAsset, now_ms: u64) {
        let anchor_snapshot = self.anchor_snapshot_for(
            asset,
            next_market_settlement_ts_ms(self.markets_by_asset.get(&asset), now_ms),
        );
        let anchor_age_limit = self
            .max_anchor_age_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        let anchor_latency_delta_limit_ms = self
            .max_anchor_latency_delta_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        let (poly_quote_age_ms, cex_quote_age_ms) = freshest_asset_book_ages(
            self.markets_by_asset.get(&asset),
            &self.observed_books,
            now_ms,
        );
        let open_sessions = self
            .active_sessions
            .values()
            .filter(|managed| managed.session.asset() == asset)
            .count();
        let net_target_delta = self
            .hedge_aggregator
            .sessions
            .values()
            .filter(|intent| intent.asset == asset)
            .fold(Decimal::ZERO, |acc, intent| {
                acc + intent.target_delta_exposure
            });
        let actual_exchange_position = self.actual_hedge_position(asset);
        let provider_id = self
            .anchor_providers
            .get(&asset)
            .map(|provider| provider.provider_id().to_owned());
        let (anchor_age_ms, anchor_latency_delta_ms) = anchor_snapshot
            .as_ref()
            .map(|snapshot| {
                let latency = now_ms.saturating_sub(snapshot.ts_ms);
                (Some(snapshot.quality.anchor_age_ms), Some(latency))
            })
            .unwrap_or((None, None));
        let (status, disable_reason) = if !self.transport_is_healthy(Exchange::Polymarket, now_ms) {
            ("degraded", Some("poly_transport_stale".to_owned()))
        } else if !self.transport_is_healthy(
            asset_hedge_exchange(self.markets_by_asset.get(&asset)),
            now_ms,
        ) {
            ("degraded", Some("hedge_transport_stale".to_owned()))
        } else {
            asset_status(
                self.markets_by_asset.get(&asset),
                anchor_snapshot.as_ref(),
                anchor_age_limit,
                anchor_latency_delta_limit_ms,
                poly_quote_age_ms,
                cex_quote_age_ms,
                self.settings
                    .strategy
                    .market_data
                    .resolved_poly_open_max_quote_age_ms(),
                self.settings
                    .strategy
                    .market_data
                    .resolved_cex_open_max_quote_age_ms(),
                now_ms,
                open_sessions,
                net_target_delta,
                actual_exchange_position,
                asset_hedge_qty_step(self.markets_by_asset.get(&asset)),
            )
        };

        let row = PulseAssetHealthRow {
            asset: asset.as_str().to_owned(),
            provider_id: provider_id.clone(),
            anchor_age_ms,
            anchor_latency_delta_ms,
            poly_quote_age_ms,
            cex_quote_age_ms,
            open_sessions,
            net_target_delta: net_target_delta.to_f64(),
            actual_exchange_position: actual_exchange_position.to_f64(),
            status: Some(status.to_owned()),
            disable_reason: disable_reason.clone(),
        };
        self.audit_sink
            .record_asset_health_lazy(|| PulseAssetHealthAuditEvent {
                asset: row.asset.clone(),
                provider_id,
                anchor_age_ms,
                anchor_latency_delta_ms,
                poly_quote_age_ms,
                cex_quote_age_ms,
                open_sessions,
                net_target_delta: row.net_target_delta.map(|value| value.to_string()),
                actual_exchange_position: row
                    .actual_exchange_position
                    .map(|value| value.to_string()),
                status: row.status.clone(),
                disable_reason,
            });
        self.asset_health.insert(asset, row);
    }

    fn record_signal_snapshot_if_enabled<F>(&mut self, ctx: &SignalAuditContext<'_>, build: F)
    where
        F: FnOnce(&mut PulseSignalSnapshotAuditEvent),
    {
        if !self.audit_sink.captures_high_frequency() {
            return;
        }
        let mut event = self.base_signal_snapshot_event(ctx);
        build(&mut event);
        self.audit_sink.record_signal_snapshot(event);
    }

    fn refresh_monitor_view(&mut self, now_ms: u64) {
        let mut active_sessions = self
            .active_sessions
            .values()
            .map(|managed| PulseSessionMonitorRow {
                session_id: managed.session.session_id().to_owned(),
                asset: managed.session.asset().as_str().to_owned(),
                state: pulse_session_state_label(managed.session.state()).to_owned(),
                remaining_secs: managed.deadline_ms.saturating_sub(now_ms) / 1_000,
                net_edge_bps: managed.net_edge_bps,
            })
            .collect::<Vec<_>>();
        active_sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        let mut asset_health = self.asset_health.values().cloned().collect::<Vec<_>>();
        asset_health.sort_by(|left, right| left.asset.cmp(&right.asset));
        let mut markets = Vec::new();
        for asset in &self.enabled_assets {
            if let Some(items) = self.markets_by_asset.get(asset) {
                for market in items {
                    markets.push(self.pulse_market_monitor_row(*asset, market, now_ms));
                }
            }
        }
        markets.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        let selected_session = self.active_sessions.values().next().map(|managed| {
            runtime_session_detail_view(
                managed,
                now_ms,
                self.books_for_symbol(&managed.market.symbol),
            )
        });
        self.last_monitor_view = Some(build_pulse_monitor_view(
            active_sessions,
            asset_health,
            markets,
            selected_session,
        ));
    }

    fn pulse_market_monitor_row(
        &self,
        asset: PulseAsset,
        market: &MarketConfig,
        now_ms: u64,
    ) -> PulseMarketMonitorRow {
        let books = self.books_for_symbol(&market.symbol);
        let yes_book = books.and_then(|books| books.yes.as_ref());
        let no_book = books.and_then(|books| books.no.as_ref());
        let cex_book = books.and_then(|books| books.cex.as_ref());
        let poly_yes_price = yes_book
            .and_then(mid_price_from_book)
            .and_then(|value| value.to_f64());
        let poly_no_price = no_book
            .and_then(mid_price_from_book)
            .and_then(|value| value.to_f64());
        let cex_price = cex_book
            .and_then(mid_price_from_book)
            .and_then(|value| value.to_f64());
        let poly_quote_age_ms = [yes_book, no_book]
            .into_iter()
            .flatten()
            .map(|book| now_ms.saturating_sub(book.received_at_ms))
            .min();
        let cex_quote_age_ms = cex_book.map(|book| now_ms.saturating_sub(book.received_at_ms));
        let anchor = self.anchor_snapshot_for(asset, Some(market.settlement_timestamp * 1_000));
        let anchor_age_ms = anchor
            .as_ref()
            .map(|snapshot| snapshot.quality.anchor_age_ms);
        let anchor_latency_delta_ms = anchor
            .as_ref()
            .map(|snapshot| now_ms.saturating_sub(snapshot.ts_ms));
        let open_sessions = self
            .active_sessions
            .values()
            .filter(|managed| managed.market.symbol == market.symbol)
            .count();
        let (status, disable_reason) = self.monitor_market_status(
            asset,
            market,
            anchor.as_ref(),
            poly_quote_age_ms,
            cex_quote_age_ms,
            now_ms,
        );
        let signal = self.monitor_market_signal(asset, market, now_ms, anchor.as_ref(), books);

        PulseMarketMonitorRow {
            symbol: market.symbol.0.clone(),
            asset: asset.as_str().to_owned(),
            claim_side: signal.as_ref().map(|snapshot| match snapshot.claim_side {
                TokenSide::Yes => "YES".to_owned(),
                TokenSide::No => "NO".to_owned(),
            }),
            pulse_score_bps: signal.as_ref().map(|snapshot| snapshot.pulse_score_bps),
            net_edge_bps: signal.as_ref().map(|snapshot| snapshot.net_edge_bps),
            poly_yes_price,
            poly_no_price,
            cex_price,
            fair_prob_yes: signal.as_ref().map(|snapshot| snapshot.fair_prob_yes),
            entry_price: signal
                .as_ref()
                .and_then(|snapshot| snapshot.entry_price.to_f64()),
            target_exit_price: signal
                .as_ref()
                .and_then(|snapshot| snapshot.target_exit_price.to_f64()),
            timeout_exit_price: signal
                .as_ref()
                .and_then(|snapshot| snapshot.timeout_exit_price.to_f64()),
            expected_net_pnl_usd: signal
                .as_ref()
                .and_then(|snapshot| snapshot.expected_net_pnl_usd.to_f64()),
            timeout_loss_estimate_usd: signal
                .as_ref()
                .and_then(|snapshot| snapshot.timeout_loss_estimate_usd.to_f64()),
            required_hit_rate: signal.as_ref().map(|snapshot| snapshot.required_hit_rate),
            reversion_pocket_ticks: signal
                .as_ref()
                .map(|snapshot| snapshot.reversion_pocket_ticks),
            reversion_pocket_notional_usd: signal
                .as_ref()
                .and_then(|snapshot| snapshot.reversion_pocket_notional_usd.to_f64()),
            vacuum_ratio: signal
                .as_ref()
                .and_then(|snapshot| snapshot.vacuum_ratio.to_f64()),
            anchor_age_ms,
            anchor_latency_delta_ms,
            poly_quote_age_ms,
            cex_quote_age_ms,
            open_sessions,
            status,
            disable_reason,
        }
    }

    fn monitor_market_view(
        &self,
        asset: PulseAsset,
        market: &MarketConfig,
        now_ms: u64,
    ) -> MarketView {
        let pulse_row = self.pulse_market_monitor_row(asset, market, now_ms);
        MarketView {
            symbol: pulse_row.symbol.clone(),
            z_score: pulse_row.pulse_score_bps.map(|value| value / 100.0),
            basis_pct: pulse_row.net_edge_bps.map(|value| value / 100.0),
            poly_price: pulse_row
                .claim_side
                .as_deref()
                .and_then(|side| match side {
                    "YES" => pulse_row.poly_yes_price,
                    "NO" => pulse_row.poly_no_price,
                    _ => None,
                })
                .or(pulse_row.poly_yes_price)
                .or(pulse_row.poly_no_price),
            poly_yes_price: pulse_row.poly_yes_price,
            poly_no_price: pulse_row.poly_no_price,
            cex_price: pulse_row.cex_price,
            fair_value: pulse_row.fair_prob_yes,
            has_position: pulse_row.open_sessions > 0,
            minutes_to_expiry: Some(
                market.settlement_timestamp.saturating_sub(now_ms / 1_000) as f64 / 60.0,
            ),
            basis_history_len: 0,
            raw_sigma: None,
            effective_sigma: None,
            sigma_source: pulse_row
                .status
                .is_empty()
                .then_some(String::new())
                .or_else(|| Some("pulse".to_owned())),
            returns_window_len: 0,
            active_token_side: pulse_row.claim_side.clone(),
            poly_updated_at_ms: pulse_row
                .poly_quote_age_ms
                .map(|age_ms| now_ms.saturating_sub(age_ms)),
            cex_updated_at_ms: pulse_row
                .cex_quote_age_ms
                .map(|age_ms| now_ms.saturating_sub(age_ms)),
            poly_quote_age_ms: pulse_row.poly_quote_age_ms,
            cex_quote_age_ms: pulse_row.cex_quote_age_ms,
            cross_leg_skew_ms: pulse_row
                .poly_quote_age_ms
                .zip(pulse_row.cex_quote_age_ms)
                .map(|(poly_age, cex_age)| poly_age.abs_diff(cex_age)),
            evaluable_status: if pulse_row.status == "ready" {
                polyalpha_core::EvaluableStatus::Evaluable
            } else if pulse_row.disable_reason.as_deref() == Some("poly_quote_stale") {
                polyalpha_core::EvaluableStatus::PolyQuoteStale
            } else if pulse_row.disable_reason.as_deref() == Some("cex_quote_stale") {
                polyalpha_core::EvaluableStatus::CexQuoteStale
            } else if pulse_row.disable_reason.as_deref() == Some("waiting_books") {
                polyalpha_core::EvaluableStatus::NotEvaluableNoPoly
            } else {
                polyalpha_core::EvaluableStatus::Unknown
            },
            async_classification: polyalpha_core::AsyncClassification::default(),
            market_tier: polyalpha_core::MarketTier::Observation,
            retention_reason: if pulse_row.open_sessions > 0 {
                polyalpha_core::MarketRetentionReason::HasPosition
            } else {
                polyalpha_core::MarketRetentionReason::None
            },
            last_focus_at_ms: None,
            last_tradeable_at_ms: None,
        }
    }

    fn monitor_market_signal(
        &self,
        asset: PulseAsset,
        market: &MarketConfig,
        now_ms: u64,
        anchor: Option<&AnchorSnapshot>,
        books: Option<&ObservedMarketBooks>,
    ) -> Option<PulseMarketSignalSnapshot> {
        let anchor = anchor?;
        let (Some(yes_book), Some(no_book), Some(cex_book)) = (
            books.and_then(|books| books.yes.as_ref()),
            books.and_then(|books| books.no.as_ref()),
            books.and_then(|books| books.cex.as_ref()),
        ) else {
            return None;
        };
        if !self.transport_is_healthy(Exchange::Polymarket, now_ms)
            || !self.transport_is_healthy(market.hedge_exchange, now_ms)
        {
            return None;
        }
        let market_rule = market.resolved_market_rule()?;
        let priced = self
            .pricer
            .price(anchor, &market_rule, market.settlement_timestamp * 1_000)
            .ok()?;
        let pulse_window_ms = self.settings.strategy.pulse_arb.entry.pulse_window_ms;
        let current_cex_mid = mid_price_from_book(cex_book).unwrap_or(Decimal::ZERO);
        let prior_tape = self.signal_tapes.get(&market.symbol.0);
        let yes_book_ts_ms = book_signal_timestamp_ms(yes_book);
        let no_book_ts_ms = book_signal_timestamp_ms(no_book);
        let cex_book_ts_ms = book_signal_timestamp_ms(cex_book);
        let prior_yes_ask = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.yes_ask, yes_book_ts_ms, pulse_window_ms));
        let prior_no_ask = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.no_ask, no_book_ts_ms, pulse_window_ms));
        let prior_cex_mid = prior_tape
            .and_then(|tape| latest_decimal_before(&tape.cex_mid, cex_book_ts_ms, pulse_window_ms));
        let prior_fair_prob_yes = prior_tape
            .and_then(|tape| latest_float_before(&tape.fair_prob_yes, now_ms, pulse_window_ms));
        let selected_poly_leg_max_age_ms = self.selected_poly_leg_max_age_ms();
        let yes_fresh = quote_is_fresh(now_ms, yes_book, selected_poly_leg_max_age_ms);
        let no_fresh = quote_is_fresh(now_ms, no_book, selected_poly_leg_max_age_ms);
        let cex_fresh = quote_is_fresh(
            now_ms,
            cex_book,
            self.settings
                .strategy
                .market_data
                .resolved_cex_open_max_quote_age_ms(),
        );
        let anchor_age_limit = self
            .max_anchor_age_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        let anchor_quality_ok = anchor.quality.anchor_age_ms <= anchor_age_limit
            && anchor.quality.has_strike_coverage
            && anchor.quality.has_liquidity
            && anchor.quality.greeks_complete;
        let opening_request_notional = self
            .settings
            .strategy
            .pulse_arb
            .session
            .effective_opening_request_notional_usd()
            .0;
        let build_snapshot = |claim_side: TokenSide,
                              claim_book: &OrderBookSnapshot,
                              prior_claim_ask: Option<Decimal>,
                              claim_fresh: bool|
         -> Option<PulseMarketSignalSnapshot> {
            if !claim_fresh || !cex_fresh {
                return None;
            }
            let entry_quote = executable_poly_quote(
                claim_book,
                ExecutableQuoteSide::Buy,
                opening_request_notional,
            )?;
            if entry_quote.filled_notional_usd <= Decimal::ZERO
                || entry_quote.avg_price <= Decimal::ZERO
            {
                return None;
            }
            let fair_claim_prob = match claim_side {
                TokenSide::Yes => priced.fair_prob_yes,
                TokenSide::No => priced.fair_prob_no,
            };
            let fair_claim_price =
                Decimal::from_f64_retain(fair_claim_prob).unwrap_or(Decimal::ZERO);
            let pocket = reversion_pocket(
                entry_quote.avg_price,
                prior_claim_ask.unwrap_or(entry_quote.avg_price),
                market.min_tick_size.0,
                entry_quote.filled_notional_usd,
            );
            let claim_price_move_bps = prior_claim_ask
                .and_then(|prior| {
                    if prior <= Decimal::ZERO || prior <= entry_quote.avg_price {
                        Some(0.0)
                    } else {
                        (((prior - entry_quote.avg_price) / prior) * Decimal::from(10_000)).to_f64()
                    }
                })
                .unwrap_or(0.0);
            let fair_claim_move_bps = prior_fair_prob_yes
                .map(|prior_yes| match claim_side {
                    TokenSide::Yes => (priced.fair_prob_yes - prior_yes).abs() * 10_000.0,
                    TokenSide::No => (priced.fair_prob_no - (1.0 - prior_yes)).abs() * 10_000.0,
                })
                .unwrap_or(0.0);
            let cex_mid_move_bps = prior_cex_mid
                .and_then(|prior| {
                    if prior <= Decimal::ZERO {
                        Some(0.0)
                    } else {
                        (((current_cex_mid - prior).abs() / prior) * Decimal::from(10_000)).to_f64()
                    }
                })
                .unwrap_or(0.0);
            let raw_pulse_score_bps = claim_price_move_bps - fair_claim_move_bps;
            let ladder_target = recommended_exit_price_for_settings(
                &self.settings,
                entry_quote.avg_price,
                raw_pulse_score_bps,
                pocket.available_ticks,
                market.min_tick_size.0,
            );
            let target_exit_price = ladder_target
                .min(pocket.target_price.max(entry_quote.avg_price))
                .min(fair_claim_price.max(entry_quote.avg_price))
                .min(Decimal::ONE);
            let timeout_exit_price =
                timeout_exit_price_for_shares(claim_book, entry_quote.filled_shares)
                    .unwrap_or(Decimal::ZERO);
            let session_edge = estimate_session_edge(
                entry_quote.filled_notional_usd,
                entry_quote.avg_price,
                target_exit_price,
                timeout_exit_price,
                self.settings.paper_slippage.cex_slippage_bps as f64,
                (self.settings.execution_costs.poly_fee_bps
                    + self.settings.execution_costs.cex_taker_fee_bps) as f64,
                self.settings.execution_costs.cex_taker_fee_bps as f64
                    + self.settings.execution_costs.poly_fee_bps as f64 / 2.0,
            );
            let has_pulse_history = prior_claim_ask.is_some()
                && prior_cex_mid.is_some()
                && prior_fair_prob_yes.is_some();
            let instant_basis_bps = ((fair_claim_price - entry_quote.avg_price)
                * Decimal::from(10_000))
            .to_f64()
            .unwrap_or_default();
            let decision = self.detector.evaluate(crate::model::PulseOpportunityInput {
                instant_basis_bps,
                poly_vwap_slippage_bps: entry_quote.slippage_bps,
                hedge_slippage_bps: self.settings.paper_slippage.cex_slippage_bps as f64,
                fee_bps: (self.settings.execution_costs.poly_fee_bps
                    + self.settings.execution_costs.cex_taker_fee_bps)
                    as f64,
                perp_basis_penalty_bps: 0.0,
                rehedge_reserve_bps: self.settings.execution_costs.cex_taker_fee_bps as f64,
                timeout_exit_reserve_bps: self.settings.execution_costs.poly_fee_bps as f64 / 2.0,
                expected_net_edge_bps: session_edge.expected_net_edge_bps,
                expected_net_pnl_usd: session_edge.expected_net_pnl_usd,
                timeout_loss_estimate_usd: session_edge.timeout_loss_estimate_usd,
                min_profitable_target_distance_bps: 0.0,
                target_distance_to_mid_bps: Some(0.0),
                predicted_hit_rate: Some(0.0),
                realizable_ev_usd: Some(Decimal::ZERO),
                reversion_pocket_ticks: pocket.available_ticks,
                vacuum_ratio: pocket.vacuum_ratio,
                anchor_quality_ok,
                pricing_quality_ok: priced.pricing_quality.delta_stable,
                data_fresh: claim_fresh && cex_fresh,
                has_pulse_history,
                claim_price_move_bps,
                fair_claim_move_bps,
                cex_mid_move_bps,
            });
            if !decision.should_trade {
                return None;
            }

            Some(PulseMarketSignalSnapshot {
                claim_side,
                mode: PulseMode::ElasticSnapback,
                pulse_score_bps: decision.pulse_score_bps,
                net_edge_bps: decision.net_session_edge_bps,
                fair_prob_yes: priced.fair_prob_yes,
                entry_price: entry_quote.avg_price,
                target_exit_price,
                timeout_exit_price,
                timeout_secs: self.settings.strategy.pulse_arb.session.max_holding_secs,
                expected_net_pnl_usd: decision.expected_net_pnl_usd,
                timeout_loss_estimate_usd: session_edge.timeout_loss_estimate_usd,
                required_hit_rate: decision.required_hit_rate,
                predicted_hit_rate: 0.0,
                realizable_ev_usd: Decimal::ZERO,
                min_profitable_target_distance_bps: 0.0,
                target_distance_to_mid_bps: 0.0,
                reversion_pocket_ticks: pocket.available_ticks,
                reversion_pocket_notional_usd: pocket.pocket_notional_usd,
                vacuum_ratio: pocket.vacuum_ratio,
            })
        };

        match (
            build_snapshot(TokenSide::Yes, yes_book, prior_yes_ask, yes_fresh),
            build_snapshot(TokenSide::No, no_book, prior_no_ask, no_fresh),
        ) {
            (Some(yes), Some(no)) => {
                if yes.expected_net_pnl_usd > no.expected_net_pnl_usd
                    || (yes.expected_net_pnl_usd == no.expected_net_pnl_usd
                        && (yes.net_edge_bps > no.net_edge_bps
                            || (yes.net_edge_bps == no.net_edge_bps
                                && yes.pulse_score_bps >= no.pulse_score_bps)))
                {
                    Some(yes)
                } else {
                    Some(no)
                }
            }
            (Some(snapshot), None) | (None, Some(snapshot)) => Some(snapshot),
            (None, None) => None,
        }
    }

    fn monitor_market_status(
        &self,
        asset: PulseAsset,
        market: &MarketConfig,
        anchor: Option<&AnchorSnapshot>,
        poly_quote_age_ms: Option<u64>,
        cex_quote_age_ms: Option<u64>,
        now_ms: u64,
    ) -> (String, Option<String>) {
        let Some(anchor) = anchor else {
            return (
                "waiting_anchor".to_owned(),
                Some("waiting_anchor".to_owned()),
            );
        };
        if !self.transport_is_healthy(Exchange::Polymarket, now_ms) {
            return (
                "degraded".to_owned(),
                Some("poly_transport_stale".to_owned()),
            );
        }
        if !self.transport_is_healthy(market.hedge_exchange, now_ms) {
            return (
                "degraded".to_owned(),
                Some("hedge_transport_stale".to_owned()),
            );
        }
        if poly_quote_age_ms.is_none() || cex_quote_age_ms.is_none() {
            return ("waiting_books".to_owned(), Some("waiting_books".to_owned()));
        }
        if !anchor.quality.has_strike_coverage
            || !anchor.quality.has_liquidity
            || !anchor.quality.greeks_complete
        {
            return (
                "degraded".to_owned(),
                Some("anchor_surface_insufficient".to_owned()),
            );
        }
        let anchor_age_limit = self
            .max_anchor_age_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        if anchor.quality.anchor_age_ms > anchor_age_limit {
            return ("degraded".to_owned(), Some("anchor_stale".to_owned()));
        }
        let anchor_latency_delta_limit_ms = self
            .max_anchor_latency_delta_ms
            .get(&asset)
            .copied()
            .unwrap_or(u64::MAX);
        if now_ms.saturating_sub(anchor.ts_ms) > anchor_latency_delta_limit_ms {
            return (
                "degraded".to_owned(),
                Some("anchor_latency_delta_high".to_owned()),
            );
        }
        if poly_quote_age_ms.unwrap_or_default() > self.selected_poly_leg_max_age_ms() {
            return ("degraded".to_owned(), Some("poly_quote_stale".to_owned()));
        }
        if cex_quote_age_ms.unwrap_or_default()
            > self
                .settings
                .strategy
                .market_data
                .resolved_cex_open_max_quote_age_ms()
        {
            return ("degraded".to_owned(), Some("cex_quote_stale".to_owned()));
        }
        if market.settlement_timestamp.saturating_sub(now_ms / 1_000) == 0 {
            return ("expired".to_owned(), Some("expired".to_owned()));
        }
        ("ready".to_owned(), None)
    }

    fn anchor_snapshot_for(
        &self,
        asset: PulseAsset,
        target_event_expiry_ts_ms: Option<u64>,
    ) -> Option<AnchorSnapshot> {
        self.anchor_providers.get(&asset).and_then(|provider| {
            provider
                .snapshot_for_target(asset, target_event_expiry_ts_ms)
                .ok()
                .flatten()
        })
    }

    fn record_session_lifecycle(&mut self, session_id: &str) -> std::result::Result<(), String> {
        let (asset, symbol) = {
            let managed = self
                .active_sessions
                .get(session_id)
                .ok_or_else(|| format!("missing managed session `{session_id}`"))?;
            (managed.session.asset(), managed.market.symbol.clone())
        };
        let books = self.books_for_symbol(&symbol).cloned();
        let actual_position = self
            .hedge_aggregator
            .actual_positions
            .get(&asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let managed = self
            .active_sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("missing managed session `{session_id}`"))?;
        self.audit_sink.record_lifecycle(runtime_lifecycle_event(
            managed,
            actual_position,
            books.as_ref(),
        ));
        Ok(())
    }

    fn record_managed_lifecycle(&mut self, managed: &ManagedSession) {
        let actual_position = self.actual_hedge_position(managed.session.asset());
        let books = self.books_for_symbol(&managed.market.symbol).cloned();
        self.audit_sink.record_lifecycle(runtime_lifecycle_event(
            managed,
            actual_position,
            books.as_ref(),
        ));
    }

    fn finalize_managed_session(
        &mut self,
        managed: ManagedSession,
        now_ms: u64,
        exit_price: Decimal,
        deadline_exit_triggered: bool,
        summary_allocated_hedge_qty: Decimal,
        exit_path_override: Option<&'static str>,
    ) {
        let session_id = managed.session.session_id().to_owned();
        let poly_realized_pnl =
            (exit_price - managed.entry_price) * managed.session.actual_poly_filled_qty();
        let realized_pnl = poly_realized_pnl + managed.hedge_realized_pnl_usd;
        let audit_event_count = self.audit_sink.audit_event_count_for_session(&session_id) + 1;
        let books = self.books_for_symbol(&managed.market.symbol).cloned();
        let distance_to_mid_bps = current_distance_to_mid_bps(&managed, books.as_ref());
        let relative_order_age_ms = Some(now_ms.saturating_sub(managed.maker_order_updated_at_ms));
        let exit_path = exit_path_override
            .map(str::to_owned)
            .or_else(|| runtime_exit_path_for_summary(&managed));
        let (target_exit_price, final_exit_price) =
            if exit_path.as_deref() == Some("opening_rejected") {
                (None, None)
            } else {
                (
                    Some(managed.target_exit_price.normalize().to_string()),
                    Some(exit_price.normalize().to_string()),
                )
            };
        self.audit_sink.finalize_session(
            PulseSessionAuditSummary {
                session_id: session_id.clone(),
                final_state: "closed".to_owned(),
                deadline_exit_triggered,
                audit_event_count,
            },
            PulseSessionSummaryRow {
                pulse_session_id: session_id,
                asset: managed.session.asset().as_str().to_owned(),
                state: "closed".to_owned(),
                opened_at_ms: managed.opened_at_ms,
                closed_at_ms: Some(now_ms),
                planned_poly_qty: managed.session.planned_poly_qty().normalize().to_string(),
                actual_poly_filled_qty: managed
                    .session
                    .actual_poly_filled_qty()
                    .normalize()
                    .to_string(),
                actual_poly_fill_ratio: managed.session.actual_poly_fill_ratio(),
                entry_price: Some(managed.entry_price.normalize().to_string()),
                actual_fill_notional_usd: managed.actual_fill_notional_usd.normalize().to_string(),
                candidate_expected_net_pnl_usd: Some(
                    managed
                        .candidate_expected_net_pnl_usd
                        .normalize()
                        .to_string(),
                ),
                expected_open_net_pnl_usd: managed
                    .expected_open_net_pnl_usd
                    .normalize()
                    .to_string(),
                timeout_loss_estimate_usd: Some(
                    managed.timeout_loss_estimate_usd.normalize().to_string(),
                ),
                required_hit_rate: Some(managed.required_hit_rate),
                pulse_score_bps: Some(managed.pulse_score_bps),
                effective_open: managed.effective_open,
                opening_outcome: managed.opening_outcome.clone(),
                opening_rejection_reason: managed.opening_rejection_reason.clone(),
                opening_allocated_hedge_qty: managed
                    .opening_allocated_hedge_qty
                    .normalize()
                    .to_string(),
                session_target_delta_exposure: managed
                    .session
                    .target_delta_exposure()
                    .normalize()
                    .to_string(),
                session_allocated_hedge_qty: summary_allocated_hedge_qty.normalize().to_string(),
                net_edge_bps: Some(managed.net_edge_bps),
                realized_pnl_usd: Some(realized_pnl.to_f64().unwrap_or_default()),
                exit_path,
                target_exit_price,
                final_exit_price,
                timeout_exit_price: Some(managed.timeout_exit_price.normalize().to_string()),
                entry_executable_notional_usd: Some(
                    managed
                        .entry_executable_notional_usd
                        .normalize()
                        .to_string(),
                ),
                reversion_pocket_ticks: Some(managed.available_reversion_ticks),
                reversion_pocket_notional_usd: Some(
                    managed
                        .reversion_pocket_notional_usd
                        .normalize()
                        .to_string(),
                ),
                vacuum_ratio: Some(managed.vacuum_ratio.normalize().to_string()),
                anchor_latency_delta_ms: Some(managed.anchor_latency_delta_ms),
                distance_to_mid_bps,
                relative_order_age_ms,
            },
        );
    }

    fn record_anchor_market_tape_if_needed(&mut self, asset: PulseAsset, anchor: &AnchorSnapshot) {
        if !self.audit_sink.captures_high_frequency() {
            return;
        }
        let already_recorded = self
            .last_anchor_tape_ts_by_asset
            .get(&asset)
            .copied()
            .is_some_and(|ts_ms| ts_ms == anchor.ts_ms);
        if already_recorded {
            return;
        }

        let approx_recv_ms = anchor.ts_ms.saturating_add(anchor.quality.anchor_age_ms);
        for point in &anchor.local_surface_points {
            self.audit_sink
                .record_market_tape_lazy(|| PulseMarketTapeAuditEvent {
                    asset: asset.as_str().to_owned(),
                    symbol: asset.as_str().to_owned(),
                    venue: "Deribit".to_owned(),
                    instrument: point.instrument_name.clone(),
                    token_side: None,
                    ts_exchange_ms: anchor.ts_ms,
                    ts_recv_ms: approx_recv_ms,
                    sequence: anchor.ts_ms,
                    update_kind: "anchor_snapshot".to_owned(),
                    top_n_depth: 0,
                    best_bid: point.best_bid.map(decimal_to_audit_string),
                    best_ask: point.best_ask.map(decimal_to_audit_string),
                    mid: point
                        .best_bid
                        .zip(point.best_ask)
                        .map(|(bid, ask)| decimal_to_audit_string((bid + ask) / Decimal::TWO)),
                    last_trade_price: None,
                    expiry_ts_ms: Some(point.expiry_ts_ms),
                    strike: Some(decimal_to_audit_string(point.strike)),
                    option_type: deribit_option_type_label(&point.instrument_name),
                    mark_iv: Some(point.mark_iv),
                    bid_iv: point.bid_iv,
                    ask_iv: point.ask_iv,
                    delta: point.delta,
                    gamma: point.gamma,
                    index_price: Some(decimal_to_audit_string(anchor.index_price)),
                    delta_bids: Vec::new(),
                    delta_asks: Vec::new(),
                    snapshot_bids: Vec::new(),
                    snapshot_asks: Vec::new(),
                });
        }
        self.last_anchor_tape_ts_by_asset
            .insert(asset, anchor.ts_ms);
    }

    fn base_signal_snapshot_event(
        &self,
        ctx: &SignalAuditContext<'_>,
    ) -> PulseSignalSnapshotAuditEvent {
        PulseSignalSnapshotAuditEvent {
            asset: ctx.asset.as_str().to_owned(),
            symbol: ctx.market.symbol.0.clone(),
            mode_candidate: PulseSignalMode::ElasticSnapback,
            admission_result: AuditGateResult::Bypassed,
            rejection_reason: None,
            provider_id: Some(ctx.anchor.provider_id.clone()),
            claim_side: None,
            fair_prob_yes: None,
            entry_price: None,
            net_edge_bps: None,
            expected_net_pnl_usd: None,
            target_exit_price: None,
            timeout_exit_price: None,
            timeout_loss_estimate_usd: None,
            pulse_score_bps: 0.0,
            claim_price_move_bps: 0.0,
            fair_claim_move_bps: 0.0,
            cex_mid_move_bps: 0.0,
            swept_notional_usd: Decimal::ZERO.to_string(),
            swept_levels_count: 0,
            post_pulse_depth_gap_bps: 0.0,
            min_profitable_target_distance_bps: 0.0,
            reachability_cap_bps: DETECTOR_REACHABILITY_CAP_BPS,
            in_gray_zone: false,
            reachable: false,
            target_distance_to_mid_bps: None,
            predicted_hit_rate: None,
            maker_net_pnl_usd: None,
            timeout_net_pnl_usd: None,
            realizable_ev_usd: None,
            used_exchange_ts: false,
            native_sequence_present: false,
            post_sweep_update_count_5s: 0,
            max_interarrival_gap_ms_5s: 0,
            observation_quality_score: None,
            admission_eligible: false,
            anchor_age_ms: Some(ctx.anchor.quality.anchor_age_ms),
            anchor_latency_delta_ms: Some(ctx.now_ms.saturating_sub(ctx.anchor.ts_ms)),
            anchor_expiry_mismatch_minutes: Some(ctx.anchor.quality.expiry_mismatch_minutes),
            yes_quote_age_ms: Some(ctx.now_ms.saturating_sub(ctx.yes_book.received_at_ms)),
            no_quote_age_ms: Some(ctx.now_ms.saturating_sub(ctx.no_book.received_at_ms)),
            cex_quote_age_ms: Some(ctx.now_ms.saturating_sub(ctx.cex_book.received_at_ms)),
            poly_transport_healthy: Some(ctx.poly_transport_healthy),
            hedge_transport_healthy: Some(ctx.hedge_transport_healthy),
            poly_yes_sequence_ref: Some(ctx.yes_book.sequence),
            poly_no_sequence_ref: Some(ctx.no_book.sequence),
            cex_sequence_ref: Some(ctx.cex_book.sequence),
            anchor_sequence_ref: Some(ctx.anchor.ts_ms),
        }
    }

    fn record_signal_sample_for_snapshot(&mut self, snapshot: &OrderBookSnapshot) {
        let retention_ms = self
            .settings
            .strategy
            .pulse_arb
            .entry
            .pulse_window_ms
            .saturating_mul(4)
            .max(1_000);
        let tape = self
            .signal_tapes
            .entry(snapshot.symbol.0.clone())
            .or_default();
        match snapshot.instrument {
            polyalpha_core::InstrumentKind::PolyYes => {
                if let Some(ask) = best_ask(Some(snapshot)) {
                    push_decimal_sample(
                        &mut tape.yes_ask,
                        snapshot.exchange_timestamp_ms,
                        snapshot.received_at_ms,
                        snapshot.sequence,
                        ask,
                        retention_ms,
                    );
                }
            }
            polyalpha_core::InstrumentKind::PolyNo => {
                if let Some(ask) = best_ask(Some(snapshot)) {
                    push_decimal_sample(
                        &mut tape.no_ask,
                        snapshot.exchange_timestamp_ms,
                        snapshot.received_at_ms,
                        snapshot.sequence,
                        ask,
                        retention_ms,
                    );
                }
            }
            polyalpha_core::InstrumentKind::CexPerp => {
                if let Some(mid) = mid_price_from_book(snapshot) {
                    push_decimal_sample(
                        &mut tape.cex_mid,
                        snapshot.exchange_timestamp_ms,
                        snapshot.received_at_ms,
                        snapshot.sequence,
                        mid,
                        retention_ms,
                    );
                }
            }
        }
    }

    fn record_fair_prob_sample(&mut self, symbol: &Symbol, ts_ms: u64, fair_prob_yes: f64) {
        let retention_ms = self
            .settings
            .strategy
            .pulse_arb
            .entry
            .pulse_window_ms
            .saturating_mul(4)
            .max(1_000);
        let tape = self.signal_tapes.entry(symbol.0.clone()).or_default();
        push_float_sample(
            &mut tape.fair_prob_yes,
            ts_ms,
            ts_ms,
            1,
            fair_prob_yes,
            retention_ms,
        );
    }

    fn books_for_symbol(&self, symbol: &Symbol) -> Option<&ObservedMarketBooks> {
        self.observed_books.get(&symbol.0)
    }

    fn is_pin_risk(
        &self,
        asset: PulseAsset,
        market: &MarketConfig,
        now_ms: u64,
        fair_prob_yes: f64,
    ) -> bool {
        let Some(anchor) =
            self.anchor_snapshot_for(asset, Some(market.settlement_timestamp * 1_000))
        else {
            return false;
        };
        let remaining_secs = market.settlement_timestamp.saturating_sub(now_ms / 1_000);
        if remaining_secs
            > self
                .settings
                .strategy
                .pulse_arb
                .pin_risk
                .pin_risk_time_window_secs
        {
            return false;
        }
        let boundaries = strike_boundaries(market.resolved_market_rule().as_ref());
        let Some(index_price) = anchor.index_price.to_f64() else {
            return false;
        };
        let zone_bps = self.settings.strategy.pulse_arb.pin_risk.pin_risk_zone_bps as f64;
        boundaries.iter().any(|boundary| {
            if *boundary <= 0.0 || fair_prob_yes <= 0.0 || fair_prob_yes >= 1.0 {
                return false;
            }
            ((index_price - boundary).abs() / boundary) * 10_000.0 <= zone_bps
        })
    }
}

pub fn runtime_fixture_for_asset(asset: PulseAsset) -> PulseRuntimeFixture {
    let settings = fixture_settings_for_asset(asset);
    let router = AnchorRouter::from_settings(&settings).expect("build fixture anchor router");
    let anchor_provider = router
        .provider_for_asset(asset)
        .expect("fixture anchor provider");

    PulseRuntimeFixture {
        settings,
        anchor_provider,
        poly_books: vec![PulseBookFeedStub { asset, depth: 5 }],
        binance_books: vec![PulseBookFeedStub { asset, depth: 5 }],
    }
}

fn apply_session_hedge_fill(managed: &mut ManagedSession, outcome: HedgeExecutionOutcome) {
    let Some(fill_price) = outcome.avg_price else {
        return;
    };
    let fill_qty = outcome.filled_qty;
    if fill_qty.is_zero() {
        return;
    }

    if managed.hedge_position_qty.is_zero() {
        managed.hedge_position_qty = fill_qty;
        managed.hedge_avg_entry_price = Some(fill_price);
        return;
    }

    if same_decimal_sign(managed.hedge_position_qty, fill_qty) {
        let current_abs = managed.hedge_position_qty.abs();
        let fill_abs = fill_qty.abs();
        let combined_abs = current_abs + fill_abs;
        let current_avg = managed.hedge_avg_entry_price.unwrap_or(fill_price);
        let weighted_avg = if combined_abs.is_zero() {
            fill_price
        } else {
            ((current_avg * current_abs) + (fill_price * fill_abs)) / combined_abs
        };
        managed.hedge_position_qty += fill_qty;
        managed.hedge_avg_entry_price = Some(weighted_avg);
        return;
    }

    let current_qty = managed.hedge_position_qty;
    let current_abs = current_qty.abs();
    let fill_abs = fill_qty.abs();
    let closing_abs = current_abs.min(fill_abs);
    let avg_entry_price = managed.hedge_avg_entry_price.unwrap_or(fill_price);
    let realized_delta = if current_qty.is_sign_positive() {
        fill_price - avg_entry_price
    } else {
        avg_entry_price - fill_price
    };
    managed.hedge_realized_pnl_usd += realized_delta * closing_abs;

    if fill_abs < current_abs {
        managed.hedge_position_qty += fill_qty;
        return;
    }

    if fill_abs == current_abs {
        managed.hedge_position_qty = Decimal::ZERO;
        managed.hedge_avg_entry_price = None;
        return;
    }

    let residual_qty = fill_qty + current_qty;
    managed.hedge_position_qty = residual_qty;
    managed.hedge_avg_entry_price = Some(fill_price);
}

fn same_decimal_sign(left: Decimal, right: Decimal) -> bool {
    (left.is_sign_positive() && right.is_sign_positive())
        || (left.is_sign_negative() && right.is_sign_negative())
}

fn runtime_exit_path_for_close(
    session_state: PulseSessionState,
    deadline_exit_triggered: bool,
) -> &'static str {
    if session_state == PulseSessionState::EmergencyFlatten {
        "emergency_flatten"
    } else if deadline_exit_triggered {
        "timeout_chase"
    } else {
        "maker_proxy_hit"
    }
}

fn runtime_exit_path_for_summary(managed: &ManagedSession) -> Option<String> {
    if managed.opening_outcome == OPENING_OUTCOME_REJECTED {
        Some("opening_rejected".to_owned())
    } else if managed.opening_outcome == OPENING_OUTCOME_EFFECTIVE_OPEN {
        Some("maker_proxy_hit".to_owned())
    } else {
        None
    }
}

fn enabled_assets_from_settings(settings: &Settings) -> Vec<PulseAsset> {
    let mut assets = settings
        .strategy
        .pulse_arb
        .routing
        .iter()
        .filter(|(_, route)| route.enabled)
        .filter_map(|(asset_key, _)| PulseAsset::from_routing_key(asset_key))
        .collect::<Vec<_>>();
    assets.sort_by_key(|asset| asset.as_str().to_owned());
    assets
}

fn runtime_lifecycle_event(
    managed: &ManagedSession,
    actual_exchange_position: Decimal,
    books: Option<&ObservedMarketBooks>,
) -> PulseLifecycleAuditEvent {
    PulseLifecycleAuditEvent {
        session_id: managed.session.session_id().to_owned(),
        asset: managed.session.asset().as_str().to_owned(),
        state: pulse_session_state_label(managed.session.state()).to_owned(),
        entry_price: Some(managed.entry_price.normalize().to_string()),
        planned_poly_qty: managed.session.planned_poly_qty().normalize().to_string(),
        actual_poly_filled_qty: managed
            .session
            .actual_poly_filled_qty()
            .normalize()
            .to_string(),
        actual_poly_fill_ratio: managed.session.actual_poly_fill_ratio(),
        actual_fill_notional_usd: managed.actual_fill_notional_usd.normalize().to_string(),
        candidate_expected_net_pnl_usd: Some(
            managed
                .candidate_expected_net_pnl_usd
                .normalize()
                .to_string(),
        ),
        expected_open_net_pnl_usd: managed.expected_open_net_pnl_usd.normalize().to_string(),
        timeout_loss_estimate_usd: Some(managed.timeout_loss_estimate_usd.normalize().to_string()),
        required_hit_rate: Some(managed.required_hit_rate),
        pulse_score_bps: Some(managed.pulse_score_bps),
        target_exit_price: Some(managed.target_exit_price.normalize().to_string()),
        timeout_exit_price: Some(managed.timeout_exit_price.normalize().to_string()),
        entry_executable_notional_usd: Some(
            managed
                .entry_executable_notional_usd
                .normalize()
                .to_string(),
        ),
        reversion_pocket_ticks: Some(managed.available_reversion_ticks),
        reversion_pocket_notional_usd: Some(
            managed
                .reversion_pocket_notional_usd
                .normalize()
                .to_string(),
        ),
        vacuum_ratio: Some(managed.vacuum_ratio.normalize().to_string()),
        effective_open: managed.effective_open,
        opening_outcome: managed.opening_outcome.clone(),
        opening_rejection_reason: managed.opening_rejection_reason.clone(),
        opening_allocated_hedge_qty: managed.opening_allocated_hedge_qty.normalize().to_string(),
        session_target_delta_exposure: managed
            .session
            .target_delta_exposure()
            .normalize()
            .to_string(),
        session_allocated_hedge_qty: managed.last_allocated_hedge_qty.normalize().to_string(),
        account_net_target_delta_before_order: managed
            .session
            .target_delta_exposure()
            .normalize()
            .to_string(),
        account_net_target_delta_after_order: actual_exchange_position.normalize().to_string(),
        delta_bump_used: "10".to_owned(),
        anchor_latency_delta_ms: Some(managed.anchor_latency_delta_ms),
        distance_to_mid_bps: current_distance_to_mid_bps(managed, books),
        relative_order_age_ms: Some(
            managed
                .deadline_ms
                .saturating_sub(managed.opened_at_ms)
                .saturating_sub(
                    managed
                        .deadline_ms
                        .saturating_sub(managed.maker_order_updated_at_ms),
                ),
        ),
        poly_yes_book: book_snapshot_audit(books.and_then(|item| item.yes.as_ref()), "poly_yes"),
        poly_no_book: book_snapshot_audit(books.and_then(|item| item.no.as_ref()), "poly_no"),
        cex_book: book_snapshot_audit(books.and_then(|item| item.cex.as_ref()), "cex_perp"),
    }
}

fn next_poly_exit_intent(
    managed: &mut ManagedSession,
    label: &str,
    price: Decimal,
    time_in_force: TimeInForce,
    post_only: bool,
) -> Option<PolyExitIntent> {
    let qty = managed.session.actual_poly_filled_qty();
    if qty <= Decimal::ZERO {
        return None;
    }
    managed.poly_order_seq = managed.poly_order_seq.saturating_add(1);
    Some(PolyExitIntent {
        client_order_id: ClientOrderId(format!(
            "{}-poly-{label}-{}",
            managed.session.session_id(),
            managed.poly_order_seq
        )),
        symbol: managed.market.symbol.clone(),
        token_side: managed.session.claim_side(),
        qty,
        price,
        time_in_force,
        post_only,
    })
}

fn lifecycle_event(
    session: &PulseSession,
    allocated_hedge_qty: Decimal,
    account_before: Decimal,
    account_after: Decimal,
) -> PulseLifecycleAuditEvent {
    PulseLifecycleAuditEvent {
        session_id: session.session_id().to_owned(),
        asset: session.asset().as_str().to_owned(),
        state: pulse_session_state_label(session.state()).to_owned(),
        entry_price: Some("0.35".to_owned()),
        planned_poly_qty: session.planned_poly_qty().normalize().to_string(),
        actual_poly_filled_qty: session.actual_poly_filled_qty().normalize().to_string(),
        actual_poly_fill_ratio: session.actual_poly_fill_ratio(),
        actual_fill_notional_usd: "1225".to_owned(),
        candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
        expected_open_net_pnl_usd: "3.85".to_owned(),
        timeout_loss_estimate_usd: Some("21.68".to_owned()),
        required_hit_rate: Some(0.768),
        pulse_score_bps: Some(182.5),
        target_exit_price: Some("0.38".to_owned()),
        timeout_exit_price: Some("0.31".to_owned()),
        entry_executable_notional_usd: Some("250".to_owned()),
        reversion_pocket_ticks: Some(4.0),
        reversion_pocket_notional_usd: Some("28.57".to_owned()),
        vacuum_ratio: Some("1".to_owned()),
        effective_open: true,
        opening_outcome: OPENING_OUTCOME_EFFECTIVE_OPEN.to_owned(),
        opening_rejection_reason: None,
        opening_allocated_hedge_qty: allocated_hedge_qty.normalize().to_string(),
        session_target_delta_exposure: session.target_delta_exposure().normalize().to_string(),
        session_allocated_hedge_qty: allocated_hedge_qty.normalize().to_string(),
        account_net_target_delta_before_order: account_before.normalize().to_string(),
        account_net_target_delta_after_order: account_after.normalize().to_string(),
        delta_bump_used: "10".to_owned(),
        anchor_latency_delta_ms: Some(18),
        distance_to_mid_bps: Some(8.0),
        relative_order_age_ms: Some(950),
        poly_yes_book: None,
        poly_no_book: None,
        cex_book: None,
    }
}

fn runtime_session_detail_view(
    managed: &ManagedSession,
    now_ms: u64,
    books: Option<&ObservedMarketBooks>,
) -> PulseSessionDetailView {
    PulseSessionDetailView {
        session_id: managed.session.session_id().to_owned(),
        asset: managed.session.asset().as_str().to_owned(),
        state: pulse_session_state_label(managed.session.state()).to_owned(),
        remaining_secs: managed.deadline_ms.saturating_sub(now_ms) / 1_000,
        net_edge_bps: managed.net_edge_bps,
        pulse_score_bps: Some(managed.pulse_score_bps),
        planned_poly_qty: Some(managed.session.planned_poly_qty().normalize().to_string()),
        actual_poly_filled_qty: Some(
            managed
                .session
                .actual_poly_filled_qty()
                .normalize()
                .to_string(),
        ),
        actual_poly_fill_ratio: Some(managed.session.actual_poly_fill_ratio()),
        entry_price: Some(managed.entry_price.normalize().to_string()),
        target_exit_price: Some(managed.target_exit_price.normalize().to_string()),
        timeout_exit_price: Some(managed.timeout_exit_price.normalize().to_string()),
        entry_executable_notional_usd: Some(
            managed
                .entry_executable_notional_usd
                .normalize()
                .to_string(),
        ),
        candidate_expected_net_pnl_usd: Some(
            managed
                .candidate_expected_net_pnl_usd
                .normalize()
                .to_string(),
        ),
        expected_open_net_pnl_usd: Some(managed.expected_open_net_pnl_usd.normalize().to_string()),
        timeout_loss_estimate_usd: Some(managed.timeout_loss_estimate_usd.normalize().to_string()),
        required_hit_rate: Some(managed.required_hit_rate),
        reversion_pocket_ticks: Some(managed.available_reversion_ticks),
        reversion_pocket_notional_usd: Some(
            managed
                .reversion_pocket_notional_usd
                .normalize()
                .to_string(),
        ),
        vacuum_ratio: Some(managed.vacuum_ratio.normalize().to_string()),
        session_target_delta_exposure: Some(
            managed
                .session
                .target_delta_exposure()
                .normalize()
                .to_string(),
        ),
        session_allocated_hedge_qty: Some(managed.last_allocated_hedge_qty.normalize().to_string()),
        anchor_latency_delta_ms: Some(managed.anchor_latency_delta_ms),
        distance_to_mid_bps: current_distance_to_mid_bps(managed, books),
        relative_order_age_ms: Some(now_ms.saturating_sub(managed.maker_order_updated_at_ms)),
    }
}

fn session_detail_view(
    session: &PulseSession,
    remaining_secs: u64,
    net_edge_bps: f64,
    allocated_hedge_qty: Decimal,
) -> PulseSessionDetailView {
    PulseSessionDetailView {
        session_id: session.session_id().to_owned(),
        asset: session.asset().as_str().to_owned(),
        state: pulse_session_state_label(session.state()).to_owned(),
        remaining_secs,
        net_edge_bps,
        pulse_score_bps: Some(182.5),
        planned_poly_qty: Some(session.planned_poly_qty().normalize().to_string()),
        actual_poly_filled_qty: Some(session.actual_poly_filled_qty().normalize().to_string()),
        actual_poly_fill_ratio: Some(session.actual_poly_fill_ratio()),
        entry_price: Some("0.35".to_owned()),
        target_exit_price: Some("0.38".to_owned()),
        timeout_exit_price: Some("0.31".to_owned()),
        entry_executable_notional_usd: Some("250".to_owned()),
        candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
        expected_open_net_pnl_usd: Some("3.85".to_owned()),
        timeout_loss_estimate_usd: Some("21.68".to_owned()),
        required_hit_rate: Some(0.768),
        reversion_pocket_ticks: Some(4.0),
        reversion_pocket_notional_usd: Some("28.57".to_owned()),
        vacuum_ratio: Some("1".to_owned()),
        session_target_delta_exposure: Some(
            session.target_delta_exposure().normalize().to_string(),
        ),
        session_allocated_hedge_qty: Some(allocated_hedge_qty.normalize().to_string()),
        anchor_latency_delta_ms: Some(18),
        distance_to_mid_bps: Some(8.0),
        relative_order_age_ms: Some(950),
    }
}

fn pulse_session_state_label(state: PulseSessionState) -> &'static str {
    match state {
        PulseSessionState::PreTradeAudit => "pre_trade_audit",
        PulseSessionState::PolyOpening => "poly_opening",
        PulseSessionState::HedgeOpening => "hedge_opening",
        PulseSessionState::MakerExitWorking => "maker_exit_working",
        PulseSessionState::Pegging => "pegging",
        PulseSessionState::Rehedging => "rehedging",
        PulseSessionState::EmergencyHedge => "emergency_hedge",
        PulseSessionState::EmergencyFlatten => "emergency_flatten",
        PulseSessionState::ChasingExit => "chasing_exit",
        PulseSessionState::Closed => "closed",
    }
}

fn max_anchor_age_from_settings(
    settings: &Settings,
    enabled_assets: &[PulseAsset],
) -> HashMap<PulseAsset, u64> {
    enabled_assets
        .iter()
        .filter_map(|asset| {
            let route = settings.strategy.pulse_arb.routing.get(asset.as_str())?;
            let provider = settings
                .strategy
                .pulse_arb
                .providers
                .get(&route.anchor_provider)?;
            Some((*asset, provider.max_anchor_age_ms))
        })
        .collect()
}

fn max_anchor_latency_delta_from_settings(
    settings: &Settings,
    enabled_assets: &[PulseAsset],
) -> HashMap<PulseAsset, u64> {
    enabled_assets
        .iter()
        .filter_map(|asset| {
            let route = settings.strategy.pulse_arb.routing.get(asset.as_str())?;
            let provider = settings
                .strategy
                .pulse_arb
                .providers
                .get(&route.anchor_provider)?;
            Some((*asset, provider.max_anchor_latency_delta_ms))
        })
        .collect()
}

fn filter_pulse_markets(
    enabled_assets: &[PulseAsset],
    markets: Vec<MarketConfig>,
) -> HashMap<PulseAsset, Vec<MarketConfig>> {
    let now_ts_secs = current_time_ms() / 1_000;
    let mut by_asset = HashMap::new();
    for market in markets {
        let asset_key = asset_key_from_cex_symbol(&market.cex_symbol);
        let Some(asset) = PulseAsset::from_routing_key(&asset_key) else {
            continue;
        };
        if !enabled_assets.contains(&asset) || market.hedge_exchange != Exchange::Binance {
            continue;
        }
        if market.resolved_market_rule().is_none() {
            continue;
        }
        if market.settlement_timestamp <= now_ts_secs {
            continue;
        }
        by_asset.entry(asset).or_insert_with(Vec::new).push(market);
    }
    by_asset
}

fn event_pricer_config(settings: &Settings) -> EventPricerConfig {
    let provider = settings
        .strategy
        .pulse_arb
        .providers
        .values()
        .find(|provider| provider.enabled);
    let soft_window_hours = provider
        .map(|provider| provider.soft_mismatch_window_minutes / 60)
        .unwrap_or(6);
    let hard_cap_hours = provider
        .map(|provider| provider.hard_expiry_mismatch_minutes / 60)
        .unwrap_or(12);
    EventPricerConfig {
        delta_bump_ratio_bps: settings.strategy.pulse_arb.rehedge.delta_bump_ratio_bps,
        min_abs_bump: settings.strategy.pulse_arb.rehedge.min_abs_bump,
        max_abs_bump: settings.strategy.pulse_arb.rehedge.max_abs_bump,
        delta_stability_warn_ratio: 0.25,
        expiry_gap_adjustment: ExpiryGapAdjustment::new(1, soft_window_hours, hard_cap_hours),
    }
}

fn populate_signal_snapshot_event(
    event: &mut PulseSignalSnapshotAuditEvent,
    claim_side: TokenSide,
    mode: PulseMode,
    fair_prob_yes: f64,
    entry_price: Decimal,
    net_edge_bps: f64,
    selected_target: crate::signal::TargetCandidate,
    timeout_exit_price: Decimal,
    timeout_loss_estimate_usd: Decimal,
    pulse_score_bps: f64,
    claim_price_move_bps: f64,
    fair_claim_move_bps: f64,
    cex_mid_move_bps: f64,
    filled_notional_usd: Decimal,
    swept_levels_count: usize,
    post_pulse_depth_gap_bps: f64,
    reachability: &crate::model::ReachabilityEnvelope,
    quality: &crate::model::RecoveryObservationQuality,
) {
    event.claim_side = Some(token_side_label(claim_side).to_owned());
    event.mode_candidate = audit_signal_mode(mode);
    event.fair_prob_yes = Some(fair_prob_yes);
    event.entry_price = Some(decimal_to_audit_string(entry_price));
    event.net_edge_bps = Some(net_edge_bps);
    event.expected_net_pnl_usd = Some(decimal_to_audit_string(selected_target.maker_net_pnl_usd));
    event.target_exit_price = Some(decimal_to_audit_string(selected_target.target_exit_price));
    event.timeout_exit_price = Some(decimal_to_audit_string(timeout_exit_price));
    event.timeout_loss_estimate_usd = Some(decimal_to_audit_string(timeout_loss_estimate_usd));
    event.pulse_score_bps = pulse_score_bps;
    event.claim_price_move_bps = claim_price_move_bps;
    event.fair_claim_move_bps = fair_claim_move_bps;
    event.cex_mid_move_bps = cex_mid_move_bps;
    event.swept_notional_usd = decimal_to_audit_string(filled_notional_usd);
    event.swept_levels_count = swept_levels_count;
    event.post_pulse_depth_gap_bps = post_pulse_depth_gap_bps;
    event.min_profitable_target_distance_bps = reachability.min_profitable_target_distance_bps;
    event.reachability_cap_bps = reachability.reachability_cap_bps;
    event.in_gray_zone = reachability.in_gray_zone;
    event.reachable = reachability.reachable;
    event.target_distance_to_mid_bps = Some(selected_target.target_distance_to_mid_bps);
    event.predicted_hit_rate = Some(selected_target.predicted_hit_rate);
    event.maker_net_pnl_usd = Some(decimal_to_audit_string(selected_target.maker_net_pnl_usd));
    event.timeout_net_pnl_usd = Some(decimal_to_audit_string(selected_target.timeout_net_pnl_usd));
    event.realizable_ev_usd = Some(decimal_to_audit_string(selected_target.realizable_ev_usd));
    event.used_exchange_ts = quality.used_exchange_ts;
    event.native_sequence_present = quality.native_sequence_present;
    event.post_sweep_update_count_5s = quality.post_sweep_update_count_5s;
    event.max_interarrival_gap_ms_5s = quality.max_interarrival_gap_ms_5s;
    event.observation_quality_score = Some(quality.observation_quality_score);
    event.admission_eligible = quality.admission_eligible;
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn gamma_cap_mode(settings: &Settings) -> GammaCapMode {
    match settings.strategy.pulse_arb.pin_risk.gamma_cap_mode.as_str() {
        "protective_only" => GammaCapMode::ProtectiveOnly,
        "freeze" => GammaCapMode::Freeze,
        _ => GammaCapMode::DeltaClamp,
    }
}

fn best_bid(books: Option<&OrderBookSnapshot>) -> Option<Decimal> {
    books?.bids.iter().map(|level| level.price.0).max()
}

fn best_ask(books: Option<&OrderBookSnapshot>) -> Option<Decimal> {
    books?.asks.iter().map(|level| level.price.0).min()
}

fn best_bid_for_claim_side(
    books: Option<&ObservedMarketBooks>,
    claim_side: TokenSide,
) -> Option<Decimal> {
    match claim_side {
        TokenSide::Yes => best_bid(books.and_then(|books| books.yes.as_ref())),
        TokenSide::No => best_bid(books.and_then(|books| books.no.as_ref())),
    }
}

fn aggressive_hedge_limit_price(
    side: OrderSide,
    books: Option<&ObservedMarketBooks>,
) -> Option<Decimal> {
    let cex_book = books.and_then(|books| books.cex.as_ref())?;
    match side {
        OrderSide::Buy => best_ask(Some(cex_book)),
        OrderSide::Sell => best_bid(Some(cex_book)),
    }
}

fn quote_is_fresh(now_ms: u64, book: &OrderBookSnapshot, max_age_ms: u64) -> bool {
    now_ms.saturating_sub(book.received_at_ms) <= max_age_ms
}

#[cfg(test)]
fn recommended_exit_price(
    entry_price: Decimal,
    pulse_score_bps: f64,
    available_reversion_ticks: f64,
    tick_size: Decimal,
) -> Decimal {
    recommended_exit_price_with_ladder(
        entry_price,
        pulse_score_bps,
        available_reversion_ticks,
        tick_size,
        1,
        2,
        3,
        150.0,
        250.0,
    )
}

fn recommended_exit_price_for_settings(
    settings: &Settings,
    entry_price: Decimal,
    pulse_score_bps: f64,
    available_reversion_ticks: f64,
    tick_size: Decimal,
) -> Decimal {
    recommended_exit_price_with_ladder(
        entry_price,
        pulse_score_bps,
        available_reversion_ticks,
        tick_size,
        settings.strategy.pulse_arb.exit.base_target_ticks,
        settings.strategy.pulse_arb.exit.medium_target_ticks,
        settings.strategy.pulse_arb.exit.strong_target_ticks,
        settings
            .strategy
            .pulse_arb
            .exit
            .medium_pulse_score_bps
            .to_f64()
            .unwrap_or(150.0),
        settings
            .strategy
            .pulse_arb
            .exit
            .strong_pulse_score_bps
            .to_f64()
            .unwrap_or(250.0),
    )
}

fn recommended_exit_price_with_ladder(
    entry_price: Decimal,
    pulse_score_bps: f64,
    available_reversion_ticks: f64,
    tick_size: Decimal,
    base_target_ticks: u64,
    medium_target_ticks: u64,
    strong_target_ticks: u64,
    medium_pulse_score_bps: f64,
    strong_pulse_score_bps: f64,
) -> Decimal {
    let mut target_ticks = if pulse_score_bps >= strong_pulse_score_bps {
        strong_target_ticks
    } else if pulse_score_bps >= medium_pulse_score_bps {
        medium_target_ticks
    } else {
        base_target_ticks
    };

    if available_reversion_ticks.is_finite() && available_reversion_ticks > 0.0 {
        let cap_ticks = available_reversion_ticks.floor() as u64;
        if cap_ticks > 0 {
            target_ticks = target_ticks.min(cap_ticks.max(base_target_ticks));
        }
    }

    (entry_price + tick_size * Decimal::from(target_ticks)).min(Decimal::ONE)
}

fn poly_open_max_avg_price(settings: &Settings, entry_price: Decimal) -> Decimal {
    let slippage_bps = Decimal::from(settings.paper_slippage.poly_slippage_bps);
    let slippage_multiplier = slippage_bps / Decimal::from(10_000);
    (entry_price * (Decimal::ONE + slippage_multiplier)).clamp(Decimal::ZERO, Decimal::ONE)
}

fn poly_open_max_cost_usd(settings: &Settings, min_notional: Decimal) -> Decimal {
    let slippage_bps = Decimal::from(settings.paper_slippage.poly_slippage_bps);
    let slippage_multiplier = slippage_bps / Decimal::from(10_000);
    (min_notional * (Decimal::ONE + slippage_multiplier)).max(min_notional)
}

fn rounded_hedge_qty_for_target(target_delta_exposure: Decimal, market: &MarketConfig) -> Decimal {
    CexBaseQty(target_delta_exposure.abs())
        .floor_to_step(market.cex_qty_step)
        .0
}

fn expected_net_pnl_usd(filled_notional: Decimal, net_edge_bps: f64) -> Decimal {
    let edge_fraction = Decimal::from_f64_retain(net_edge_bps / 10_000.0).unwrap_or(Decimal::ZERO);
    filled_notional * edge_fraction
}

fn consumed_ask_levels_for_notional(
    book: &OrderBookSnapshot,
    requested_notional: Decimal,
) -> usize {
    if requested_notional <= Decimal::ZERO {
        return 0;
    }

    let mut remaining_notional = requested_notional;
    let mut consumed_levels = 0_usize;
    for level in &book.asks {
        if remaining_notional <= Decimal::ZERO {
            break;
        }
        let VenueQuantity::PolyShares(shares) = level.quantity else {
            continue;
        };
        if shares.0 <= Decimal::ZERO || level.price.0 <= Decimal::ZERO {
            continue;
        }
        let level_notional = shares.0 * level.price.0;
        if level_notional <= Decimal::ZERO {
            continue;
        }
        consumed_levels += 1;
        remaining_notional -= level_notional.min(remaining_notional);
    }

    consumed_levels
}

fn base_hit_rate_prior(target_distance_to_mid_bps: f64) -> f64 {
    if target_distance_to_mid_bps <= 100.0 {
        0.40
    } else if target_distance_to_mid_bps <= 200.0 {
        0.35
    } else if target_distance_to_mid_bps <= 300.0 {
        0.10
    } else if target_distance_to_mid_bps <= 500.0 {
        0.05
    } else {
        0.0
    }
}

fn sweep_hit_rate_bonus(swept_notional_usd: Decimal, swept_levels_count: usize) -> f64 {
    if swept_notional_usd >= Decimal::new(400, 0) && swept_levels_count >= 3 {
        0.15
    } else if swept_notional_usd >= Decimal::new(300, 0) && swept_levels_count >= 2 {
        0.10
    } else {
        0.0
    }
}

fn depth_gap_hit_rate_bonus(post_pulse_depth_gap_bps: f64) -> f64 {
    if post_pulse_depth_gap_bps >= 120.0 {
        0.05
    } else {
        0.0
    }
}

fn fill_ratio_hit_rate_adjustment(depth_fill_ratio: Decimal) -> f64 {
    if depth_fill_ratio < Decimal::new(8, 1) {
        -0.05
    } else {
        0.0
    }
}

fn recovery_hit_rate_bonus(recovery_observation: &RecoveryObservation) -> f64 {
    if !recovery_observation.quality.admission_eligible {
        return 0.0;
    }
    if recovery_observation.max_recovery_ratio_within_5s >= 0.60 {
        0.10
    } else if recovery_observation.max_recovery_ratio_within_5s >= 0.30 {
        0.05
    } else {
        0.0
    }
}

fn estimate_base_hit_rate(
    target_distance_to_mid_bps: f64,
    swept_notional_usd: Decimal,
    swept_levels_count: usize,
    post_pulse_depth_gap_bps: f64,
    depth_fill_ratio: Decimal,
) -> f64 {
    (base_hit_rate_prior(target_distance_to_mid_bps)
        + sweep_hit_rate_bonus(swept_notional_usd, swept_levels_count)
        + depth_gap_hit_rate_bonus(post_pulse_depth_gap_bps)
        + fill_ratio_hit_rate_adjustment(depth_fill_ratio))
    .clamp(0.0, 0.90)
}

fn estimate_predicted_hit_rate(
    base_hit_rate: f64,
    recovery_observation: &RecoveryObservation,
) -> f64 {
    (base_hit_rate + recovery_hit_rate_bonus(recovery_observation)).clamp(0.0, 0.90)
}

fn realizable_ev_usd(
    predicted_hit_rate: f64,
    maker_net_pnl_usd: Decimal,
    timeout_net_pnl_usd: Decimal,
) -> Decimal {
    let predicted_hit_rate =
        Decimal::from_f64_retain(predicted_hit_rate.clamp(0.0, 1.0)).unwrap_or(Decimal::ZERO);
    predicted_hit_rate * maker_net_pnl_usd
        + (Decimal::ONE - predicted_hit_rate) * timeout_net_pnl_usd
}

fn opening_fill_rejection_reason(
    planned_qty: Decimal,
    filled_qty: Decimal,
    avg_price: Decimal,
    min_notional: Decimal,
    min_open_fill_ratio: Decimal,
) -> Option<&'static str> {
    if filled_qty <= Decimal::ZERO {
        return Some(OPENING_REJECTION_ZERO_FILL);
    }

    let filled_notional = avg_price * filled_qty;
    if filled_notional < min_notional {
        return Some(OPENING_REJECTION_MIN_OPEN_NOTIONAL);
    }

    let fill_ratio = if planned_qty <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        filled_qty / planned_qty
    };
    if fill_ratio < min_open_fill_ratio {
        return Some(OPENING_REJECTION_MIN_OPEN_FILL_RATIO);
    }

    None
}

fn filled_poly_qty(response: &OrderResponse) -> Decimal {
    match response.filled_quantity {
        VenueQuantity::PolyShares(shares) => shares.0,
        VenueQuantity::CexBaseQty(_) => Decimal::ZERO,
    }
}

fn filled_cex_qty(response: &OrderResponse) -> Decimal {
    match response.filled_quantity {
        VenueQuantity::CexBaseQty(base_qty) => base_qty.0,
        VenueQuantity::PolyShares(_) => Decimal::ZERO,
    }
}

fn strike_boundaries(market_rule: Option<&MarketRule>) -> Vec<f64> {
    let Some(market_rule) = market_rule else {
        return Vec::new();
    };
    match market_rule.kind {
        polyalpha_core::MarketRuleKind::Above => market_rule
            .lower_strike
            .map(|price| price.to_f64())
            .into_iter()
            .collect(),
        polyalpha_core::MarketRuleKind::Below => market_rule
            .upper_strike
            .map(|price| price.to_f64())
            .into_iter()
            .collect(),
        polyalpha_core::MarketRuleKind::Between => {
            let mut boundaries = Vec::new();
            if let Some(lower) = market_rule.lower_strike {
                boundaries.push(lower.to_f64());
            }
            if let Some(upper) = market_rule.upper_strike {
                boundaries.push(upper.to_f64());
            }
            boundaries
        }
    }
}

fn current_distance_to_mid_bps(
    managed: &ManagedSession,
    books: Option<&ObservedMarketBooks>,
) -> Option<f64> {
    let mid = match managed.session.claim_side() {
        TokenSide::Yes => books.and_then(|books| books.yes.as_ref()),
        TokenSide::No => books.and_then(|books| books.no.as_ref()),
    }
    .and_then(|book| {
        let best_bid = best_bid(Some(book))?;
        let best_ask = best_ask(Some(book))?;
        Some((best_bid + best_ask) / Decimal::from(2))
    })?;
    if mid <= Decimal::ZERO {
        return None;
    }
    ((managed.target_exit_price - mid).abs() / mid * Decimal::from(10_000)).to_f64()
}

fn mid_price_from_book(book: &OrderBookSnapshot) -> Option<Decimal> {
    let best_bid = best_bid(Some(book))?;
    let best_ask = best_ask(Some(book))?;
    Some((best_bid + best_ask) / Decimal::from(2))
}

fn book_signal_timestamp_ms(book: &OrderBookSnapshot) -> u64 {
    if book.exchange_timestamp_ms > 0 {
        book.exchange_timestamp_ms
    } else {
        book.received_at_ms
    }
}

fn signal_sample_timestamp_ms<T>(sample: &SignalSample<T>) -> u64 {
    if sample.exchange_timestamp_ms > 0 {
        sample.exchange_timestamp_ms
    } else {
        sample.received_at_ms
    }
}

fn push_decimal_sample(
    samples: &mut VecDeque<SignalSample<Decimal>>,
    exchange_timestamp_ms: u64,
    received_at_ms: u64,
    sequence: u64,
    value: Decimal,
    retention_ms: u64,
) {
    let sample = SignalSample {
        exchange_timestamp_ms,
        received_at_ms,
        sequence,
        value,
    };
    insert_signal_sample(samples, sample);
    let newest_ts_ms = latest_retained_signal_timestamp_ms(samples).unwrap_or(0);
    prune_decimal_samples(samples, newest_ts_ms.saturating_sub(retention_ms));
}

fn push_float_sample(
    samples: &mut VecDeque<SignalSample<f64>>,
    exchange_timestamp_ms: u64,
    received_at_ms: u64,
    sequence: u64,
    value: f64,
    retention_ms: u64,
) {
    let sample = SignalSample {
        exchange_timestamp_ms,
        received_at_ms,
        sequence,
        value,
    };
    insert_signal_sample(samples, sample);
    let newest_ts_ms = latest_retained_signal_timestamp_ms(samples).unwrap_or(0);
    prune_float_samples(samples, newest_ts_ms.saturating_sub(retention_ms));
}

fn insert_signal_sample<T>(samples: &mut VecDeque<SignalSample<T>>, sample: SignalSample<T>) {
    let insert_idx = {
        let slice = samples.make_contiguous();
        slice.partition_point(|existing| compare_signal_samples(existing, &sample).is_le())
    };
    samples.insert(insert_idx, sample);
}

fn compare_signal_samples<T>(left: &SignalSample<T>, right: &SignalSample<T>) -> Ordering {
    signal_sample_timestamp_ms(left)
        .cmp(&signal_sample_timestamp_ms(right))
        .then_with(|| match (left.sequence > 0, right.sequence > 0) {
            (true, true) => left.sequence.cmp(&right.sequence),
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => Ordering::Equal,
        })
        .then_with(|| left.received_at_ms.cmp(&right.received_at_ms))
}

fn latest_retained_signal_timestamp_ms<T>(samples: &VecDeque<SignalSample<T>>) -> Option<u64> {
    samples.back().map(signal_sample_timestamp_ms)
}

fn prune_decimal_samples(samples: &mut VecDeque<SignalSample<Decimal>>, min_ts_ms: u64) {
    while samples
        .front()
        .is_some_and(|sample| signal_sample_timestamp_ms(sample) < min_ts_ms)
    {
        samples.pop_front();
    }
}

fn prune_float_samples(samples: &mut VecDeque<SignalSample<f64>>, min_ts_ms: u64) {
    while samples
        .front()
        .is_some_and(|sample| signal_sample_timestamp_ms(sample) < min_ts_ms)
    {
        samples.pop_front();
    }
}

fn latest_decimal_before(
    samples: &VecDeque<SignalSample<Decimal>>,
    before_ts_ms: u64,
    window_ms: u64,
) -> Option<Decimal> {
    let min_ts_ms = before_ts_ms.saturating_sub(window_ms);
    samples
        .iter()
        .rev()
        .find(|sample| {
            let sample_ts_ms = signal_sample_timestamp_ms(sample);
            sample_ts_ms >= min_ts_ms && sample_ts_ms < before_ts_ms
        })
        .map(|sample| sample.value)
}

fn latest_float_before(
    samples: &VecDeque<SignalSample<f64>>,
    before_ts_ms: u64,
    window_ms: u64,
) -> Option<f64> {
    let min_ts_ms = before_ts_ms.saturating_sub(window_ms);
    samples
        .iter()
        .rev()
        .find(|sample| {
            let sample_ts_ms = signal_sample_timestamp_ms(sample);
            sample_ts_ms >= min_ts_ms && sample_ts_ms < before_ts_ms
        })
        .map(|sample| sample.value)
}

fn book_snapshot_audit(
    book: Option<&OrderBookSnapshot>,
    instrument: &str,
) -> Option<PulseBookSnapshotAudit> {
    let book = book?;
    Some(PulseBookSnapshotAudit {
        exchange: format!("{:?}", book.exchange),
        instrument: instrument.to_owned(),
        received_at_ms: book.received_at_ms,
        sequence: book.sequence,
        bids: book
            .bids
            .iter()
            .take(5)
            .map(|level| PulseBookLevelAuditRow {
                price: level.price.0.normalize().to_string(),
                quantity: match level.quantity {
                    VenueQuantity::PolyShares(shares) => shares.0,
                    VenueQuantity::CexBaseQty(base_qty) => base_qty.0,
                }
                .normalize()
                .to_string(),
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .take(5)
            .map(|level| PulseBookLevelAuditRow {
                price: level.price.0.normalize().to_string(),
                quantity: match level.quantity {
                    VenueQuantity::PolyShares(shares) => shares.0,
                    VenueQuantity::CexBaseQty(base_qty) => base_qty.0,
                }
                .normalize()
                .to_string(),
            })
            .collect(),
    })
}

fn decimal_to_audit_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn token_side_label(side: TokenSide) -> &'static str {
    match side {
        TokenSide::Yes => "yes",
        TokenSide::No => "no",
    }
}

fn audit_signal_mode(mode: PulseMode) -> PulseSignalMode {
    match mode {
        PulseMode::ElasticSnapback => PulseSignalMode::ElasticSnapback,
        PulseMode::DeepReversion => PulseSignalMode::DeepReversion,
    }
}

fn deribit_option_type_label(instrument_name: &str) -> Option<String> {
    let suffix = instrument_name.rsplit('-').next()?;
    match suffix {
        "C" => Some("call".to_owned()),
        "P" => Some("put".to_owned()),
        _ => None,
    }
}

fn freshest_asset_book_ages(
    markets: Option<&Vec<MarketConfig>>,
    observed_books: &HashMap<String, ObservedMarketBooks>,
    now_ms: u64,
) -> (Option<u64>, Option<u64>) {
    let Some(markets) = markets else {
        return (None, None);
    };

    let mut poly_quote_age_ms = None::<u64>;
    let mut cex_quote_age_ms = None::<u64>;
    for market in markets {
        let Some(books) = observed_books.get(&market.symbol.0) else {
            continue;
        };

        if let (Some(yes), Some(no)) = (books.yes.as_ref(), books.no.as_ref()) {
            let market_poly_age = now_ms
                .saturating_sub(yes.received_at_ms)
                .max(now_ms.saturating_sub(no.received_at_ms));
            poly_quote_age_ms = Some(
                poly_quote_age_ms
                    .map(|current| current.min(market_poly_age))
                    .unwrap_or(market_poly_age),
            );
        }

        if let Some(cex) = books.cex.as_ref() {
            let market_cex_age = now_ms.saturating_sub(cex.received_at_ms);
            cex_quote_age_ms = Some(
                cex_quote_age_ms
                    .map(|current| current.min(market_cex_age))
                    .unwrap_or(market_cex_age),
            );
        }
    }

    (poly_quote_age_ms, cex_quote_age_ms)
}

fn next_market_settlement_ts_ms(markets: Option<&Vec<MarketConfig>>, now_ms: u64) -> Option<u64> {
    let now_ts_secs = now_ms / 1_000;
    markets?
        .iter()
        .filter(|market| market.settlement_timestamp > now_ts_secs)
        .map(|market| market.settlement_timestamp * 1_000)
        .min()
}

fn asset_status(
    markets: Option<&Vec<MarketConfig>>,
    anchor_snapshot: Option<&AnchorSnapshot>,
    anchor_age_limit: u64,
    anchor_latency_delta_limit_ms: u64,
    poly_quote_age_ms: Option<u64>,
    cex_quote_age_ms: Option<u64>,
    poly_quote_age_limit_ms: u64,
    cex_quote_age_limit_ms: u64,
    now_ms: u64,
    open_sessions: usize,
    net_target_delta: Decimal,
    actual_exchange_position: Decimal,
    hedge_qty_step: Decimal,
) -> (&'static str, Option<String>) {
    let Some(markets) = markets else {
        return ("disabled", Some("no_markets".to_owned()));
    };
    if markets.is_empty() {
        return ("disabled", Some("no_future_markets".to_owned()));
    }

    let Some(anchor) = anchor_snapshot else {
        return ("degraded", Some("waiting_anchor".to_owned()));
    };
    if !anchor.quality.has_strike_coverage
        || !anchor.quality.has_liquidity
        || !anchor.quality.greeks_complete
    {
        return ("degraded", Some("anchor_surface_insufficient".to_owned()));
    }
    if anchor.quality.anchor_age_ms > anchor_age_limit {
        return ("degraded", Some("anchor_stale".to_owned()));
    }
    if now_ms.saturating_sub(anchor.ts_ms) > anchor_latency_delta_limit_ms {
        return ("degraded", Some("anchor_latency_delta_high".to_owned()));
    }

    match poly_quote_age_ms {
        None => return ("degraded", Some("waiting_poly_quote".to_owned())),
        Some(age) if age > poly_quote_age_limit_ms => {
            return ("degraded", Some("poly_quote_stale".to_owned()))
        }
        _ => {}
    }
    match cex_quote_age_ms {
        None => return ("degraded", Some("waiting_cex_quote".to_owned())),
        Some(age) if age > cex_quote_age_limit_ms => {
            return ("degraded", Some("cex_quote_stale".to_owned()))
        }
        _ => {}
    }

    let hedge_drift = (actual_exchange_position - net_target_delta).abs();
    let hedge_drift_tolerance = if hedge_qty_step > Decimal::ZERO {
        hedge_qty_step * Decimal::TWO
    } else {
        Decimal::ZERO
    };
    if open_sessions == 0 && actual_exchange_position != Decimal::ZERO {
        return ("degraded", Some("residual_hedge".to_owned()));
    }
    if open_sessions > 0 && hedge_drift > hedge_drift_tolerance {
        return ("warning", Some("hedge_drift".to_owned()));
    }

    ("enabled", None)
}

fn asset_hedge_qty_step(markets: Option<&Vec<MarketConfig>>) -> Decimal {
    markets
        .into_iter()
        .flatten()
        .map(|market| market.cex_qty_step.abs())
        .filter(|step| *step > Decimal::ZERO)
        .min()
        .unwrap_or(Decimal::ZERO)
}

fn asset_hedge_exchange(markets: Option<&Vec<MarketConfig>>) -> Exchange {
    markets
        .into_iter()
        .flatten()
        .next()
        .map(|market| market.hedge_exchange)
        .unwrap_or(Exchange::Binance)
}

struct SessionRuntimeInputs {
    current_sell_price: Decimal,
    target_exit_price: Decimal,
    pin_risk_active: bool,
    anchor_latency_delta_ms: u64,
    event_delta_yes: f64,
}

fn fixture_settings_for_asset(asset: PulseAsset) -> Settings {
    let (btc_enabled, eth_enabled) = match asset {
        PulseAsset::Btc => (true, false),
        PulseAsset::Eth => (false, true),
        PulseAsset::Sol | PulseAsset::Xrp => (false, false),
    };

    serde_json::from_value(serde_json::json!({
        "general": {
            "log_level": "info",
            "data_dir": "./data",
            "monitor_socket_path": "/tmp/polyalpha.sock"
        },
        "polymarket": {
            "clob_api_url": "https://clob.polymarket.com",
            "ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            "chain_id": 137
        },
        "binance": {
            "rest_url": "https://fapi.binance.com",
            "ws_url": "wss://fstream.binance.com"
        },
        "deribit": {
            "rest_url": "https://www.deribit.com/api/v2",
            "ws_url": "wss://www.deribit.com/ws/api/v2"
        },
        "okx": {
            "rest_url": "https://www.okx.com",
            "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
            "ws_private_url": "wss://ws.okx.com:8443/ws/v5/private"
        },
        "markets": [],
        "strategy": {
            "basis": {
                "entry_z_score_threshold": "4.0",
                "exit_z_score_threshold": "0.5",
                "rolling_window_secs": 36000,
                "min_warmup_samples": 600,
                "min_basis_bps": "50.0",
                "max_position_usd": "200",
                "delta_rebalance_threshold": "0.05",
                "delta_rebalance_interval_secs": 60
            },
            "dmm": {
                "gamma": "0.1",
                "sigma_window_secs": 300,
                "max_inventory": "5000",
                "order_refresh_secs": 10,
                "num_levels": 3,
                "level_spacing_bps": "10.0",
                "min_spread_bps": "20.0"
            },
            "negrisk": {
                "min_arb_bps": "30.0",
                "max_legs": 8,
                "enable_inventory_backed_short": false
            },
            "pulse_arb": {
                "runtime": { "enabled": true, "max_concurrent_sessions_per_asset": 2 },
                "session": { "max_holding_secs": 900, "min_opening_notional_usd": "250" },
                "entry": {
                    "min_net_session_edge_bps": "25",
                    "max_timeout_loss_usd": "1000",
                    "max_required_hit_rate": "1.0"
                },
                "rehedge": {
                    "delta_drift_threshold": "0.03",
                    "delta_bump_mode": "relative_with_clamp",
                    "delta_bump_ratio_bps": 1,
                    "min_abs_bump": "5",
                    "max_abs_bump": "25"
                },
                "pin_risk": {
                    "gamma_cap_mode": "delta_clamp",
                    "max_abs_event_delta": "0.75",
                    "pin_risk_zone_bps": 15,
                    "pin_risk_time_window_secs": 1800
                },
                "providers": {
                    "deribit_primary": {
                        "kind": "deribit",
                        "enabled": true,
                        "max_anchor_age_ms": 250,
                        "soft_mismatch_window_minutes": 360,
                        "hard_expiry_mismatch_minutes": 720
                    }
                },
                "routing": {
                    "btc": {
                        "enabled": btc_enabled,
                        "anchor_provider": "deribit_primary",
                        "hedge_venue": "binance_perp"
                    },
                    "eth": {
                        "enabled": eth_enabled,
                        "anchor_provider": "deribit_primary",
                        "hedge_venue": "binance_perp"
                    }
                }
            },
            "settlement": {
                "stop_new_position_hours": 24,
                "force_reduce_hours": 12,
                "force_reduce_target_ratio": "0.5",
                "close_only_hours": 6,
                "emergency_close_hours": 1,
                "dispute_close_only": true
            }
        },
        "risk": {
            "max_total_exposure_usd": "10000",
            "max_single_position_usd": "200",
            "max_daily_loss_usd": "500",
            "max_drawdown_pct": "10.0",
            "max_open_orders": 50,
            "circuit_breaker_cooldown_secs": 300,
            "rate_limit_orders_per_sec": 5,
            "max_persistence_lag_secs": 10
        }
    }))
    .expect("pulse runtime fixture settings")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use polyalpha_core::{
        CexBaseQty, CexOrderRequest, Exchange, MarketConfig, MarketDataEvent, MarketRule,
        MarketRuleKind, OrderBookSnapshot, OrderExecutor, OrderId, OrderRequest, OrderResponse,
        OrderStatus, OrderType, PolyOrderRequest, PolymarketIds, Price, PriceLevel, Symbol,
        TimeInForce, TokenSide, UsdNotional, VenueQuantity,
    };
    use polyalpha_data::{
        DeribitAsset, DeribitOptionType, DeribitOptionsClient, DeribitTickerMessage,
        DiscoveryFilter,
    };
    use polyalpha_executor::{dry_run::SlippageConfig, DryRunExecutor, InMemoryOrderbookProvider};
    use rust_decimal::Decimal;

    use super::*;
    use crate::anchor::deribit::DeribitAnchorProvider;
    use crate::anchor::provider::{AnchorError, AnchorProvider};
    use crate::model::{AnchorQualityMetrics, AnchorSnapshot, LocalSurfacePoint};

    #[derive(Clone)]
    enum EndToEndScenario {
        TimeoutExit,
        OpposingSessions,
    }

    #[derive(Clone)]
    struct EndToEndFixture {
        settings: Settings,
        scenario: EndToEndScenario,
    }

    struct EndToEndRuntime {
        runtime: PulseRuntime,
        scenario: EndToEndScenario,
    }

    impl EndToEndFixture {
        async fn build_paper_runtime(&self) -> Result<EndToEndRuntime> {
            let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
            let runtime = PulseRuntimeBuilder::new(self.settings.clone())
                .with_anchor_provider(fixture.anchor_provider.clone())
                .with_poly_books(fixture.poly_books.clone())
                .with_binance_books(fixture.binance_books.clone())
                .with_execution_mode(PulseExecutionMode::Paper)
                .build()
                .await?;
            Ok(EndToEndRuntime {
                runtime,
                scenario: self.scenario.clone(),
            })
        }

        async fn build_live_mock_runtime(&self) -> Result<EndToEndRuntime> {
            let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
            let runtime = PulseRuntimeBuilder::new(self.settings.clone())
                .with_anchor_provider(fixture.anchor_provider.clone())
                .with_poly_books(fixture.poly_books.clone())
                .with_binance_books(fixture.binance_books.clone())
                .with_execution_mode(PulseExecutionMode::LiveMock)
                .build()
                .await?;
            Ok(EndToEndRuntime {
                runtime,
                scenario: self.scenario.clone(),
            })
        }
    }

    impl EndToEndRuntime {
        async fn run_until_closed(&mut self, session_id: &str) -> std::result::Result<(), String> {
            match self.scenario {
                EndToEndScenario::TimeoutExit => {}
                EndToEndScenario::OpposingSessions => {
                    return Err("fixture scenario does not support run_until_closed".to_owned())
                }
            }

            self.runtime
                .run_timeout_acceptance_scenario(session_id)
                .await
        }

        async fn run_until_hedge_reconcile(&mut self) -> std::result::Result<(), String> {
            match self.scenario {
                EndToEndScenario::OpposingSessions => {}
                EndToEndScenario::TimeoutExit => {
                    return Err(
                        "fixture scenario does not support run_until_hedge_reconcile".to_owned(),
                    )
                }
            }

            self.runtime
                .run_opposing_sessions_acceptance_scenario()
                .await
        }
    }

    fn end_to_end_fixture_for_timeout_exit() -> EndToEndFixture {
        EndToEndFixture {
            settings: fixture_settings_for_asset(PulseAsset::Btc),
            scenario: EndToEndScenario::TimeoutExit,
        }
    }

    fn end_to_end_fixture_for_opposing_sessions() -> EndToEndFixture {
        EndToEndFixture {
            settings: fixture_settings_for_asset(PulseAsset::Btc),
            scenario: EndToEndScenario::OpposingSessions,
        }
    }

    #[derive(Clone)]
    struct StaticAnchorProvider {
        provider_id: String,
        snapshot: AnchorSnapshot,
    }

    impl StaticAnchorProvider {
        fn new(snapshot: AnchorSnapshot) -> Self {
            Self {
                provider_id: "deribit_primary".to_owned(),
                snapshot,
            }
        }
    }

    impl AnchorProvider for StaticAnchorProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn snapshot_for_target(
            &self,
            asset: PulseAsset,
            _target_event_expiry_ts_ms: Option<u64>,
        ) -> crate::anchor::provider::Result<Option<AnchorSnapshot>> {
            if asset != self.snapshot.asset {
                return Err(AnchorError::UnsupportedAsset {
                    asset: asset.as_str().to_owned(),
                });
            }
            Ok(Some(self.snapshot.clone()))
        }
    }

    #[derive(Clone)]
    struct MutableAnchorProvider {
        provider_id: String,
        snapshot: Arc<Mutex<Option<AnchorSnapshot>>>,
    }

    impl MutableAnchorProvider {
        fn new(snapshot: AnchorSnapshot) -> Self {
            Self {
                provider_id: "deribit_primary".to_owned(),
                snapshot: Arc::new(Mutex::new(Some(snapshot))),
            }
        }

        fn set_snapshot(&self, snapshot: Option<AnchorSnapshot>) {
            let mut guard = self
                .snapshot
                .lock()
                .expect("mutable anchor snapshot lock poisoned");
            *guard = snapshot;
        }
    }

    impl AnchorProvider for MutableAnchorProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn snapshot_for_target(
            &self,
            asset: PulseAsset,
            _target_event_expiry_ts_ms: Option<u64>,
        ) -> crate::anchor::provider::Result<Option<AnchorSnapshot>> {
            let snapshot = self
                .snapshot
                .lock()
                .expect("mutable anchor snapshot lock poisoned")
                .clone();
            match snapshot {
                Some(snapshot) if snapshot.asset == asset => Ok(Some(snapshot)),
                Some(_) => Err(AnchorError::UnsupportedAsset {
                    asset: asset.as_str().to_owned(),
                }),
                None => Ok(None),
            }
        }
    }

    #[derive(Clone)]
    struct MockPulseExecutor {
        requests: Arc<Mutex<Vec<OrderRequest>>>,
        cancellations: Arc<Mutex<Vec<(Exchange, Symbol)>>>,
        poly_response: OrderResponse,
        cex_response_price: Decimal,
        cex_response_prices: Arc<Mutex<VecDeque<Decimal>>>,
    }

    impl Default for MockPulseExecutor {
        fn default() -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                cancellations: Arc::new(Mutex::new(Vec::new())),
                poly_response: OrderResponse {
                    client_order_id: ClientOrderId("mock-poly-open".to_owned()),
                    exchange_order_id: OrderId("mock-poly-1".to_owned()),
                    status: OrderStatus::Filled,
                    filled_quantity: VenueQuantity::PolyShares(polyalpha_core::PolyShares(
                        Decimal::new(714_285_715, 6),
                    )),
                    average_price: Some(Price(Decimal::new(35, 2))),
                    rejection_reason: None,
                    timestamp_ms: 1_717_171_717_001,
                },
                cex_response_price: Decimal::new(100_000, 0),
                cex_response_prices: Arc::new(Mutex::new(VecDeque::new())),
            }
        }
    }

    impl MockPulseExecutor {
        fn with_poly_fill(filled_qty: Decimal, avg_price: Decimal) -> Self {
            Self {
                poly_response: OrderResponse {
                    client_order_id: ClientOrderId("mock-poly-open".to_owned()),
                    exchange_order_id: OrderId("mock-poly-1".to_owned()),
                    status: OrderStatus::Filled,
                    filled_quantity: VenueQuantity::PolyShares(polyalpha_core::PolyShares(
                        filled_qty,
                    )),
                    average_price: Some(Price(avg_price)),
                    rejection_reason: None,
                    timestamp_ms: 1_717_171_717_001,
                },
                ..Self::default()
            }
        }

        fn with_cex_response_prices(prices: impl IntoIterator<Item = Decimal>) -> Self {
            Self {
                cex_response_prices: Arc::new(Mutex::new(prices.into_iter().collect())),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl OrderExecutor for MockPulseExecutor {
        async fn submit_order(
            &self,
            request: OrderRequest,
        ) -> polyalpha_core::Result<OrderResponse> {
            self.requests
                .lock()
                .expect("mock executor requests lock poisoned")
                .push(request.clone());

            let response = match request {
                OrderRequest::Poly(PolyOrderRequest {
                    client_order_id, ..
                }) => {
                    let mut response = self.poly_response.clone();
                    response.client_order_id = client_order_id;
                    response
                }
                OrderRequest::Cex(CexOrderRequest {
                    client_order_id,
                    side,
                    base_qty,
                    ..
                }) => OrderResponse {
                    client_order_id,
                    exchange_order_id: OrderId(format!("mock-hedge-{side:?}")),
                    status: OrderStatus::Filled,
                    filled_quantity: VenueQuantity::CexBaseQty(base_qty),
                    average_price: Some(Price(
                        self.cex_response_prices
                            .lock()
                            .expect("mock executor cex response prices lock poisoned")
                            .pop_front()
                            .unwrap_or(self.cex_response_price),
                    )),
                    rejection_reason: None,
                    timestamp_ms: 1_717_171_717_002,
                },
            };

            Ok(response)
        }

        async fn cancel_order(
            &self,
            _exchange: Exchange,
            _order_id: &OrderId,
        ) -> polyalpha_core::Result<()> {
            Ok(())
        }

        async fn cancel_all(
            &self,
            exchange: Exchange,
            symbol: &Symbol,
        ) -> polyalpha_core::Result<u32> {
            self.cancellations
                .lock()
                .expect("mock executor cancellations lock poisoned")
                .push((exchange, symbol.clone()));
            Ok(0)
        }

        async fn query_order(
            &self,
            _exchange: Exchange,
            _order_id: &OrderId,
        ) -> polyalpha_core::Result<OrderResponse> {
            Err(polyalpha_core::CoreError::Channel(
                "mock executor does not support query_order".to_owned(),
            ))
        }
    }

    fn btc_signal_market(settlement_timestamp: u64) -> MarketConfig {
        btc_signal_market_with_symbol("btc-above-100k", settlement_timestamp)
    }

    fn btc_signal_market_with_symbol(symbol: &str, settlement_timestamp: u64) -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new(symbol),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some("Will BTC be above 100k?".to_owned()),
            market_rule: Some(MarketRule {
                kind: MarketRuleKind::Above,
                lower_strike: Some(Price(Decimal::new(100_000, 0))),
                upper_strike: None,
            }),
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(100_000, 0))),
            settlement_timestamp,
            min_tick_size: Price(Decimal::new(1, 3)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn level(price: i64, price_scale: u32, qty: i64, qty_scale: u32) -> PriceLevel {
        PriceLevel {
            price: Price(Decimal::new(price, price_scale)),
            quantity: VenueQuantity::PolyShares(polyalpha_core::PolyShares(Decimal::new(
                qty, qty_scale,
            ))),
        }
    }

    fn cex_level(price: i64, price_scale: u32, qty: i64, qty_scale: u32) -> PriceLevel {
        PriceLevel {
            price: Price(Decimal::new(price, price_scale)),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(qty, qty_scale))),
        }
    }

    fn poly_book_event(
        symbol: &str,
        side: TokenSide,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        timestamp_ms: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new(symbol),
                instrument: match side {
                    TokenSide::Yes => polyalpha_core::InstrumentKind::PolyYes,
                    TokenSide::No => polyalpha_core::InstrumentKind::PolyNo,
                },
                bids,
                asks,
                exchange_timestamp_ms: timestamp_ms,
                received_at_ms: timestamp_ms,
                sequence: 1,
                last_trade_price: None,
            },
        }
    }

    fn cex_book_event(symbol: &str, timestamp_ms: u64) -> MarketDataEvent {
        cex_book_event_with_prices(symbol, 99_995, 100_005, timestamp_ms)
    }

    fn cex_book_event_with_prices(
        symbol: &str,
        bid_price: i64,
        ask_price: i64,
        timestamp_ms: u64,
    ) -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new(symbol),
                instrument: polyalpha_core::InstrumentKind::CexPerp,
                bids: vec![cex_level(bid_price, 0, 50, 3)],
                asks: vec![cex_level(ask_price, 0, 50, 3)],
                exchange_timestamp_ms: timestamp_ms,
                received_at_ms: timestamp_ms,
                sequence: 1,
                last_trade_price: None,
            },
        }
    }

    fn cex_venue_book_event(venue_symbol: &str, timestamp_ms: u64) -> MarketDataEvent {
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange: Exchange::Binance,
            venue_symbol: venue_symbol.to_owned(),
            bids: vec![cex_level(99_995, 0, 50, 3)],
            asks: vec![cex_level(100_005, 0, 50, 3)],
            exchange_timestamp_ms: timestamp_ms,
            received_at_ms: timestamp_ms,
            sequence: 1,
        }
    }

    fn connection_event(
        exchange: Exchange,
        status: polyalpha_core::ConnectionStatus,
    ) -> MarketDataEvent {
        MarketDataEvent::ConnectionEvent { exchange, status }
    }

    async fn seed_btc_no_pulse_baseline(runtime: &mut PulseRuntime, now_ms: u64) {
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::Yes,
            vec![level(560, 3, 10_000, 0)],
            vec![level(570, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(430, 3, 10_000, 0)],
            vec![level(440, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms));
    }

    async fn seed_btc_no_pulse_baseline_with_cex_venue(runtime: &mut PulseRuntime, now_ms: u64) {
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::Yes,
            vec![level(560, 3, 10_000, 0)],
            vec![level(570, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(430, 3, 10_000, 0)],
            vec![level(440, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_venue_book_event("BTCUSDT", now_ms));
    }

    async fn open_btc_session_from_confirmed_no_pulse(runtime: &mut PulseRuntime, now_ms: u64) {
        drive_btc_pulse_entry_attempt(runtime, now_ms).await;
        assert_eq!(runtime.active_session_count(), 1);
    }

    async fn drive_btc_pulse_entry_attempt(runtime: &mut PulseRuntime, now_ms: u64) {
        seed_btc_no_pulse_baseline(runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 250));
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("pulse tick should run");
    }

    async fn open_btc_session_from_confirmed_no_pulse_with_cex_venue(
        runtime: &mut PulseRuntime,
        now_ms: u64,
    ) {
        drive_btc_pulse_entry_attempt_with_cex_venue(runtime, now_ms).await;
        assert_eq!(runtime.active_session_count(), 1);
    }

    async fn drive_btc_pulse_entry_attempt_with_cex_venue(runtime: &mut PulseRuntime, now_ms: u64) {
        seed_btc_no_pulse_baseline_with_cex_venue(runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_venue_book_event("BTCUSDT", now_ms + 250));
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("pulse tick should run");
    }

    fn btc_anchor_snapshot(ts_ms: u64, expiry_ts_ms: u64) -> AnchorSnapshot {
        AnchorSnapshot {
            asset: PulseAsset::Btc,
            provider_id: "deribit_primary".to_owned(),
            ts_ms,
            index_price: Decimal::new(100_000, 0),
            expiry_ts_ms,
            atm_iv: 55.0,
            local_surface_points: vec![
                LocalSurfacePoint {
                    instrument_name: "BTC-TEST-100000-C".to_owned(),
                    strike: Decimal::new(100_000, 0),
                    expiry_ts_ms,
                    bid_iv: Some(54.5),
                    ask_iv: Some(55.5),
                    mark_iv: 55.0,
                    delta: Some(0.5),
                    gamma: Some(0.0002),
                    best_bid: Some(Decimal::new(12, 2)),
                    best_ask: Some(Decimal::new(13, 2)),
                },
                LocalSurfacePoint {
                    instrument_name: "BTC-TEST-102000-C".to_owned(),
                    strike: Decimal::new(102_000, 0),
                    expiry_ts_ms,
                    bid_iv: Some(55.0),
                    ask_iv: Some(56.0),
                    mark_iv: 55.4,
                    delta: Some(0.42),
                    gamma: Some(0.00018),
                    best_bid: Some(Decimal::new(9, 2)),
                    best_ask: Some(Decimal::new(10, 2)),
                },
            ],
            quality: AnchorQualityMetrics {
                anchor_age_ms: 10,
                max_quote_spread_bps: Some(Decimal::new(25, 0)),
                has_strike_coverage: true,
                has_liquidity: true,
                expiry_mismatch_minutes: 0,
                greeks_complete: true,
            },
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct EntryCandidateDiagnostics {
        base_hit_rate: f64,
        predicted_hit_rate: f64,
        observation_quality_score: f64,
    }

    struct RuntimeCandidateCase {
        runtime: PulseRuntime,
        now_ms: u64,
        markets: Vec<MarketConfig>,
    }

    impl RuntimeCandidateCase {
        async fn new_low_quality_recovery() -> Self {
            let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
            let now_ms = current_time_ms();
            let settlement_timestamp = (now_ms / 1_000) + 3_600;
            let market = btc_signal_market_with_symbol("btc-above-100k", settlement_timestamp);
            let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
                now_ms,
                settlement_timestamp * 1_000,
            )));
            let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
                .with_anchor_provider(anchor_provider)
                .with_markets(vec![market.clone()])
                .with_execution_mode(PulseExecutionMode::Paper)
                .build()
                .await
                .expect("build runtime");
            seed_btc_no_pulse_baseline(&mut runtime, now_ms).await;
            runtime
                .run_tick(now_ms + 100)
                .await
                .expect("baseline tick should not open");
            Self {
                runtime,
                now_ms,
                markets: vec![market],
            }
        }

        async fn new_dual_market_realizable_ev_case() -> Self {
            Self::new_btc_mode_case("btc-shallow-snapback", "btc-deep-pulse").await
        }

        async fn new_mode_split_case() -> Self {
            Self::new_btc_mode_case("btc-snapback", "btc-deep").await
        }

        async fn new_btc_mode_case(snapback_symbol: &str, deep_symbol: &str) -> Self {
            let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
            let mut settings = fixture.settings.clone();
            settings
                .strategy
                .pulse_arb
                .session
                .opening_request_notional_usd = Some(UsdNotional(Decimal::new(400, 0)));
            let now_ms = current_time_ms();
            let settlement_timestamp = (now_ms / 1_000) + 3_600;
            let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
                now_ms,
                settlement_timestamp * 1_000,
            )));
            let mut snapback_market =
                btc_signal_market_with_symbol(snapback_symbol, settlement_timestamp);
            snapback_market.min_tick_size = Price(Decimal::new(1, 3));
            let mut deep_market = btc_signal_market_with_symbol(deep_symbol, settlement_timestamp);
            deep_market.min_tick_size = Price(Decimal::new(1, 3));
            let mut runtime = PulseRuntimeBuilder::new(settings)
                .with_anchor_provider(anchor_provider)
                .with_markets(vec![snapback_market.clone(), deep_market.clone()])
                .with_execution_mode(PulseExecutionMode::Paper)
                .build()
                .await
                .expect("build runtime");

            for market in [&snapback_market, &deep_market] {
                runtime.observe_market_event(poly_book_event(
                    &market.symbol.0,
                    TokenSide::Yes,
                    vec![level(449, 3, 10_000, 0)],
                    vec![level(450, 3, 10_000, 0)],
                    now_ms,
                ));
                runtime.observe_market_event(poly_book_event(
                    &market.symbol.0,
                    TokenSide::No,
                    vec![level(549, 3, 10_000, 0)],
                    vec![level(550, 3, 10_000, 0)],
                    now_ms,
                ));
                runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));
            }
            runtime
                .run_tick(now_ms + 100)
                .await
                .expect("baseline tick should not open");

            runtime.observe_market_event(poly_book_event(
                &snapback_market.symbol.0,
                TokenSide::Yes,
                vec![level(390, 3, 10_000, 0)],
                vec![
                    level(400, 3, 500, 0),
                    level(401, 3, 500, 0),
                    level(405, 3, 10_000, 0),
                ],
                now_ms + 250,
            ));
            runtime.observe_market_event(poly_book_event(
                &snapback_market.symbol.0,
                TokenSide::No,
                vec![level(599, 3, 10_000, 0)],
                vec![level(600, 3, 10_000, 0)],
                now_ms + 250,
            ));
            runtime.observe_market_event(cex_book_event_with_prices(
                &snapback_market.symbol.0,
                100_295,
                100_305,
                now_ms + 250,
            ));

            runtime.observe_market_event(poly_book_event(
                &deep_market.symbol.0,
                TokenSide::Yes,
                vec![level(360, 3, 10_000, 0)],
                vec![
                    level(390, 3, 300, 0),
                    level(391, 3, 300, 0),
                    level(392, 3, 300, 0),
                    level(400, 3, 10_000, 0),
                ],
                now_ms + 250,
            ));
            runtime.observe_market_event(poly_book_event(
                &deep_market.symbol.0,
                TokenSide::No,
                vec![level(609, 3, 10_000, 0)],
                vec![level(610, 3, 10_000, 0)],
                now_ms + 250,
            ));
            runtime.observe_market_event(cex_book_event(&deep_market.symbol.0, now_ms + 250));

            Self {
                runtime,
                now_ms,
                markets: vec![snapback_market, deep_market],
            }
        }

        async fn inspect_entry_candidate_diagnostics(
            &mut self,
            symbol: &str,
        ) -> Option<EntryCandidateDiagnostics> {
            assert!(self.markets.iter().any(|market| market.symbol.0 == symbol));
            inject_low_quality_recovery_pulse(&mut self.runtime, self.now_ms, symbol).await;
            self.runtime.inspect_entry_candidate_for_test(symbol).await
        }

        async fn inspect_entry_candidate(&mut self, symbol: &str) -> Option<EntryCandidate> {
            let (asset, market) = self
                .markets
                .iter()
                .find(|market| market.symbol.0 == symbol)
                .cloned()
                .map(|market| (PulseAsset::Btc, market))?;
            self.runtime
                .entry_candidate_for_market(asset, &market, self.now_ms + 1_500)
                .ok()
                .flatten()
        }

        async fn best_entry_candidate(
            &mut self,
            asset: PulseAsset,
        ) -> std::result::Result<Option<EntryCandidate>, String> {
            self.runtime
                .best_entry_candidate(asset, self.now_ms + 1_500)
        }
    }

    impl PulseRuntime {
        async fn inspect_entry_candidate_for_test(
            &mut self,
            symbol: &str,
        ) -> Option<EntryCandidateDiagnostics> {
            let (asset, market) = self.markets_by_asset.iter().find_map(|(asset, markets)| {
                markets
                    .iter()
                    .find(|market| market.symbol.0 == symbol)
                    .cloned()
                    .map(|market| (*asset, market))
            })?;
            let books = self.books_for_symbol(&market.symbol).cloned()?;
            let now_ms = [books.yes.as_ref(), books.no.as_ref(), books.cex.as_ref()]
                .into_iter()
                .flatten()
                .map(|book| book.received_at_ms)
                .max()?
                .saturating_add(50);
            let candidate = self
                .entry_candidate_for_market(asset, &market, now_ms)
                .ok()??;
            Some(EntryCandidateDiagnostics {
                base_hit_rate: candidate.base_hit_rate,
                predicted_hit_rate: candidate.predicted_hit_rate,
                observation_quality_score: candidate.observation_quality_score,
            })
        }
    }

    async fn inject_low_quality_recovery_pulse(
        runtime: &mut PulseRuntime,
        now_ms: u64,
        symbol: &str,
    ) {
        let pulse_ts_ms = now_ms + 250;
        runtime.observe_market_event(poly_book_event(
            symbol,
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            pulse_ts_ms,
        ));
        runtime.observe_market_event(cex_book_event(symbol, pulse_ts_ms));

        for (exchange_timestamp_ms, received_at_ms, ask_price, sequence) in [
            (pulse_ts_ms + 600, pulse_ts_ms + 850, 389, 0),
            (pulse_ts_ms + 1_200, pulse_ts_ms + 1_450, 388, 0),
        ] {
            runtime.record_signal_sample_for_snapshot(&OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new(symbol),
                instrument: polyalpha_core::InstrumentKind::PolyNo,
                bids: vec![level(388, 3, 10_000, 0)],
                asks: vec![level(ask_price, 3, 10_000, 0)],
                exchange_timestamp_ms,
                received_at_ms,
                sequence,
                last_trade_price: None,
            });
        }
    }

    fn deribit_ticker(
        instrument_name: &str,
        expiry_ts_ms: u64,
        strike: i64,
        mark_iv: f64,
        timestamp_ms: u64,
    ) -> DeribitTickerMessage {
        DeribitTickerMessage {
            instrument_name: instrument_name.to_owned(),
            asset: DeribitAsset::Btc,
            expiry_ts_ms,
            strike: strike as f64,
            option_type: DeribitOptionType::Call,
            timestamp_ms,
            received_at_ms: timestamp_ms,
            mark_price: 0.12,
            mark_iv,
            best_bid_price: Some(0.11),
            best_ask_price: Some(0.13),
            bid_iv: Some(mark_iv - 0.5),
            ask_iv: Some(mark_iv + 0.5),
            delta: Some(0.5),
            gamma: Some(0.0002),
            index_price: Some(100_000.0),
        }
    }

    #[tokio::test]
    async fn runtime_builds_live_mock_stack_for_btc_and_uses_mock_execution() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(fixture.anchor_provider.clone())
            .with_poly_books(fixture.poly_books.clone())
            .with_binance_books(fixture.binance_books.clone())
            .with_execution_mode(PulseExecutionMode::LiveMock)
            .build()
            .await
            .expect("build pulse runtime");

        assert_eq!(runtime.execution_mode(), PulseExecutionMode::LiveMock);
        assert_eq!(runtime.enabled_assets(), vec![PulseAsset::Btc]);
        assert!(runtime.uses_global_hedge_aggregator());
    }

    #[test]
    fn pulse_session_state_labels_use_snake_case() {
        assert_eq!(
            pulse_session_state_label(PulseSessionState::MakerExitWorking),
            "maker_exit_working"
        );
        assert_eq!(
            pulse_session_state_label(PulseSessionState::EmergencyFlatten),
            "emergency_flatten"
        );
        assert_eq!(
            pulse_session_state_label(PulseSessionState::PolyOpening),
            "poly_opening"
        );
    }

    #[test]
    fn recommended_exit_price_uses_tick_ladder_for_pulse_strength() {
        let entry_price = Decimal::new(35, 2);
        let tick_size = Decimal::new(1, 2);

        let weak = recommended_exit_price(entry_price, 80.0, 0.0, tick_size);
        let strong = recommended_exit_price(entry_price, 260.0, 0.0, tick_size);

        assert_eq!(weak, Decimal::new(36, 2));
        assert_eq!(strong, Decimal::new(38, 2));
    }

    #[tokio::test]
    async fn best_entry_candidate_prefers_higher_expected_net_pnl_over_higher_pulse_score() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let thick_market =
            btc_signal_market_with_symbol("btc-above-100k-thick", settlement_timestamp);
        let thin_market =
            btc_signal_market_with_symbol("btc-above-100k-thin", settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![thin_market.clone(), thick_market.clone()])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        for market in [&thin_market, &thick_market] {
            runtime.observe_market_event(poly_book_event(
                &market.symbol.0,
                TokenSide::Yes,
                vec![level(560, 3, 10_000, 0)],
                vec![level(570, 3, 10_000, 0)],
                now_ms,
            ));
            runtime.observe_market_event(poly_book_event(
                &market.symbol.0,
                TokenSide::No,
                vec![level(430, 3, 10_000, 0)],
                vec![level(440, 3, 10_000, 0)],
                now_ms,
            ));
            runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));
        }

        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");

        runtime.observe_market_event(poly_book_event(
            &thin_market.symbol.0,
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 100, 0),
                level(430, 3, 50, 0),
                level(500, 3, 10_000, 0),
            ],
            now_ms + 250,
        ));
        runtime.observe_market_event(poly_book_event(
            &thick_market.symbol.0,
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![level(391, 3, 10_000, 0)],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_book_event(&thin_market.symbol.0, now_ms + 250));
        runtime.observe_market_event(cex_book_event(&thick_market.symbol.0, now_ms + 250));

        let candidate = runtime
            .best_entry_candidate(PulseAsset::Btc, now_ms + 300)
            .expect("select candidate")
            .expect("candidate");

        assert_eq!(candidate.market.symbol, thick_market.symbol);
    }

    #[tokio::test]
    async fn best_entry_candidate_prefers_higher_realizable_ev_over_higher_pulse_score() {
        let mut case = RuntimeCandidateCase::new_dual_market_realizable_ev_case().await;
        let candidate = case
            .best_entry_candidate(PulseAsset::Btc)
            .await
            .expect("best candidate")
            .expect("candidate");

        assert_eq!(candidate.market.symbol.0, "btc-shallow-snapback");
        assert!(candidate.realizable_ev_usd > Decimal::ZERO);
        assert!(candidate.min_profitable_target_distance_bps > 0.0);
        assert!(candidate.target_distance_to_mid_bps > 0.0);
    }

    #[tokio::test]
    async fn runtime_assigns_short_timeout_to_snapback_and_long_timeout_to_deep_reversion() {
        let mut case = RuntimeCandidateCase::new_mode_split_case().await;
        let snapback = case
            .inspect_entry_candidate("btc-snapback")
            .await
            .expect("snapback");
        let deep = case
            .inspect_entry_candidate("btc-deep")
            .await
            .expect("deep");

        assert_eq!(snapback.mode, PulseMode::ElasticSnapback);
        assert_eq!(deep.mode, PulseMode::DeepReversion);
        assert!(snapback.timeout_secs <= 120);
        assert!(deep.timeout_secs >= 300);
    }

    #[tokio::test]
    async fn runtime_open_session_uses_candidate_timeout_for_deadline() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let market = btc_signal_market(settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        seed_btc_no_pulse_baseline(&mut runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 250));

        let candidate = runtime
            .entry_candidate_for_market(PulseAsset::Btc, &market, now_ms + 300)
            .expect("evaluate market")
            .expect("candidate");

        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("pulse tick should open");

        let managed = runtime
            .active_sessions
            .get("pulse-btc-1")
            .expect("active pulse session");
        assert_eq!(
            managed.deadline_ms - managed.opened_at_ms,
            candidate.timeout_secs * 1_000
        );
    }

    #[tokio::test]
    async fn runtime_rejects_market_when_top_of_book_edge_is_positive_but_executable_session_edge_is_negative(
    ) {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let market = btc_signal_market_with_symbol("btc-above-100k-thin", settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::Yes,
            vec![level(68, 2, 10_000, 0)],
            vec![level(69, 2, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::No,
            vec![level(39, 2, 10_000, 0)],
            vec![level(40, 2, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));

        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");

        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::No,
            vec![level(31, 2, 10_000, 0)],
            vec![
                level(35, 2, 10, 0),
                level(45, 2, 50, 0),
                level(60, 2, 10_000, 0),
            ],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms + 250));

        let candidate = runtime
            .entry_candidate_for_market(PulseAsset::Btc, &market, now_ms + 300)
            .expect("evaluate market");

        assert!(candidate.is_none());
    }

    #[tokio::test]
    async fn runtime_tick_opens_session_and_syncs_actual_position_from_executor_feedback() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.recorded_binance_orders().len(), 1);
        assert!(runtime.actual_hedge_position(PulseAsset::Btc) > Decimal::ZERO);
    }

    #[tokio::test]
    async fn runtime_tick_with_dry_run_executor_opens_session_when_top_of_book_covers_budget() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let orderbook_provider = Arc::new(InMemoryOrderbookProvider::new());
        let executor = Arc::new(DryRunExecutor::with_orderbook(
            orderbook_provider.clone(),
            SlippageConfig {
                poly_slippage_bps: fixture.settings.paper_slippage.poly_slippage_bps,
                cex_slippage_bps: fixture.settings.paper_slippage.cex_slippage_bps,
                min_liquidity: fixture.settings.paper_slippage.min_liquidity,
                allow_partial_fill: fixture.settings.paper_slippage.allow_partial_fill,
            },
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let yes_book = OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-above-100k"),
            instrument: polyalpha_core::InstrumentKind::PolyYes,
            bids: vec![level(560, 3, 10_000, 0)],
            asks: vec![level(570, 3, 10_000, 0)],
            exchange_timestamp_ms: now_ms,
            received_at_ms: now_ms,
            sequence: 1,
            last_trade_price: None,
        };
        let no_book = OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-above-100k"),
            instrument: polyalpha_core::InstrumentKind::PolyNo,
            bids: vec![level(430, 3, 10_000, 0)],
            asks: vec![level(440, 3, 10_000, 0)],
            exchange_timestamp_ms: now_ms,
            received_at_ms: now_ms,
            sequence: 1,
            last_trade_price: None,
        };
        let cex_book = OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-above-100k"),
            instrument: polyalpha_core::InstrumentKind::CexPerp,
            bids: vec![cex_level(99_995, 0, 5_000, 0)],
            asks: vec![cex_level(100_005, 0, 5_000, 0)],
            exchange_timestamp_ms: now_ms,
            received_at_ms: now_ms,
            sequence: 1,
            last_trade_price: None,
        };

        orderbook_provider.update(yes_book.clone());
        orderbook_provider.update(no_book.clone());
        orderbook_provider.update_cex(cex_book.clone(), "BTCUSDT".to_owned());

        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate { snapshot: yes_book });
        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate {
            snapshot: no_book.clone(),
        });
        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate {
            snapshot: cex_book.clone(),
        });

        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");

        let no_pulse_book = OrderBookSnapshot {
            bids: vec![level(388, 3, 10_000, 0)],
            asks: vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            exchange_timestamp_ms: now_ms + 250,
            received_at_ms: now_ms + 250,
            sequence: 2,
            ..no_book
        };
        let cex_pulse_book = OrderBookSnapshot {
            exchange_timestamp_ms: now_ms + 250,
            received_at_ms: now_ms + 250,
            sequence: 2,
            ..cex_book
        };

        orderbook_provider.update(no_pulse_book.clone());
        orderbook_provider.update_cex(cex_pulse_book.clone(), "BTCUSDT".to_owned());
        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate {
            snapshot: no_pulse_book,
        });
        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate {
            snapshot: cex_pulse_book,
        });

        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("pulse tick should open");

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.recorded_binance_orders().len(), 1);
        assert!(runtime.actual_hedge_position(PulseAsset::Btc) > Decimal::ZERO);
    }

    #[tokio::test]
    async fn runtime_tick_records_pegging_lifecycle_in_audit() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(45, 2, 10_000, 0)],
            vec![level(46, 2, 10_000, 0)],
            now_ms + 200,
        ));
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("manage session");

        let lifecycle_states = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" =>
                {
                    Some(event.state.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(lifecycle_states.contains(&"pegging"));
    }

    #[tokio::test]
    async fn runtime_tick_records_rehedging_lifecycle_in_audit() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings.strategy.pulse_arb.rehedge.delta_drift_threshold = Decimal::new(-1, 0);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(340, 3, 10_000, 0)],
            vec![level(341, 3, 10_000, 0)],
            now_ms + 275,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 275));
        {
            let managed = runtime
                .active_sessions
                .get_mut("pulse-btc-1")
                .expect("active pulse session");
            managed.target_exit_price = recommended_exit_price_for_settings(
                &runtime.settings,
                managed.entry_price,
                managed.pulse_score_bps,
                managed.available_reversion_ticks,
                managed.market.min_tick_size.0,
            );
        }
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("manage session");

        let lifecycle_states = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" =>
                {
                    Some(event.state.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(lifecycle_states.contains(&"rehedging"));
    }

    #[tokio::test]
    async fn runtime_tick_records_emergency_flatten_lifecycle_in_audit() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(MutableAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider.clone())
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        anchor_provider.set_snapshot(None);
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("manage session");

        let lifecycle_states = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" =>
                {
                    Some(event.state.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(lifecycle_states.contains(&"emergency_flatten"));
    }

    #[tokio::test]
    async fn runtime_tick_uses_deribit_expiry_closest_to_market_settlement() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 24 * 60 * 60;
        let target_expiry_ts_ms = settlement_timestamp * 1_000;
        let early_expiry_ts_ms = now_ms + 60 * 60 * 1_000;

        let client = Arc::new(DeribitOptionsClient::new(
            "https://www.deribit.com/api/v2",
            "wss://www.deribit.com/ws/api/v2",
            DiscoveryFilter::default(),
        ));
        client.ingest_ticker(deribit_ticker(
            "BTC-EARLY-100000-C",
            early_expiry_ts_ms,
            100_000,
            55.0,
            now_ms,
        ));
        client.ingest_ticker(deribit_ticker(
            "BTC-EARLY-102000-C",
            early_expiry_ts_ms,
            102_000,
            55.4,
            now_ms,
        ));
        client.ingest_ticker(deribit_ticker(
            "BTC-TARGET-100000-C",
            target_expiry_ts_ms,
            100_000,
            55.0,
            now_ms,
        ));
        client.ingest_ticker(deribit_ticker(
            "BTC-TARGET-102000-C",
            target_expiry_ts_ms,
            102_000,
            55.4,
            now_ms,
        ));

        let provider = Arc::new(DeribitAnchorProvider::with_client(
            "deribit_primary",
            fixture
                .settings
                .strategy
                .pulse_arb
                .providers
                .get("deribit_primary")
                .expect("deribit provider config")
                .clone(),
            client,
        ));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.recorded_binance_orders().len(), 1);
    }

    #[tokio::test]
    async fn runtime_tick_submits_bounded_ioc_limit_for_binance_hedge() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        let requests = executor
            .requests
            .lock()
            .expect("mock executor requests lock poisoned");
        let hedge_request = requests
            .iter()
            .find_map(|request| match request {
                OrderRequest::Cex(request) => Some(request),
                OrderRequest::Poly(_) => None,
            })
            .expect("hedge request");

        assert_eq!(hedge_request.order_type, OrderType::Limit);
        assert_eq!(hedge_request.time_in_force, TimeInForce::Ioc);
        assert_eq!(hedge_request.price, Some(Price(Decimal::new(100_005, 0))));
    }

    #[tokio::test]
    async fn reconcile_uses_external_actual_position_sync_before_sending_hedge() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let market = btc_signal_market(settlement_timestamp);
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_executor(executor)
            .with_position_sync(Arc::new(|asset, _market| {
                if asset == PulseAsset::Btc {
                    Ok(Some(Decimal::new(20, 2)))
                } else {
                    Ok(None)
                }
            }))
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms));
        runtime
            .hedge_aggregator
            .upsert("session-a", PulseAsset::Btc, Decimal::new(35, 2));

        let allocated = runtime
            .reconcile_and_execute_hedge(PulseAsset::Btc, &market)
            .await
            .expect("reconcile hedge");

        assert_eq!(allocated, Decimal::new(15, 2));
        assert_eq!(
            runtime.actual_hedge_position(PulseAsset::Btc),
            Decimal::new(35, 2)
        );
    }

    #[tokio::test]
    async fn runtime_tick_submits_poly_maker_exit_after_hedge_open() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let requests = executor
            .requests
            .lock()
            .expect("mock executor requests lock poisoned");
        assert_eq!(requests.len(), 3);

        let maker_request = match &requests[2] {
            OrderRequest::Poly(request) => request,
            other => panic!("expected poly maker exit request, got {other:?}"),
        };

        assert_eq!(maker_request.side, OrderSide::Sell);
        assert_eq!(maker_request.time_in_force, TimeInForce::Gtc);
        assert!(maker_request.post_only);
    }

    #[tokio::test]
    async fn runtime_timeout_cancels_maker_and_submits_poly_chasing_exit() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(340, 3, 10_000, 0)],
            vec![level(341, 3, 10_000, 0)],
            now_ms + 900_000,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 900_000));
        runtime
            .run_tick(now_ms + 901_000)
            .await
            .expect("timeout tick");

        let cancellations = executor
            .cancellations
            .lock()
            .expect("mock executor cancellations lock poisoned");
        assert_eq!(cancellations.len(), 1);
        assert_eq!(cancellations[0].0, Exchange::Polymarket);
        assert_eq!(cancellations[0].1, Symbol::new("btc-above-100k"));

        let requests = executor
            .requests
            .lock()
            .expect("mock executor requests lock poisoned");
        let chasing_request = requests
            .iter()
            .rev()
            .find_map(|request| match request {
                OrderRequest::Poly(request)
                    if request.side == OrderSide::Sell
                        && matches!(request.time_in_force, TimeInForce::Ioc) =>
                {
                    Some(request)
                }
                _ => None,
            })
            .expect("poly chasing exit request");

        assert!(!chasing_request.post_only);
        assert_eq!(runtime.active_session_count(), 0);
    }

    #[tokio::test]
    async fn runtime_summary_realized_pnl_includes_hedge_leg_after_timeout_close() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::with_cex_response_prices([
            Decimal::new(100_000, 0),
            Decimal::new(100_120, 0),
        ]));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        let open_hedge_qty = runtime
            .recorded_binance_orders()
            .first()
            .expect("opening hedge")
            .qty;

        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(31, 2, 10_000, 0)],
            vec![level(32, 2, 10_000, 0)],
            now_ms + 500,
        ));
        runtime.observe_market_event(MarketDataEvent::OrderBookUpdate {
            snapshot: OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-above-100k"),
                instrument: polyalpha_core::InstrumentKind::CexPerp,
                bids: vec![cex_level(100_115, 0, 50, 3)],
                asks: vec![cex_level(100_125, 0, 50, 3)],
                exchange_timestamp_ms: now_ms + 500,
                received_at_ms: now_ms + 500,
                sequence: 2,
                last_trade_price: None,
            },
        });
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        runtime
            .run_tick(now_ms + 901_000)
            .await
            .expect("timeout close");

        let summary = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary");
        let poly_realized =
            (Decimal::new(31, 2) - Decimal::new(35, 2)) * Decimal::new(714_285_715, 6);
        let expected_total = poly_realized + open_hedge_qty * Decimal::new(120, 0);

        let realized = Decimal::from_f64_retain(
            summary
                .realized_pnl_usd
                .expect("summary realized pnl should be present"),
        )
        .expect("convert realized pnl");

        assert_eq!(summary.exit_path.as_deref(), Some("timeout_chase"),);
        let target_exit_price = summary
            .target_exit_price
            .as_deref()
            .and_then(|value| Decimal::from_str_exact(value).ok())
            .expect("timeout summary target exit price");
        let final_exit_price = summary
            .final_exit_price
            .as_deref()
            .and_then(|value| Decimal::from_str_exact(value).ok())
            .expect("timeout summary final exit price");
        assert!(final_exit_price < target_exit_price);
        assert_eq!(realized.round_dp(6), expected_total.round_dp(6));
    }

    #[tokio::test]
    async fn runtime_summary_records_maker_proxy_hit_exit_path_and_prices() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(80, 2, 10_000, 0)],
            vec![level(81, 2, 10_000, 0)],
            now_ms + 200,
        ));
        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("maker proxy hit close");

        let summary = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary");
        let target_exit_price = summary
            .target_exit_price
            .as_deref()
            .and_then(|value| Decimal::from_str_exact(value).ok())
            .expect("maker summary target exit price");
        let final_exit_price = summary
            .final_exit_price
            .as_deref()
            .and_then(|value| Decimal::from_str_exact(value).ok())
            .expect("maker summary final exit price");

        assert_eq!(summary.exit_path.as_deref(), Some("maker_proxy_hit"),);
        assert_eq!(final_exit_price, target_exit_price);
    }

    #[tokio::test]
    async fn runtime_tick_accepts_cex_venue_orderbook_updates_from_data_manager() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse_with_cex_venue(&mut runtime, now_ms).await;

        assert_eq!(runtime.recorded_binance_orders().len(), 1);
        assert!(runtime.actual_hedge_position(PulseAsset::Btc) > Decimal::ZERO);
    }

    #[tokio::test]
    async fn runtime_tick_records_rejection_stats_when_stale_quotes_block_entry() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let book_ts_ms = current_time_ms();
        let run_ts_ms = book_ts_ms + 6_000;
        let settlement_timestamp = (run_ts_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            run_ts_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::Yes,
            vec![level(68, 2, 10_000, 0)],
            vec![level(69, 2, 10_000, 0)],
            book_ts_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(31, 2, 10_000, 0)],
            vec![level(35, 2, 10_000, 0)],
            book_ts_ms,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", book_ts_ms));

        runtime.run_tick(run_ts_ms).await.expect("run tick");

        let signal_stats = runtime.signal_stats();
        let evaluation_stats = runtime.evaluation_stats();

        assert_eq!(signal_stats.seen, 0);
        assert_eq!(signal_stats.rejected, 1);
        assert_eq!(evaluation_stats.attempts, 0);
        assert_eq!(
            signal_stats
                .rejection_reasons
                .get("poly_transport_rejected"),
            Some(&1)
        );
        assert_eq!(runtime.active_session_count(), 0);
    }

    #[tokio::test]
    async fn runtime_tick_allows_entry_when_selected_poly_leg_is_fresh_but_opposite_leg_is_stale() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings.strategy.market_data.poly_open_max_quote_age_ms = Some(3_000);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        seed_btc_no_pulse_baseline(&mut runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(388, 3, 10_000, 0)],
            vec![
                level(390, 3, 500, 0),
                level(391, 3, 500, 0),
                level(395, 3, 10_000, 0),
            ],
            now_ms + 4_250,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 4_250));

        runtime
            .run_tick(now_ms + 4_300)
            .await
            .expect("pulse tick should run");

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.signal_stats().seen, 1);
    }

    #[tokio::test]
    async fn runtime_tick_rejects_entry_when_hedge_ws_is_reconnecting_even_with_fresh_books() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        seed_btc_no_pulse_baseline(&mut runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");
        runtime.observe_market_event(poly_book_event(
            "btc-above-100k",
            TokenSide::No,
            vec![level(31, 2, 10_000, 0)],
            vec![level(35, 2, 10_000, 0)],
            now_ms + 250,
        ));
        runtime.observe_market_event(cex_book_event("btc-above-100k", now_ms + 250));
        runtime.observe_market_event(connection_event(
            Exchange::Binance,
            polyalpha_core::ConnectionStatus::Reconnecting,
        ));

        runtime
            .run_tick(now_ms + 300)
            .await
            .expect("pulse tick should run");

        assert_eq!(runtime.active_session_count(), 0);
        assert_eq!(runtime.signal_stats().seen, 0);
        assert!(runtime.signal_stats().rejected >= 1);
        assert_eq!(
            runtime
                .signal_stats()
                .rejection_reasons
                .get("hedge_transport_rejected"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn runtime_tick_records_rejection_stats_when_anchor_latency_delta_exceeds_limit() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings
            .strategy
            .pulse_arb
            .providers
            .get_mut("deribit_primary")
            .expect("deribit provider")
            .max_anchor_latency_delta_ms = 250;
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        assert_eq!(runtime.active_session_count(), 0);
        assert_eq!(runtime.signal_stats().seen, 0);
        assert_eq!(
            runtime
                .signal_stats()
                .rejection_reasons
                .get("anchor_latency_delta_rejected"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn runtime_tick_records_timeout_risk_rejection_stats() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings.strategy.pulse_arb.entry.max_timeout_loss_usd = UsdNotional(Decimal::ONE);
        settings.strategy.pulse_arb.entry.max_required_hit_rate = Decimal::new(10, 2);
        settings.strategy.pulse_arb.session.min_expected_net_pnl_usd =
            Some(UsdNotional(Decimal::new(5, 1)));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let signal_stats = runtime.signal_stats();
        assert_eq!(runtime.active_session_count(), 0);
        assert_eq!(signal_stats.seen, 0);
        assert!(signal_stats.rejected >= 1);
        assert_eq!(
            signal_stats.rejection_reasons.get("timeout_risk_rejected"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn runtime_does_not_apply_recovery_bonus_when_observation_quality_is_low() {
        let mut case = RuntimeCandidateCase::new_low_quality_recovery().await;
        let market = case
            .markets
            .iter()
            .find(|market| market.symbol.0 == "btc-above-100k")
            .cloned()
            .expect("market");
        inject_low_quality_recovery_pulse(&mut case.runtime, case.now_ms, "btc-above-100k").await;
        let candidate = case
            .runtime
            .entry_candidate_for_market(PulseAsset::Btc, &market, case.now_ms + 1_500)
            .expect("evaluate market")
            .expect("candidate");

        assert!(candidate.observation_quality_score < 0.5);
        assert_eq!(candidate.predicted_hit_rate, candidate.base_hit_rate);
    }

    #[tokio::test]
    async fn inspect_entry_candidate_for_test_reads_production_candidate_fields() {
        let mut case = RuntimeCandidateCase::new_low_quality_recovery().await;
        let diagnostics = case
            .inspect_entry_candidate_diagnostics("btc-above-100k")
            .await
            .expect("helper diagnostics");
        let market = case
            .markets
            .iter()
            .find(|market| market.symbol.0 == "btc-above-100k")
            .cloned()
            .expect("market");
        let candidate = case
            .runtime
            .entry_candidate_for_market(PulseAsset::Btc, &market, case.now_ms + 1_500)
            .expect("evaluate market")
            .expect("candidate");

        assert_eq!(diagnostics.base_hit_rate, candidate.base_hit_rate);
        assert_eq!(diagnostics.predicted_hit_rate, candidate.predicted_hit_rate);
        assert_eq!(
            diagnostics.observation_quality_score,
            candidate.observation_quality_score
        );
    }

    #[test]
    fn latest_decimal_before_uses_signal_timestamp_under_out_of_order_receive() {
        let mut samples = VecDeque::new();
        push_decimal_sample(&mut samples, 1_200, 1_200, 1, Decimal::new(42, 2), 10_000);
        push_decimal_sample(&mut samples, 1_100, 1_500, 2, Decimal::new(41, 2), 10_000);

        let latest = latest_decimal_before(&samples, 1_300, 500).expect("latest");

        assert_eq!(latest, Decimal::new(42, 2));
    }

    #[test]
    fn latest_decimal_before_prefers_higher_sequence_for_same_signal_timestamp() {
        let mut samples = VecDeque::new();
        push_decimal_sample(&mut samples, 1_200, 1_220, 11, Decimal::new(42, 2), 10_000);
        push_decimal_sample(&mut samples, 1_200, 1_500, 10, Decimal::new(41, 2), 10_000);

        let latest = latest_decimal_before(&samples, 1_300, 500).expect("latest");

        assert_eq!(latest, Decimal::new(42, 2));
    }

    #[test]
    fn push_decimal_sample_prunes_late_stale_packet_against_latest_signal_timestamp() {
        let mut samples = VecDeque::new();
        push_decimal_sample(&mut samples, 2_000, 2_020, 20, Decimal::new(42, 2), 500);
        push_decimal_sample(&mut samples, 1_000, 3_000, 10, Decimal::new(41, 2), 500);

        assert_eq!(samples.len(), 1);
        assert_eq!(
            signal_sample_timestamp_ms(samples.front().expect("retained sample")),
            2_000
        );
        assert_eq!(
            samples.front().expect("retained sample").value,
            Decimal::new(42, 2)
        );
    }

    #[tokio::test]
    async fn runtime_tick_requires_pulse_history_before_opening_session() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        seed_btc_no_pulse_baseline(&mut runtime, now_ms).await;
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("baseline tick should not open");

        assert_eq!(runtime.active_session_count(), 0);
        assert_eq!(
            runtime
                .signal_stats()
                .rejection_reasons
                .get("pulse_confirmation_rejected"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn runtime_tick_starts_opening_reject_cooldown_after_min_open_notional_rejection() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings
            .strategy
            .pulse_arb
            .session
            .min_open_notional_reject_cooldown_secs = 60;
        settings
            .strategy
            .pulse_arb
            .session
            .opening_request_notional_usd = Some(UsdNotional(Decimal::new(250, 0)));
        settings.strategy.pulse_arb.session.min_opening_notional_usd =
            UsdNotional(Decimal::new(35, 0));
        settings.strategy.pulse_arb.session.require_nonzero_hedge = true;
        settings.strategy.pulse_arb.session.min_expected_net_pnl_usd =
            Some(UsdNotional(Decimal::new(1, 2)));
        settings.strategy.pulse_arb.session.min_open_fill_ratio = Some(Decimal::new(5, 2));

        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::new(40, 0),
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;
        drive_btc_pulse_entry_attempt(&mut runtime, now_ms + 1_000).await;

        let session_rows = runtime.pulse_session_rows();

        assert_eq!(runtime.active_session_count(), 0);
        assert_eq!(session_rows.len(), 1);
        assert_eq!(
            session_rows[0].opening_rejection_reason.as_deref(),
            Some("min_open_notional")
        );
        assert!(
            runtime
                .signal_stats()
                .rejection_reasons
                .get("opening_reject_cooldown_active")
                .copied()
                .unwrap_or_default()
                >= 1
        );
    }

    #[tokio::test]
    async fn runtime_tick_opens_after_confirmed_no_side_pulse() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.recorded_binance_orders().len(), 1);
    }

    #[tokio::test]
    async fn runtime_tick_records_zero_fill_session_attempt_in_audit_without_active_session() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::ZERO,
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let lifecycle_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" =>
                {
                    Some(event)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let session_row = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary row");

        assert_eq!(runtime.active_session_count(), 0);
        assert!(runtime.recorded_binance_orders().is_empty());
        assert_eq!(lifecycle_events.len(), 2);
        assert_eq!(lifecycle_events[0].state, "poly_opening");
        assert_eq!(lifecycle_events[1].state, "closed");
        assert_eq!(session_row.actual_poly_filled_qty, "0");
    }

    #[tokio::test]
    async fn runtime_tick_finalizes_below_min_partial_fill_with_actual_fill_qty() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::new(100, 0),
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let lifecycle_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" =>
                {
                    Some(event)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let session_row = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary row");

        assert_eq!(runtime.active_session_count(), 0);
        assert!(runtime.recorded_binance_orders().is_empty());
        assert_eq!(lifecycle_events.len(), 2);
        assert_eq!(lifecycle_events[0].state, "poly_opening");
        assert_eq!(lifecycle_events[1].state, "closed");
        assert_eq!(session_row.actual_poly_filled_qty, "100");
        assert_eq!(session_row.session_allocated_hedge_qty, "0");
    }

    #[tokio::test]
    async fn runtime_tick_hedges_partial_fill_between_min_and_request_notional_when_b1_gates_pass()
    {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings
            .strategy
            .pulse_arb
            .session
            .opening_request_notional_usd = Some(UsdNotional(Decimal::new(250, 0)));
        settings.strategy.pulse_arb.session.min_opening_notional_usd =
            UsdNotional(Decimal::new(50, 0));
        settings.strategy.pulse_arb.session.require_nonzero_hedge = true;
        settings.strategy.pulse_arb.session.min_expected_net_pnl_usd =
            Some(UsdNotional(Decimal::new(1, 2)));
        settings.strategy.pulse_arb.session.min_open_fill_ratio = Some(Decimal::new(5, 2));

        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::new(300, 0),
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let lifecycle = runtime
            .audit_records()
            .iter()
            .find_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseLifecycle(event)
                    if event.session_id == "pulse-btc-1" && event.state == "maker_exit_working" =>
                {
                    Some(event)
                }
                _ => None,
            })
            .expect("maker exit lifecycle event");

        assert_eq!(runtime.active_session_count(), 1);
        assert_eq!(runtime.recorded_binance_orders().len(), 1);
        assert!(runtime.actual_hedge_position(PulseAsset::Btc) > Decimal::ZERO);
        assert!(lifecycle.effective_open);
        assert_eq!(lifecycle.opening_outcome, "effective_open");
        assert_eq!(lifecycle.opening_rejection_reason, None);
        assert_ne!(lifecycle.opening_allocated_hedge_qty, "0");
    }

    #[tokio::test]
    async fn runtime_tick_closes_when_nonzero_hedge_required_but_floor_to_step_is_zero() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings
            .strategy
            .pulse_arb
            .session
            .opening_request_notional_usd = Some(UsdNotional(Decimal::new(250, 0)));
        settings.strategy.pulse_arb.session.min_opening_notional_usd =
            UsdNotional(Decimal::new(3, 1));
        settings.strategy.pulse_arb.session.require_nonzero_hedge = true;
        settings.strategy.pulse_arb.session.min_expected_net_pnl_usd = Some(UsdNotional::ZERO);
        settings.strategy.pulse_arb.session.min_open_fill_ratio = Some(Decimal::ZERO);

        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::ONE,
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let session_row = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary row");

        assert_eq!(runtime.active_session_count(), 0);
        assert!(runtime.recorded_binance_orders().is_empty());
        assert_eq!(session_row.actual_poly_filled_qty, "1");
        assert_eq!(session_row.session_allocated_hedge_qty, "0");
        assert!(!session_row.effective_open);
        assert_eq!(session_row.opening_outcome, "rejected");
        assert_eq!(
            session_row.opening_rejection_reason.as_deref(),
            Some("zero_hedge_qty")
        );
        assert_eq!(session_row.opening_allocated_hedge_qty, "0");
    }

    #[tokio::test]
    async fn runtime_tick_closes_when_expected_net_pnl_is_below_minimum_after_actual_fill_scaling()
    {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut settings = fixture.settings.clone();
        settings
            .strategy
            .pulse_arb
            .session
            .opening_request_notional_usd = Some(UsdNotional(Decimal::new(250, 0)));
        settings.strategy.pulse_arb.session.min_opening_notional_usd =
            UsdNotional(Decimal::new(50, 0));
        settings.strategy.pulse_arb.session.require_nonzero_hedge = false;
        settings.strategy.pulse_arb.session.min_expected_net_pnl_usd =
            Some(UsdNotional(Decimal::ZERO));
        settings.strategy.pulse_arb.session.min_open_fill_ratio = Some(Decimal::new(5, 2));
        settings.strategy.pulse_arb.entry.max_required_hit_rate = Decimal::new(1000, 2);

        let executor = Arc::new(MockPulseExecutor::with_poly_fill(
            Decimal::new(300, 0),
            Decimal::new(35, 2),
        ));
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(settings)
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");
        runtime
            .settings
            .strategy
            .pulse_arb
            .session
            .min_expected_net_pnl_usd = Some(UsdNotional(Decimal::new(100, 0)));

        drive_btc_pulse_entry_attempt(&mut runtime, now_ms).await;

        let session_row = runtime
            .pulse_session_rows()
            .into_iter()
            .find(|row| row.pulse_session_id == "pulse-btc-1")
            .expect("session summary row");

        assert_eq!(runtime.active_session_count(), 0);
        assert!(runtime.recorded_binance_orders().is_empty());
        assert_eq!(session_row.actual_poly_filled_qty, "300");
        assert_eq!(session_row.session_allocated_hedge_qty, "0");
        assert!(!session_row.effective_open);
        assert_eq!(session_row.opening_outcome, "rejected");
        assert_eq!(
            session_row.opening_rejection_reason.as_deref(),
            Some("min_expected_net_pnl")
        );
        assert_eq!(session_row.opening_allocated_hedge_qty, "0");
    }

    #[tokio::test]
    async fn runtime_does_not_record_raw_book_tape_for_active_session_updates() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let executor = Arc::new(MockPulseExecutor::default());
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![btc_signal_market(settlement_timestamp)])
            .with_executor(executor)
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        open_btc_session_from_confirmed_no_pulse(&mut runtime, now_ms).await;

        let tape_ts_ms = now_ms + 250;
        runtime.observe_market_event(cex_book_event("btc-above-100k", tape_ts_ms));

        let tape_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseBookTape(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(
            tape_events.is_empty(),
            "runtime should not emit raw book tape from market updates"
        );
    }

    #[tokio::test]
    async fn runtime_does_not_record_raw_market_tape_for_poly_and_cex_updates() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let market = btc_signal_market(settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::Yes,
            vec![level(449, 3, 10_000, 0)],
            vec![level(450, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::No,
            vec![level(549, 3, 10_000, 0)],
            vec![level(550, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));

        let tape_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseMarketTape(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(
            tape_events.is_empty(),
            "raw poly/binance tape must be recorded outside runtime"
        );
    }

    #[tokio::test]
    async fn runtime_records_deribit_anchor_tape_during_tick() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let market = btc_signal_market(settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::Yes,
            vec![level(449, 3, 10_000, 0)],
            vec![level(450, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::No,
            vec![level(549, 3, 10_000, 0)],
            vec![level(550, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));
        runtime
            .run_tick(now_ms + 100)
            .await
            .expect("run runtime tick");

        let tape_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseMarketTape(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(!tape_events.is_empty());
        assert!(tape_events.iter().all(|event| event.venue == "Deribit"));
    }

    #[tokio::test]
    async fn runtime_records_signal_snapshot_for_rejected_evaluation_attempt() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let now_ms = current_time_ms();
        let settlement_timestamp = (now_ms / 1_000) + 3_600;
        let anchor_provider = Arc::new(StaticAnchorProvider::new(btc_anchor_snapshot(
            now_ms,
            settlement_timestamp * 1_000,
        )));
        let market = btc_signal_market(settlement_timestamp);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(anchor_provider)
            .with_markets(vec![market.clone()])
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build runtime");

        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::Yes,
            vec![level(449, 3, 10_000, 0)],
            vec![level(450, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(poly_book_event(
            &market.symbol.0,
            TokenSide::No,
            vec![level(549, 3, 10_000, 0)],
            vec![level(550, 3, 10_000, 0)],
            now_ms,
        ));
        runtime.observe_market_event(cex_book_event(&market.symbol.0, now_ms));

        let candidate = runtime
            .best_entry_candidate(PulseAsset::Btc, now_ms + 100)
            .expect("evaluate candidate");
        assert!(candidate.is_none());

        let signal_events = runtime
            .audit_records()
            .iter()
            .filter_map(|record| match &record.payload {
                polyalpha_audit::AuditEventPayload::PulseSignalSnapshot(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(signal_events.len(), 1);
        assert_eq!(signal_events[0].symbol, market.symbol.0);
        assert_eq!(
            signal_events[0].rejection_reason.as_deref(),
            Some("pulse_confirmation_rejected")
        );
        assert_eq!(
            signal_events[0].provider_id.as_deref(),
            Some("deribit_primary")
        );
        assert_eq!(signal_events[0].poly_yes_sequence_ref, Some(1));
        assert_eq!(signal_events[0].poly_no_sequence_ref, Some(1));
        assert_eq!(signal_events[0].cex_sequence_ref, Some(1));
    }

    #[test]
    fn asset_status_uses_local_anchor_age_instead_of_exchange_timestamp_delta() {
        let mut anchor = btc_anchor_snapshot(1_700_000_000_000, 1_700_086_400_000);
        anchor.ts_ms = 1_700_000_000_000;
        anchor.quality.anchor_age_ms = 50;

        let (status, disable_reason) = asset_status(
            Some(&vec![btc_signal_market(1_700_086_400)]),
            Some(&anchor),
            250,
            5_000,
            Some(100),
            Some(80),
            5_000,
            5_000,
            1_700_000_002_000,
            0,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::new(1, 3),
        );

        assert_eq!(status, "enabled");
        assert_eq!(disable_reason, None);
    }

    #[test]
    fn asset_status_degrades_when_anchor_latency_delta_exceeds_limit() {
        let mut anchor = btc_anchor_snapshot(1_700_000_000_000, 1_700_086_400_000);
        anchor.ts_ms = 1_700_000_000_000;
        anchor.quality.anchor_age_ms = 50;

        let (status, disable_reason) = asset_status(
            Some(&vec![btc_signal_market(1_700_086_400)]),
            Some(&anchor),
            250,
            500,
            Some(100),
            Some(80),
            5_000,
            5_000,
            1_700_000_002_000,
            0,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::new(1, 3),
        );

        assert_eq!(status, "degraded");
        assert_eq!(disable_reason.as_deref(), Some("anchor_latency_delta_high"));
    }

    #[test]
    fn asset_status_marks_residual_hedge_only_after_sessions_are_flat() {
        let anchor = btc_anchor_snapshot(1_700_000_000_000, 1_700_086_400_000);

        let (status, disable_reason) = asset_status(
            Some(&vec![btc_signal_market(1_700_086_400)]),
            Some(&anchor),
            250,
            5_000,
            Some(100),
            Some(80),
            5_000,
            5_000,
            1_700_000_002_000,
            0,
            Decimal::ZERO,
            Decimal::new(15, 2),
            Decimal::new(1, 3),
        );

        assert_eq!(status, "degraded");
        assert_eq!(disable_reason.as_deref(), Some("residual_hedge"));
    }

    #[test]
    fn asset_status_does_not_treat_live_session_hedge_as_residual_inventory() {
        let anchor = btc_anchor_snapshot(1_700_000_000_000, 1_700_086_400_000);

        let (status, disable_reason) = asset_status(
            Some(&vec![btc_signal_market(1_700_086_400)]),
            Some(&anchor),
            250,
            5_000,
            Some(100),
            Some(80),
            5_000,
            5_000,
            1_700_000_002_000,
            2,
            Decimal::new(291, 3),
            Decimal::new(145, 3),
            Decimal::new(1, 3),
        );

        assert_eq!(status, "warning");
        assert_eq!(disable_reason.as_deref(), Some("hedge_drift"));
    }

    #[tokio::test]
    async fn runtime_keeps_audit_and_monitor_state_after_timeout_acceptance_scenario() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(fixture.anchor_provider.clone())
            .with_poly_books(fixture.poly_books.clone())
            .with_binance_books(fixture.binance_books.clone())
            .with_execution_mode(PulseExecutionMode::Paper)
            .build()
            .await
            .expect("build pulse runtime");

        runtime
            .run_timeout_acceptance_scenario("pulse-session-timeout")
            .await
            .expect("run timeout scenario");

        assert_eq!(
            runtime
                .session_summary("pulse-session-timeout")
                .expect("summary")
                .final_state,
            "closed"
        );
        assert!(runtime.last_monitor_view().is_some());
    }

    #[tokio::test]
    async fn runtime_records_netted_binance_order_inside_runtime_state() {
        let fixture = runtime_fixture_for_asset(PulseAsset::Btc);
        let mut runtime = PulseRuntimeBuilder::new(fixture.settings.clone())
            .with_anchor_provider(fixture.anchor_provider.clone())
            .with_poly_books(fixture.poly_books.clone())
            .with_binance_books(fixture.binance_books.clone())
            .with_execution_mode(PulseExecutionMode::LiveMock)
            .build()
            .await
            .expect("build pulse runtime");

        runtime
            .run_opposing_sessions_acceptance_scenario()
            .await
            .expect("run opposing session scenario");

        let orders = runtime.recorded_binance_orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].qty, Decimal::new(15, 2));
    }

    #[tokio::test]
    async fn paper_runtime_times_out_to_chasing_exit_and_emits_audit() {
        let fixture = end_to_end_fixture_for_timeout_exit();
        let mut runtime = fixture.build_paper_runtime().await.expect("build runtime");

        runtime
            .run_until_closed("pulse-session-timeout")
            .await
            .expect("run session");

        let summary = runtime
            .runtime
            .session_summary("pulse-session-timeout")
            .expect("load pulse summary");

        assert_eq!(summary.final_state, "closed");
        assert!(summary.deadline_exit_triggered);
        assert!(summary.audit_event_count > 0);
        assert!(runtime.runtime.last_monitor_view().is_some());
    }

    #[tokio::test]
    async fn live_mock_runtime_nets_two_btc_sessions_before_sending_binance_order() {
        let fixture = end_to_end_fixture_for_opposing_sessions();
        let mut runtime = fixture
            .build_live_mock_runtime()
            .await
            .expect("build runtime");

        runtime
            .run_until_hedge_reconcile()
            .await
            .expect("run reconcile");

        let orders = runtime.runtime.recorded_binance_orders();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].qty, Decimal::new(15, 2));
        assert_eq!(
            runtime
                .runtime
                .last_monitor_view()
                .expect("monitor view")
                .active_sessions
                .len(),
            2
        );
    }
}
