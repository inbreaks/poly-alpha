use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;

use polyalpha_core::{
    cex_venue_symbol, CoreError, Exchange, MarketConfig, MarketDataEvent, OpenCandidate,
    OrderBookSnapshot, OrderExecutor, OrderId, OrderRequest, OrderResponse, OrderStatus,
    PlanningIntent, Settings, Symbol, SymbolRegistry, PLANNING_SCHEMA_VERSION,
};
use polyalpha_data::{DataManager, MarketDataNormalizer};
use polyalpha_executor::{
    dry_run::{DryRunOrderSnapshot, SlippageConfig},
    BinanceFuturesExecutor, DryRunExecutor, ExecutionManager, ExecutionPlanner,
    InMemoryOrderbookProvider, LiveExecutionRouter, OkxExecutor, PolymarketExecutor,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeExecutionMode {
    Paper,
    LiveMock,
    Live,
}

#[derive(Clone, Debug)]
pub enum RuntimeExecutor {
    DryRun(DryRunExecutor),
    Live(LiveRuntimeExecutor),
}

impl RuntimeExecutor {
    pub fn open_order_count(&self) -> Result<usize> {
        match self {
            Self::DryRun(inner) => inner
                .open_order_count()
                .map_err(|err| anyhow!("failed to count dry-run open orders: {err}")),
            Self::Live(inner) => inner.open_order_count(),
        }
    }

    pub fn open_order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        match self {
            Self::DryRun(inner) => inner
                .open_order_snapshots()
                .map_err(|err| anyhow!("failed to list dry-run open orders: {err}")),
            Self::Live(inner) => inner.open_order_snapshots(),
        }
    }
}

#[async_trait]
impl OrderExecutor for RuntimeExecutor {
    async fn submit_order(&self, request: OrderRequest) -> polyalpha_core::Result<OrderResponse> {
        match self {
            Self::DryRun(inner) => inner.submit_order(request).await,
            Self::Live(inner) => inner.submit_order(request).await,
        }
    }

    async fn cancel_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<()> {
        match self {
            Self::DryRun(inner) => inner.cancel_order(exchange, order_id).await,
            Self::Live(inner) => inner.cancel_order(exchange, order_id).await,
        }
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> polyalpha_core::Result<u32> {
        match self {
            Self::DryRun(inner) => inner.cancel_all(exchange, symbol).await,
            Self::Live(inner) => inner.cancel_all(exchange, symbol).await,
        }
    }

    async fn query_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<OrderResponse> {
        match self {
            Self::DryRun(inner) => inner.query_order(exchange, order_id).await,
            Self::Live(inner) => inner.query_order(exchange, order_id).await,
        }
    }
}

pub fn build_data_manager(
    registry: &SymbolRegistry,
    market_data_tx: tokio::sync::broadcast::Sender<MarketDataEvent>,
) -> DataManager {
    DataManager::new(MarketDataNormalizer::new(registry.clone()), market_data_tx)
}

fn build_runtime_slippage_config(settings: &Settings) -> SlippageConfig {
    SlippageConfig {
        poly_slippage_bps: settings.paper_slippage.poly_slippage_bps,
        cex_slippage_bps: settings.paper_slippage.cex_slippage_bps,
        min_liquidity: settings.paper_slippage.min_liquidity,
        allow_partial_fill: settings.paper_slippage.allow_partial_fill,
    }
}

fn build_runtime_execution_planner(settings: &Settings) -> ExecutionPlanner {
    let mut planner = ExecutionPlanner::from_execution_costs(&settings.execution_costs);
    planner.poly_slippage_bps = settings.paper_slippage.poly_slippage_bps as u32;
    planner.cex_slippage_bps = settings.paper_slippage.cex_slippage_bps as u32;
    planner
}

#[derive(Clone, Default)]
struct LiveOrderTracker {
    open_orders: Arc<Mutex<HashMap<OrderId, DryRunOrderSnapshot>>>,
}

impl LiveOrderTracker {
    fn open_order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        let orders = self
            .open_orders
            .lock()
            .map_err(|_| anyhow!("live order tracker lock poisoned"))?;
        let mut snapshots = orders.values().cloned().collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.exchange_order_id.0.cmp(&right.exchange_order_id.0));
        Ok(snapshots)
    }

    fn open_order_count(&self) -> Result<usize> {
        Ok(self.open_order_snapshots()?.len())
    }

    fn track_submit(&self, request: &OrderRequest, response: &OrderResponse) -> Result<()> {
        let mut orders = self
            .open_orders
            .lock()
            .map_err(|_| anyhow!("live order tracker lock poisoned"))?;
        if is_open_like_status(response.status) {
            let (exchange, symbol, venue_symbol) = order_identity(request);
            orders.insert(
                response.exchange_order_id.clone(),
                DryRunOrderSnapshot {
                    exchange_order_id: response.exchange_order_id.clone(),
                    exchange,
                    symbol,
                    venue_symbol,
                    status: response.status,
                    timestamp_ms: response.timestamp_ms,
                },
            );
        } else {
            orders.remove(&response.exchange_order_id);
        }
        Ok(())
    }

    fn track_cancel(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        let mut orders = self
            .open_orders
            .lock()
            .map_err(|_| anyhow!("live order tracker lock poisoned"))?;
        if orders
            .get(order_id)
            .is_some_and(|snapshot| snapshot.exchange == exchange)
        {
            orders.remove(order_id);
        }
        Ok(())
    }

    fn track_cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<()> {
        let mut orders = self
            .open_orders
            .lock()
            .map_err(|_| anyhow!("live order tracker lock poisoned"))?;
        orders.retain(|_, snapshot| !(snapshot.exchange == exchange && &snapshot.symbol == symbol));
        Ok(())
    }

    fn track_query(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
        response: &OrderResponse,
    ) -> Result<()> {
        let mut orders = self
            .open_orders
            .lock()
            .map_err(|_| anyhow!("live order tracker lock poisoned"))?;
        match orders.get_mut(order_id) {
            Some(snapshot)
                if snapshot.exchange == exchange && is_open_like_status(response.status) =>
            {
                snapshot.status = response.status;
                snapshot.timestamp_ms = response.timestamp_ms;
            }
            Some(snapshot) if snapshot.exchange == exchange => {
                orders.remove(order_id);
            }
            _ => {}
        }
        Ok(())
    }
}

fn is_open_like_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartialFill
    )
}

fn order_identity(request: &OrderRequest) -> (Exchange, Symbol, Option<String>) {
    match request {
        OrderRequest::Poly(req) => (Exchange::Polymarket, req.symbol.clone(), None),
        OrderRequest::Cex(req) => (
            req.exchange,
            req.symbol.clone(),
            Some(req.venue_symbol.clone()),
        ),
    }
}

fn tracker_error(context: &str, err: anyhow::Error) -> CoreError {
    CoreError::Channel(format!("{context}: {err}"))
}

#[derive(Clone)]
struct TrackedOrderExecutor<E> {
    inner: E,
    tracker: LiveOrderTracker,
}

impl<E> std::fmt::Debug for TrackedOrderExecutor<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedOrderExecutor")
            .finish_non_exhaustive()
    }
}

impl<E> TrackedOrderExecutor<E> {
    fn new(inner: E) -> Self {
        Self {
            inner,
            tracker: LiveOrderTracker::default(),
        }
    }

    fn open_order_count(&self) -> Result<usize> {
        self.tracker.open_order_count()
    }

    fn open_order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        self.tracker.open_order_snapshots()
    }
}

#[async_trait]
impl<E> OrderExecutor for TrackedOrderExecutor<E>
where
    E: OrderExecutor + Send + Sync,
{
    async fn submit_order(&self, request: OrderRequest) -> polyalpha_core::Result<OrderResponse> {
        let response = self.inner.submit_order(request.clone()).await?;
        self.tracker
            .track_submit(&request, &response)
            .map_err(|err| tracker_error("failed to track live submit", err))?;
        Ok(response)
    }

    async fn cancel_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<()> {
        self.inner.cancel_order(exchange, order_id).await?;
        self.tracker
            .track_cancel(exchange, order_id)
            .map_err(|err| tracker_error("failed to track live cancel", err))?;
        Ok(())
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> polyalpha_core::Result<u32> {
        let cancelled = self.inner.cancel_all(exchange, symbol).await?;
        self.tracker
            .track_cancel_all(exchange, symbol)
            .map_err(|err| tracker_error("failed to track live cancel_all", err))?;
        Ok(cancelled)
    }

    async fn query_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<OrderResponse> {
        let response = self.inner.query_order(exchange, order_id).await?;
        self.tracker
            .track_query(exchange, order_id, &response)
            .map_err(|err| tracker_error("failed to track live query", err))?;
        Ok(response)
    }
}

#[derive(Clone, Debug)]
pub struct LiveRuntimeExecutor {
    inner: TrackedOrderExecutor<LiveExecutionRouter>,
}

impl LiveRuntimeExecutor {
    fn new(router: LiveExecutionRouter) -> Self {
        Self {
            inner: TrackedOrderExecutor::new(router),
        }
    }

    pub fn open_order_count(&self) -> Result<usize> {
        self.inner.open_order_count()
    }

    pub fn open_order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        self.inner.open_order_snapshots()
    }
}

#[async_trait]
impl OrderExecutor for LiveRuntimeExecutor {
    async fn submit_order(&self, request: OrderRequest) -> polyalpha_core::Result<OrderResponse> {
        self.inner.submit_order(request).await
    }

    async fn cancel_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<()> {
        self.inner.cancel_order(exchange, order_id).await
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> polyalpha_core::Result<u32> {
        self.inner.cancel_all(exchange, symbol).await
    }

    async fn query_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> polyalpha_core::Result<OrderResponse> {
        self.inner.query_order(exchange, order_id).await
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var `{name}`"))
}

fn ensure_live_hedge_exchange(exchange: Exchange) -> Result<Exchange> {
    match exchange {
        Exchange::Binance | Exchange::Okx => Ok(exchange),
        Exchange::Polymarket => Err(anyhow!(
            "armed live execution requires a hedge venue, got polymarket"
        )),
    }
}

fn live_exchange_sort_key(exchange: Exchange) -> u8 {
    match exchange {
        Exchange::Polymarket => 0,
        Exchange::Binance => 1,
        Exchange::Okx => 2,
    }
}

fn resolve_live_hedge_exchange(
    settings: &Settings,
    live_market: Option<&MarketConfig>,
) -> Result<Exchange> {
    if let Some(market) = live_market {
        return ensure_live_hedge_exchange(market.hedge_exchange);
    }

    let mut exchanges: Vec<Exchange> = settings.markets.iter().map(|m| m.hedge_exchange).collect();
    exchanges.sort_by_key(|exchange| live_exchange_sort_key(*exchange));
    exchanges.dedup();
    match exchanges.len() {
        0 => Err(anyhow!(
            "armed live execution requires at least one configured market"
        )),
        1 => ensure_live_hedge_exchange(exchanges[0]),
        _ => Err(anyhow!(
            "armed live execution for multi-market runtime requires a single hedge exchange, found: {}",
            exchanges
                .into_iter()
                .map(|exchange| format!("{exchange:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

pub async fn build_execution_stack(
    settings: &Settings,
    registry: &SymbolRegistry,
    execution_mode: RuntimeExecutionMode,
    executor_start_ms: u64,
    live_market: Option<&MarketConfig>,
) -> Result<(
    Arc<InMemoryOrderbookProvider>,
    RuntimeExecutor,
    ExecutionManager<RuntimeExecutor>,
)> {
    let orderbook_provider = Arc::new(InMemoryOrderbookProvider::new());
    let executor = match execution_mode {
        RuntimeExecutionMode::Paper | RuntimeExecutionMode::LiveMock => {
            RuntimeExecutor::DryRun(DryRunExecutor::with_orderbook_and_start_timestamp(
                orderbook_provider.clone(),
                build_runtime_slippage_config(settings),
                executor_start_ms,
            ))
        }
        RuntimeExecutionMode::Live => {
            let hedge_exchange = resolve_live_hedge_exchange(settings, live_market)?;
            let polymarket =
                PolymarketExecutor::from_config(&settings.polymarket, registry.clone())
                    .await
                    .context("failed to build polymarket live executor")?;
            let router = match hedge_exchange {
                Exchange::Binance => LiveExecutionRouter::binance(
                    polymarket,
                    BinanceFuturesExecutor::new(
                        settings.binance.rest_url.clone(),
                        required_env("BINANCE_API_KEY")?,
                        required_env("BINANCE_API_SECRET")?,
                    ),
                ),
                Exchange::Okx => LiveExecutionRouter::okx(
                    polymarket,
                    OkxExecutor::new(
                        settings.okx.rest_url.clone(),
                        required_env("OKX_API_KEY")?,
                        required_env("OKX_API_SECRET")?,
                        required_env("OKX_PASSPHRASE")?,
                    ),
                ),
                Exchange::Polymarket => unreachable!("polymarket hedge exchange rejected above"),
            };
            RuntimeExecutor::Live(LiveRuntimeExecutor::new(router))
        }
    };

    let execution = ExecutionManager::with_symbol_registry_and_orderbook_provider(
        executor.clone(),
        registry.clone(),
        orderbook_provider.clone(),
    )
    .with_planner(build_runtime_execution_planner(settings))
    .with_planner_depth_levels(settings.strategy.market_data.planner_depth_levels);

    Ok((orderbook_provider, executor, execution))
}

pub fn open_intent_from_candidate(candidate: &OpenCandidate) -> PlanningIntent {
    PlanningIntent::OpenPosition {
        schema_version: candidate.schema_version,
        intent_id: format!("intent-open-{}", candidate.candidate_id),
        correlation_id: candidate.correlation_id.clone(),
        symbol: candidate.symbol.clone(),
        candidate: candidate.clone(),
        max_budget_usd: candidate.risk_budget_usd,
        max_residual_delta: 0.05,
        max_shock_loss_usd: candidate.risk_budget_usd.max(25.0),
    }
}

pub fn close_intent_for_symbol(
    symbol: &Symbol,
    reason: &str,
    correlation_id: &str,
    now_ms: u64,
) -> PlanningIntent {
    PlanningIntent::ClosePosition {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: format!("intent-close-{}-{now_ms}", symbol.0),
        correlation_id: correlation_id.to_owned(),
        symbol: symbol.clone(),
        close_reason: reason.to_owned(),
        target_close_ratio: 1.0,
    }
}

pub fn delta_rebalance_intent_for_symbol(
    symbol: &Symbol,
    correlation_id: &str,
    now_ms: u64,
    target_residual_delta_max: f64,
    target_shock_loss_max: f64,
) -> PlanningIntent {
    PlanningIntent::DeltaRebalance {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: format!("intent-delta-rebalance-{}-{now_ms}", symbol.0),
        correlation_id: correlation_id.to_owned(),
        symbol: symbol.clone(),
        target_residual_delta_max,
        target_shock_loss_max,
    }
}

pub fn force_exit_intent_for_symbol(
    symbol: &Symbol,
    reason: &str,
    correlation_id: &str,
    now_ms: u64,
    allow_negative_edge: bool,
) -> PlanningIntent {
    PlanningIntent::ForceExit {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: format!("intent-force-exit-{}-{now_ms}", symbol.0),
        correlation_id: correlation_id.to_owned(),
        symbol: symbol.clone(),
        force_reason: reason.to_owned(),
        allow_negative_edge,
    }
}

pub fn apply_orderbook_snapshot(
    provider: &InMemoryOrderbookProvider,
    registry: &SymbolRegistry,
    snapshot: &OrderBookSnapshot,
) {
    match snapshot.instrument {
        polyalpha_core::InstrumentKind::CexPerp => {
            let venue_symbol = registry
                .get_config(&snapshot.symbol)
                .map(|config| cex_venue_symbol(config.hedge_exchange, &config.cex_symbol))
                .unwrap_or_else(|| {
                    cex_venue_symbol(
                        snapshot.exchange,
                        &snapshot.symbol.0.to_ascii_uppercase().replace('-', ""),
                    )
                });
            provider.update_cex(snapshot.clone(), venue_symbol);
        }
        polyalpha_core::InstrumentKind::PolyYes | polyalpha_core::InstrumentKind::PolyNo => {
            provider.update(snapshot.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use polyalpha_core::{
        create_channels, BandPolicyMode, BasisStrategyConfig, CexBaseQty, CexOrderRequest,
        ClientOrderId, CoreError, DmmStrategyConfig, MarketConfig, NegRiskStrategyConfig,
        OpenCandidate, OrderExecutor, OrderId, OrderRequest, OrderResponse, OrderSide, OrderStatus,
        OrderType, PolyShares, PolymarketConfig, PolymarketIds, Price, RiskConfig, RiskRejection,
        Settings, SettlementRules, StrategyConfig, TimeInForce, TokenSide, UsdNotional,
        VenueQuantity,
    };
    use polyalpha_data::{CexBookLevel, CexBookUpdate, PolyBookLevel, PolyBookUpdate};
    use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

    use super::*;

    fn sample_market() -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new("btc-runtime"),
            poly_ids: PolymarketIds {
                condition_id: "cond-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: None,
            market_rule: None,
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: None,
            settlement_timestamp: 0,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::ONE,
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn sample_settings() -> Settings {
        Settings {
            general: polyalpha_core::GeneralConfig {
                log_level: "info".to_owned(),
                data_dir: "/tmp/polyalpha-runtime-tests".to_owned(),
                monitor_socket_path: "/tmp/polyalpha-runtime.sock".to_owned(),
            },
            polymarket: PolymarketConfig {
                clob_api_url: "https://example.com".to_owned(),
                ws_url: "wss://example.com/ws".to_owned(),
                chain_id: 137,
                private_key: None,
                signature_type: None,
                funder: None,
                api_key_nonce: None,
                use_server_time: true,
            },
            binance: polyalpha_core::BinanceConfig {
                rest_url: "https://example.com".to_owned(),
                ws_url: "wss://example.com/ws".to_owned(),
            },
            okx: polyalpha_core::OkxConfig {
                rest_url: "https://example.com".to_owned(),
                ws_public_url: "wss://example.com/public".to_owned(),
                ws_private_url: "wss://example.com/private".to_owned(),
            },
            strategy: StrategyConfig {
                basis: BasisStrategyConfig {
                    entry_z_score_threshold: Decimal::new(2, 0),
                    exit_z_score_threshold: Decimal::new(5, 1),
                    rolling_window_secs: 300,
                    min_warmup_samples: 10,
                    min_basis_bps: Decimal::new(10, 0),
                    max_position_usd: UsdNotional(Decimal::new(200, 0)),
                    delta_rebalance_threshold: Decimal::new(5, 2),
                    delta_rebalance_interval_secs: 30,
                    band_policy: BandPolicyMode::ConfiguredBand,
                    min_poly_price: None,
                    max_poly_price: None,
                    max_data_age_minutes: 1,
                    max_time_diff_minutes: 1,
                    enable_freshness_check: true,
                    reject_on_disconnect: true,
                    overrides: std::collections::HashMap::new(),
                },
                dmm: DmmStrategyConfig {
                    gamma: Decimal::new(1, 0),
                    sigma_window_secs: 60,
                    max_inventory: UsdNotional(Decimal::new(500, 0)),
                    order_refresh_secs: 5,
                    num_levels: 1,
                    level_spacing_bps: Decimal::new(5, 0),
                    min_spread_bps: Decimal::new(10, 0),
                },
                negrisk: NegRiskStrategyConfig {
                    min_arb_bps: Decimal::new(10, 0),
                    max_legs: 2,
                    enable_inventory_backed_short: false,
                },
                settlement: SettlementRules::default(),
                market_data: polyalpha_core::MarketDataConfig::default(),
            },
            risk: RiskConfig {
                max_total_exposure_usd: UsdNotional(Decimal::new(10_000, 0)),
                max_single_position_usd: UsdNotional(Decimal::new(500, 0)),
                max_daily_loss_usd: UsdNotional(Decimal::new(1_000, 0)),
                max_drawdown_pct: Decimal::new(20, 0),
                max_open_orders: 10,
                circuit_breaker_cooldown_secs: 60,
                rate_limit_orders_per_sec: 10,
                max_persistence_lag_secs: 60,
            },
            markets: vec![sample_market()],
            paper: polyalpha_core::PaperTradingConfig::default(),
            paper_slippage: polyalpha_core::PaperSlippageConfig::default(),
            execution_costs: polyalpha_core::ExecutionCostConfig::default(),
            audit: polyalpha_core::AuditConfig::default(),
        }
    }

    fn sample_okx_market() -> MarketConfig {
        let mut market = sample_market();
        market.symbol = Symbol::new("eth-runtime");
        market.cex_symbol = "ETH-USDT-SWAP".to_owned();
        market.hedge_exchange = Exchange::Okx;
        market
    }

    #[derive(Clone, Default)]
    struct ScriptedExecutor {
        submit_responses: Arc<Mutex<VecDeque<OrderResponse>>>,
        query_responses: Arc<Mutex<VecDeque<OrderResponse>>>,
    }

    impl ScriptedExecutor {
        fn with_submit_responses(responses: Vec<OrderResponse>) -> Self {
            Self {
                submit_responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                query_responses: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        fn with_query_responses(mut self, responses: Vec<OrderResponse>) -> Self {
            self.query_responses = Arc::new(Mutex::new(VecDeque::from(responses)));
            self
        }
    }

    #[async_trait]
    impl OrderExecutor for ScriptedExecutor {
        async fn submit_order(
            &self,
            _request: OrderRequest,
        ) -> polyalpha_core::Result<OrderResponse> {
            self.submit_responses
                .lock()
                .expect("submit queue lock")
                .pop_front()
                .ok_or_else(|| CoreError::Channel("missing scripted submit response".to_owned()))
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
            _exchange: Exchange,
            _symbol: &Symbol,
        ) -> polyalpha_core::Result<u32> {
            Ok(1)
        }

        async fn query_order(
            &self,
            _exchange: Exchange,
            _order_id: &OrderId,
        ) -> polyalpha_core::Result<OrderResponse> {
            self.query_responses
                .lock()
                .expect("query queue lock")
                .pop_front()
                .ok_or_else(|| CoreError::Channel("missing scripted query response".to_owned()))
        }
    }

    fn sample_cex_order_request(
        client_id: &str,
        symbol: Symbol,
        venue_symbol: &str,
    ) -> OrderRequest {
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId(client_id.to_owned()),
            exchange: Exchange::Binance,
            symbol,
            venue_symbol: venue_symbol.to_owned(),
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            price: Some(Price(Decimal::new(100_000, 0))),
            base_qty: CexBaseQty(Decimal::new(5, 3)),
            time_in_force: TimeInForce::Gtc,
            reduce_only: false,
        })
    }

    fn scripted_cex_response(
        order_id: &str,
        status: OrderStatus,
        timestamp_ms: u64,
    ) -> OrderResponse {
        OrderResponse {
            client_order_id: ClientOrderId(format!("client-{order_id}")),
            exchange_order_id: OrderId(order_id.to_owned()),
            status,
            filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty::ZERO),
            average_price: Some(Price(Decimal::new(100_000, 0))),
            rejection_reason: None,
            timestamp_ms,
        }
    }

    #[tokio::test]
    async fn tracked_live_executor_reports_open_order_snapshots_after_submit() {
        let tracked = TrackedOrderExecutor::new(ScriptedExecutor::with_submit_responses(vec![
            scripted_cex_response("order-open-1", OrderStatus::Open, 10),
        ]));

        tracked
            .submit_order(sample_cex_order_request(
                "client-1",
                sample_market().symbol.clone(),
                "BTCUSDT",
            ))
            .await
            .expect("submit open order");

        let open_orders = tracked
            .open_order_snapshots()
            .expect("tracked live open orders");
        assert_eq!(tracked.open_order_count().expect("open order count"), 1);
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].exchange, Exchange::Binance);
        assert_eq!(open_orders[0].symbol, sample_market().symbol);
        assert_eq!(open_orders[0].venue_symbol.as_deref(), Some("BTCUSDT"));
    }

    #[tokio::test]
    async fn tracked_live_executor_clears_orders_on_cancel_all_and_terminal_query() {
        let market = sample_market();
        let other_market = sample_okx_market();
        let tracked = TrackedOrderExecutor::new(
            ScriptedExecutor::with_submit_responses(vec![
                scripted_cex_response("order-open-1", OrderStatus::Open, 10),
                scripted_cex_response("order-open-2", OrderStatus::Open, 11),
            ])
            .with_query_responses(vec![scripted_cex_response(
                "order-open-2",
                OrderStatus::Filled,
                12,
            )]),
        );

        let first = tracked
            .submit_order(sample_cex_order_request(
                "client-1",
                market.symbol.clone(),
                "BTCUSDT",
            ))
            .await
            .expect("submit first open order");
        let second = tracked
            .submit_order(sample_cex_order_request(
                "client-2",
                other_market.symbol.clone(),
                "ETHUSDT",
            ))
            .await
            .expect("submit second open order");

        tracked
            .cancel_all(Exchange::Binance, &market.symbol)
            .await
            .expect("cancel all for first symbol");
        assert_eq!(
            tracked.open_order_count().expect("count after cancel all"),
            1
        );

        tracked
            .query_order(Exchange::Binance, &second.exchange_order_id)
            .await
            .expect("query second order");
        assert_eq!(
            tracked
                .open_order_count()
                .expect("count after terminal query"),
            0
        );

        let remaining = tracked.open_order_snapshots().expect("final snapshots");
        assert!(
            remaining.is_empty(),
            "terminal query should clear tracked order"
        );
        assert_eq!(first.exchange_order_id.0, "order-open-1");
    }

    #[test]
    fn runtime_intents_keep_expected_metadata() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: sample_market().symbol,
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.55,
            raw_mispricing: 0.03,
            delta_estimate: 0.12,
            risk_budget_usd: 200.0,
            strength: polyalpha_core::SignalStrength::Normal,
            z_score: Some(2.4),
            timestamp_ms: 1,
        };

        let open = open_intent_from_candidate(&candidate);
        let close =
            close_intent_for_symbol(&candidate.symbol, "manual_command", "corr-close-1", 99);
        let delta = delta_rebalance_intent_for_symbol(
            &candidate.symbol,
            "corr-rebalance-1",
            100,
            0.02,
            5.0,
        );
        let force = force_exit_intent_for_symbol(
            &candidate.symbol,
            "emergency_stop",
            "corr-force-1",
            101,
            true,
        );

        match open {
            PlanningIntent::OpenPosition { correlation_id, .. } => {
                assert_eq!(correlation_id, "corr-1");
            }
            other => panic!("unexpected open intent: {other:?}"),
        }
        match close {
            PlanningIntent::ClosePosition { correlation_id, .. } => {
                assert_eq!(correlation_id, "corr-close-1");
            }
            other => panic!("unexpected close intent: {other:?}"),
        }
        match delta {
            PlanningIntent::DeltaRebalance {
                correlation_id,
                target_residual_delta_max,
                target_shock_loss_max,
                ..
            } => {
                assert_eq!(correlation_id, "corr-rebalance-1");
                assert_eq!(target_residual_delta_max, 0.02);
                assert_eq!(target_shock_loss_max, 5.0);
            }
            other => panic!("unexpected delta intent: {other:?}"),
        }
        match force {
            PlanningIntent::ForceExit {
                correlation_id,
                force_reason,
                allow_negative_edge,
                ..
            } => {
                assert_eq!(correlation_id, "corr-force-1");
                assert_eq!(force_reason, "emergency_stop");
                assert!(allow_negative_edge);
            }
            other => panic!("unexpected force-exit intent: {other:?}"),
        }
    }

    #[test]
    fn build_execution_stack_wires_orderbook_provider_into_execution_manager() {
        let settings = sample_settings();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, executor, execution) = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(build_execution_stack(
                &settings,
                &registry,
                RuntimeExecutionMode::Paper,
                1_700_000_000_000,
                None,
            ))
            .expect("paper execution stack");

        assert!(matches!(executor, RuntimeExecutor::DryRun(_)));
        assert!(execution.orderbook_provider().is_some());
    }

    #[test]
    fn sample_settings_keep_live_default_band_policy() {
        let settings = sample_settings();
        assert_eq!(
            settings.strategy.basis.band_policy,
            BandPolicyMode::ConfiguredBand
        );
    }

    #[test]
    fn build_execution_stack_uses_configured_execution_costs() {
        let mut settings = sample_settings();
        settings.execution_costs.poly_fee_bps = 17;
        settings.execution_costs.cex_taker_fee_bps = 9;
        settings.execution_costs.cex_maker_fee_bps = 3;
        settings.execution_costs.fallback_funding_bps_per_day = 4;
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, _executor, execution) = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(build_execution_stack(
                &settings,
                &registry,
                RuntimeExecutionMode::Paper,
                1_700_000_000_000,
                None,
            ))
            .expect("paper execution stack");

        let planner = execution.planner();
        assert_eq!(planner.poly_fee_bps, 17);
        assert_eq!(planner.cex_taker_fee_bps, 9);
        assert_eq!(planner.cex_maker_fee_bps, 3);
        assert_eq!(planner.fallback_funding_bps_per_day, 4);
    }

    #[test]
    fn build_execution_stack_uses_dry_run_for_live_mock_mode() {
        let settings = sample_settings();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, executor, _execution) = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(build_execution_stack(
                &settings,
                &registry,
                RuntimeExecutionMode::LiveMock,
                1_700_000_000_000,
                Some(&settings.markets[0]),
            ))
            .expect("live mock execution stack");

        assert!(matches!(executor, RuntimeExecutor::DryRun(_)));
    }

    #[test]
    fn build_execution_stack_live_mode_fails_fast_without_auth() {
        let settings = sample_settings();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let result =
            tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(build_execution_stack(
                    &settings,
                    &registry,
                    RuntimeExecutionMode::Live,
                    1_700_000_000_000,
                    Some(&settings.markets[0]),
                ));

        match result {
            Ok(_) => panic!("armed live must fail without auth"),
            Err(err) => {
                let rendered = format!("{err:#}");
                assert!(rendered.contains("failed to build polymarket live executor"));
                assert!(rendered.contains("polymarket.private_key"));
            }
        }
    }

    #[test]
    fn build_execution_stack_live_mode_rejects_mixed_multi_market_venues() {
        let mut settings = sample_settings();
        settings.markets.push(sample_okx_market());
        let registry = SymbolRegistry::new(settings.markets.clone());

        let err =
            match tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(build_execution_stack(
                    &settings,
                    &registry,
                    RuntimeExecutionMode::Live,
                    1_700_000_000_000,
                    None,
                )) {
                Ok(_) => panic!("mixed-venue armed live must be rejected"),
                Err(err) => err,
            };

        let rendered = format!("{err:#}");
        assert!(rendered.contains("single hedge exchange"));
        assert!(rendered.contains("Binance"));
        assert!(rendered.contains("Okx"));
    }

    #[test]
    fn build_execution_stack_live_mode_accepts_uniform_multi_market_target() {
        let mut settings = sample_settings();
        let mut second_market = sample_market();
        second_market.symbol = Symbol::new("eth-runtime");
        second_market.cex_symbol = "ETHUSDT".to_owned();
        settings.markets.push(second_market);
        let registry = SymbolRegistry::new(settings.markets.clone());

        let err =
            match tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(build_execution_stack(
                    &settings,
                    &registry,
                    RuntimeExecutionMode::Live,
                    1_700_000_000_000,
                    None,
                )) {
                Ok(_) => {
                    panic!(
                        "uniform multi-market armed live should reach live executor construction"
                    )
                }
                Err(err) => err,
            };

        let rendered = format!("{err:#}");
        assert!(rendered.contains("failed to build polymarket live executor"));
        assert!(rendered.contains("polymarket.private_key"));
    }

    #[test]
    fn normalized_books_build_shared_planning_context() {
        let settings = sample_settings();
        let market = sample_market();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let channels = create_channels(std::slice::from_ref(&market.symbol));
        let mut market_data_rx = channels.market_data_tx.subscribe();
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let (provider, _executor, mut execution) = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(build_execution_stack(
                &settings,
                &registry,
                RuntimeExecutionMode::Paper,
                1_700_000_000_000,
                None,
            ))
            .expect("execution stack");

        manager
            .normalize_and_publish_poly_orderbook(PolyBookUpdate {
                asset_id: market.poly_ids.yes_token_id.clone(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(49, 2)),
                    shares: PolyShares(Decimal::new(10, 0)),
                }],
                asks: vec![PolyBookLevel {
                    price: Price(Decimal::new(51, 2)),
                    shares: PolyShares(Decimal::new(12, 0)),
                }],
                exchange_timestamp_ms: 10,
                received_at_ms: 11,
                sequence: 7,
                last_trade_price: None,
            })
            .expect("publish yes book");
        manager
            .normalize_and_publish_poly_orderbook(PolyBookUpdate {
                asset_id: market.poly_ids.no_token_id.clone(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(48, 2)),
                    shares: PolyShares(Decimal::new(8, 0)),
                }],
                asks: vec![PolyBookLevel {
                    price: Price(Decimal::new(52, 2)),
                    shares: PolyShares(Decimal::new(9, 0)),
                }],
                exchange_timestamp_ms: 12,
                received_at_ms: 13,
                sequence: 8,
                last_trade_price: None,
            })
            .expect("publish no book");
        manager
            .normalize_and_publish_cex_orderbook(CexBookUpdate {
                exchange: Exchange::Binance,
                venue_symbol: market.cex_symbol.clone(),
                bids: vec![CexBookLevel {
                    price: Price(Decimal::new(100_000, 0)),
                    base_qty: CexBaseQty(Decimal::new(2, 0)),
                }],
                asks: vec![CexBookLevel {
                    price: Price(Decimal::new(100_010, 0)),
                    base_qty: CexBaseQty(Decimal::new(3, 0)),
                }],
                exchange_timestamp_ms: 14,
                received_at_ms: 15,
                sequence: 21,
            })
            .expect("publish cex book");

        loop {
            match market_data_rx.try_recv() {
                Ok(MarketDataEvent::OrderBookUpdate { snapshot }) => {
                    apply_orderbook_snapshot(&provider, &registry, &snapshot);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
                | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }

        let context = execution
            .planning_context_for_symbol(&market.symbol)
            .expect("planning context");

        assert_eq!(context.poly_yes_book.sequence, 7);
        assert_eq!(context.poly_no_book.sequence, 8);
        assert_eq!(context.cex_book.sequence, 21);
        assert_eq!(context.poly_yes_book.asks.len(), 1);
        assert_eq!(context.poly_no_book.asks.len(), 1);
        assert_eq!(context.cex_book.asks.len(), 1);
    }

    #[test]
    fn previewed_open_plan_can_be_rejected_by_plan_aware_risk_gate() {
        let settings = sample_settings();
        let market = sample_market();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let channels = create_channels(std::slice::from_ref(&market.symbol));
        let mut market_data_rx = channels.market_data_tx.subscribe();
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let (provider, _executor, mut execution) = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(build_execution_stack(
                &settings,
                &registry,
                RuntimeExecutionMode::Paper,
                1_700_000_000_000,
                None,
            ))
            .expect("execution stack");

        manager
            .normalize_and_publish_poly_orderbook(PolyBookUpdate {
                asset_id: market.poly_ids.yes_token_id.clone(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(49, 2)),
                    shares: PolyShares(Decimal::new(10, 0)),
                }],
                asks: vec![PolyBookLevel {
                    price: Price(Decimal::new(51, 2)),
                    shares: PolyShares(Decimal::new(12, 0)),
                }],
                exchange_timestamp_ms: 10,
                received_at_ms: 11,
                sequence: 7,
                last_trade_price: None,
            })
            .expect("publish yes book");
        manager
            .normalize_and_publish_poly_orderbook(PolyBookUpdate {
                asset_id: market.poly_ids.no_token_id.clone(),
                bids: vec![PolyBookLevel {
                    price: Price(Decimal::new(48, 2)),
                    shares: PolyShares(Decimal::new(8, 0)),
                }],
                asks: vec![PolyBookLevel {
                    price: Price(Decimal::new(52, 2)),
                    shares: PolyShares(Decimal::new(9, 0)),
                }],
                exchange_timestamp_ms: 12,
                received_at_ms: 13,
                sequence: 8,
                last_trade_price: None,
            })
            .expect("publish no book");
        manager
            .normalize_and_publish_cex_orderbook(CexBookUpdate {
                exchange: Exchange::Binance,
                venue_symbol: market.cex_symbol.clone(),
                bids: vec![CexBookLevel {
                    price: Price(Decimal::new(100_000, 0)),
                    base_qty: CexBaseQty(Decimal::new(2, 0)),
                }],
                asks: vec![CexBookLevel {
                    price: Price(Decimal::new(100_010, 0)),
                    base_qty: CexBaseQty(Decimal::new(3, 0)),
                }],
                exchange_timestamp_ms: 14,
                received_at_ms: 15,
                sequence: 21,
            })
            .expect("publish cex book");

        loop {
            match market_data_rx.try_recv() {
                Ok(MarketDataEvent::OrderBookUpdate { snapshot }) => {
                    apply_orderbook_snapshot(&provider, &registry, &snapshot);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
                | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }

        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-risk-plan".to_owned(),
            correlation_id: "corr-risk-plan".to_owned(),
            symbol: market.symbol.clone(),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.55,
            raw_mispricing: 0.04,
            delta_estimate: 0.00012,
            risk_budget_usd: 200.0,
            strength: polyalpha_core::SignalStrength::Normal,
            z_score: Some(2.2),
            timestamp_ms: 20,
        };
        let plan = execution
            .preview_intent(&open_intent_from_candidate(&candidate))
            .expect("candidate should preview into a trade plan");

        let risk = InMemoryRiskManager::new(RiskLimits {
            max_total_exposure_usd: UsdNotional(Decimal::new(10_000, 0)),
            max_single_position_usd: UsdNotional(Decimal::new(150, 0)),
            max_daily_loss_usd: UsdNotional(Decimal::new(1_000, 0)),
        });
        let err = risk
            .pre_trade_check_open_plan(&plan)
            .expect_err("plan-aware gate should reject poly budget above limit");

        assert!(matches!(err, RiskRejection::LimitBreached(_)));
    }
}
