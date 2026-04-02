use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use polymarket_client_sdk::{
    auth::{state::Authenticated, Normal, Signer as _},
    clob::{
        types::{
            Amount, OrderType as SdkOrderType, Side as SdkSide, SignatureType as SdkSignatureType,
        },
        Client as SdkClient, Config as SdkConfig,
    },
    types::{Address, B256, U256},
};
use rust_decimal::Decimal;

use polyalpha_core::{
    ClientOrderId, CoreError, Exchange, OrderExecutor, OrderId, OrderRequest, OrderResponse,
    OrderSide, OrderStatus, OrderType, PolyOrderRequest, PolyShares, PolySizingInstruction,
    PolymarketConfig, PolymarketSignatureType, Price, Result, Symbol, SymbolRegistry, TimeInForce,
    TokenSide, VenueQuantity,
};

type AuthenticatedClient = SdkClient<Authenticated<Normal>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreparedPolymarketAmount {
    Usdc(Decimal),
    Shares(Decimal),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreparedPolymarketOrderType {
    GoodTilCancel,
    FillAndKill,
    FillOrKill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedPolymarketOrder {
    token_id: U256,
    side: SdkSide,
    limit_price: Decimal,
    order_type: PreparedPolymarketOrderType,
    amount: PreparedPolymarketAmount,
    limit_size: Option<Decimal>,
    post_only: bool,
}

#[derive(Clone, Debug)]
pub struct PolymarketExecutor {
    client: Option<AuthenticatedClient>,
    signer: Option<PrivateKeySigner>,
    symbol_registry: SymbolRegistry,
}

impl Default for PolymarketExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl PolymarketExecutor {
    pub fn new() -> Self {
        Self::disabled(SymbolRegistry::default())
    }

    pub fn disabled(symbol_registry: SymbolRegistry) -> Self {
        Self {
            client: None,
            signer: None,
            symbol_registry,
        }
    }

    pub async fn from_config(
        config: &PolymarketConfig,
        symbol_registry: SymbolRegistry,
    ) -> Result<Self> {
        let private_key = config.private_key.as_deref().ok_or_else(|| {
            CoreError::Channel(
                "polymarket live executor requires polymarket.private_key".to_owned(),
            )
        })?;
        let signer = PrivateKeySigner::from_str(private_key)
            .map_err(|err| CoreError::Channel(format!("invalid polymarket private key: {err}")))?
            .with_chain_id(Some(config.chain_id));
        let mut auth = SdkClient::new(
            &config.clob_api_url,
            SdkConfig::builder()
                .use_server_time(config.use_server_time)
                .build(),
        )
        .map_err(Self::map_sdk_error)?
        .authentication_builder(&signer);
        if let Some(nonce) = config.api_key_nonce {
            auth = auth.nonce(nonce);
        }
        if let Some(signature_type) = config.signature_type.as_ref() {
            auth = auth.signature_type(map_signature_type(signature_type));
        }
        if let Some(funder) = config.funder.as_deref() {
            let address = Address::from_str(funder).map_err(|err| {
                CoreError::Channel(format!(
                    "invalid polymarket funder address `{funder}`: {err}"
                ))
            })?;
            auth = auth.funder(address);
        }
        let client = auth.authenticate().await.map_err(Self::map_sdk_error)?;
        Ok(Self {
            client: Some(client),
            signer: Some(signer),
            symbol_registry,
        })
    }

    fn map_sdk_error(err: impl std::fmt::Display) -> CoreError {
        CoreError::Channel(format!("polymarket sdk error: {err}"))
    }

    fn disabled_error() -> CoreError {
        CoreError::Channel(
            "polymarket live executor is disabled; authenticate via polymarket.private_key first"
                .to_owned(),
        )
    }

    fn ensure_polymarket(request: &OrderRequest) -> Result<&PolyOrderRequest> {
        match request {
            OrderRequest::Poly(req) => Ok(req),
            OrderRequest::Cex(_) => Err(CoreError::Channel(
                "polymarket executor received a non-polymarket request".to_owned(),
            )),
        }
    }

    fn ensure_polymarket_exchange(exchange: Exchange) -> Result<()> {
        if exchange == Exchange::Polymarket {
            Ok(())
        } else {
            Err(CoreError::Channel(format!(
                "polymarket executor received non-polymarket exchange {exchange:?}"
            )))
        }
    }

    fn client(&self) -> Result<&AuthenticatedClient> {
        self.client.as_ref().ok_or_else(Self::disabled_error)
    }

    fn signer(&self) -> Result<&PrivateKeySigner> {
        self.signer.as_ref().ok_or_else(Self::disabled_error)
    }

    fn market_tick_size(&self, symbol: &Symbol) -> Result<Decimal> {
        self.symbol_registry
            .get_config(symbol)
            .map(|config| config.min_tick_size.0)
            .ok_or_else(|| {
                CoreError::Channel(format!(
                    "missing symbol registry entry for polymarket symbol {}",
                    symbol.0
                ))
            })
    }

    fn condition_id_for(&self, symbol: &Symbol) -> Result<B256> {
        let condition_id = self
            .symbol_registry
            .get_config(symbol)
            .map(|config| config.poly_ids.condition_id.as_str())
            .ok_or_else(|| {
                CoreError::Channel(format!(
                    "missing symbol registry entry for polymarket symbol {}",
                    symbol.0
                ))
            })?;
        B256::from_str(condition_id).map_err(|err| {
            CoreError::Channel(format!(
                "invalid polymarket condition_id `{condition_id}` for {}: {err}",
                symbol.0
            ))
        })
    }
}

#[async_trait]
impl OrderExecutor for PolymarketExecutor {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let request = Self::ensure_polymarket(&request)?;
        let prepared = prepare_order_request(
            &self.symbol_registry,
            request,
            self.market_tick_size(&request.symbol)?,
        )?;
        let client = self.client()?;
        let signer = self.signer()?;

        let signable = match prepared.limit_size {
            Some(size) => {
                let mut builder = client
                    .limit_order()
                    .token_id(prepared.token_id)
                    .side(prepared.side)
                    .order_type(map_prepared_order_type(prepared.order_type))
                    .price(prepared.limit_price)
                    .size(size);
                if prepared.post_only {
                    builder = builder.post_only(true);
                }
                builder.build().await.map_err(Self::map_sdk_error)?
            }
            None => {
                let amount = match prepared.amount {
                    PreparedPolymarketAmount::Usdc(amount) => {
                        Amount::usdc(amount).map_err(Self::map_sdk_error)?
                    }
                    PreparedPolymarketAmount::Shares(amount) => {
                        Amount::shares(amount).map_err(Self::map_sdk_error)?
                    }
                };
                client
                    .market_order()
                    .token_id(prepared.token_id)
                    .side(prepared.side)
                    .order_type(map_prepared_order_type(prepared.order_type))
                    .price(prepared.limit_price)
                    .amount(amount)
                    .build()
                    .await
                    .map_err(Self::map_sdk_error)?
            }
        };

        let signed = client
            .sign(signer, signable)
            .await
            .map_err(Self::map_sdk_error)?;
        let response = client
            .post_order(signed)
            .await
            .map_err(Self::map_sdk_error)?;
        Ok(map_post_order_response(request, response))
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        Self::ensure_polymarket_exchange(exchange)?;
        let response = self
            .client()?
            .cancel_order(&order_id.0)
            .await
            .map_err(Self::map_sdk_error)?;
        if response.canceled.iter().any(|id| id == &order_id.0) {
            return Ok(());
        }
        let detail = response
            .not_canceled
            .get(&order_id.0)
            .cloned()
            .unwrap_or_else(|| "unknown cancel failure".to_owned());
        Err(CoreError::Channel(format!(
            "polymarket cancel rejected for {}: {}",
            order_id.0, detail
        )))
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        use polymarket_client_sdk::clob::types::request::CancelMarketOrderRequest;

        Self::ensure_polymarket_exchange(exchange)?;
        let request = CancelMarketOrderRequest::builder()
            .market(self.condition_id_for(symbol)?)
            .build();
        let response = self
            .client()?
            .cancel_market_orders(&request)
            .await
            .map_err(Self::map_sdk_error)?;
        Ok(response.canceled.len() as u32)
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        let order = self.query_open_order(exchange, order_id).await?;
        Ok(map_open_order_response(order))
    }
}

impl PolymarketExecutor {
    async fn query_open_order(
        &self,
        exchange: Exchange,
        order_id: &OrderId,
    ) -> Result<polymarket_client_sdk::clob::types::response::OpenOrderResponse> {
        Self::ensure_polymarket_exchange(exchange)?;
        self.client()?
            .order(&order_id.0)
            .await
            .map_err(Self::map_sdk_error)
    }
}

fn prepare_order_request(
    symbol_registry: &SymbolRegistry,
    request: &PolyOrderRequest,
    tick_size: Decimal,
) -> Result<PreparedPolymarketOrder> {
    let token_id = resolve_token_id(symbol_registry, &request.symbol, request.token_side)?;
    let side = map_side(request.side);
    let (order_type, post_only) =
        map_order_type(request.order_type, request.time_in_force, request.post_only)?;

    match request.sizing {
        PolySizingInstruction::BuyBudgetCap {
            max_cost_usd,
            max_avg_price,
            max_shares,
        } => {
            validate_positive(max_cost_usd.0, "buy budget max_cost_usd")?;
            validate_price(max_avg_price.0, "buy budget max_avg_price")?;
            validate_positive(max_shares.0, "buy budget max_shares")?;

            match request.order_type {
                OrderType::Market => Ok(PreparedPolymarketOrder {
                    token_id,
                    side,
                    limit_price: max_avg_price.0,
                    order_type,
                    amount: PreparedPolymarketAmount::Usdc(max_cost_usd.0),
                    limit_size: None,
                    post_only,
                }),
                OrderType::Limit | OrderType::PostOnly => {
                    let implied_notional = max_shares.0 * max_avg_price.0;
                    if implied_notional > max_cost_usd.0 {
                        return Err(adapter_constraint_error(
                            "buy limit order would exceed max_cost_usd",
                        ));
                    }
                    Ok(PreparedPolymarketOrder {
                        token_id,
                        side,
                        limit_price: max_avg_price.0,
                        order_type,
                        amount: PreparedPolymarketAmount::Shares(max_shares.0),
                        limit_size: Some(max_shares.0),
                        post_only,
                    })
                }
            }
        }
        PolySizingInstruction::SellExactShares {
            shares,
            min_avg_price,
        } => {
            validate_positive(shares.0, "sell exact shares")?;
            validate_price(min_avg_price.0, "sell min_avg_price")?;
            Ok(PreparedPolymarketOrder {
                token_id,
                side,
                limit_price: min_avg_price.0,
                order_type,
                amount: PreparedPolymarketAmount::Shares(shares.0),
                limit_size: matches!(request.order_type, OrderType::Limit | OrderType::PostOnly)
                    .then_some(shares.0),
                post_only,
            })
        }
        PolySizingInstruction::SellMinProceeds {
            shares,
            min_proceeds_usd,
        } => {
            validate_positive(shares.0, "sell min proceeds shares")?;
            validate_positive(min_proceeds_usd.0, "sell min_proceeds_usd")?;
            let min_avg_price = ceil_to_tick(min_proceeds_usd.0 / shares.0, tick_size);
            validate_price(min_avg_price, "sell min proceeds implied price")?;
            Ok(PreparedPolymarketOrder {
                token_id,
                side,
                limit_price: min_avg_price,
                order_type,
                amount: PreparedPolymarketAmount::Shares(shares.0),
                limit_size: matches!(request.order_type, OrderType::Limit | OrderType::PostOnly)
                    .then_some(shares.0),
                post_only,
            })
        }
    }
}

fn resolve_token_id(
    symbol_registry: &SymbolRegistry,
    symbol: &Symbol,
    token_side: TokenSide,
) -> Result<U256> {
    let raw = symbol_registry
        .get_poly_ids(symbol)
        .map(|ids| match token_side {
            TokenSide::Yes => ids.yes_token_id.as_str(),
            TokenSide::No => ids.no_token_id.as_str(),
        })
        .ok_or_else(|| {
            CoreError::Channel(format!(
                "missing symbol registry entry for polymarket symbol {}",
                symbol.0
            ))
        })?;

    U256::from_str(raw).map_err(|err| {
        CoreError::Channel(format!(
            "invalid polymarket token_id `{raw}` for {}: {err}",
            symbol.0
        ))
    })
}

fn map_signature_type(signature_type: &PolymarketSignatureType) -> SdkSignatureType {
    match signature_type {
        PolymarketSignatureType::Eoa => SdkSignatureType::Eoa,
        PolymarketSignatureType::Proxy => SdkSignatureType::Proxy,
        PolymarketSignatureType::GnosisSafe => SdkSignatureType::GnosisSafe,
    }
}

fn map_side(side: OrderSide) -> SdkSide {
    match side {
        OrderSide::Buy => SdkSide::Buy,
        OrderSide::Sell => SdkSide::Sell,
    }
}

fn map_order_type(
    order_type: OrderType,
    time_in_force: TimeInForce,
    post_only: bool,
) -> Result<(PreparedPolymarketOrderType, bool)> {
    match order_type {
        OrderType::Market => {
            if post_only {
                return Err(adapter_constraint_error(
                    "market order cannot preserve post_only constraint",
                ));
            }
            let mapped = match time_in_force {
                TimeInForce::Ioc => PreparedPolymarketOrderType::FillAndKill,
                TimeInForce::Fok => PreparedPolymarketOrderType::FillOrKill,
                TimeInForce::Gtc => {
                    return Err(adapter_constraint_error(
                        "market order cannot preserve GTC semantics",
                    ))
                }
            };
            Ok((mapped, false))
        }
        OrderType::Limit => {
            let mapped = match time_in_force {
                TimeInForce::Gtc => PreparedPolymarketOrderType::GoodTilCancel,
                TimeInForce::Ioc => PreparedPolymarketOrderType::FillAndKill,
                TimeInForce::Fok => PreparedPolymarketOrderType::FillOrKill,
            };
            if post_only && mapped != PreparedPolymarketOrderType::GoodTilCancel {
                return Err(adapter_constraint_error(
                    "post_only limit order requires GTC semantics",
                ));
            }
            Ok((mapped, post_only))
        }
        OrderType::PostOnly => {
            if time_in_force != TimeInForce::Gtc {
                return Err(adapter_constraint_error(
                    "post_only order requires GTC semantics",
                ));
            }
            Ok((PreparedPolymarketOrderType::GoodTilCancel, true))
        }
    }
}

fn map_prepared_order_type(order_type: PreparedPolymarketOrderType) -> SdkOrderType {
    match order_type {
        PreparedPolymarketOrderType::GoodTilCancel => SdkOrderType::GTC,
        PreparedPolymarketOrderType::FillAndKill => SdkOrderType::FAK,
        PreparedPolymarketOrderType::FillOrKill => SdkOrderType::FOK,
    }
}

fn map_post_order_response(
    request: &PolyOrderRequest,
    response: polymarket_client_sdk::clob::types::response::PostOrderResponse,
) -> OrderResponse {
    let filled_shares = filled_shares_from_post_response(request.side, &response);
    let average_price = average_price_from_post_response(request.side, &response, filled_shares);
    let status = match response.status {
        polymarket_client_sdk::clob::types::OrderStatusType::Matched => {
            if filled_shares.0 > Decimal::ZERO {
                OrderStatus::Filled
            } else {
                OrderStatus::Pending
            }
        }
        polymarket_client_sdk::clob::types::OrderStatusType::Canceled => {
            if filled_shares.0 > Decimal::ZERO {
                OrderStatus::PartialFill
            } else {
                OrderStatus::Cancelled
            }
        }
        polymarket_client_sdk::clob::types::OrderStatusType::Live
        | polymarket_client_sdk::clob::types::OrderStatusType::Delayed
        | polymarket_client_sdk::clob::types::OrderStatusType::Unmatched => OrderStatus::Open,
        polymarket_client_sdk::clob::types::OrderStatusType::Unknown(_) => OrderStatus::Pending,
        _ => OrderStatus::Pending,
    };

    OrderResponse {
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: OrderId(response.order_id),
        status,
        filled_quantity: VenueQuantity::PolyShares(filled_shares),
        average_price,
        rejection_reason: response.error_msg,
        timestamp_ms: now_timestamp_ms(),
    }
}

fn map_open_order_response(
    order: polymarket_client_sdk::clob::types::response::OpenOrderResponse,
) -> OrderResponse {
    let filled_shares = PolyShares(order.size_matched);
    let status = match order.status {
        polymarket_client_sdk::clob::types::OrderStatusType::Canceled => {
            if order.size_matched > Decimal::ZERO {
                OrderStatus::PartialFill
            } else {
                OrderStatus::Cancelled
            }
        }
        polymarket_client_sdk::clob::types::OrderStatusType::Matched => OrderStatus::Filled,
        polymarket_client_sdk::clob::types::OrderStatusType::Live
        | polymarket_client_sdk::clob::types::OrderStatusType::Delayed
        | polymarket_client_sdk::clob::types::OrderStatusType::Unmatched => {
            if order.size_matched > Decimal::ZERO {
                OrderStatus::PartialFill
            } else {
                OrderStatus::Open
            }
        }
        polymarket_client_sdk::clob::types::OrderStatusType::Unknown(_) => OrderStatus::Pending,
        _ => OrderStatus::Pending,
    };

    OrderResponse {
        client_order_id: ClientOrderId(order.id.clone()),
        exchange_order_id: OrderId(order.id),
        status,
        filled_quantity: VenueQuantity::PolyShares(filled_shares),
        average_price: (order.size_matched > Decimal::ZERO).then_some(Price(order.price)),
        rejection_reason: None,
        timestamp_ms: order.created_at.timestamp_millis() as u64,
    }
}

fn filled_shares_from_post_response(
    side: OrderSide,
    response: &polymarket_client_sdk::clob::types::response::PostOrderResponse,
) -> PolyShares {
    match side {
        OrderSide::Buy => PolyShares(response.taking_amount),
        OrderSide::Sell => PolyShares(response.making_amount),
    }
}

fn average_price_from_post_response(
    side: OrderSide,
    response: &polymarket_client_sdk::clob::types::response::PostOrderResponse,
    filled_shares: PolyShares,
) -> Option<Price> {
    if filled_shares.0 <= Decimal::ZERO {
        return None;
    }
    let total_usd = match side {
        OrderSide::Buy => response.making_amount,
        OrderSide::Sell => response.taking_amount,
    };
    Some(Price(total_usd / filled_shares.0))
}

fn now_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn validate_positive(value: Decimal, label: &str) -> Result<()> {
    if value > Decimal::ZERO {
        Ok(())
    } else {
        Err(adapter_constraint_error(&format!(
            "{label} must be strictly positive"
        )))
    }
}

fn validate_price(value: Decimal, label: &str) -> Result<()> {
    if value > Decimal::ZERO && value < Decimal::ONE {
        Ok(())
    } else {
        Err(adapter_constraint_error(&format!(
            "{label} must stay inside (0, 1)"
        )))
    }
}

fn ceil_to_tick(value: Decimal, tick_size: Decimal) -> Decimal {
    let tick_size = tick_size.abs();
    if tick_size <= Decimal::ZERO {
        return value;
    }
    ((value / tick_size).ceil() * tick_size).round_dp(tick_size.scale())
}

fn adapter_constraint_error(detail: &str) -> CoreError {
    CoreError::Channel(format!("adapter_cannot_preserve_constraints: {detail}"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use polyalpha_core::{
        ClientOrderId, Exchange, MarketConfig, OrderRequest, OrderSide, OrderType,
        PolyOrderRequest, PolyShares, PolySizingInstruction, PolymarketIds, Price, Symbol,
        SymbolRegistry, TimeInForce, TokenSide, UsdNotional,
    };

    use super::*;

    fn sample_registry() -> SymbolRegistry {
        SymbolRegistry::new(vec![MarketConfig {
            symbol: Symbol::new("btc-100k-mar-2026"),
            poly_ids: PolymarketIds {
                condition_id: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_owned(),
                yes_token_id:
                    "15871154585880608648532107628464183779895785213830018178010423617714102767076"
                        .to_owned(),
                no_token_id:
                    "25871154585880608648532107628464183779895785213830018178010423617714102767076"
                        .to_owned(),
            },
            market_question: None,
            market_rule: None,
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: None,
            settlement_timestamp: 0,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }])
    }

    #[test]
    fn prepare_buy_budget_cap_market_order_preserves_cost_and_limit_price() {
        let prepared = prepare_order_request(
            &sample_registry(),
            &PolyOrderRequest {
                client_order_id: ClientOrderId("cid".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::Yes,
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                sizing: PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd: polyalpha_core::UsdNotional(Decimal::new(20_000, 2)),
                    max_avg_price: Price(Decimal::new(32137, 5)),
                    max_shares: PolyShares(Decimal::new(6557, 1)),
                },
                time_in_force: TimeInForce::Fok,
                post_only: false,
            },
            Decimal::new(1, 2),
        )
        .expect("prepared polymarket order");

        assert_eq!(
            prepared.token_id,
            U256::from_str(
                "15871154585880608648532107628464183779895785213830018178010423617714102767076"
            )
            .unwrap()
        );
        assert_eq!(prepared.limit_price, Decimal::new(32137, 5));
        assert_eq!(prepared.order_type, PreparedPolymarketOrderType::FillOrKill);
        assert_eq!(
            prepared.amount,
            PreparedPolymarketAmount::Usdc(Decimal::new(20_000, 2))
        );
        assert_eq!(prepared.limit_size, None);
    }

    #[test]
    fn prepare_sell_min_proceeds_rounds_price_up_to_tick() {
        let prepared = prepare_order_request(
            &sample_registry(),
            &PolyOrderRequest {
                client_order_id: ClientOrderId("cid".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::Yes,
                side: OrderSide::Sell,
                order_type: OrderType::Market,
                sizing: PolySizingInstruction::SellMinProceeds {
                    shares: PolyShares(Decimal::new(3, 0)),
                    min_proceeds_usd: UsdNotional(Decimal::new(1001, 3)),
                },
                time_in_force: TimeInForce::Fok,
                post_only: false,
            },
            Decimal::new(1, 2),
        )
        .expect("prepared sell-min-proceeds order");

        assert_eq!(prepared.limit_price, Decimal::new(34, 2));
        assert_eq!(
            prepared.amount,
            PreparedPolymarketAmount::Shares(Decimal::new(3, 0))
        );
    }

    #[test]
    fn prepare_limit_buy_rejects_when_budget_cap_identity_cannot_be_preserved() {
        let err = prepare_order_request(
            &sample_registry(),
            &PolyOrderRequest {
                client_order_id: ClientOrderId("cid".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::Yes,
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                sizing: PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd: UsdNotional(Decimal::new(100, 0)),
                    max_avg_price: Price(Decimal::new(5, 1)),
                    max_shares: PolyShares(Decimal::new(300, 0)),
                },
                time_in_force: TimeInForce::Gtc,
                post_only: false,
            },
            Decimal::new(1, 2),
        )
        .expect_err("inconsistent limit buy must reject");

        assert!(err
            .to_string()
            .contains("adapter_cannot_preserve_constraints"));
    }

    #[tokio::test]
    async fn rejects_non_polymarket_requests() {
        let executor = PolymarketExecutor::disabled(sample_registry());
        let err = executor
            .submit_order(OrderRequest::Cex(polyalpha_core::CexOrderRequest {
                client_order_id: ClientOrderId("cid".to_owned()),
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-100k-mar-2026"),
                venue_symbol: "BTCUSDT".to_owned(),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                price: None,
                base_qty: polyalpha_core::CexBaseQty::default(),
                time_in_force: TimeInForce::Ioc,
                reduce_only: false,
            }))
            .await
            .expect_err("must reject non-polymarket requests");
        match err {
            CoreError::Channel(text) => {
                assert!(text.contains("non-polymarket"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
