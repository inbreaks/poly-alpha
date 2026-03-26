use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use sha2::Sha256;

use polyalpha_core::{
    CexBaseQty, CexOrderRequest, ClientOrderId, CoreError, Exchange, OrderExecutor, OrderId,
    OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, Result, Symbol, TimeInForce,
    VenueQuantity,
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceSignedRequestPreview {
    pub method: &'static str,
    pub path: &'static str,
    pub url: String,
    pub query: String,
}

#[derive(Clone, Debug)]
pub struct BinanceFuturesExecutor {
    client: Client,
    rest_url: String,
    api_key: String,
    api_secret: String,
    recv_window: u64,
    order_symbol_map: Arc<Mutex<HashMap<OrderId, String>>>,
    symbol_venue_map: Arc<Mutex<HashMap<Symbol, String>>>,
}

impl BinanceFuturesExecutor {
    pub fn new(
        rest_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_url: rest_url.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window: 5_000,
            order_symbol_map: Arc::new(Mutex::new(HashMap::new())),
            symbol_venue_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_recv_window(mut self, recv_window: u64) -> Self {
        self.recv_window = recv_window;
        self
    }

    pub fn preview_submit_order(
        &self,
        request: &CexOrderRequest,
        timestamp_ms: u64,
    ) -> Result<BinanceSignedRequestPreview> {
        if request.exchange != Exchange::Binance {
            return Err(CoreError::Channel(
                "binance preview requires a binance cex request".to_owned(),
            ));
        }

        let query = self.signed_query(Self::build_submit_params(request), timestamp_ms)?;
        let path = "/fapi/v1/order";
        Ok(BinanceSignedRequestPreview {
            method: "POST",
            path,
            url: format!("{}{}?{}", self.normalize_base_url(), path, query),
            query,
        })
    }

    fn ensure_binance(request: &OrderRequest) -> Result<&CexOrderRequest> {
        match request {
            OrderRequest::Cex(req) if req.exchange == Exchange::Binance => Ok(req),
            OrderRequest::Cex(_) => Err(CoreError::Channel(
                "binance executor received a non-binance cex request".to_owned(),
            )),
            OrderRequest::Poly(_) => Err(CoreError::Channel(
                "binance executor does not handle polymarket requests".to_owned(),
            )),
        }
    }

    fn normalize_base_url(&self) -> String {
        self.rest_url.trim_end_matches('/').to_owned()
    }

    fn now_timestamp_ms() -> Result<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| CoreError::Channel(format!("system clock before unix epoch: {err}")))?;
        Ok(duration.as_millis() as u64)
    }

    fn build_query_string(params: &[(String, String)]) -> String {
        params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    fn sign_payload(secret: &str, payload: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|err| CoreError::Channel(format!("invalid binance secret for hmac: {err}")))?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn signed_query(&self, mut params: Vec<(String, String)>, timestamp_ms: u64) -> Result<String> {
        params.push(("recvWindow".to_owned(), self.recv_window.to_string()));
        params.push(("timestamp".to_owned(), timestamp_ms.to_string()));
        let raw_query = Self::build_query_string(&params);
        let signature = Self::sign_payload(&self.api_secret, &raw_query)?;
        Ok(format!("{raw_query}&signature={signature}"))
    }

    async fn signed_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<T> {
        let timestamp_ms = Self::now_timestamp_ms()?;
        let query = self.signed_query(params, timestamp_ms)?;
        let url = format!("{}{}?{}", self.normalize_base_url(), path, query);
        let response = self
            .client
            .request(method.clone(), &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|err| CoreError::Channel(format!("binance http request failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_owned());
            return Err(CoreError::Channel(format!(
                "binance {} {} failed ({}): {}",
                method, path, status, body
            )));
        }

        response
            .json::<T>()
            .await
            .map_err(|err| CoreError::Channel(format!("binance response parse failed: {err}")))
    }

    fn build_submit_params(req: &CexOrderRequest) -> Vec<(String, String)> {
        let mut params = vec![
            ("symbol".to_owned(), req.venue_symbol.clone()),
            ("side".to_owned(), map_side(req.side).to_owned()),
            ("quantity".to_owned(), decimal_to_string(req.base_qty.0)),
            ("newClientOrderId".to_owned(), req.client_order_id.0.clone()),
            ("newOrderRespType".to_owned(), "RESULT".to_owned()),
        ];

        match req.order_type {
            OrderType::Market => {
                params.push(("type".to_owned(), "MARKET".to_owned()));
            }
            OrderType::Limit => {
                params.push(("type".to_owned(), "LIMIT".to_owned()));
                params.push((
                    "timeInForce".to_owned(),
                    map_time_in_force(req.time_in_force).to_owned(),
                ));
                if let Some(price) = req.price {
                    params.push(("price".to_owned(), decimal_to_string(price.0)));
                }
            }
            OrderType::PostOnly => {
                params.push(("type".to_owned(), "LIMIT".to_owned()));
                params.push(("timeInForce".to_owned(), "GTX".to_owned()));
                if let Some(price) = req.price {
                    params.push(("price".to_owned(), decimal_to_string(price.0)));
                }
            }
        }

        if req.reduce_only {
            params.push(("reduceOnly".to_owned(), "true".to_owned()));
        }

        params
    }

    fn remember_order(
        &self,
        order_id: &OrderId,
        symbol: &Symbol,
        venue_symbol: &str,
    ) -> Result<()> {
        self.order_symbol_map
            .lock()
            .map_err(|_| CoreError::Channel("binance order symbol map poisoned".to_owned()))?
            .insert(order_id.clone(), venue_symbol.to_owned());
        self.symbol_venue_map
            .lock()
            .map_err(|_| CoreError::Channel("binance symbol map poisoned".to_owned()))?
            .insert(symbol.clone(), venue_symbol.to_owned());
        Ok(())
    }

    fn venue_symbol_for_order(&self, order_id: &OrderId) -> Result<String> {
        self.order_symbol_map
            .lock()
            .map_err(|_| CoreError::Channel("binance order symbol map poisoned".to_owned()))?
            .get(order_id)
            .cloned()
            .ok_or_else(|| {
                CoreError::Channel(format!(
                    "missing venue symbol for order {}; submit it first with this executor",
                    order_id.0
                ))
            })
    }

    fn venue_symbol_for_business_symbol(&self, symbol: &Symbol) -> Result<String> {
        let maybe = self
            .symbol_venue_map
            .lock()
            .map_err(|_| CoreError::Channel("binance symbol map poisoned".to_owned()))?
            .get(symbol)
            .cloned();
        Ok(maybe.unwrap_or_else(|| fallback_venue_symbol(symbol)))
    }
}

#[async_trait]
impl OrderExecutor for BinanceFuturesExecutor {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let req = Self::ensure_binance(&request)?;
        if req.base_qty.0 <= Decimal::ZERO {
            return Err(CoreError::Channel(
                "binance reject zero quantity order".to_owned(),
            ));
        }
        let params = Self::build_submit_params(req);
        let payload: BinanceOrderPayload = self
            .signed_json(Method::POST, "/fapi/v1/order", params)
            .await?;

        let order_id = OrderId(payload.order_id.as_string());
        self.remember_order(&order_id, &req.symbol, &req.venue_symbol)?;

        Ok(OrderResponse {
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: order_id,
            status: map_binance_status(&payload.status),
            filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty(
                parse_decimal(&payload.executed_qty).unwrap_or_default(),
            )),
            average_price: parse_decimal(&payload.avg_price).and_then(|v| {
                if v > Decimal::ZERO {
                    Some(polyalpha_core::Price(v))
                } else {
                    None
                }
            }),
            rejection_reason: None,
            timestamp_ms: payload.update_time,
        })
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        if exchange != Exchange::Binance {
            return Err(CoreError::Channel(
                "binance executor received cancel request for non-binance exchange".to_owned(),
            ));
        }

        let venue_symbol = self.venue_symbol_for_order(order_id)?;
        let params = vec![
            ("symbol".to_owned(), venue_symbol),
            ("orderId".to_owned(), order_id.0.clone()),
        ];
        let _payload: BinanceOrderPayload = self
            .signed_json(Method::DELETE, "/fapi/v1/order", params)
            .await?;
        Ok(())
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        if exchange != Exchange::Binance {
            return Err(CoreError::Channel(
                "binance executor received cancel_all request for non-binance exchange".to_owned(),
            ));
        }

        let venue_symbol = self.venue_symbol_for_business_symbol(symbol)?;
        let open_orders: Vec<BinanceOpenOrder> = self
            .signed_json(
                Method::GET,
                "/fapi/v1/openOrders",
                vec![("symbol".to_owned(), venue_symbol.clone())],
            )
            .await?;

        let mut cancelled = 0u32;
        for order in open_orders {
            let order_id = OrderId(order.order_id.as_string());
            let params = vec![
                ("symbol".to_owned(), venue_symbol.clone()),
                ("orderId".to_owned(), order_id.0.clone()),
            ];
            let _payload: BinanceOrderPayload = self
                .signed_json(Method::DELETE, "/fapi/v1/order", params)
                .await?;
            cancelled += 1;
        }
        Ok(cancelled)
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        if exchange != Exchange::Binance {
            return Err(CoreError::Channel(
                "binance executor received query request for non-binance exchange".to_owned(),
            ));
        }

        let venue_symbol = self.venue_symbol_for_order(order_id)?;
        let payload: BinanceOrderPayload = self
            .signed_json(
                Method::GET,
                "/fapi/v1/order",
                vec![
                    ("symbol".to_owned(), venue_symbol),
                    ("orderId".to_owned(), order_id.0.clone()),
                ],
            )
            .await?;

        Ok(OrderResponse {
            client_order_id: ClientOrderId(payload.client_order_id),
            exchange_order_id: OrderId(payload.order_id.as_string()),
            status: map_binance_status(&payload.status),
            filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty(
                parse_decimal(&payload.executed_qty).unwrap_or_default(),
            )),
            average_price: parse_decimal(&payload.avg_price).and_then(|v| {
                if v > Decimal::ZERO {
                    Some(polyalpha_core::Price(v))
                } else {
                    None
                }
            }),
            rejection_reason: None,
            timestamp_ms: payload.update_time,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum StringOrNum {
    String(String),
    I64(i64),
    U64(u64),
}

impl StringOrNum {
    fn as_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::I64(value) => value.to_string(),
            Self::U64(value) => value.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceOrderPayload {
    #[serde(rename = "orderId")]
    order_id: StringOrNum,
    #[serde(default)]
    client_order_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    executed_qty: String,
    #[serde(default)]
    avg_price: String,
    #[serde(default)]
    update_time: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceOpenOrder {
    #[serde(rename = "orderId")]
    order_id: StringOrNum,
}

fn decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_decimal(value: &str) -> Option<Decimal> {
    if value.trim().is_empty() {
        return None;
    }
    value.parse::<Decimal>().ok()
}

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn map_time_in_force(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc => "GTC",
        TimeInForce::Ioc => "IOC",
        TimeInForce::Fok => "FOK",
    }
}

fn map_binance_status(status: &str) -> OrderStatus {
    match status {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartialFill,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" | "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Rejected,
        _ => OrderStatus::Pending,
    }
}

fn fallback_venue_symbol(symbol: &Symbol) -> String {
    symbol.0.to_ascii_uppercase().replace('-', "")
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{CexOrderRequest, OrderSide, OrderType, TimeInForce};

    use super::*;

    fn sample_order(order_type: OrderType, tif: TimeInForce) -> CexOrderRequest {
        CexOrderRequest {
            client_order_id: ClientOrderId("cid-1".to_owned()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            venue_symbol: "BTCUSDT".to_owned(),
            side: OrderSide::Buy,
            order_type,
            price: Some(polyalpha_core::Price(Decimal::new(100_000, 0))),
            base_qty: CexBaseQty(Decimal::new(5, 1)),
            time_in_force: tif,
            reduce_only: false,
        }
    }

    #[test]
    fn hmac_signature_matches_known_sha256_vector() {
        let signature = BinanceFuturesExecutor::sign_payload(
            "key",
            "The quick brown fox jumps over the lazy dog",
        )
        .expect("signature should build");
        assert_eq!(
            signature,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn submit_params_for_post_only_map_to_gtx_limit() {
        let req = sample_order(OrderType::PostOnly, TimeInForce::Gtc);
        let params = BinanceFuturesExecutor::build_submit_params(&req);
        let as_map: HashMap<_, _> = params.into_iter().collect();

        assert_eq!(as_map.get("type"), Some(&"LIMIT".to_owned()));
        assert_eq!(as_map.get("timeInForce"), Some(&"GTX".to_owned()));
        assert_eq!(as_map.get("symbol"), Some(&"BTCUSDT".to_owned()));
    }

    #[test]
    fn signed_query_contains_timestamp_recv_window_and_signature() {
        let executor = BinanceFuturesExecutor::new("https://fapi.binance.com", "k", "s");
        let params = vec![("symbol".to_owned(), "BTCUSDT".to_owned())];
        let query = executor
            .signed_query(params, 1_700_000_000_000)
            .expect("query should build");

        assert!(query.contains("symbol=BTCUSDT"));
        assert!(query.contains("timestamp=1700000000000"));
        assert!(query.contains("recvWindow=5000"));
        assert!(query.contains("signature="));
    }

    #[test]
    fn map_status_covers_common_binance_states() {
        assert_eq!(map_binance_status("NEW"), OrderStatus::Open);
        assert_eq!(map_binance_status("FILLED"), OrderStatus::Filled);
        assert_eq!(
            map_binance_status("PARTIALLY_FILLED"),
            OrderStatus::PartialFill
        );
        assert_eq!(map_binance_status("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(map_binance_status("REJECTED"), OrderStatus::Rejected);
    }
}
