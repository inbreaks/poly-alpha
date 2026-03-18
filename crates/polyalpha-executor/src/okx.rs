use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use polyalpha_core::{
    CexBaseQty, CexOrderRequest, ClientOrderId, CoreError, Exchange, OrderExecutor, OrderId,
    OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, Result, Symbol, TimeInForce,
    VenueQuantity,
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OkxSignedRequestPreview {
    pub method: &'static str,
    pub path: &'static str,
    pub timestamp: String,
    pub signature: String,
    pub body: String,
}

#[derive(Clone, Debug)]
pub struct OkxExecutor {
    client: Client,
    rest_url: String,
    api_key: String,
    api_secret: String,
    passphrase: String,
    order_inst_map: Arc<Mutex<HashMap<OrderId, String>>>,
    symbol_inst_map: Arc<Mutex<HashMap<Symbol, String>>>,
}

impl OkxExecutor {
    pub fn new(
        rest_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_url: rest_url.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            passphrase: passphrase.into(),
            order_inst_map: Arc::new(Mutex::new(HashMap::new())),
            symbol_inst_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn preview_submit_order(
        &self,
        request: &CexOrderRequest,
        timestamp: impl Into<String>,
    ) -> Result<OkxSignedRequestPreview> {
        if request.exchange != Exchange::Okx {
            return Err(CoreError::Channel(
                "okx preview requires an okx cex request".to_owned(),
            ));
        }

        let body = serde_json::to_string(&Self::build_submit_body(request))
            .map_err(|err| CoreError::Channel(format!("okx body serialize failed: {err}")))?;
        let timestamp = timestamp.into();
        let path = "/api/v5/trade/order";
        let signature =
            Self::sign_payload(&self.api_secret, &format!("{timestamp}POST{path}{body}"))?;
        Ok(OkxSignedRequestPreview {
            method: "POST",
            path,
            timestamp,
            signature,
            body,
        })
    }

    fn ensure_okx(request: &OrderRequest) -> Result<&CexOrderRequest> {
        match request {
            OrderRequest::Cex(req) if req.exchange == Exchange::Okx => Ok(req),
            OrderRequest::Cex(_) => Err(CoreError::Channel(
                "okx executor received a non-okx cex request".to_owned(),
            )),
            OrderRequest::Poly(_) => Err(CoreError::Channel(
                "okx executor does not handle polymarket requests".to_owned(),
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

    fn now_iso8601() -> String {
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    fn sign_payload(secret: &str, payload: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|err| CoreError::Channel(format!("invalid okx secret for hmac: {err}")))?;
        mac.update(payload.as_bytes());
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    fn build_query_string(params: &[(String, String)]) -> String {
        params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    async fn signed_get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: Vec<(String, String)>,
    ) -> Result<T> {
        let query_string = Self::build_query_string(&query);
        let path_with_query = if query_string.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query_string}")
        };
        let timestamp = Self::now_iso8601();
        let prehash = format!("{timestamp}GET{path_with_query}");
        let signature = Self::sign_payload(&self.api_secret, &prehash)?;
        let url = format!("{}{}", self.normalize_base_url(), path_with_query);

        let response = self
            .client
            .request(Method::GET, &url)
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .send()
            .await
            .map_err(|err| CoreError::Channel(format!("okx http get request failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_owned());
            return Err(CoreError::Channel(format!(
                "okx GET {} failed ({}): {}",
                path, status, body
            )));
        }

        response
            .json::<T>()
            .await
            .map_err(|err| CoreError::Channel(format!("okx get response parse failed: {err}")))
    }

    async fn signed_post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let body_text = serde_json::to_string(body)
            .map_err(|err| CoreError::Channel(format!("okx body serialize failed: {err}")))?;
        let timestamp = Self::now_iso8601();
        let prehash = format!("{timestamp}POST{path}{body_text}");
        let signature = Self::sign_payload(&self.api_secret, &prehash)?;
        let url = format!("{}{}", self.normalize_base_url(), path);

        let response = self
            .client
            .request(Method::POST, &url)
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json")
            .body(body_text)
            .send()
            .await
            .map_err(|err| CoreError::Channel(format!("okx http post request failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_owned());
            return Err(CoreError::Channel(format!(
                "okx POST {} failed ({}): {}",
                path, status, body
            )));
        }

        response
            .json::<T>()
            .await
            .map_err(|err| CoreError::Channel(format!("okx post response parse failed: {err}")))
    }

    fn remember_order(&self, order_id: &OrderId, symbol: &Symbol, inst_id: &str) -> Result<()> {
        self.order_inst_map
            .lock()
            .map_err(|_| CoreError::Channel("okx order inst map poisoned".to_owned()))?
            .insert(order_id.clone(), inst_id.to_owned());
        self.symbol_inst_map
            .lock()
            .map_err(|_| CoreError::Channel("okx symbol inst map poisoned".to_owned()))?
            .insert(symbol.clone(), inst_id.to_owned());
        Ok(())
    }

    fn inst_id_for_order(&self, order_id: &OrderId) -> Result<String> {
        self.order_inst_map
            .lock()
            .map_err(|_| CoreError::Channel("okx order inst map poisoned".to_owned()))?
            .get(order_id)
            .cloned()
            .ok_or_else(|| {
                CoreError::Channel(format!(
                    "missing instId for order {}; submit it first with this executor",
                    order_id.0
                ))
            })
    }

    fn inst_id_for_symbol(&self, symbol: &Symbol) -> Result<String> {
        let maybe = self
            .symbol_inst_map
            .lock()
            .map_err(|_| CoreError::Channel("okx symbol inst map poisoned".to_owned()))?
            .get(symbol)
            .cloned();
        Ok(maybe.unwrap_or_else(|| fallback_venue_symbol(symbol)))
    }

    fn build_submit_body(req: &CexOrderRequest) -> OkxSubmitOrderBody {
        OkxSubmitOrderBody {
            inst_id: req.venue_symbol.clone(),
            td_mode: "cross".to_owned(),
            side: map_side(req.side).to_owned(),
            ord_type: map_ord_type(req.order_type, req.time_in_force).to_owned(),
            size: decimal_to_string(req.base_qty.0),
            price: req.price.map(|v| decimal_to_string(v.0)),
            cl_ord_id: req.client_order_id.0.clone(),
            reduce_only: if req.reduce_only { Some(true) } else { None },
        }
    }

    async fn cancel_order_internal(&self, inst_id: &str, ord_id: &OrderId) -> Result<()> {
        let body = OkxCancelOrderBody {
            inst_id: inst_id.to_owned(),
            ord_id: ord_id.0.clone(),
        };
        let envelope: OkxEnvelope<OkxCancelData> = self
            .signed_post_json("/api/v5/trade/cancel-order", &body)
            .await?;
        ensure_okx_success(&envelope)?;
        Ok(())
    }
}

#[async_trait]
impl OrderExecutor for OkxExecutor {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let req = Self::ensure_okx(&request)?;
        let body = Self::build_submit_body(req);
        let envelope: OkxEnvelope<OkxSubmitData> =
            self.signed_post_json("/api/v5/trade/order", &body).await?;
        ensure_okx_success(&envelope)?;
        let data = envelope
            .data
            .first()
            .ok_or_else(|| CoreError::Channel("okx submit returned empty data".to_owned()))?;
        if let Some(code) = &data.s_code {
            if code != "0" {
                return Err(CoreError::Channel(format!(
                    "okx submit rejected: {}",
                    data.s_msg.clone().unwrap_or_default()
                )));
            }
        }

        let order_id = OrderId(data.ord_id.clone());
        self.remember_order(&order_id, &req.symbol, &req.venue_symbol)?;

        Ok(OrderResponse {
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: order_id,
            status: OrderStatus::Pending,
            filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty::default()),
            average_price: None,
            timestamp_ms: Self::now_timestamp_ms()?,
        })
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        if exchange != Exchange::Okx {
            return Err(CoreError::Channel(
                "okx executor received cancel request for non-okx exchange".to_owned(),
            ));
        }
        let inst_id = self.inst_id_for_order(order_id)?;
        self.cancel_order_internal(&inst_id, order_id).await
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        if exchange != Exchange::Okx {
            return Err(CoreError::Channel(
                "okx executor received cancel_all request for non-okx exchange".to_owned(),
            ));
        }
        let inst_id = self.inst_id_for_symbol(symbol)?;
        let envelope: OkxEnvelope<OkxPendingOrderData> = self
            .signed_get_json(
                "/api/v5/trade/orders-pending",
                vec![("instId".to_owned(), inst_id.clone())],
            )
            .await?;
        ensure_okx_success(&envelope)?;

        let mut cancelled = 0u32;
        for row in envelope.data {
            self.cancel_order_internal(&inst_id, &OrderId(row.ord_id))
                .await?;
            cancelled += 1;
        }
        Ok(cancelled)
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        if exchange != Exchange::Okx {
            return Err(CoreError::Channel(
                "okx executor received query request for non-okx exchange".to_owned(),
            ));
        }
        let inst_id = self.inst_id_for_order(order_id)?;
        let envelope: OkxEnvelope<OkxOrderData> = self
            .signed_get_json(
                "/api/v5/trade/order",
                vec![
                    ("instId".to_owned(), inst_id),
                    ("ordId".to_owned(), order_id.0.clone()),
                ],
            )
            .await?;
        ensure_okx_success(&envelope)?;
        let row = envelope
            .data
            .first()
            .ok_or_else(|| CoreError::Channel("okx order query returned empty data".to_owned()))?;

        Ok(OrderResponse {
            client_order_id: ClientOrderId(
                row.cl_ord_id
                    .clone()
                    .unwrap_or_else(|| format!("okx-{}", row.ord_id)),
            ),
            exchange_order_id: OrderId(row.ord_id.clone()),
            status: map_okx_status(&row.state),
            filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty(
                row.fill_sz
                    .as_deref()
                    .and_then(parse_decimal)
                    .unwrap_or_default(),
            )),
            average_price: row.avg_px.as_deref().and_then(parse_decimal).and_then(|v| {
                if v > Decimal::ZERO {
                    Some(polyalpha_core::Price(v))
                } else {
                    None
                }
            }),
            timestamp_ms: row
                .u_time
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct OkxEnvelope<T> {
    #[serde(default)]
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<T>,
}

#[derive(Clone, Debug, Serialize)]
struct OkxSubmitOrderBody {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "tdMode")]
    td_mode: String,
    side: String,
    #[serde(rename = "ordType")]
    ord_type: String,
    #[serde(rename = "sz")]
    size: String,
    #[serde(rename = "px", skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(rename = "clOrdId")]
    cl_ord_id: String,
    #[serde(rename = "reduceOnly", skip_serializing_if = "Option::is_none")]
    reduce_only: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
struct OkxCancelOrderBody {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "ordId")]
    ord_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OkxSubmitData {
    #[serde(rename = "ordId")]
    ord_id: String,
    #[serde(rename = "sCode")]
    s_code: Option<String>,
    #[serde(rename = "sMsg")]
    s_msg: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OkxCancelData {
    #[serde(rename = "ordId")]
    _ord_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OkxPendingOrderData {
    #[serde(rename = "ordId")]
    ord_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OkxOrderData {
    #[serde(rename = "ordId")]
    ord_id: String,
    #[serde(rename = "clOrdId")]
    cl_ord_id: Option<String>,
    #[serde(rename = "state")]
    state: String,
    #[serde(rename = "fillSz")]
    fill_sz: Option<String>,
    #[serde(rename = "avgPx")]
    avg_px: Option<String>,
    #[serde(rename = "uTime")]
    u_time: Option<String>,
}

fn ensure_okx_success<T>(envelope: &OkxEnvelope<T>) -> Result<()> {
    if envelope.code == "0" {
        return Ok(());
    }
    Err(CoreError::Channel(format!(
        "okx api returned code {}: {}",
        envelope.code, envelope.msg
    )))
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
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn map_ord_type(order_type: OrderType, tif: TimeInForce) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        OrderType::PostOnly => "post_only",
        OrderType::Limit => match tif {
            TimeInForce::Gtc => "limit",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
        },
    }
}

fn map_okx_status(state: &str) -> OrderStatus {
    match state {
        "live" | "partially_filled" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "canceled" => OrderStatus::Cancelled,
        "mmp_canceled" => OrderStatus::Cancelled,
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
            client_order_id: ClientOrderId("cid-okx".to_owned()),
            exchange: Exchange::Okx,
            symbol: Symbol::new("btc-100k-mar-2026"),
            venue_symbol: "BTC-USDT-SWAP".to_owned(),
            side: OrderSide::Sell,
            order_type,
            price: Some(polyalpha_core::Price(Decimal::new(100_000, 0))),
            base_qty: CexBaseQty(Decimal::new(3, 1)),
            time_in_force: tif,
            reduce_only: true,
        }
    }

    #[test]
    fn okx_signature_matches_known_sha256_base64_vector() {
        let signature =
            OkxExecutor::sign_payload("key", "The quick brown fox jumps over the lazy dog")
                .expect("signature should build");
        assert_eq!(signature, "97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=");
    }

    #[test]
    fn submit_body_serializes_okx_field_names() {
        let body =
            OkxExecutor::build_submit_body(&sample_order(OrderType::Limit, TimeInForce::Ioc));
        let json = serde_json::to_value(body).expect("json should serialize");

        assert_eq!(
            json.get("instId").and_then(|v| v.as_str()),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(json.get("ordType").and_then(|v| v.as_str()), Some("ioc"));
        assert_eq!(
            json.get("clOrdId").and_then(|v| v.as_str()),
            Some("cid-okx")
        );
        assert_eq!(json.get("reduceOnly").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn order_type_mapping_covers_limit_tif_and_post_only() {
        assert_eq!(map_ord_type(OrderType::Market, TimeInForce::Ioc), "market");
        assert_eq!(
            map_ord_type(OrderType::PostOnly, TimeInForce::Gtc),
            "post_only"
        );
        assert_eq!(map_ord_type(OrderType::Limit, TimeInForce::Gtc), "limit");
        assert_eq!(map_ord_type(OrderType::Limit, TimeInForce::Ioc), "ioc");
        assert_eq!(map_ord_type(OrderType::Limit, TimeInForce::Fok), "fok");
    }

    #[test]
    fn okx_status_mapping_handles_live_and_final_states() {
        assert_eq!(map_okx_status("live"), OrderStatus::Open);
        assert_eq!(map_okx_status("partially_filled"), OrderStatus::Open);
        assert_eq!(map_okx_status("filled"), OrderStatus::Filled);
        assert_eq!(map_okx_status("canceled"), OrderStatus::Cancelled);
    }
}
