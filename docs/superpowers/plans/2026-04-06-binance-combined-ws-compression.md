# Binance Combined WS Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in Binance combined-stream websocket path that explicitly requests websocket compression, logs the negotiated extension, and falls back once to the current per-symbol Binance sockets if the combined path fails.

**Architecture:** Keep the current Binance per-symbol websocket runner as the stable baseline, then add a gated combined runner alongside it. The runtime selects one Binance transport path at startup, uses the existing Binance depth parser for both raw and combined envelopes, and performs a one-shot runtime fallback to the old path only when the combined transport itself fails.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-cli`, `polyalpha-data`), `tokio-tungstenite`, existing Rust unit tests, `polyalpha-cli check-config`, `cargo test`, `cargo build`

---

## File Structure

**Primary files**
- Modify: `crates/polyalpha-core/src/config.rs`
  Add the new `strategy.market_data.binance_combined_ws_enabled` field, defaults, and config deserialization tests.
- Modify: `config/multi-market-active.fresh.toml`
  Opt in the validation environment to the new Binance combined path.
- Modify: `crates/polyalpha-cli/src/paper.rs`
  Add the custom websocket request helper, compression negotiation logging, combined Binance runner, fallback wiring, and runtime tests.
- Modify: `crates/polyalpha-data/src/live/binance.rs`
  Add small helpers for combined endpoint / stream list construction, and extend parser tests to assert combined wrapper parsing explicitly.

**No new runtime module is needed**
- The current websocket transport logic already lives in `paper.rs`.
- The current Binance parser already accepts `{"data": ...}` wrapped payloads, so a new parser file would be unnecessary.

---

### Task 1: Add The Config Gate And Failing Tests

**Files:**
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `config/multi-market-active.fresh.toml`
- Test: `crates/polyalpha-core/src/config.rs`

- [ ] **Step 1: Add failing config tests for the new field**

Add one default test assertion and one deserialization test assertion in `crates/polyalpha-core/src/config.rs`:

```rust
#[test]
fn market_data_config_defaults_disable_binance_combined_ws() {
    let config = MarketDataConfig::default();
    assert!(!config.binance_combined_ws_enabled);
}

#[test]
fn market_data_config_deserializes_binance_combined_ws_flag() {
    let config: MarketDataConfig = serde_json::from_str(
        r#"{
            "mode":"ws",
            "binance_combined_ws_enabled":true
        }"#,
    )
    .expect("ws market data config");

    assert!(config.binance_combined_ws_enabled);
}
```

- [ ] **Step 2: Run the focused config tests and verify they fail**

Run:

```bash
cargo test -p polyalpha-core market_data_config_defaults_disable_binance_combined_ws market_data_config_deserializes_binance_combined_ws_flag -- --nocapture
```

Expected:
- compile or assertion failure because `MarketDataConfig` does not yet contain `binance_combined_ws_enabled`

- [ ] **Step 3: Implement the config field and default**

Extend `MarketDataConfig` in `crates/polyalpha-core/src/config.rs`:

```rust
fn default_binance_combined_ws_enabled() -> bool {
    false
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataConfig {
    #[serde(default = "default_market_data_mode")]
    pub mode: MarketDataMode,
    #[serde(default = "default_max_stale_ms")]
    pub max_stale_ms: u64,
    #[serde(default = "default_planner_depth_levels")]
    pub planner_depth_levels: usize,
    #[serde(default)]
    pub poly_open_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cex_open_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub close_max_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub max_cross_leg_skew_ms: Option<u64>,
    #[serde(default)]
    pub borderline_poly_quote_age_ms: Option<u64>,
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
    #[serde(default = "default_snapshot_refresh_secs")]
    pub snapshot_refresh_secs: u64,
    #[serde(default = "default_funding_poll_secs")]
    pub funding_poll_secs: u64,
    #[serde(default = "default_binance_combined_ws_enabled")]
    pub binance_combined_ws_enabled: bool,
}
```

And update `impl Default for MarketDataConfig`:

```rust
binance_combined_ws_enabled: default_binance_combined_ws_enabled(),
```

- [ ] **Step 4: Opt in `multi-market-active.fresh`**

Add the config flag in `config/multi-market-active.fresh.toml` under `[strategy.market_data]`:

```toml
[strategy.market_data]
mode = "ws"
max_stale_ms = 5000
poly_open_max_quote_age_ms = 3000
cex_open_max_quote_age_ms = 1000
close_max_quote_age_ms = 5000
max_cross_leg_skew_ms = 2500
borderline_poly_quote_age_ms = 1500
reconnect_backoff_ms = 1000
snapshot_refresh_secs = 300
funding_poll_secs = 60
binance_combined_ws_enabled = true
```

- [ ] **Step 5: Re-run the focused config tests**

Run:

```bash
cargo test -p polyalpha-core market_data_config_defaults_disable_binance_combined_ws market_data_config_deserializes_binance_combined_ws_flag -- --nocapture
```

Expected:
- PASS

- [ ] **Step 6: Commit the config gate**

```bash
git add crates/polyalpha-core/src/config.rs config/multi-market-active.fresh.toml
git commit -m "feat: gate Binance combined websocket transport"
```

### Task 2: Add Binance Combined Stream Helpers And Parser Coverage

**Files:**
- Modify: `crates/polyalpha-data/src/live/binance.rs`
- Test: `crates/polyalpha-data/src/live/binance.rs`

- [ ] **Step 1: Add failing tests for combined stream helpers and wrapper parsing**

Add tests in `crates/polyalpha-data/src/live/binance.rs`:

```rust
#[test]
fn build_combined_depth_stream_path_joins_multiple_symbols() {
    let path = BinanceFuturesDataSource::build_combined_partial_depth_stream_path(
        &["BTCUSDT".to_owned(), "ETHUSDT".to_owned()],
        5,
    );

    assert_eq!(
        path,
        "/stream?streams=btcusdt@depth5@100ms/ethusdt@depth5@100ms"
    );
}

#[test]
fn parse_binance_partial_depth_ws_payload_accepts_combined_wrapper() {
    let payload = r#"{
        "stream":"btcusdt@depth5@100ms",
        "data":{
            "e":"depthUpdate",
            "E":1712400000000,
            "s":"BTCUSDT",
            "U":1,
            "u":2,
            "b":[["68000.1","1.25"]],
            "a":[["68000.2","0.75"]]
        }
    }"#;

    let update = BinanceFuturesDataSource::parse_partial_depth_ws_payload(payload, "")
        .expect("parse combined payload")
        .expect("depth update");

    assert_eq!(update.venue_symbol, "BTCUSDT");
    assert_eq!(update.sequence, 2);
}
```

- [ ] **Step 2: Run the Binance parser tests and verify they fail**

Run:

```bash
cargo test -p polyalpha-data build_combined_depth_stream_path_joins_multiple_symbols parse_binance_partial_depth_ws_payload_accepts_combined_wrapper -- --nocapture
```

Expected:
- FAIL because the combined stream helper does not exist yet

- [ ] **Step 3: Implement the combined stream helper**

Add helper(s) to `crates/polyalpha-data/src/live/binance.rs`:

```rust
pub fn build_combined_partial_depth_stream_path(
    venue_symbols: &[String],
    depth_limit: u16,
) -> String {
    let streams = venue_symbols
        .iter()
        .map(|symbol| Self::build_partial_depth_stream_name(symbol, depth_limit))
        .collect::<Vec<_>>()
        .join("/");
    format!("/stream?streams={streams}")
}
```

If useful in `paper.rs`, also add:

```rust
pub fn build_combined_partial_depth_stream_names(
    venue_symbols: &[String],
    depth_limit: u16,
) -> Vec<String> {
    venue_symbols
        .iter()
        .map(|symbol| Self::build_partial_depth_stream_name(symbol, depth_limit))
        .collect()
}
```

- [ ] **Step 4: Re-run the focused Binance parser tests**

Run:

```bash
cargo test -p polyalpha-data build_combined_depth_stream_path_joins_multiple_symbols parse_binance_partial_depth_ws_payload_accepts_combined_wrapper -- --nocapture
```

Expected:
- PASS

- [ ] **Step 5: Commit the Binance helper changes**

```bash
git add crates/polyalpha-data/src/live/binance.rs
git commit -m "feat: add Binance combined stream helpers"
```

### Task 3: Add Custom Binance Handshake With Compression Negotiation Tests

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Add failing tests for request construction and negotiation logging state**

Add focused tests in `crates/polyalpha-cli/src/paper.rs`:

```rust
#[test]
fn build_ws_request_includes_permessage_deflate_header() {
    let request = build_ws_request_with_optional_extensions(
        "wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms",
        Some("permessage-deflate; client_max_window_bits"),
    )
    .expect("ws request");

    assert_eq!(
        request
            .headers()
            .get("Sec-WebSocket-Extensions")
            .and_then(|value| value.to_str().ok()),
        Some("permessage-deflate; client_max_window_bits")
    );
}

#[test]
fn response_negotiated_extensions_extracts_header_value() {
    let response = http::Response::builder()
        .status(101)
        .header(
            "Sec-WebSocket-Extensions",
            "permessage-deflate; server_no_context_takeover",
        )
        .body(())
        .expect("response");

    assert_eq!(
        negotiated_ws_extensions(&response),
        Some("permessage-deflate; server_no_context_takeover".to_owned())
    );
}
```

- [ ] **Step 2: Run the focused handshake tests and verify they fail**

Run:

```bash
cargo test -p polyalpha-cli build_ws_request_includes_permessage_deflate_header response_negotiated_extensions_extracts_header_value -- --nocapture
```

Expected:
- FAIL because the helpers do not exist yet

- [ ] **Step 3: Implement custom request / negotiation helpers**

In `crates/polyalpha-cli/src/paper.rs`, add helpers along these lines:

```rust
fn build_ws_request_with_optional_extensions(
    ws_url: &str,
    extensions: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = ws_url.into_client_request()?;
    if let Some(extensions) = extensions {
        request.headers_mut().insert(
            "Sec-WebSocket-Extensions",
            HeaderValue::from_str(extensions)?,
        );
    }
    Ok(request)
}

fn negotiated_ws_extensions(
    response: &tokio_tungstenite::tungstenite::handshake::client::Response,
) -> Option<String> {
    response
        .headers()
        .get("Sec-WebSocket-Extensions")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
```

Then add a Binance-specific connect helper:

```rust
async fn connect_websocket_with_extensions(
    ws_url: &str,
    proxy: Option<&WsProxyConfig>,
    extensions: Option<&str>,
) -> std::result::Result<
    (
        WsSocket,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    let target = parse_ws_target(ws_url).map_err(|err| ws_transport_error(err.to_string()))?;
    let stream = match proxy {
        Some(proxy) => connect_via_proxy(&target, proxy).await?,
        None => TcpStream::connect((target.host.as_str(), target.port))
            .await
            .map_err(|err| ws_transport_error(format!("tcp connect failed for {}: {err}", target.authority())))?,
    };
    let request = build_ws_request_with_optional_extensions(ws_url, extensions)
        .map_err(|err| ws_transport_error(err.to_string()))?;
    client_async_tls_with_config(request, stream, None, None).await
}
```

- [ ] **Step 4: Re-run the focused handshake tests**

Run:

```bash
cargo test -p polyalpha-cli build_ws_request_includes_permessage_deflate_header response_negotiated_extensions_extracts_header_value -- --nocapture
```

Expected:
- PASS

- [ ] **Step 5: Commit the handshake helper changes**

```bash
git add crates/polyalpha-cli/src/paper.rs
git commit -m "feat: add websocket extension handshake helpers"
```

### Task 4: Implement Combined Binance Runner And One-Shot Fallback

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Add failing tests for transport selection and fallback state**

Add focused tests in `crates/polyalpha-cli/src/paper.rs`:

```rust
#[test]
fn exchange_connection_tracker_mark_disconnected_requires_all_streams_to_drop() {
    let settings = test_settings_with_price_filter(None, None);
    let registry = SymbolRegistry::new(settings.markets.clone());
    let symbol = settings.markets[0].symbol.clone();
    let channels = create_channels(std::slice::from_ref(&symbol));
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());
    let tracker = ExchangeConnectionTracker::new(Exchange::Binance, manager);

    tracker.mark_connected("btcusdt");
    tracker.mark_connected("ethusdt");
    tracker.mark_disconnected("btcusdt");

    assert_eq!(
        tracker.status.lock().map(|guard| *guard).ok(),
        Some(ConnectionStatus::Connected)
    );
}
```

And add a selection helper test:

```rust
#[test]
fn binance_transport_mode_uses_combined_when_flag_enabled() {
    let mut settings = test_settings_with_price_filter(None, None);
    settings.strategy.market_data.binance_combined_ws_enabled = true;

    assert_eq!(
        select_binance_ws_transport_mode(&settings),
        BinanceWsTransportMode::Combined
    );
}
```

- [ ] **Step 2: Run the focused runtime tests and verify they fail**

Run:

```bash
cargo test -p polyalpha-cli exchange_connection_tracker_mark_disconnected_requires_all_streams_to_drop binance_transport_mode_uses_combined_when_flag_enabled -- --nocapture
```

Expected:
- FAIL because the transport mode helper and/or new tracker usage does not exist yet

- [ ] **Step 3: Add transport mode selection and combined task spawn**

In `crates/polyalpha-cli/src/paper.rs`, add:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinanceWsTransportMode {
    PerSymbol,
    Combined,
}

fn select_binance_ws_transport_mode(settings: &Settings) -> BinanceWsTransportMode {
    if settings.strategy.market_data.binance_combined_ws_enabled {
        BinanceWsTransportMode::Combined
    } else {
        BinanceWsTransportMode::PerSymbol
    }
}
```

Update the Binance section of `spawn_ws_market_data_tasks(...)` so it:

- still deduplicates Binance `venue_symbol`s
- branches on `select_binance_ws_transport_mode(settings)`
- calls `spawn_binance_combined_ws_task(...)` when the flag is enabled
- otherwise preserves the current per-symbol `spawn_binance_ws_task(...)` behavior

- [ ] **Step 4: Implement the combined Binance task**

Add a helper shaped like:

```rust
fn spawn_binance_combined_ws_task(
    manager: DataManager,
    ws_url: String,
    venue_symbols: Vec<String>,
    depth: u16,
    reconnect_backoff_ms: u64,
    tracker: ExchangeConnectionTracker,
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> { ... }
```

Inside it:

- build the combined path with `BinanceFuturesDataSource::build_combined_partial_depth_stream_path(...)`
- connect using `connect_websocket_with_extensions(..., Some("permessage-deflate; client_max_window_bits"))`
- log either:

```rust
eprintln!("binance combined ws negotiated extensions: {extensions}");
```

or:

```rust
eprintln!("binance combined ws connected without negotiated extensions");
```

- on successful connect, call:

```rust
for venue_symbol in &venue_symbols {
    tracker.mark_connected(venue_symbol);
}
```

- on each `Message::Text(text)`, call:

```rust
tracker.reaffirm_connected();
match BinanceFuturesDataSource::parse_partial_depth_ws_payload(&text, "") {
    Ok(Some(update)) => {
        if let Err(err) = manager.normalize_and_publish_cex_orderbook(update) {
            log_ws_warning(format!("binance combined ws publish failed: {err}"));
        }
    }
    Ok(None) => {}
    Err(err) => log_ws_warning(format!("binance combined ws payload ignored: {err}")),
}
```

- on close / read error / early connect failure, drop into a one-shot fallback path instead of retrying the combined socket forever

- [ ] **Step 5: Implement the one-shot fallback**

Inside the combined task, after any fatal combined-path failure:

```rust
log_ws_warning("binance combined ws failed; falling back to per-symbol transport");
for venue_symbol in &venue_symbols {
    tracker.mark_disconnected(venue_symbol);
}

let mut fallback_handles = venue_symbols
    .iter()
    .cloned()
    .map(|venue_symbol| {
        spawn_binance_ws_task(
            manager.clone(),
            ws_url.clone(),
            venue_symbol,
            depth,
            reconnect_backoff_ms,
            tracker.clone(),
            proxy_settings.clone(),
        )
    })
    .collect::<Vec<_>>();

for handle in fallback_handles.drain(..) {
    let _ = handle.await;
}
```

Use a `fell_back` guard so this path happens at most once per runtime session.

- [ ] **Step 6: Re-run the focused runtime tests**

Run:

```bash
cargo test -p polyalpha-cli exchange_connection_tracker_mark_disconnected_requires_all_streams_to_drop binance_transport_mode_uses_combined_when_flag_enabled -- --nocapture
```

Expected:
- PASS

- [ ] **Step 7: Commit the combined transport runner**

```bash
git add crates/polyalpha-cli/src/paper.rs
git commit -m "feat: add Binance combined websocket runner"
```

### Task 5: Full Verification And Acceptance

**Files:**
- Verify: `crates/polyalpha-core/src/config.rs`
- Verify: `crates/polyalpha-data/src/live/binance.rs`
- Verify: `crates/polyalpha-cli/src/paper.rs`
- Verify: `config/multi-market-active.fresh.toml`

- [ ] **Step 1: Run focused Rust tests for config, parser, and runtime helpers**

Run:

```bash
cargo test -p polyalpha-core market_data_config_defaults_disable_binance_combined_ws market_data_config_deserializes_binance_combined_ws_flag -- --nocapture
cargo test -p polyalpha-data build_combined_depth_stream_path_joins_multiple_symbols parse_binance_partial_depth_ws_payload_accepts_combined_wrapper -- --nocapture
cargo test -p polyalpha-cli build_ws_request_includes_permessage_deflate_header response_negotiated_extensions_extracts_header_value exchange_connection_tracker_mark_disconnected_requires_all_streams_to_drop binance_transport_mode_uses_combined_when_flag_enabled -- --nocapture
```

Expected:
- PASS

- [ ] **Step 2: Run a wider CLI test sweep**

Run:

```bash
cargo test -p polyalpha-cli ws -- --nocapture
```

Expected:
- PASS for existing websocket-related tests, including tracker behavior and runtime WS helpers

- [ ] **Step 3: Verify the opt-in config loads**

Run:

```bash
./target/debug/polyalpha-cli check-config --env multi-market-active.fresh
```

Expected:
- exit code `0`
- config loads with `binance_combined_ws_enabled = true`

- [ ] **Step 4: Build the CLI**

Run:

```bash
cargo build -p polyalpha-cli
```

Expected:
- PASS

- [ ] **Step 5: Run a live-mock comparison with the opt-in environment**

Run:

```bash
./target/debug/polyalpha-cli live run-multi \
  --env multi-market-active.fresh \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 1000 \
  --print-every 60 \
  --warmup-klines 600
```

Expected:
- Binance logs show either negotiated extensions or a no-extension log line
- no persistent Binance disconnect regression
- runtime completes

- [ ] **Step 6: Inspect the latest run**

Run:

```bash
./target/debug/polyalpha-cli live inspect --env multi-market-active.fresh --format table
```

Expected:
- inspect output loads successfully
- compare `cex_quote_stale`, `connection_lost`, and Binance connection behavior against the previous baseline run

- [ ] **Step 7: Commit the verified feature**

```bash
git add crates/polyalpha-core/src/config.rs crates/polyalpha-data/src/live/binance.rs crates/polyalpha-cli/src/paper.rs config/multi-market-active.fresh.toml
git commit -m "feat: add opt-in Binance combined websocket transport"
```
