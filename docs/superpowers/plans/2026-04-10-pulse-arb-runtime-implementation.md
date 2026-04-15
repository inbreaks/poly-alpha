# Pulse Arb Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一条独立的 `pulse_arb` runtime，在 `paper` 和 `live-mock` 模式下完成 `BTC/ETH` 的 Polymarket 脉冲套利闭环，使用 `Deribit` 作为 options anchor、`Binance USD-M perp` 作为 hedge 执行腿，并从第一天保留未来扩展到 `SOL/XRP`、多 provider、多 hedge venue 的结构。

**Architecture:** 新增 `polyalpha-pulse` crate 承载 pulse 专属的模型、定价、检测、会话状态机、全局对冲聚合、审计与监控快照；把共享配置、monitor socket 协议和少量枚举扩展放在 `polyalpha-core`；把 Deribit 原始接入放在 `polyalpha-data`；继续复用 `polyalpha-executor` 的底层下单适配器；在 `polyalpha-cli` 只做命令行入口、runtime 装配和 monitor/audit 呈现，不把 pulse 逻辑塞回旧的 `paper.rs` basis 路线。

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-data`, `polyalpha-executor`, `polyalpha-audit`, `polyalpha-cli`, new `polyalpha-pulse`), `tokio`, `reqwest`, `tokio-tungstenite`, existing dry-run/live-mock execution stack, targeted `cargo test` runs.

---

## Execution Status

更新日期：`2026-04-12`

当前这份实现计划对应的 MVP 代码主线已经落地，剩余未执行项主要是计划里的 `Commit` 步骤，本轮故意保持未提交状态，方便继续走读和补小口径。

`2026-04-13` 的一个重要修正是：entry 主线已经从“静态 value gap 触发”收敛成“pulse-first 触发”。

当前代码语义是：

- `net_session_edge` 仍然存在，但只负责经济准入
- 新 entry gating 增加了短窗 `claim_price_move / fair_claim_move / cex_mid_move`
- `PulseDetector` 先验证 pulse confirmation，再看摩擦后的净 edge
- runtime 不再按 `max net_edge_bps` 选市场，而是按 `max pulse_score`
- `target_exit_price` 改成了 reachability-first 的 tick ladder，而不是按理论 edge 线性推目标价

所以如果后续继续做 live-ready 或参数实验，应当围绕 `pulse_window / pulse_score / tick ladder` 去调，而不是回到单纯的 `anchor latency / notional / edge threshold` 微调。

已完成的 MVP 范围：

- `Task 1` 到 `Task 6` 的功能目标已经接通：独立 `pulse_arb` crate、`Deribit` anchor、`EventPricer` / `PulseDetector`、`PulseSession`、`GlobalHedgeAggregator`、`paper/live-mock` runtime、audit、monitor、CLI 入口都已存在
- `Task 7` 的关键验收测试和 operator 文档也已补上：README、`docs/current-status.md`、timeout / hedge netting 等 end-to-end runtime tests 已落地
- `paper` / `live-mock` 对 `BTC / ETH` 的真实 feed-driven 短跑已经验证能稳定起 runtime，并落出非空 `pulse_session_summaries`
- `pulse` 的真实 Polymarket WS 现在已经复用本地 book cache、`price_change` 增量更新和单调 sequence 语义，不再停留在 `waiting_anchor / waiting_poly_quote`
- `actual position sync` 已经接进 runtime reconcile 路径，Aggregator 不再只依赖名义仓位推断

本轮 fresh verification：

```bash
cargo test -p polyalpha-pulse -- --nocapture
cargo test -p polyalpha-cli pulse_session_timeline_collects_lifecycle_and_tape_events -- --nocapture
cargo build -p polyalpha-cli
./target/debug/polyalpha-cli paper pulse inspect --env multi-market-active.fresh
./target/debug/polyalpha-cli audit pulse-sessions --env multi-market-active.fresh --session-id 270da19c-3aba-41b5-9cfa-51a6882b2694 --format json
./target/debug/polyalpha-cli audit pulse-session-detail --env multi-market-active.fresh --session-id 270da19c-3aba-41b5-9cfa-51a6882b2694 --pulse-session-id pulse-btc-4 --format json
```

本轮确认到的事实：

- `polyalpha-pulse`：`37 passed, 0 failed`
- runtime tests 已覆盖 `actual_filled_qty` 缩放、pin risk 抑制、timeout exit、maker exit、pegging、rehedging、emergency flatten、多 session netting、actual position sync、executor feedback
- `paper pulse inspect` 已确认 `BTC / ETH` 都统一走 `Deribit -> Binance USD-M perp`
- 真实 `audit pulse-sessions` 已确认 `pulse_session_summaries` 非空
- 真实 `audit pulse-session-detail` 已确认 session timeline 能回放 `poly_opening -> closed`，并保留足够原始字段供后续 queue reconstruction 升级

当前仍需如实注明的边界：

- 真实短跑样本里，已经观察到 `PulseSession` 创建和审计落盘，但这轮窗口里的多数样本仍是 `zero-fill`，会话表现为 `poly_opening -> closed`
- 所以，“真实 feed runtime 已接通” 和 “完整 maker/rehedge/flatten 生命周期在 deterministic runtime tests 中已覆盖” 这两件事要分开理解，不能混写成已经在 live window 里频繁观察到非零 fill
- `SOL / XRP`、多 anchor provider、次级 hedge venue 仍然只是结构扩展位，不属于当前 MVP 已交付范围

### 2026-04-15 Admission 修正补充

本轮要落实的不是执行骨架重写，而是把 `15m` timeout 风险正式接进 entry economics。

新增统一语义：

```text
deadline_ms = opened_at_ms + 900_000
not_sold_15m = remaining_qty(deadline_ms) > 0

p_timeout_proxy = sell_vwap(entry_bid_book, actual_open_qty)
G = maker 场景净利润估计
L = timeout 场景净亏损估计绝对值
required_hit_rate = (L + m) / (G + L)
```

当前样本已经说明，不加这层 admission 会系统性高估 `15m` 内 maker 落袋能力：

| 会话 | `candidate EV` | `timeout_net_loss_est` | `required_hit_rate` | 实际 PnL |
| --- | ---: | ---: | ---: | ---: |
| `pulse-eth-5` | `+$13.66` | `-$46.12` | `78.0%` | `-$75.01` |
| `pulse-btc-6` | `+$7.20` | `-$21.68` | `76.8%` | `-$42.09` |
| `pulse-btc-10` | `+$5.38` | `-$35.66` | `88.1%` | `-$25.18` |
| `pulse-eth-11` | `+$15.62` | `-$51.96` | `77.6%` | `-$63.21` |
| `pulse-eth-8` | `+$1.01` | `-$110.74` | `99.5%` | `-$77.21` |

本轮实现目标：

- 在 `signal.rs` 里把 `timeout_exit_price_for_shares` 产出的代理价正式转成 `timeout_net_loss_est`
- 在 `model.rs` / `detector.rs` 里引入 `required_hit_rate`、`max_timeout_loss_usd`、`required_hit_rate_max`
- 在 `runtime.rs` 的 candidate 评估链中，把这组值写进 `PulseOpportunityInput`、审计和 monitor
- 先挡掉“必须要求 `75%+` 的 `15m` maker 命中率才成立”的伪机会

## File Structure

- Modify: `Cargo.toml`
  Responsibility: 把 `crates/polyalpha-pulse` 加入 workspace。
- Modify: `config/default.toml`
  Responsibility: 增加 `[deribit]` 与 `[strategy.pulse_arb.*]` 默认配置，明确 `BTC/ETH` routing 和未来 `SOL/XRP` 扩展位。
- Modify: `crates/polyalpha-core/src/config.rs`
  Responsibility: 定义 `DeribitConfig`、`PulseArbStrategyConfig` 及其子配置，接入 `Settings`。
- Modify: `crates/polyalpha-core/src/types/market.rs`
  Responsibility: 给 `Exchange` 增加 `Deribit`，保留连接状态和路由可见性。
- Modify: `crates/polyalpha-core/src/socket/message.rs`
  Responsibility: 给通用 monitor 协议增加可选 `pulse` 视图，不污染 basis 现有字段语义。
- Create: `crates/polyalpha-pulse/Cargo.toml`
  Responsibility: 新 crate 依赖声明。
- Create: `crates/polyalpha-pulse/src/lib.rs`
  Responsibility: 导出 pulse 模块边界。
- Create: `crates/polyalpha-pulse/src/model.rs`
  Responsibility: 定义 `AnchorSnapshot`、`EventPriceOutput`、`PulseOpportunity`、`PulseSessionState`、`PulseFailureCode` 等核心模型。
- Create: `crates/polyalpha-pulse/src/anchor/mod.rs`
  Responsibility: pulse anchor 层总入口。
- Create: `crates/polyalpha-pulse/src/anchor/provider.rs`
  Responsibility: `AnchorProvider` trait 与 pulse-local anchor channel 接口。
- Create: `crates/polyalpha-pulse/src/anchor/router.rs`
  Responsibility: 按资产 routing 到 provider。
- Create: `crates/polyalpha-pulse/src/anchor/deribit.rs`
  Responsibility: 把 Deribit 原始数据变成 `AnchorSnapshot`。
- Create: `crates/polyalpha-pulse/src/pricing.rs`
  Responsibility: `EventPricer`，实现 fair probability、event delta、`dS` 相对 bump、expiry gap adjustment。
- Create: `crates/polyalpha-pulse/src/detector.rs`
  Responsibility: `PulseDetector`，做 pulse confirmation，并计算 `net_session_edge` 与准入闸门。
- Create: `crates/polyalpha-pulse/src/session.rs`
  Responsibility: `PulseSession` 聚合对象与生命周期命令。
- Create: `crates/polyalpha-pulse/src/hedge.rs`
  Responsibility: `GlobalHedgeAggregator` 与 session-level hedge attribution。
- Create: `crates/polyalpha-pulse/src/runtime.rs`
  Responsibility: pulse runtime 主循环，串联 Poly/Binance feeds、anchor、signal tape、detector、FSM、hedge executor。
- Create: `crates/polyalpha-pulse/src/audit.rs`
  Responsibility: `PulseAuditSink`，把 session 级事件映射到共享 audit writer。
- Create: `crates/polyalpha-pulse/src/monitor.rs`
  Responsibility: `PulseMonitorView`、asset health、active session list、session detail 快照。
- Create: `crates/polyalpha-data/src/live/deribit.rs`
  Responsibility: Deribit 原始 REST/WS client，不实现旧的 `MarketDataSource` trait，而是给 pulse anchor provider 提供原始 options ticker / instrument 数据。
- Modify: `crates/polyalpha-data/src/live/mod.rs`
  Responsibility: 导出 Deribit live adapter。
- Modify: `crates/polyalpha-data/src/lib.rs`
  Responsibility: 导出 `DeribitOptionsClient` 等类型。
- Modify: `crates/polyalpha-audit/src/model.rs`
  Responsibility: 新增 pulse 审计 payload / summary 模型。
- Modify: `crates/polyalpha-audit/src/writer.rs`
  Responsibility: 复用现有 writer 写 pulse 生命周期事件与 summary。
- Modify: `crates/polyalpha-audit/src/reader.rs`
  Responsibility: 增加 pulse session summary/detail 读取函数。
- Modify: `crates/polyalpha-audit/src/warehouse.rs`
  Responsibility: 为 pulse summary / lifecycle 建表并同步。
- Modify: `crates/polyalpha-cli/Cargo.toml`
  Responsibility: 引入 `polyalpha-pulse` 依赖。
- Modify: `crates/polyalpha-cli/src/args.rs`
  Responsibility: 增加 `paper pulse ...`、`live pulse ...`、`audit pulse ...` 命令定义。
- Modify: `crates/polyalpha-cli/src/main.rs`
  Responsibility: 路由 pulse 入口。
- Create: `crates/polyalpha-cli/src/pulse.rs`
  Responsibility: CLI 侧 runtime 装配与 inspect helpers。
- Modify: `crates/polyalpha-cli/src/audit.rs`
  Responsibility: 输出 pulse session summary/detail 报表。
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
  Responsibility: 维护 pulse monitor 选中 session / asset health 状态。
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
  Responsibility: 渲染 active sessions、session detail、asset health 三块 pulse 面板。
- Modify: `crates/polyalpha-cli/src/monitor/json_mode.rs`
  Responsibility: 暴露 pulse JSON monitor 快照。
- Verify: `crates/polyalpha-cli/src/runtime.rs`
  Responsibility: 仅复用 execution stack / data manager helpers；不要把 pulse 策略逻辑塞进旧 runtime helper。

### Task 1: Scaffold The Pulse Workspace, Config Surface, And CLI Entry Points

**Files:**
- Modify: `Cargo.toml`
- Modify: `config/default.toml`
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `crates/polyalpha-core/src/types/market.rs`
- Create: `crates/polyalpha-pulse/Cargo.toml`
- Create: `crates/polyalpha-pulse/src/lib.rs`
- Modify: `crates/polyalpha-cli/Cargo.toml`
- Modify: `crates/polyalpha-cli/src/args.rs`
- Modify: `crates/polyalpha-cli/src/main.rs`
- Test: `crates/polyalpha-core/src/config.rs`
- Test: `crates/polyalpha-cli/src/args.rs`

- [ ] **Step 1: Add a failing core config regression for `pulse_arb` defaults and routing**

```rust
#[test]
fn pulse_arb_config_deserializes_deribit_binance_routing() {
    let config: PulseArbStrategyConfig = serde_json::from_value(serde_json::json!({
        "runtime": { "enabled": true, "max_concurrent_sessions_per_asset": 2 },
        "session": { "max_holding_secs": 900, "min_opening_notional_usd": "250" },
        "entry": { "min_net_session_edge_bps": "25" },
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
            "btc": { "enabled": true, "anchor_provider": "deribit_primary", "hedge_venue": "binance_perp" },
            "eth": { "enabled": true, "anchor_provider": "deribit_primary", "hedge_venue": "binance_perp" },
            "sol": { "enabled": false, "anchor_provider": "binance_options_primary", "hedge_venue": "binance_perp" }
        }
    }))
    .expect("pulse config should deserialize");

    assert_eq!(config.routing["btc"].anchor_provider, "deribit_primary");
    assert_eq!(config.routing["eth"].hedge_venue.as_str(), "binance_perp");
    assert!(!config.routing["sol"].enabled);
}

#[test]
fn exchange_deribit_round_trips_in_config_models() {
    let exchange: Exchange = serde_json::from_str("\"Deribit\"").expect("deserialize exchange");
    assert_eq!(exchange, Exchange::Deribit);
    assert_eq!(serde_json::to_string(&exchange).unwrap(), "\"Deribit\"");
}
```

- [ ] **Step 2: Run the targeted config and CLI parsing tests to confirm they fail**

Run:

```bash
cargo test -p polyalpha-core pulse_arb_config_deserializes_deribit_binance_routing -- --nocapture
cargo test -p polyalpha-core exchange_deribit_round_trips_in_config_models -- --nocapture
```

Expected: FAIL because `PulseArbStrategyConfig`, `DeribitConfig`, and `Exchange::Deribit` do not exist yet.

- [ ] **Step 3: Add the minimal workspace/config/CLI scaffolding**

Implementation notes:
- Add `crates/polyalpha-pulse` to the workspace and `polyalpha-cli` dependencies.
- Extend `Settings` with:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeribitConfig {
    pub rest_url: String,
    pub ws_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PulseArbStrategyConfig {
    pub runtime: PulseRuntimeConfig,
    pub session: PulseSessionConfig,
    pub entry: PulseEntryConfig,
    pub rehedge: PulseRehedgeConfig,
    pub pin_risk: PulsePinRiskConfig,
    pub providers: HashMap<String, PulseProviderConfig>,
    pub routing: HashMap<String, PulseRoutingConfig>,
    #[serde(default)]
    pub asset_policy: HashMap<String, PulseAssetPolicyConfig>,
}
```

- Add `Exchange::Deribit` to `crates/polyalpha-core/src/types/market.rs`; do not overload old basis logic with Deribit-specific branches.
- Seed `config/default.toml` with:
  - `[deribit]`
  - `[strategy.pulse_arb.runtime]`
  - `[strategy.pulse_arb.session]`
  - `[strategy.pulse_arb.entry]`
  - `[strategy.pulse_arb.rehedge]`
  - `[strategy.pulse_arb.pin_risk]`
  - `[strategy.pulse_arb.providers.deribit_primary]`
  - `[strategy.pulse_arb.routing.btc]`
  - `[strategy.pulse_arb.routing.eth]`
  - disabled routing stubs for `sol` / `xrp`
- Create `crates/polyalpha-pulse/src/lib.rs` with module declarations only:

```rust
pub mod anchor;
pub mod audit;
pub mod detector;
pub mod hedge;
pub mod model;
pub mod monitor;
pub mod pricing;
pub mod runtime;
pub mod session;
```

- Add CLI parsing and compile-safe handler stubs so `paper pulse ...` and `live pulse ...` already have stable entry points before the runtime is wired.

- [ ] **Step 4: Add a failing CLI parse regression for the new pulse command shape**

```rust
#[test]
fn parses_paper_pulse_run_command() {
    let cli = Cli::parse_from([
        "polyalpha-cli",
        "paper",
        "pulse",
        "run",
        "--env",
        "default",
        "--assets",
        "btc,eth"
    ]);

    assert!(matches!(
        cli.command,
        Command::Paper {
            command: PaperCommand::Pulse { .. }
        }
    ));
}
```

- [ ] **Step 5: Re-run the targeted tests**

Run:

```bash
cargo test -p polyalpha-core pulse_arb_config_deserializes_deribit_binance_routing -- --nocapture
cargo test -p polyalpha-core exchange_deribit_round_trips_in_config_models -- --nocapture
cargo test -p polyalpha-cli parses_paper_pulse_run_command -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml config/default.toml crates/polyalpha-core/src/config.rs crates/polyalpha-core/src/types/market.rs crates/polyalpha-pulse/Cargo.toml crates/polyalpha-pulse/src/lib.rs crates/polyalpha-cli/Cargo.toml crates/polyalpha-cli/src/args.rs crates/polyalpha-cli/src/main.rs
git commit -m "feat: scaffold pulse arb workspace and config"
```

### Task 2: Add Deribit Raw Transport And A Normalized Anchor Provider Layer

**Files:**
- Create: `crates/polyalpha-data/src/live/deribit.rs`
- Modify: `crates/polyalpha-data/src/live/mod.rs`
- Modify: `crates/polyalpha-data/src/lib.rs`
- Create: `crates/polyalpha-pulse/src/model.rs`
- Create: `crates/polyalpha-pulse/src/anchor/mod.rs`
- Create: `crates/polyalpha-pulse/src/anchor/provider.rs`
- Create: `crates/polyalpha-pulse/src/anchor/router.rs`
- Create: `crates/polyalpha-pulse/src/anchor/deribit.rs`
- Test: `crates/polyalpha-data/src/live/deribit.rs`
- Test: `crates/polyalpha-pulse/src/anchor/router.rs`

- [ ] **Step 1: Add a failing Deribit parser regression in `polyalpha-data`**

```rust
#[test]
fn parse_deribit_ticker_payload_extracts_iv_and_greeks() {
    let payload = r#"{
        "jsonrpc":"2.0",
        "method":"subscription",
        "params":{
            "channel":"ticker.BTC-27SEP24-100000-C.raw",
            "data":{
                "instrument_name":"BTC-27SEP24-100000-C",
                "timestamp":1725000000000,
                "mark_price":0.1435,
                "mark_iv":63.4,
                "best_bid_price":0.141,
                "best_ask_price":0.146,
                "bid_iv":62.8,
                "ask_iv":64.1,
                "greeks":{"delta":0.47,"gamma":0.00019}
            }
        }
    }"#;

    let ticker = DeribitTickerMessage::from_text(payload).expect("parse ticker payload");

    assert_eq!(ticker.instrument_name, "BTC-27SEP24-100000-C");
    assert_eq!(ticker.mark_iv, 63.4);
    assert_eq!(ticker.bid_iv, Some(62.8));
    assert_eq!(ticker.ask_iv, Some(64.1));
    assert_eq!(ticker.delta, Some(0.47));
    assert_eq!(ticker.gamma, Some(0.00019));
}
```

- [ ] **Step 2: Run the targeted Deribit parser test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-data parse_deribit_ticker_payload_extracts_iv_and_greeks -- --nocapture
```

Expected: FAIL because `DeribitTickerMessage` and the new module do not exist yet.

- [ ] **Step 3: Implement raw Deribit transport in `polyalpha-data`**

Implementation notes:
- Keep this layer venue-specific and dumb:
  - fetch instrument list for `BTC` / `ETH`
  - subscribe to ticker channels
  - parse raw ticker / order book payloads
  - expose a cache/query API for the pulse provider
- Add a `DiscoveryFilter` so the client never subscribes to the full Deribit options universe.
  - initial filter: only keep contracts where `abs(strike - index_price) / index_price < 0.15`
  - initial filter: only keep contracts with `expiry <= 30 days`
  - refresh policy: re-prune the subscription set at least every `10 minutes`
  - refresh policy: allow an earlier re-prune when index price drifts far enough from the last prune anchor
- Do **not** force Deribit options data into old `MarketDataEvent`; pulse anchor snapshots should travel on a pulse-local channel instead.
- Export a small raw client surface:

```rust
pub struct DiscoveryFilter {
    pub max_relative_strike_distance: f64,
    pub max_expiry_days: u32,
    pub reprune_interval_secs: u64,
}

pub struct DeribitOptionsClient {
    // reqwest client, ws task handles, instrument cache, latest ticker cache
}

impl DeribitOptionsClient {
    pub async fn connect(&mut self) -> Result<()>;
    pub async fn prime_asset(&self, asset: DeribitAsset) -> Result<()>;
    pub fn latest_tickers(&self, asset: DeribitAsset) -> Vec<DeribitTickerMessage>;
    pub async fn refresh_subscriptions(&self, asset: DeribitAsset) -> Result<()>;
}
```

- [ ] **Step 4: Add a failing pulse routing/provider regression**

```rust
#[tokio::test]
async fn anchor_router_uses_deribit_primary_for_btc_and_eth() {
    let settings = pulse_test_settings();
    let router = AnchorRouter::from_settings(&settings).expect("build anchor router");

    let btc = router.provider_for_asset(PulseAsset::Btc).expect("btc provider");
    let eth = router.provider_for_asset(PulseAsset::Eth).expect("eth provider");

    assert_eq!(btc.provider_id(), "deribit_primary");
    assert_eq!(eth.provider_id(), "deribit_primary");
    assert!(router.provider_for_asset(PulseAsset::Sol).is_err());
}
```

- [ ] **Step 5: Implement `AnchorProvider`, `AnchorRouter`, and `DeribitAnchorProvider`**

Implementation notes:
- `crates/polyalpha-pulse/src/model.rs` should define:

```rust
pub struct AnchorSnapshot {
    pub asset: PulseAsset,
    pub provider_id: String,
    pub ts_ms: u64,
    pub index_price: Decimal,
    pub expiry_ts_ms: u64,
    pub atm_iv: f64,
    pub local_surface_points: Vec<LocalSurfacePoint>,
    pub quality: AnchorQualityMetrics,
}
```

- `AnchorProvider` should return normalized `AnchorSnapshot`, not venue raw fields.
- `AnchorRouter` should read the `routing` map from `PulseArbStrategyConfig`, not hard-code `BTC` / `ETH`.
- `DeribitAnchorProvider` should:
  - choose the nearest valid expiry
  - collect local strikes around the market rule target
  - compute quality flags: age, spread quality, strike coverage, liquidity, expiry mismatch, required Greeks availability

- [ ] **Step 6: Re-run the targeted tests**

Run:

```bash
cargo test -p polyalpha-data parse_deribit_ticker_payload_extracts_iv_and_greeks -- --nocapture
cargo test -p polyalpha-pulse anchor_router_uses_deribit_primary_for_btc_and_eth -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/polyalpha-data/src/live/deribit.rs crates/polyalpha-data/src/live/mod.rs crates/polyalpha-data/src/lib.rs crates/polyalpha-pulse/src/model.rs crates/polyalpha-pulse/src/anchor/mod.rs crates/polyalpha-pulse/src/anchor/provider.rs crates/polyalpha-pulse/src/anchor/router.rs crates/polyalpha-pulse/src/anchor/deribit.rs
git commit -m "feat: add deribit anchor provider for pulse runtime"
```

### Task 3: Implement `EventPricer` And `PulseDetector`

**Files:**
- Create: `crates/polyalpha-pulse/src/pricing.rs`
- Create: `crates/polyalpha-pulse/src/detector.rs`
- Modify: `crates/polyalpha-pulse/src/model.rs`
- Test: `crates/polyalpha-pulse/src/pricing.rs`
- Test: `crates/polyalpha-pulse/src/detector.rs`

- [ ] **Step 1: Add failing pricing regressions for `dS` bump and expiry gap adjustment**

```rust
#[test]
fn event_delta_uses_relative_bump_with_floor_and_cap() {
    let config = pricing_test_config(1, Decimal::new(5, 0), Decimal::new(25, 0));

    let bump_eth = compute_delta_bump(Decimal::new(2_000, 0), &config);
    let bump_btc = compute_delta_bump(Decimal::new(100_000, 0), &config);

    assert_eq!(bump_eth, Decimal::new(5, 0));
    assert_eq!(bump_btc, Decimal::new(10, 0));
}

#[test]
fn expiry_gap_adjustment_bridges_small_positive_gap_before_hard_cap() {
    let adjustment = ExpiryGapAdjustment::new(60, 360, 720);

    let adjusted = adjustment
        .apply(0.61, 0.64, 2.0 / 365.0, 6.0 / 365.0)
        .expect("soft mismatch should still price");

    assert!(adjusted.fair_prob_yes > 0.61);
    assert!(adjusted.fair_prob_yes < 0.64);
}
```

- [ ] **Step 2: Run the targeted pricing tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-pulse event_delta_uses_relative_bump_with_floor_and_cap -- --nocapture
cargo test -p polyalpha-pulse expiry_gap_adjustment_bridges_small_positive_gap_before_hard_cap -- --nocapture
```

Expected: FAIL because `EventPricer`, `compute_delta_bump`, and `ExpiryGapAdjustment` do not exist yet.

- [ ] **Step 3: Implement `EventPricer`**

Implementation notes:
- `EventPricer` should output:

```rust
pub struct EventPriceOutput {
    pub fair_prob_yes: f64,
    pub fair_prob_no: f64,
    pub event_delta_yes: f64,
    pub event_delta_no: f64,
    pub gamma_estimate: Option<f64>,
    pub delta_bump_used: Decimal,
    pub expiry_gap_adjustment_applied: bool,
    pub pricing_quality: PricingQuality,
}
```

- Required behavior:
  - support `Above`, `Below`, `Between`
  - use local-strike interpolation, not global surface fit
  - compute `dS = clamp(S_ref * ratio, min_abs_bump, max_abs_bump)`
  - use anchor-aligned `index_price` as `S_ref`
  - compare `delta(dS)` vs `delta(2*dS)` and downgrade `pricing_quality` if unstable
  - apply `soft adjustment + hard cap` for expiry mismatch

- [ ] **Step 4: Add a failing detector regression for full-session edge economics**

```rust
#[test]
fn detector_rejects_when_friction_exceeds_pulse_basis() {
    let detector = PulseDetector::new(detector_test_config(25));
    let decision = detector.evaluate(PulseOpportunityInput {
        instant_basis_bps: 31.0,
        poly_vwap_slippage_bps: 8.0,
        hedge_slippage_bps: 6.0,
        fee_bps: 3.0,
        perp_basis_penalty_bps: 5.0,
        rehedge_reserve_bps: 6.0,
        timeout_exit_reserve_bps: 5.0,
        anchor_quality_ok: true,
        data_fresh: true,
    });

    assert!(!decision.should_trade);
    assert_eq!(decision.rejection_code.unwrap().as_str(), "net_session_edge_below_threshold");
}
```

- [ ] **Step 5: Implement `PulseDetector`**

Implementation notes:
- `PulseDetector` should consume:
  - Poly top-N VWAP
  - hedge executable cost
  - fees
  - slippage
  - perp/index basis penalty
  - rehedge reserve
  - timeout exit reserve
  - freshness / depth / anchor quality gates
- Expose a single output type:

```rust
pub struct DetectorDecision {
    pub should_trade: bool,
    pub net_session_edge_bps: f64,
    pub rejection_code: Option<PulseFailureCode>,
}
```

- [ ] **Step 6: Re-run the targeted pricing and detector tests**

Run:

```bash
cargo test -p polyalpha-pulse event_delta_uses_relative_bump_with_floor_and_cap -- --nocapture
cargo test -p polyalpha-pulse expiry_gap_adjustment_bridges_small_positive_gap_before_hard_cap -- --nocapture
cargo test -p polyalpha-pulse detector_rejects_when_friction_exceeds_pulse_basis -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/polyalpha-pulse/src/pricing.rs crates/polyalpha-pulse/src/detector.rs crates/polyalpha-pulse/src/model.rs
git commit -m "feat: add pulse pricer and detector"
```

### Task 4: Build The Session FSM, Partial-Fill Logic, Pin-Risk Suppression, And Global Hedge Aggregation

**Files:**
- Create: `crates/polyalpha-pulse/src/session.rs`
- Create: `crates/polyalpha-pulse/src/hedge.rs`
- Modify: `crates/polyalpha-pulse/src/model.rs`
- Test: `crates/polyalpha-pulse/src/session.rs`
- Test: `crates/polyalpha-pulse/src/hedge.rs`

- [ ] **Step 1: Add failing FSM regressions for partial fill and pin-risk suppression**

```rust
#[test]
fn poly_opening_uses_actual_filled_qty_for_hedge_opening() {
    let mut session = test_session();
    session.on_poly_fill(PolyFillOutcome {
        planned_qty: Decimal::new(10_000, 0),
        filled_qty: Decimal::new(3_500, 0),
        avg_price: Decimal::new(35, 2),
    });

    let cmd = session.next_command().expect("hedge opening command");

    assert_eq!(session.actual_poly_filled_qty(), Decimal::new(3_500, 0));
    assert_eq!(cmd.hedge_reference_qty(), Decimal::new(3_500, 0));
}

#[test]
fn pin_risk_delta_clamp_caps_target_delta_before_hedge_storm() {
    let mut session = test_session_in_pin_risk_zone();
    session.recompute_target_delta(1.84);

    assert_eq!(session.pin_risk_mode(), Some(GammaCapMode::DeltaClamp));
    assert_eq!(session.target_event_delta(), 0.75);
}
```

- [ ] **Step 2: Add a failing aggregation regression for multi-session netting**

```rust
#[test]
fn global_hedge_aggregator_nets_opposing_session_targets() {
    let mut aggregator = GlobalHedgeAggregator::new(Decimal::new(1, 3), 100);
    aggregator.upsert("session-a", PulseAsset::Btc, Decimal::new(35, 2));
    aggregator.upsert("session-b", PulseAsset::Btc, Decimal::new(-20, 2));
    aggregator.sync_actual_position(PulseAsset::Btc, Decimal::new(8, 2));

    let order = aggregator.reconcile(PulseAsset::Btc).expect("net order");

    assert_eq!(order.net_target_delta, Decimal::new(15, 2));
    assert_eq!(order.actual_exchange_position, Decimal::new(8, 2));
    assert_eq!(order.order_qty, Decimal::new(7, 2));
}
```

- [ ] **Step 3: Run the targeted FSM and hedge tests to confirm they fail**

Run:

```bash
cargo test -p polyalpha-pulse poly_opening_uses_actual_filled_qty_for_hedge_opening -- --nocapture
cargo test -p polyalpha-pulse pin_risk_delta_clamp_caps_target_delta_before_hedge_storm -- --nocapture
cargo test -p polyalpha-pulse global_hedge_aggregator_nets_opposing_session_targets -- --nocapture
```

Expected: FAIL because `PulseSession`, `GammaCapMode`, and `GlobalHedgeAggregator` do not exist yet.

- [ ] **Step 4: Implement the session FSM**

Implementation notes:
- Model the lifecycle explicitly:

```rust
pub enum PulseSessionState {
    PreTradeAudit,
    PolyOpening,
    HedgeOpening,
    MakerExitWorking,
    Pegging,
    Rehedging,
    EmergencyHedge,
    EmergencyFlatten,
    ChasingExit,
    Closed,
}
```

- `PulseSession` must:
  - carry `planned_poly_qty` and `actual_poly_filled_qty`
  - abort if filled notional `< min_opening_notional_usd`
  - switch all downstream sizing to `actual_filled_qty`
  - expose maker exit replace commands
  - support timeout exit and emergency flatten
  - enter pin-risk suppression with `delta_clamp`, `protective_only`, or `freeze`

- Use command generation, not direct exchange side effects:

```rust
pub enum SessionCommand {
    SubmitPolyOpen { max_price: Decimal, qty: Decimal },
    SubmitMakerExit { price: Decimal, qty: Decimal },
    ReplaceMakerExit { price: Decimal, qty: Decimal },
    RequestHedgeReconcile,
    EmergencyFlatten { reason: PulseFailureCode },
}
```

- `EmergencyFlatten` is **not** a direct Binance flatten order.
  - session side effect 1: set this session's `target_delta_exposure` to `0`
  - session side effect 2: move the session into closing / closed state so it stops contributing hedge intent
  - hedge side effect: let `GlobalHedgeAggregator` release the residual hedge on the next reconcile pass
  - if the Poly leg must be force-closed, that remains a session-local aggressive Poly exit path; only the hedge leg is forbidden from bypassing the aggregator

- [ ] **Step 5: Implement `GlobalHedgeAggregator`**

Implementation notes:
- Keep it in `polyalpha-pulse`, not `polyalpha-executor`.
- Input: per-session `target_delta_exposure`
- Input: fresh actual exchange position samples from `paper` / `live-mock` account state
- Output: per-asset incremental hedge order plus attribution

```rust
pub struct GlobalHedgeAggregator {
    pub sessions: HashMap<String, SessionHedgeIntent>,
    pub actual_positions: HashMap<PulseAsset, Decimal>,
    pub min_order_qty: Decimal,
    pub reconcile_throttle_ms: u64,
}
```

- Required behavior:
  - net same-asset opposing targets
  - compute `order_qty = net_target_delta - actual_exchange_position`
  - own an explicit `ActualPositionSync` path; never rely only on internally inferred nominal hedge state
  - suppress dust orders below min quantity
  - throttle repeated reconcile attempts
  - retain per-session attribution for audit

- [ ] **Step 6: Re-run the targeted tests**

Run:

```bash
cargo test -p polyalpha-pulse poly_opening_uses_actual_filled_qty_for_hedge_opening -- --nocapture
cargo test -p polyalpha-pulse pin_risk_delta_clamp_caps_target_delta_before_hedge_storm -- --nocapture
cargo test -p polyalpha-pulse global_hedge_aggregator_nets_opposing_session_targets -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/polyalpha-pulse/src/session.rs crates/polyalpha-pulse/src/hedge.rs crates/polyalpha-pulse/src/model.rs
git commit -m "feat: add pulse session fsm and global hedge aggregation"
```

### Task 5: Wire The Paper/Live-Mock Runtime And CLI Around `polyalpha-pulse`

**Files:**
- Create: `crates/polyalpha-pulse/src/runtime.rs`
- Create: `crates/polyalpha-cli/src/pulse.rs`
- Modify: `crates/polyalpha-cli/src/args.rs`
- Modify: `crates/polyalpha-cli/src/main.rs`
- Verify: `crates/polyalpha-cli/src/runtime.rs`
- Test: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-cli/src/args.rs`

- [ ] **Step 1: Add a failing pulse runtime smoke test in the new crate**

```rust
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
```

- [ ] **Step 2: Run the targeted runtime smoke test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-pulse runtime_builds_live_mock_stack_for_btc_and_uses_mock_execution -- --nocapture
```

Expected: FAIL because `PulseRuntimeBuilder` and `PulseExecutionMode` do not exist yet.

- [ ] **Step 3: Implement runtime orchestration in `polyalpha-pulse` and thin CLI glue in `polyalpha-cli`**

Implementation notes:
- `PulseRuntimeBuilder` should accept:
  - `Settings`
  - `SymbolRegistry`
  - anchor router/provider
  - Poly/Binance feed handles
  - existing execution stack from `polyalpha-cli/src/runtime.rs`
- Reuse old helper functions only for:
  - `build_data_manager(...)`
  - `build_execution_stack(...)`
- Do **not** call `SimpleAlphaEngine` or old basis planner from the pulse runtime.
- Keep CLI orchestration in a new file:

```rust
pub async fn run_pulse_paper(env: &str, assets: &[PulseAsset]) -> Result<()>;
pub async fn run_pulse_live_mock(env: &str, assets: &[PulseAsset], depth: u16) -> Result<()>;
pub fn inspect_pulse(env: &str) -> Result<()>;
```

- Add command shape:
  - `polyalpha-cli paper pulse run --env default --assets btc,eth`
  - `polyalpha-cli live pulse run --env default --assets btc,eth --executor-mode mock`
  - `polyalpha-cli paper pulse inspect --env default`

- [ ] **Step 4: Add a CLI parsing regression for the live pulse command**

```rust
#[test]
fn parses_live_pulse_run_command() {
    let cli = Cli::parse_from([
        "polyalpha-cli",
        "live",
        "pulse",
        "run",
        "--env",
        "default",
        "--assets",
        "btc,eth",
        "--executor-mode",
        "mock"
    ]);

    assert!(matches!(
        cli.command,
        Command::Live {
            command: LiveCommand::Pulse { .. }
        }
    ));
}
```

- [ ] **Step 5: Re-run the targeted runtime and CLI tests**

Run:

```bash
cargo test -p polyalpha-pulse runtime_builds_live_mock_stack_for_btc_and_uses_mock_execution -- --nocapture
cargo test -p polyalpha-cli parses_paper_pulse_run_command -- --nocapture
cargo test -p polyalpha-cli parses_live_pulse_run_command -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/polyalpha-pulse/src/runtime.rs crates/polyalpha-cli/src/pulse.rs crates/polyalpha-cli/src/args.rs crates/polyalpha-cli/src/main.rs
git commit -m "feat: wire pulse paper and live-mock runtime commands"
```

### Task 6: Extend Audit And Monitor For Session-Centric Pulse Observability

**Files:**
- Create: `crates/polyalpha-pulse/src/audit.rs`
- Create: `crates/polyalpha-pulse/src/monitor.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-audit/src/model.rs`
- Modify: `crates/polyalpha-audit/src/writer.rs`
- Modify: `crates/polyalpha-audit/src/reader.rs`
- Modify: `crates/polyalpha-audit/src/warehouse.rs`
- Modify: `crates/polyalpha-cli/src/audit.rs`
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
- Modify: `crates/polyalpha-cli/src/monitor/json_mode.rs`
- Test: `crates/polyalpha-audit/src/model.rs`
- Test: `crates/polyalpha-audit/src/warehouse.rs`
- Test: `crates/polyalpha-core/src/socket/message.rs`

- [ ] **Step 1: Add a failing audit payload regression**

```rust
#[test]
fn pulse_session_record_serializes_partial_fill_and_hedge_attribution() {
    let payload = AuditEventPayload::PulseLifecycle(PulseLifecycleAuditEvent {
        session_id: "pulse-session-1".to_owned(),
        asset: "btc".to_owned(),
        state: "maker_exit_working".to_owned(),
        planned_poly_qty: "10000".to_owned(),
        actual_poly_filled_qty: "3500".to_owned(),
        actual_poly_fill_ratio: 0.35,
        session_target_delta_exposure: "0.41".to_owned(),
        session_allocated_hedge_qty: "0.39".to_owned(),
        account_net_target_delta_before_order: "0.52".to_owned(),
        account_net_target_delta_after_order: "0.13".to_owned(),
        delta_bump_used: "10".to_owned(),
    });

    let encoded = serde_json::to_string(&payload).expect("serialize payload");
    assert!(encoded.contains("\"pulse_lifecycle\""));
    assert!(encoded.contains("\"actual_poly_filled_qty\":\"3500\""));
}
```

- [ ] **Step 2: Add a failing monitor state regression**

```rust
#[test]
fn monitor_state_serializes_optional_pulse_snapshot() {
    let mut state = MonitorState::default();
    state.pulse = Some(PulseMonitorView {
        active_sessions: vec![PulseSessionMonitorRow {
            session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            remaining_secs: 540,
            net_edge_bps: 31.4,
        }],
        asset_health: vec![],
        selected_session: None,
    });

    let encoded = serde_json::to_string(&state).expect("serialize monitor state");
    assert!(encoded.contains("\"pulse\""));
    assert!(encoded.contains("\"active_sessions\""));
}
```

- [ ] **Step 3: Run the targeted audit and monitor tests to confirm they fail**

Run:

```bash
cargo test -p polyalpha-audit pulse_session_record_serializes_partial_fill_and_hedge_attribution -- --nocapture
cargo test -p polyalpha-core monitor_state_serializes_optional_pulse_snapshot -- --nocapture
```

Expected: FAIL because pulse audit payloads and pulse monitor view do not exist yet.

- [ ] **Step 4: Implement pulse audit and monitor surfaces**

Implementation notes:
- Extend `AuditEventPayload` with pulse-specific variants such as:

```rust
pub enum AuditEventPayload {
    // existing variants ...
    PulseLifecycle(PulseLifecycleAuditEvent),
    PulseSessionSummary(PulseSessionSummaryEvent),
    PulseAssetHealth(PulseAssetHealthAuditEvent),
}
```

- Keep using `AuditWriter` / `AuditWarehouse`; do not invent a second persistence root.
- Add `PulseMonitorView` into `MonitorState` as an optional field:

```rust
pub struct MonitorState {
    // existing fields ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulse: Option<PulseMonitorView>,
}
```

- `PulseMonitorView` must carry:
  - `active_sessions`
  - `asset_health`
  - `selected_session`
- Update TUI and JSON monitor to render pulse-specific panes only when `state.pulse.is_some()`.
- Add minimal audit CLI surfaces:
  - `polyalpha-cli audit pulse-sessions --env default`
  - `polyalpha-cli audit pulse-session-detail --env default --session-id ...`

- [ ] **Step 5: Add a failing warehouse sync regression**

```rust
#[test]
fn warehouse_sync_persists_pulse_session_summary_rows() {
    let root = temp_pulse_audit_root();
    let session_id = "pulse-audit-session";
    write_test_pulse_session(&root, session_id).expect("write pulse session");

    let warehouse = AuditWarehouse::new(&root).expect("create warehouse");
    warehouse.sync_session(&root, session_id).expect("sync pulse session");

    let conn = duckdb::Connection::open(warehouse.db_path()).expect("open warehouse");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pulse_session_summaries WHERE session_id = ?",
            duckdb::params![session_id],
            |row| row.get(0),
        )
        .expect("query pulse summary count");

    assert_eq!(count, 1);
}
```

- [ ] **Step 6: Re-run the targeted tests**

Run:

```bash
cargo test -p polyalpha-audit pulse_session_record_serializes_partial_fill_and_hedge_attribution -- --nocapture
cargo test -p polyalpha-core monitor_state_serializes_optional_pulse_snapshot -- --nocapture
cargo test -p polyalpha-audit warehouse_sync_persists_pulse_session_summary_rows -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/polyalpha-pulse/src/audit.rs crates/polyalpha-pulse/src/monitor.rs crates/polyalpha-core/src/socket/message.rs crates/polyalpha-audit/src/model.rs crates/polyalpha-audit/src/writer.rs crates/polyalpha-audit/src/reader.rs crates/polyalpha-audit/src/warehouse.rs crates/polyalpha-cli/src/audit.rs crates/polyalpha-cli/src/monitor/state.rs crates/polyalpha-cli/src/monitor/ui.rs crates/polyalpha-cli/src/monitor/json_mode.rs
git commit -m "feat: add pulse audit and monitor views"
```

### Task 7: Add End-To-End Acceptance Coverage And User-Facing Runtime Docs

**Files:**
- Modify: `README.md`
- Modify: `docs/current-status.md`
- Modify: `config/default.toml`
- Test: `crates/polyalpha-pulse/src/runtime.rs`
- Test: `crates/polyalpha-cli/src/pulse.rs`

- [ ] **Step 1: Add a failing end-to-end runtime regression for timeout exit and audit emission**

```rust
#[tokio::test]
async fn paper_runtime_times_out_to_chasing_exit_and_emits_audit() {
    let fixture = end_to_end_fixture_for_timeout_exit();
    let mut runtime = fixture.build_paper_runtime().await.expect("build runtime");

    runtime.run_until_closed("pulse-session-timeout").await.expect("run session");

    let summary = fixture
        .load_pulse_session_summary("pulse-session-timeout")
        .expect("load pulse summary");

    assert_eq!(summary.final_state, "closed");
    assert!(summary.deadline_exit_triggered);
    assert!(summary.audit_event_count > 0);
}
```

- [ ] **Step 2: Add a failing end-to-end regression for multi-session hedge netting**

```rust
#[tokio::test]
async fn live_mock_runtime_nets_two_btc_sessions_before_sending_binance_order() {
    let fixture = end_to_end_fixture_for_opposing_sessions();
    let mut runtime = fixture.build_live_mock_runtime().await.expect("build runtime");

    runtime.run_until_hedge_reconcile().await.expect("run reconcile");

    let orders = fixture.recorded_binance_orders();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].qty, Decimal::new(15, 2));
}
```

- [ ] **Step 3: Run the targeted end-to-end tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-pulse paper_runtime_times_out_to_chasing_exit_and_emits_audit -- --nocapture
cargo test -p polyalpha-pulse live_mock_runtime_nets_two_btc_sessions_before_sending_binance_order -- --nocapture
```

Expected: FAIL until runtime, audit, and hedge attribution are wired end-to-end.

- [ ] **Step 4: Implement the last-mile runtime assertions and operator docs**

Implementation notes:
- Ensure the end-to-end fixture path covers:
  - paper mode
  - live-mock mode
  - partial fill scaling
  - pin-risk suppression
  - global hedge aggregation
  - timeout exit
  - audit emission
  - monitor snapshot generation
- Update `README.md` with the exact operator commands:

```bash
cargo run -p polyalpha-cli -- paper pulse run --env default --assets btc,eth
cargo run -p polyalpha-cli -- live pulse run --env default --assets btc,eth --executor-mode mock
cargo run -p polyalpha-cli -- audit pulse-sessions --env default
```

- Update `docs/current-status.md` with MVP scope and non-goals:
  - BTC/ETH only
  - Deribit anchor only
  - Binance perp hedge only
  - paper / live-mock only
  - no real-money live trading

- [ ] **Step 5: Run the full targeted verification bundle**

Run:

```bash
cargo test -p polyalpha-core pulse_arb_config_deserializes_deribit_binance_routing -- --nocapture
cargo test -p polyalpha-data parse_deribit_ticker_payload_extracts_iv_and_greeks -- --nocapture
cargo test -p polyalpha-pulse -- --nocapture
cargo test -p polyalpha-cli parses_paper_pulse_run_command -- --nocapture
cargo test -p polyalpha-cli parses_live_pulse_run_command -- --nocapture
cargo test -p polyalpha-audit warehouse_sync_persists_pulse_session_summary_rows -- --nocapture
cargo build
```

Expected: PASS. The new crate builds, targeted pulse regressions pass, and the workspace compiles cleanly.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/current-status.md config/default.toml crates/polyalpha-pulse/src/runtime.rs crates/polyalpha-cli/src/pulse.rs
git commit -m "feat: complete pulse arb runtime mvp"
```

## Self-Review Notes

- Spec coverage:
  - 独立 `pulse_arb` crate / runtime: Task 1, 5
  - `Deribit` anchor + routing 扩展位: Task 1, 2
  - `EventPricer` / `fair_prob` / relative `dS` / expiry gap adjustment: Task 3
  - `net_session_edge` 准入: Task 3
  - `PulseSession` / partial fill / maker exit / pegging / timeout / emergency flatten: Task 4, 7
  - pin risk suppression: Task 4
  - `GlobalHedgeAggregator`: Task 4, 7
  - paper / live-mock runtime: Task 5, 7
  - audit / monitor / raw tape future-proofing: Task 6, 7
- Placeholder scan:
  - 无占位词
  - 所有测试名、命令、文件路径已明确写出
- Type consistency:
  - `PulseArbStrategyConfig`
  - `AnchorSnapshot`
  - `EventPriceOutput`
  - `PulseSessionState`
  - `GlobalHedgeAggregator`
  - `PulseMonitorView`
  - `PulseLifecycleAuditEvent`
  这些名字在后续任务中保持一致，不要中途改名。
