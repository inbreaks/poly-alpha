# PolyAlpha-Prime: Polymarket 基差对冲 + 动态做市系统

## Context

Polymarket 作为基于 Polygon 链的预测市场，其 CLOB（中央限价订单簿）定价机制与 CEX 衍生品市场之间存在持续性的定价偏差。本项目旨在构建一套纯 Rust 的自动化交易系统，通过**基差对冲**和**动态做市**两大核心策略，从 Polymarket 散户情绪溢价中提取 Delta 中性的 Alpha 收益。

**目标用户**: 专业量化交易员，资金规模 $10k-$200k
**核心语言**: Pure Rust + Tokio 异步运行时
**对冲 CEX**: Binance + OKX（永续合约）
**事件类型**: 加密货币价格预测类（如 "BTC 月底前是否突破 $100k"）

---

## 1. 系统架构总览

### 1.1 架构图

```
                          +----------------------------------+
                          |        polyalpha-cli (bin)        |
                          |  Orchestrator / CLI / Logging     |
                          +------------------+---------------+
                                             |
            +--------------------------------+--------------------------------+
            |                                |                                |
   +--------v---------+          +-----------v----------+          +----------v---------+
   |  polyalpha-data   |          |  polyalpha-engine    |          |  polyalpha-risk    |
   | WebSocket Clients |          | Alpha / DMM / Signal |          | Risk / Breaker /   |
   | OrderBook Mgmt    |          | Pricing Models       |          | Position Tracking  |
   +--------+---------+          +-----------+----------+          +----------+---------+
            |                                |                                |
            | broadcast::Sender              | watch::Sender + mpsc::Sender   | watch::Sender
            | (MarketDataEvent)              | (DMM / Arb Signals)            | (RiskStateSnapshot)
            |                                |                                |
            +--------------------------------+--------------------------------+
                                             |
                                  +----------v----------+
                                  | polyalpha-executor   |
                                  | Order Execution /    |
                                  | EIP-712 / Hedge FSM  |
                                  +----------+----------+
                                             |
                                  +----------v----------+
                                  |  polyalpha-core      |
                                  | Shared Types/Traits  |
                                  | Config / Errors      |
                                  +---------------------+
```

### 1.2 数据流

```
Polymarket WS --+                                               +-- Polymarket CLOB REST
Binance WS -----+--> [polyalpha-data] --broadcast--> [engine] --+
OKX WS ---------+    OrderBook Agg.   MarketEvent    Alpha/DMM  |   mpsc
                                                      Signals ---+--> [executor]
                      [polyalpha-risk] <--watch--- RiskState     |    Order Mgmt
                       Position Mgr.                             +-- Binance Futures REST
                       Circuit Breaker <--mpsc--- FillEvent      +-- OKX Futures REST
                                                   from executor
```

### 1.3 Crate 依赖关系（关键设计决策）

```
polyalpha-cli
  +-- polyalpha-core
  +-- polyalpha-data     --> polyalpha-core
  +-- polyalpha-engine   --> polyalpha-core
  +-- polyalpha-executor --> polyalpha-core
  +-- polyalpha-risk     --> polyalpha-core
```

**data / engine / executor / risk 之间零直接依赖**。它们只依赖 core，通过 Tokio channel 在 cli 层连接。这确保 4 个 Agent 可以完全并行开发。

---

## 2. Cargo Workspace 目录结构

```
polymarket-cex/
+-- Cargo.toml                    # workspace root
+-- config/
|   +-- default.toml              # 默认配置
|   +-- dev.toml                  # 开发环境覆盖
+-- crates/
|   +-- polyalpha-core/
|   |   +-- Cargo.toml
|   |   +-- src/
|   |       +-- lib.rs
|   |       +-- types/
|   |       |   +-- mod.rs
|   |       |   +-- orderbook.rs  # OrderBook, PriceLevel, Side
|   |       |   +-- order.rs      # Order, OrderType, OrderStatus
|   |       |   +-- fill.rs       # Fill, FillSide
|   |       |   +-- position.rs   # Position, PositionSide
|   |       |   +-- market.rs     # MarketConfig, MarketId, ConditionId
|   |       |   +-- signal.rs     # DMM/Arb outputs, SignalStrength
|   |       |   +-- decimal.rs    # Price / Probability / UsdNotional / PolyShares / CexBaseQty
|   |       +-- traits/
|   |       |   +-- mod.rs
|   |       |   +-- data_source.rs
|   |       |   +-- alpha.rs
|   |       |   +-- executor.rs
|   |       |   +-- risk.rs
|   |       +-- config.rs         # 全局配置结构体
|   |       +-- error.rs          # 统一错误类型 (thiserror)
|   |       +-- event.rs          # 跨模块事件枚举
|   |       +-- channel.rs        # channel 类型别名 & 工厂函数
|   +-- polyalpha-data/
|   |   +-- Cargo.toml
|   |   +-- src/
|   |       +-- lib.rs
|   |       +-- polymarket/
|   |       |   +-- mod.rs
|   |       |   +-- stream.rs     # 基于官方SDK的WS流封装 (subscribe_orderbook)
|   |       |   +-- normalizer.rs # SDK类型 -> 内部统一类型转换
|   |       +-- binance/
|   |       |   +-- mod.rs
|   |       |   +-- ws_client.rs
|   |       |   +-- rest_client.rs
|   |       |   +-- types.rs
|   |       +-- okx/
|   |       |   +-- mod.rs
|   |       |   +-- ws_client.rs
|   |       |   +-- rest_client.rs
|   |       |   +-- types.rs
|   |       +-- orderbook/
|   |       |   +-- mod.rs
|   |       |   +-- book.rs       # L2OrderBook (BTreeMap 实现)
|   |       |   +-- aggregator.rs # 多源聚合
|   |       +-- normalizer.rs     # 原始数据 -> 统一类型
|   |       +-- manager.rs        # DataManager 入口
|   +-- polyalpha-engine/
|   |   +-- Cargo.toml
|   |   +-- src/
|   |       +-- lib.rs
|   |       +-- basis/
|   |       |   +-- mod.rs
|   |       |   +-- implied_prob.rs   # Poly orderbook -> 隐含概率
|   |       |   +-- cex_implied.rs    # CEX 价格+波动率 -> 隐含概率 (Black-Scholes)
|   |       |   +-- z_score.rs        # 滚动 Z-score (Welford 算法)
|   |       |   +-- basis_signal.rs   # 基差信号生成
|   |       +-- dmm/
|   |       |   +-- mod.rs
|   |       |   +-- avellaneda_stoikov.rs  # A-S 模型核心
|   |       |   +-- inventory.rs      # 库存管理
|   |       |   +-- quote_gen.rs      # 多层报价生成
|   |       +-- negrisk/
|   |       |   +-- mod.rs
|   |       |   +-- bundle_arb.rs     # NegRisk 组合套利
|   |       +-- features/
|   |       |   +-- mod.rs
|   |       |   +-- rolling_window.rs # 通用滚动窗口容器
|   |       |   +-- volatility.rs     # 波动率估计
|   |       +-- engine.rs             # AlphaEngine 主循环
|   +-- polyalpha-executor/
|   |   +-- Cargo.toml
|   |   +-- src/
|   |       +-- lib.rs
|   |       +-- polymarket/
|   |       |   +-- mod.rs
|   |       |   +-- poly_executor.rs # 基于官方SDK封装, 调用 client.sign() + post_order()
|   |       +-- binance/
|   |       |   +-- mod.rs
|   |       |   +-- futures_client.rs # Binance 永续合约下单
|   |       +-- okx/
|   |       |   +-- mod.rs
|   |       |   +-- futures_client.rs # OKX 永续合约下单
|   |       +-- hedge/
|   |       |   +-- mod.rs
|   |       |   +-- state_machine.rs  # 对冲状态机 (FSM) — N-Leg 通用
|   |       |   +-- leg_manager.rs    # N-Leg 订单生命周期管理
|   |       +-- precision.rs          # PrecisionNormalizer: tick/step size 对齐
|   |       +-- executor.rs           # ExecutionManager 主循环
|   +-- polyalpha-risk/
|   |   +-- Cargo.toml
|   |   +-- src/
|   |       +-- lib.rs
|   |       +-- position/
|   |       |   +-- mod.rs
|   |       |   +-- tracker.rs       # 仓位追踪
|   |       |   +-- pnl.rs           # PnL 计算 + Drawdown
|   |       +-- limits/
|   |       |   +-- mod.rs
|   |       |   +-- pre_trade.rs     # 下单前检查
|   |       |   +-- post_trade.rs    # 成交后检查
|   |       +-- breaker/
|   |       |   +-- mod.rs
|   |       |   +-- circuit_breaker.rs # 熔断器 FSM
|   |       +-- persistence/
|   |       |   +-- mod.rs
|   |       |   +-- sqlite.rs        # SQLite 持久化
|   |       +-- manager.rs           # RiskManager 主循环
|   +-- polyalpha-cli/
|       +-- Cargo.toml
|       +-- src/
|           +-- main.rs              # 入口点
|           +-- orchestrator.rs      # 创建 channels, 启动所有模块
|           +-- cli.rs               # clap 命令行参数
|           +-- display.rs           # tracing 日志配置
+-- tests/
    +-- integration/
        +-- mock_ws_server.rs
        +-- test_basis_flow.rs
        +-- test_dmm_flow.rs
```

---

## 3. 核心类型定义 (polyalpha-core)

### 3.1 双精度与单位安全体系: Decimal (账务) vs f64 (计算)

**设计原则:**
- `rust_decimal::Decimal` 仍用于所有账务、下单、仓位、PnL 字段，避免浮点误差。
- 但**不能**再用一个裸 `Quantity` 覆盖所有场景。Polymarket shares、USDC notional、CEX base qty 是三种不同单位。
- `f64` 只用于 engine 内部的数学计算（ln, exp, sqrt, normal_cdf, A-S 公式等），在模块边界统一转回 Decimal 包装类型。
- Polymarket 的市价单可按 `UsdNotional` 下单，限价单/订单簿深度则天然是 `PolyShares`；这两个量必须同时建模，不能互相偷换。

```rust
// types/decimal.rs

/// 通用成交价 / 挂单价
pub struct Price(pub Decimal);

/// [0, 1] 区间内的隐含概率，仅 engine / signal 使用
pub struct Probability(pub Decimal);

/// USDC / USD 名义金额
pub struct UsdNotional(pub Decimal);

/// Polymarket YES/NO share 数量
pub struct PolyShares(pub Decimal);

/// CEX 永续 base asset 数量，例如 BTCUSDT 中的 BTC 数量
pub struct CexBaseQty(pub Decimal);

/// 跨 venue 传递时显式保留数量单位
pub enum VenueQuantity {
    PolyShares(PolyShares),
    CexBaseQty(CexBaseQty),
}

impl Price {
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }

    pub fn from_f64_rounded(v: f64, tick_size: Decimal) -> Self {
        let d = Decimal::from_f64(v).unwrap_or(Decimal::ZERO);
        Price((d / tick_size).floor() * tick_size)
    }
}

impl Probability {
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }

    pub fn from_f64_clamped(v: f64) -> Self {
        let d = Decimal::from_f64(v).unwrap_or(Decimal::ZERO);
        Probability(d.max(Decimal::ZERO).min(Decimal::ONE))
    }
}

impl PolyShares {
    pub fn to_usd_notional(&self, avg_price: Price) -> UsdNotional {
        UsdNotional(self.0 * avg_price.0)
    }

    /// Binary option delta = d(option_price_per_share) / dS
    /// 因此 shares * delta 直接得到需要对冲的 CEX base asset 数量
    pub fn to_cex_base_qty(&self, delta: f64) -> CexBaseQty {
        let d = Decimal::from_f64(delta.abs()).unwrap_or(Decimal::ZERO);
        CexBaseQty(self.0 * d)
    }
}

impl UsdNotional {
    pub fn from_poly(shares: PolyShares, avg_price: Price) -> Self {
        UsdNotional(shares.0 * avg_price.0)
    }
}
```

**边界规则:**
- `data` 层: 原始交易所数据 -> `Price` + `VenueQuantity`，不丢失真实单位。
- `engine` 层: 输入边界做 `.to_f64()`，内部临时使用 `f64`；输出信号时必须重新包装为 `Probability` / `UsdNotional` / `PolyShares` / `CexBaseQty`。
- `executor` 层: 只消费显式 typed request，不负责猜测 share / notional / base qty。
- `risk` 层: 先把 unit-safe quantity mark-to-market 为 `UsdNotional`，再做限额判断。

### 3.2 OrderBook

```rust
// types/orderbook.rs

pub enum InstrumentKind {
    PolyYes,
    PolyNo,
    CexPerp,
}

pub struct PriceLevel {
    pub price: Price,
    pub quantity: VenueQuantity,
}

pub enum Side { Bid, Ask }

pub enum Exchange { Polymarket, Binance, Okx }

pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
    pub bids: Vec<PriceLevel>,  // 价格降序
    pub asks: Vec<PriceLevel>,  // 价格升序
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
}
```

### 3.3 Order

```rust
// types/order.rs
pub struct OrderId(pub String);
pub struct ClientOrderId(pub String);

pub enum OrderSide { Buy, Sell }
pub enum OrderType { Limit, Market, PostOnly }
pub enum TimeInForce { GTC, IOC, FOK }
pub enum OrderStatus { Pending, Open, PartialFill, Filled, Cancelled, Rejected }

pub enum OrderRequest {
    Poly(PolyOrderRequest),
    Cex(CexOrderRequest),
}

pub struct PolyOrderRequest {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub limit_price: Option<Price>,
    pub shares: Option<PolyShares>,
    pub quote_notional: Option<UsdNotional>,   // 市价单优先使用
    pub time_in_force: TimeInForce,
    pub post_only: bool,
}

pub struct CexOrderRequest {
    pub client_order_id: ClientOrderId,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub venue_symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub base_qty: CexBaseQty,
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

pub struct OrderResponse {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: OrderId,
    pub status: OrderStatus,
    pub filled_quantity: VenueQuantity,
    pub average_price: Option<Price>,
    pub timestamp_ms: u64,
}
```

### 3.4 Fill / Position / Market / Signal

```rust
// types/fill.rs
pub struct Fill {
    pub fill_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
    pub order_id: OrderId,
    pub side: OrderSide,
    pub price: Price,
    pub quantity: VenueQuantity,
    pub notional_usd: UsdNotional,
    pub fee: UsdNotional,
    pub is_maker: bool,
    pub timestamp_ms: u64,
}

// types/position.rs
pub enum PositionSide { Long, Short, Flat }

pub struct PositionKey {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
}

pub struct Position {
    pub key: PositionKey,
    pub side: PositionSide,
    pub quantity: VenueQuantity,
    pub entry_price: Price,
    pub entry_notional: UsdNotional,
    pub unrealized_pnl: UsdNotional,
    pub realized_pnl: UsdNotional,
}

// types/market.rs
/// 系统内部唯一标识符, 所有模块只认这一个 ID
/// 例如: "btc-100k-mar-2026"
pub struct Symbol(pub String);

/// Polymarket 交易所特有的 ID 映射 (仅 data 层和 executor 层使用)
pub struct PolymarketIds {
    pub condition_id: String,    // WS 订阅用
    pub yes_token_id: String,    // 下单用
    pub no_token_id: String,     // 下单用
}

pub enum MarketPhase {
    Trading,
    PreSettlement { hours_remaining: f64 },
    ForceReduce { target_ratio: Decimal, hours_remaining: f64 },
    CloseOnly { hours_remaining: f64 },
    SettlementPending,
    Disputed,
    Resolved,
}

pub struct SettlementRules {
    pub stop_new_position_hours: u64,
    pub force_reduce_hours: u64,
    pub force_reduce_target_ratio: Decimal,
    pub close_only_hours: u64,
    pub emergency_close_hours: u64,
    pub dispute_close_only: bool,
}

impl MarketPhase {
    pub fn from_settlement(
        settlement_timestamp: u64,
        now_timestamp: u64,
        rules: &SettlementRules,
    ) -> Self {
        let hours_remaining = (settlement_timestamp.saturating_sub(now_timestamp) as f64) / 3600.0;

        if hours_remaining <= 0.0 {
            return MarketPhase::SettlementPending;
        }
        if hours_remaining <= rules.emergency_close_hours as f64 {
            return MarketPhase::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.close_only_hours as f64 {
            return MarketPhase::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.force_reduce_hours as f64 {
            return MarketPhase::ForceReduce {
                target_ratio: rules.force_reduce_target_ratio,
                hours_remaining,
            };
        }
        if hours_remaining <= rules.stop_new_position_hours as f64 {
            return MarketPhase::PreSettlement { hours_remaining };
        }
        MarketPhase::Trading
    }
}

/// 完整的市场配置 (从 TOML 加载)
pub struct MarketConfig {
    pub symbol: Symbol,                     // 业务主键, engine/risk 只看这个
    pub poly_ids: PolymarketIds,            // Poly 交易所特有 ID
    pub cex_symbol: String,                 // CEX 合约代码, e.g. "BTCUSDT"
    pub hedge_exchange: Exchange,           // 对冲用哪个 CEX
    pub strike_price: Option<Price>,        // 行权价 (如 BTC $100k)
    pub settlement_timestamp: u64,          // 结算时间 (unix timestamp, seconds)
    pub min_tick_size: Price,               // Poly 最小价格步长
    pub neg_risk: bool,                     // 是否属于 NegRisk bundle
    pub cex_price_tick: Decimal,            // CEX 价格精度 (如 0.1)
    pub cex_qty_step: Decimal,              // CEX 数量精度 (如 0.001)
    pub cex_contract_multiplier: Decimal,   // venue qty 与 base qty 的映射
}

/// ID 映射表 (data 和 executor 内部持有)
/// engine/risk 永远只用 Symbol, 不感知底层 token_id
pub struct SymbolRegistry {
    configs: HashMap<Symbol, MarketConfig>,
    poly_token_to_symbol: HashMap<String, (Symbol, TokenSide)>,
    poly_condition_to_symbols: HashMap<String, Vec<Symbol>>,
    cex_symbol_to_symbol: HashMap<(Exchange, String), Symbol>,
}

impl SymbolRegistry {
    pub fn get_config(&self, symbol: &Symbol) -> Option<&MarketConfig>;
    pub fn get_poly_ids(&self, symbol: &Symbol) -> Option<&PolymarketIds>;
    pub fn get_cex_symbol(&self, symbol: &Symbol) -> Option<&str>;
    pub fn get_tick_sizes(&self, symbol: &Symbol) -> Option<(Decimal, Decimal)>;
    pub fn lookup_poly_asset(&self, asset_id: &str) -> Option<(Symbol, TokenSide)>;
    pub fn lookup_poly_condition(&self, condition_id: &str) -> Option<&[Symbol]>;
    pub fn lookup_cex_symbol(&self, exchange: Exchange, venue_symbol: &str) -> Option<&Symbol>;
}

// types/signal.rs
pub enum SignalStrength { Strong, Normal, Weak }
pub enum TokenSide { Yes, No }

pub struct ArbLeg {
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub quantity: PolyShares,
}

pub struct DmmQuoteState {
    pub symbol: Symbol,
    pub bid: Price,
    pub ask: Price,
    pub bid_qty: PolyShares,
    pub ask_qty: PolyShares,
    pub updated_at_ms: u64,
}

pub type DmmQuoteSlot = Option<DmmQuoteState>;  // None = clear/cancel outstanding quote intent

pub struct DmmQuoteUpdate {
    pub symbol: Symbol,
    pub next_state: DmmQuoteSlot,
}

pub enum ArbSignalAction {
    BasisLong {
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,  // = filled_poly_shares * delta_binary (NOT 1:1 notionally)
        delta: f64,
    },
    BasisShort {
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,
        delta: f64,
    },
    DeltaRebalance {
        cex_side: OrderSide,
        cex_qty_adjust: CexBaseQty,  // CEX 腿增减量
        new_delta: f64,
    },
    NegRiskArb { legs: Vec<ArbLeg> },
    ClosePosition { reason: String },
}

pub struct ArbSignalEvent {
    pub signal_id: String,
    pub symbol: Symbol,
    pub action: ArbSignalAction,
    pub strength: SignalStrength,
    pub basis_value: Option<Decimal>,
    pub z_score: Option<Decimal>,
    pub expected_pnl: UsdNotional,
    pub timestamp_ms: u64,
}

pub struct AlphaEngineOutput {
    pub dmm_updates: Vec<DmmQuoteUpdate>,
    pub arb_signals: Vec<ArbSignalEvent>,
}
```

### 3.5 跨模块事件

```rust
// event.rs — 全部使用 Symbol 作为业务键
pub enum MarketDataEvent {
    OrderBookUpdate { snapshot: OrderBookSnapshot },
    TradeUpdate {
        exchange: Exchange,
        symbol: Symbol,
        instrument: InstrumentKind,
        price: Price,
        quantity: VenueQuantity,
        side: OrderSide,
        timestamp_ms: u64,
    },
    FundingRate {
        exchange: Exchange,
        symbol: Symbol,
        rate: Decimal,
        next_funding_time_ms: u64,
    },
    MarketLifecycle {
        symbol: Symbol,
        phase: MarketPhase,
        timestamp_ms: u64,
    },
    ConnectionEvent { exchange: Exchange, status: ConnectionStatus },
}

pub enum ExecutionEvent {
    OrderSubmitted { symbol: Symbol, exchange: Exchange, response: OrderResponse },
    OrderFilled(Fill),
    OrderCancelled { symbol: Symbol, order_id: OrderId, exchange: Exchange },
    HedgeStateChanged {
        symbol: Symbol,
        session_id: Uuid,
        old_state: HedgeState,
        new_state: HedgeState,
        timestamp_ms: u64,
    },
    ReconcileRequired { symbol: Option<Symbol>, reason: String },
}

pub enum SystemCommand {
    Shutdown,
    PauseStrategy(Symbol),
    ResumeStrategy(Symbol),
    TriggerCircuitBreaker(String),
    ResetCircuitBreaker,
}
```

### 3.6 Channel 容量与背压策略

```rust
// channel.rs

// ---- 容量常量 ----
pub const MARKET_DATA_CAPACITY: usize = 10240;   // broadcast: 高频行情, 宁大勿小
pub const ARB_SIGNAL_CAPACITY: usize = 64;       // mpsc: 套利信号, 有界 + 非阻塞
pub const EXECUTION_CAPACITY: usize = 256;       // mpsc: 执行事件, 不能丢
pub const COMMAND_CAPACITY: usize = 64;          // broadcast: 系统命令极少

// ---- 信号拆分: DMM 用 latest-wins, Arb 用 queue ----
pub struct DmmQuoteState {
    pub symbol: Symbol,
    pub bid: Price,
    pub ask: Price,
    pub bid_qty: PolyShares,
    pub ask_qty: PolyShares,
    pub updated_at_ms: u64,
}

pub enum ArbSignalAction {
    BasisLong {
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,
        delta: f64,
    },
    BasisShort {
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty,
        delta: f64,
    },
    DeltaRebalance {
        cex_side: OrderSide,
        cex_qty_adjust: CexBaseQty,
        new_delta: f64,
    },
    NegRiskArb { legs: Vec<ArbLeg> },
    ClosePosition { reason: String },
}

pub struct ArbSignalEvent {
    pub signal_id: String,
    pub symbol: Symbol,
    pub action: ArbSignalAction,
    pub strength: SignalStrength,
    pub basis_value: Option<Decimal>,
    pub z_score: Option<Decimal>,
    pub expected_pnl: UsdNotional,
    pub timestamp_ms: u64,
}

// ---- 类型别名 ----
pub type MarketDataTx = broadcast::Sender<MarketDataEvent>;       // 1-to-many
pub type DmmSignalTx = watch::Sender<Option<DmmQuoteState>>;      // per-symbol latest-wins
pub type ArbSignalTx = mpsc::Sender<ArbSignalEvent>;              // engine -> executor
pub type ExecutionTx = mpsc::Sender<ExecutionEvent>;              // executor -> risk
pub type RiskStateTx = watch::Sender<RiskStateSnapshot>;          // risk -> all (最新状态语义)
pub type CommandTx = broadcast::Sender<SystemCommand>;            // cli -> all

// ---- 工厂函数 ----
pub fn create_channels(symbols: &[Symbol]) -> ChannelBundle {
    let (market_data_tx, _) = broadcast::channel(MARKET_DATA_CAPACITY);
    let dmm_channels = symbols
        .iter()
        .map(|symbol| {
            let (tx, rx) = watch::channel(None);
            (symbol.clone(), (tx, rx))
        })
        .collect::<HashMap<Symbol, (DmmSignalTx, watch::Receiver<Option<DmmQuoteState>>)>>();
    let (arb_signal_tx, arb_signal_rx) = mpsc::channel(ARB_SIGNAL_CAPACITY);
    let (execution_tx, execution_rx) = mpsc::channel(EXECUTION_CAPACITY);
    let (risk_state_tx, risk_state_rx) = watch::channel(RiskStateSnapshot::default());
    let (command_tx, _) = broadcast::channel(COMMAND_CAPACITY);

    ChannelBundle { market_data_tx, dmm_channels, arb_signal_tx, arb_signal_rx, /* ... */ }
}
```

**背压处理规则:**

| Channel | 类型 | 满时行为 |
|---------|------|----------|
| market_data | broadcast(10240) | 消费者 lag 时收到 `RecvError::Lagged(n)`, **跳过旧数据继续** |
| dmm_signal | watch(per-symbol) | **自动覆盖旧值**，只保留最新报价 |
| arb_signal | mpsc(64) | **try_send, 满了丢弃并告警**，不阻塞 engine |
| execution | mpsc(256) | sender 阻塞等待 (risk 必须收到每个 fill) |
| risk_state | watch | 自动保留最新值, 无背压问题 |
| command | broadcast(64) | 基本不可能满, 命令极少 |

**关键设计决策:**
- DMM Quote 用 `watch`: 做市只关心“当前应该挂什么价”，过时报价没有价值。
- Arb Signal 用 `try_send`: 套利信号拥塞时宁可丢旧机会，也不能把单线程 engine 卡死。
- Execution 仍用阻塞 `mpsc`: fill / cancel / state transition 不能丢。

```rust
// DMM Quote: latest-wins
dmm_tx.send(Some(DmmQuoteState { /* ... */ })).ok();

// Arb Signal: try_send, 满了就丢弃并告警
match arb_signal_tx.try_send(ArbSignalEvent { /* ... */ }) {
    Ok(()) => {}
    Err(mpsc::error::TrySendError::Full(_)) => {
        tracing::warn!("[ENGINE] arb signal queue full, dropping stale signal");
    }
    Err(mpsc::error::TrySendError::Closed(_)) => {
        tracing::error!("[ENGINE] arb signal channel closed");
        break;
    }
}
```

```rust
// Engine 中处理 broadcast lag 的模式:
loop {
    match market_data_rx.recv().await {
        Ok(event) => { /* 正常处理 */ }
        Err(broadcast::error::RecvError::Lagged(n)) => {
            tracing::warn!("[ENGINE] Lagged {n} market data events, skipping stale data");
            continue;  // 跳过, 等下一条新数据
        }
        Err(broadcast::error::RecvError::Closed) => break,
    }
}
```

**设计决策: watch 而非 Arc\<RwLock\>**

risk_state 使用 `watch::channel` 而非 `Arc<RwLock<RiskStateSnapshot>>`:
- Polymarket 跑在 Polygon 链上, 出块 2 秒, 撮合确认百毫秒级
- watch 的 1ms 延迟对当前套利频次完全够用
- Arc\<RwLock\> 会引入锁竞争风险, 在 tokio 中容易导致线程死锁
- watch 保持纯事件驱动架构, 无锁

```rust
pub struct RiskStateSnapshot {
    pub circuit_breaker: CircuitBreakerStatus,
    pub total_exposure_usd: UsdNotional,
    pub positions: HashMap<PositionKey, Position>,
    pub daily_pnl: UsdNotional,
    pub max_drawdown_pct: Decimal,
    pub persistence_lag_secs: u64,
    pub timestamp_ms: u64,
}
```

### 3.7 核心 Traits

```rust
// traits/data_source.rs
#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn subscribe_market(&self, symbol: &Symbol) -> Result<()>;
    fn connection_status(&self) -> ConnectionStatus;
    fn exchange(&self) -> Exchange;
}

// traits/alpha.rs
#[async_trait]
pub trait AlphaEngine: Send + Sync {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> AlphaEngineOutput;
    fn update_params(&mut self, params: EngineParams);
}

// traits/executor.rs
#[async_trait]
pub trait OrderExecutor: Send + Sync {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse>;
    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()>;
    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32>;
    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse>;
}

// traits/risk.rs
#[async_trait]
pub trait RiskManager: Send + Sync {
    async fn pre_trade_check(&self, request: OrderRequest) -> Result<OrderRequest, RiskRejection>;
    async fn on_fill(&mut self, fill: &Fill) -> Result<()>;
    fn circuit_breaker_status(&self) -> CircuitBreakerStatus;
    fn trigger_circuit_breaker(&mut self, reason: &str);
}
```

---

## 4. 核心策略算法

### 4.1 基差对冲 (Basis Hedge) — Delta Neutral

**核心认知: 二元期权 vs 永续合约不能 1:1 对冲**

Polymarket 卖的是二元期权 (Binary Option, 结算 0 或 1)，CEX 的永续合约是线性衍生品。
例: "BTC 突破 100k" 的 YES token 在 BTC=90k 时价格 0.35，如果 BTC 涨到 91k，
YES 价格可能只涨到 0.38。这个 0.03 的变化对应 1000 美元的 BTC 变动 — 这就是 Delta。

**Delta 计算 (Binary Cash-or-Nothing Option, 简单 BS, 不引入 Merton):**
```
// engine 内部全部用 f64 计算
// 不引入资金费率作为连续股息率 (Merton), 因为:
// 1. Polymarket 事件通常短期, funding rate 对 delta 斜率影响 <2%
// 2. Merton 偏导数公式复杂一倍, Agent 容易写错
// 资金费率仅在 Alpha 绝对值中扣除成本, 不影响 Delta 计算

d2 = (ln(S/K) + (-sigma^2/2) * T) / (sigma * sqrt(T))
P_cex_implied = normal_cdf(d2)

// Binary option Delta = dP/dS (单个 share 对底层价格的敏感度)
delta_binary = normal_pdf(d2) / (S * sigma * sqrt(T))

// 对冲数量计算
// delta_binary 的量纲已经是 "CEX base qty per Poly share"
hedge_qty_cex = filled_poly_shares * delta_binary
// 例: 实际成交 1000 shares 的 YES token, delta=0.0001
//     -> 在 CEX 做空 0.1 BTC
```

**Delta 动态调整 (Gamma Risk):**
```
// delta 不是常数, 随 S/T/sigma 变化, 需要定期 rebalance
// Gamma = d(delta)/dS, 当 S 接近 K 时 Gamma 最大
gamma_binary = -d2 * normal_pdf(d2) / (S^2 * sigma^2 * T)

// Rebalance 触发条件:
// 1. |current_delta - hedged_delta| > delta_threshold (如 0.05)
// 2. 定时 rebalance (如每 60 秒)
// 3. BTC 价格变动 > price_threshold (如 0.5%)
```

**Alpha 公式 (修正版):**
```
basis = P_poly - P_cex_implied
cost = gas_cost + taker_fee + |funding_rate * holding_periods| + gamma_rebalance_cost
Alpha = |basis| - cost
```

**Z-Score 信号 + Delta 对冲量:**
```
z_score = (current_basis - rolling_mean) / rolling_std

if z_score > entry_threshold AND alpha > 0:
    signal = BasisShort {
        poly_side: Sell YES,
        cex_side: Buy (Long),
        poly_target_shares: shares_from_position_sizing,
        poly_target_notional: usd_notional_from_position_sizing,
        cex_hedge_qty: poly_target_shares * delta_binary,  // <-- 关键: 按 shares 对冲, 不是按 notional
        delta: delta_binary,
    }
```

**套利信号结构必须携带 Delta 信息:**
```rust
pub enum ArbSignalAction {
    BasisLong {
        poly_side: OrderSide,
        poly_target_shares: PolyShares,
        poly_target_notional: UsdNotional,
        cex_side: OrderSide,
        cex_hedge_qty: CexBaseQty, // = filled_poly_shares * delta_binary
        delta: f64,                // 当前 delta 值, executor 记录用于 rebalance
    },
    BasisShort { /* 同上 */ },
    DeltaRebalance {               // 新增: 仅调整 CEX 腿, 不动 Poly 腿
        cex_side: OrderSide,
        cex_qty_adjust: CexBaseQty,  // 需要增减的 CEX 数量
        new_delta: f64,
    },
    NegRiskArb { legs: Vec<ArbLeg> },
    ClosePosition { reason: String },
}

pub struct ArbLeg {
    pub symbol: Symbol,
    pub token_side: TokenSide,  // YES or NO
    pub side: OrderSide,
    pub quantity: PolyShares,
}
```

`DMM` 报价不再混在套利信号枚举里，而是通过 `DmmQuoteUpdate { symbol, next_state }` 单独下发到 per-symbol `watch` channel。

**执行边界规则:**
- engine 负责先得到目标 `UsdNotional`，再结合 Poly 当前价格计算目标 `PolyShares`。
- executor 在 Poly 市价单上优先使用 `poly_target_notional`，在限价单/撤改单路径使用 `poly_target_shares`。
- risk 对 Basis session 的真实对冲量一律使用**已成交 Poly shares**回推，不使用目标 notional 做假设。

### 4.2 波动率来源与预热期 (Warm-up)

**波动率来源: CEX 永续合约 1 小时滚动历史波动率 (HV)**
```
// 数据源: Binance/OKX 永续 mid-price, 每秒采样
// 滚动窗口: 3600 秒 (1 小时)
// 计算: log return 的标准差, 年化

returns[i] = ln(price[i] / price[i-1])
sigma_1s = std(returns, window=3600)
sigma_annualized = sigma_1s * sqrt(365 * 24 * 3600)
```

**预热期 (Warm-up) — 系统启动后的安全守卫:**
```
const MIN_SAMPLES: usize = 600;  // 至少 10 分钟数据才开始算
const MIN_WARMUP_SECS: u64 = 600;

impl RollingWindow {
    fn is_warm(&self) -> bool {
        self.count >= MIN_SAMPLES
    }
}

// Engine 主循环中:
if !volatility_window.is_warm() || !basis_window.is_warm() {
    tracing::info!("Warming up: {}/{} samples", window.count, MIN_SAMPLES);
    return AlphaEngineOutput::default();  // 预热期不产出任何信号, 只收集数据
}
```

**预热期行为:**
- 系统启动后, engine 只接收行情、填充 RollingWindow, 不产出任何 `AlphaEngineOutput`（`dmm_updates/arb_signals` 都为空）
- 日志每 10 秒打印一次预热进度 (如 "Warming up: 342/600 samples")
- 预热完成后打印 "[ENGINE] Warm-up complete, starting signal generation"
- DMM 策略同样需要预热: sigma 未就绪时不挂单

### 4.3 动态做市 (Avellaneda-Stoikov)

**核心公式 (f64 全程):**
```
reservation_price = s - q * gamma * sigma^2 * (T - t)
optimal_spread = gamma * sigma^2 * (T - t) + (2/gamma) * ln(1 + gamma/k)

bid = reservation_price - optimal_spread / 2
ask = reservation_price + optimal_spread / 2

Clamp to [min_tick, 1.0 - min_tick], 对齐 tick size

参数:
  s = Poly YES token mid price
  q = 当前库存 (正=多, 负=空)
  gamma = 风险厌恶系数 (config, 默认 0.1)
  sigma = 波动率估计
  T-t = 距结算剩余时间
  k = 订单到达强度
```

**多层报价:** 在 optimal bid/ask 基础上每层偏移 `level_spacing_bps`，越远层数量越大。

### 4.4 NegRisk 组合套利

```
sum_ask = sum(best_ask[i] for i in all_outcomes)
sum_bid = sum(best_bid[i] for i in all_outcomes)

if sum_ask < 1.0 - total_fees:  买入所有 YES tokens, 锁定利润
```

**V1 范围收缩:**
- 只实现 **buy-side complete-set arb**，即 `sum_ask < 1.0 - total_fees` 时买入所有结果腿。
- `sum_bid > 1.0 + total_fees` 的 sell-side 路径默认**不实现**，除非后续补充 inventory-backed close、mint/redeem 或 borrow 机制。
- 这样 NegRisk 从 flat 状态出发始终有可执行路径，不会出现“策略层能发信号，执行层无货可卖”的断层。

---

## 5. 对冲状态机 (Hedge FSM) — N-Leg 通用设计

### 5.1 双腿模式 (Basis Hedge: 1 Poly + 1 CEX)

```
    IDLE --[signal]--> POLY_PENDING --[poly fill]--> HEDGING --[cex fill]--> HEDGED
      ^                    |                           |                       |
      |              [timeout/cancel]           [cex fail:                [close signal
      |                    |                  panic close poly]           or rebalance]
      +--------------------+-------------------+                              |
                                                                          CLOSING
                                                                             |
                                                                     [all legs closed]
                                                                             |
                                                                        --> IDLE
```

### 5.2 N-Leg 模式 (NegRisk: 全 Poly 内部腿)

```
    IDLE --[negrisk signal]--> SUBMITTING_LEGS --[all filled]--> LOCKED
      ^                             |                               |
      |                   [any leg timeout:                    [settle or
      |                    cancel all,                          close signal]
      |                    unwind filled]                           |
      |                             |                          UNWINDING
      +-----------------------------+-------- [all unwound] -------+
```

### 5.3 通用 HedgeSession 结构

```rust
pub struct HedgeSession {
    pub session_id: Uuid,
    pub symbol: Symbol,
    pub session_type: SessionType,
    pub state: HedgeState,
    pub market_phase: MarketPhase,
    pub entry_delta: Option<f64>,
    pub hedged_delta: Option<f64>,
    pub legs: Vec<LegInfo>,            // N-Leg 通用, 不再硬编码 poly_leg/cex_leg
    pub created_at_ms: u64,            // 可持久化
    pub updated_at_ms: u64,            // 可持久化
    pub state_deadline_ms: u64,        // 当前状态的超时时间点
}

pub enum SessionType {
    BasisHedge,       // 2 legs: 1 poly + 1 cex
    DeltaRebalance,   // 1 leg: 仅 cex 调仓
    NegRiskArb,       // N legs: 全部 poly
}

pub struct LegInfo {
    pub leg_id: Uuid,
    pub exchange: Exchange,
    pub symbol: Symbol,              // 对应的 Symbol (NegRisk 中各 leg 可能不同)
    pub instrument: InstrumentKind,
    pub token_side: Option<TokenSide>,
    pub side: OrderSide,
    pub target_qty: VenueQuantity,
    pub filled_qty: VenueQuantity,
    pub filled_notional: UsdNotional,
    pub filled_price: Option<Price>,
    pub order_id: Option<OrderId>,
    pub status: LegStatus,
}

pub enum LegStatus {
    Pending,       // 尚未提交
    Submitted,     // 已提交, 等待成交
    PartialFill,   // 部分成交
    Filled,        // 全部成交
    Cancelled,     // 已撤销
    Failed,        // 失败
}

pub enum HedgeState {
    Idle,
    SubmittingLegs,  // 正在提交各腿订单
    Hedging,         // 部分腿已成交, 等待剩余腿
    Hedged,          // 所有腿已到位 (basis) 或锁定 (negrisk)
    Rebalancing,     // Delta rebalance 中
    Closing,         // 正在平仓/解锁
}

/// 仅 runtime 本地使用的单调时钟状态，不写入 SQLite
pub struct RuntimeHedgeSession {
    pub persisted: HedgeSession,
    pub state_entered_at: Instant,
}
```

### 5.4 状态转换规则

**BasisHedge (2-leg) — 含 Partial Fill 比例追单:**
- `Idle -> SubmittingLegs`: 收到 BasisLong/BasisShort 信号, 先提交 Poly leg
- `SubmittingLegs -> Hedging`: Poly leg filled (全部或部分), 立即按**实际成交 shares**提交 CEX leg:
  ```
  // 目标 1000 shares, 实际成交 600 shares
  poly_filled_shares = sum(fill.shares)
  cex_target_qty = poly_filled_shares * session.entry_delta
  cex_increment = cex_target_qty - already_hedged_qty
  // 再对齐 CEX step_size 后下单
  ```
- `Hedging -> Hedged`: CEX leg filled
- `Hedged -> Rebalancing`: DeltaRebalance 信号, 调整 CEX 腿数量
- `Hedged -> Closing`: ClosePosition 信号, 提交两腿反向平仓单
- **Poly 持续部分成交**: 如果 Poly leg 收到多次 partial fill, 每次都增量追加 CEX hedge:
  ```
  // 第一次 fill: 300 shares -> 先对冲 300 * delta
  // 第二次 fill: 600 shares -> CEX 追加到 600 * delta
  cex_increment = (new_poly_filled_shares - prev_poly_filled_shares) * session.entry_delta
  ```
- **超时处理**:
  - `SubmittingLegs` 超时 -> 撤 Poly 单, 对已成交部分提交反向平仓 -> `Idle`
  - `Hedging` 超时 -> 先尝试 reduce-only 平掉已成交 CEX 腿，再处置剩余 Poly 风险；若市场已进入 `CloseOnly` / `Disputed`，禁止继续补开新腿
- **恢复规则**: session 从 SQLite 恢复后**不能直接相信本地状态**，必须先与 venue live positions/open orders reconcile，再决定进入 `Hedged` / `Closing` / `Idle`

**NegRiskArb (N-leg) — 含 Partial Fill 处理:**
- `Idle -> SubmittingLegs`: 收到 NegRiskArb 信号, 仅支持 buy-side complete-set，按深度最浅的 leg 先下单
- `SubmittingLegs -> Hedged`: 所有 legs filled (locked)
- **部分成交处理**: NegRisk 套利要求所有腿**等量成交**才能锁定利润:
  - 取所有腿的 `min(filled_qty)` 作为有效锁定量
  - 超出 min 的部分视为多余敞口, 需要尝试撤单或反向平仓
  - 如果某个 leg 超时, 撤销未成交 legs, 对已成交 legs 尝试反向平仓

---

## 6. 风控系统

### 6.1 Pre-Trade 检查链 (按序, 纯内存, 零 IO)

所有风控检查在内存中完成，不涉及任何数据库读写:

1. 熔断器状态检查 (`Closed` 或 `HalfOpen`)
2. 市场生命周期检查 (`Trading` / `PreSettlement` / `ForceReduce` / `CloseOnly` / `Disputed`)
3. 持久化健康检查 (`last_successful_flush <= max_persistence_lag_secs`)
4. 单位归一检查: `OrderRequest` 必须携带合法单位，禁止裸 Decimal 透传
5. 单仓位上限: `mark_to_market(position + new_order) <= max_single_position_usd`
6. 总暴露上限: `total_exposure_usd + new_order_usd <= max_total_exposure_usd`
7. 日亏损限制: `daily_loss < max_daily_loss_usd`
8. 最大回撤: `drawdown < max_drawdown_pct`
9. 挂单数量: `open_orders < max_open_orders`
10. 频率限制: `orders/sec < rate_limit`
11. **精度对齐**: 通过 `PrecisionNormalizer` 对 price/qty 做 tick_size/step_size floor；若 floor 后为 0，则直接拒单

### 6.2 熔断器 (Circuit Breaker)

```
CLOSED (正常) --[触发条件]--> OPEN (拒绝新单, 仅允许平仓)
                               |
                          [cooldown到期]
                               |
                          HALF_OPEN (允许 1/4 size)
                               |
                   [M次成功] --> CLOSED
                   [失败]    --> OPEN

触发条件:
  - daily_loss > max_daily_loss_usd
  - drawdown > max_drawdown_pct
  - 连续 N 次执行失败
  - UMA 预言机争议事件
```

### 6.3 持久化与恢复策略: 纯内存风控 + 单 writer 异步刷盘 + Live Reconcile

**设计原则:** 交易关键路径 (fill -> position update -> pre_trade check) 绝不等待磁盘 IO。
SQLite 仅用于**审计、warm start 提示、事后复盘**，不再被视为唯一真相源。
系统重启后的权威状态必须来自 **Polymarket + Binance/OKX 的 live positions / open orders / balances**。

```
[Risk Manager 主循环]
    |
    |---> PositionTracker (HashMap, 纯内存)
    |---> PnlCalculator (纯内存)
    |---> CircuitBreaker (纯内存)
    |
    |---> flush_buffer: Vec<PersistenceCommand>  // 累积待刷盘的事件
    |
    +---> 每 1 秒 或 buffer 满 100 条:
          try_send(flush_buffer) 到 AsyncPersistence writer task
          // 单个长生命周期 writer，保证顺序写入
          // 写失败或 lag 过长会触发 persistence-degraded
```

```rust
// persistence 模块
pub enum PersistenceCommand {
    SaveFill(Fill),
    SavePositionSnapshot(Vec<Position>),
    SaveArbSignal(ArbSignalEvent),
    SaveHedgeSession(HedgeSession),
}

pub struct AsyncPersistence {
    tx: mpsc::Sender<Vec<PersistenceCommand>>,  // 后台 writer task 接收端
    pub last_successful_flush_ms: Arc<AtomicU64>,
}

impl AsyncPersistence {
    /// 启动单个后台 writer task
    pub async fn start(db_path: &str) -> Result<Self>;

    /// 非阻塞: 把 commands 发到 channel, 立即返回
    pub fn flush(&self, commands: Vec<PersistenceCommand>) -> Result<(), PersistenceLag>;
}
```

**启动恢复流程:**
1. 系统启动时, 先从 SQLite 读取最近的 `position_snapshots` / `hedge_sessions` 作为 warm-start 提示
2. 立刻向 Polymarket / Binance / OKX 拉取 live positions、open orders、balances、最近 fills
3. 用 live state 重建内存仓位与 session；SQLite 仅用于补全上下文，不得覆盖 live truth
4. 若出现 orphan position、unknown open order、持仓方向不一致、session 无法重建:
   - affected symbol 进入 `CloseOnly`
   - 触发 `ExecutionEvent::ReconcileRequired`
   - 默认不恢复新开仓交易
5. 只有 live reconcile 通过后，系统才从 warm-up / safe mode 进入正常交易

**持久化降级规则:**
- SQLite 短暂失败: 记日志并继续重试
- `last_successful_flush_ms` 落后超过 `max_persistence_lag_secs`: 打开 breaker，拒绝新开仓，只允许对冲与平仓
- 持久化恢复后，breaker 可进入 `HalfOpen`

SQLite 表结构大体保持不变，但所有写入通过 `AsyncPersistence` 单 writer 顺序批量完成。

### 6.4 结算 / 争议期规则

**市场生命周期守卫:**
- `[strategy.settlement]` 定义统一阶段规则:
  - `stop_new_position_hours`
  - `force_reduce_hours`
  - `force_reduce_target_ratio`
  - `close_only_hours`
  - `emergency_close_hours`
  - `dispute_close_only`
- `hours_remaining > stop_new_position_hours`: `Trading`
  - 允许正常 Basis / DMM / NegRisk
- `force_reduce_hours < hours_remaining <= stop_new_position_hours`: `PreSettlement`
  - 禁止新开 Basis / NegRisk
  - DMM 可选择继续，但只保留更保守的库存限额
- `close_only_hours < hours_remaining <= force_reduce_hours`: `ForceReduce`
  - 主动缩减仓位到 `force_reduce_target_ratio`
  - 禁止任何增加净风险的 rebalance
- `0 < hours_remaining <= close_only_hours`: `CloseOnly`
  - 取消 DMM 报价，不再刷新挂单
  - 只允许减仓、撤单、CEX 对冲腿回补
- `now >= settlement_timestamp` 且结果尚未最终确认: `SettlementPending`
  - 停止自动交易
  - 保留仓位核对、结算入账检查、异常告警
- `UMA dispute / market paused / resolution pending`: `Disputed`
  - 该 symbol breaker 直接 `Open`
  - 不允许新单；只保留手动确认后的风险处置
- `Resolved`: 停止交易，等待结算现金流入账并核对残余仓位

---

## 7. 配置管理

TOML 配置，环境变量覆盖敏感字段。使用 `config` crate 实现层叠加载: `default.toml` -> `{env}.toml` -> 环境变量。

```toml
# config/default.toml

[general]
log_level = "info"
data_dir = "./data"

[polymarket]
clob_api_url = "https://clob.polymarket.com"
ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
chain_id = 137
# 敏感字段必须通过环境变量: POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE

[binance]
rest_url = "https://fapi.binance.com"
ws_url = "wss://fstream.binance.com"
# 环境变量: BINANCE_API_KEY, BINANCE_API_SECRET

[okx]
rest_url = "https://www.okx.com"
ws_public_url = "wss://ws.okx.com:8443/ws/v5/public"
ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"
# 环境变量: OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE

[[markets]]
symbol = "btc-100k-mar-2026"
condition_id = "0x..."
yes_token_id = "0x..."
no_token_id = "0x..."
settlement_timestamp = 1775001600   # 2026-04-01 00:00:00 UTC
min_tick_size = 0.01
neg_risk = false
cex_symbol = "BTCUSDT"
hedge_exchange = "Binance"
strike_price = 100000
cex_price_tick = 0.1
cex_qty_step = 0.001
cex_contract_multiplier = 1.0
strategy = "Combined"

[strategy.basis]
entry_z_score_threshold = 2.0
exit_z_score_threshold = 0.5
rolling_window_secs = 3600
min_warmup_samples = 600            # 至少 600 个样本 (10 分钟) 才开始产出信号
min_basis_bps = 50.0
max_position_usd = 10000.0
# hedge_ratio 已废弃, 改用 Delta 动态计算
delta_rebalance_threshold = 0.05   # |current_delta - hedged_delta| > 此值触发 rebalance
delta_rebalance_interval_secs = 60 # 或定时 rebalance

[strategy.dmm]
gamma = 0.1
sigma_window_secs = 300
max_inventory = 5000.0
order_refresh_secs = 10
num_levels = 3
level_spacing_bps = 10.0
min_spread_bps = 20.0

[strategy.negrisk]
min_arb_bps = 30.0
max_legs = 8
enable_inventory_backed_short = false

[strategy.settlement]
stop_new_position_hours = 24
force_reduce_hours = 12
force_reduce_target_ratio = 0.5
close_only_hours = 6
emergency_close_hours = 1
dispute_close_only = true

[risk]
max_total_exposure_usd = 50000.0
max_single_position_usd = 10000.0
max_daily_loss_usd = 2000.0
max_drawdown_pct = 5.0
max_open_orders = 50
circuit_breaker_cooldown_secs = 300
rate_limit_orders_per_sec = 5
max_persistence_lag_secs = 10
```

---

## 8. Polymarket 官方 Rust SDK 集成

**使用官方 SDK `polymarket-client-sdk` v0.4.3** (https://github.com/Polymarket/rs-clob-client)

这是 Polymarket 官方维护的 Rust SDK，覆盖了 REST API、WebSocket、EIP-712 签名、认证的全部功能。
**无需手写 Polymarket 的 REST client、WS client、签名逻辑、API 认证**。

### 8.1 SDK 功能清单

| Feature Flag | 功能 |
|-------------|------|
| `clob` | CLOB REST API (订单、交易、市场、余额) |
| `ws` | WebSocket 实时流 (orderbook, prices, midpoints, user events) |
| `data` | Data API |
| `gamma` | Gamma API |
| `bridge` | Bridge API |
| `rfq` | RFQ API |
| `tracing` | 结构化日志集成 |
| `heartbeats` | 连接保活 |
| `ctf` | CTF 合约交互 |

### 8.2 核心用法示例

```rust
// 认证客户端
let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
let client = Client::new("https://clob.polymarket.com", Config::default())?
    .authentication_builder(&signer)
    .authenticate()
    .await?;

// 下单 (Market Order)
let order = client
    .market_order()
    .token_id("<token-id>")
    .amount(Amount::usdc(Decimal::ONE_HUNDRED)?)
    .side(Side::Buy)
    .order_type(OrderType::FOK)
    .build()
    .await?;
let signed_order = client.sign(&signer, order).await?;
let response = client.post_order(signed_order).await?;

// WebSocket 订阅 Orderbook
let asset_ids = vec!["<asset-id>".to_owned()];
let stream = client.subscribe_orderbook(asset_ids)?;
let mut stream = Box::pin(stream);
while let Some(book_result) = stream.next().await {
    let book = book_result?;
    // book.asset_id, book.bids, book.asks
}
```

### 8.3 SDK 已包含的依赖 (无需重复引入)

`alloy` (签名), `rust_decimal`, `chrono`, `uuid`, `reqwest`, `futures`, `url`, `secrecy`

### 8.4 对架构的影响

由于官方 SDK 已封装全部 Polymarket 交互逻辑:
- `polyalpha-data` 中的 `polymarket/` 子模块简化为 SDK 的薄封装，只做数据归一化
- `polyalpha-executor` 中的 `polymarket/` 子模块无需手写签名和订单构建，直接调用 SDK
- **删除原计划中的**: `signer.rs`, `order_builder.rs`, `clob_client.rs` (均由 SDK 提供)
- Binance 和 OKX 仍需手写客户端 (它们没有统一的 Rust SDK)

---

## 9. 外部依赖清单

| Crate | 版本 | 用途 | 使用者 |
|-------|------|------|--------|
| tokio | 1.x | 异步运行时 (features: full) | all |
| polymarket-client-sdk | 0.4.x | Polymarket 官方 SDK (features: clob,ws,tracing) | data, executor |
| tokio-tungstenite | 0.24.x | WebSocket 客户端 (Binance/OKX) | data |
| serde + serde_json | 1.x | 序列化 | all |
| rust_decimal | 1.x | 精确小数 (features: serde) | core |
| alloy | 0.9.x | EIP-712 签名 + Polygon | executor |
| reqwest | 0.12.x | HTTP (features: json, rustls-tls) | data, executor |
| tracing + tracing-subscriber | 0.1/0.3 | 结构化日志 | all |
| clap | 4.x | CLI (features: derive) | cli |
| config | 0.14.x | 配置管理 | core, cli |
| sqlx | 0.8.x | SQLite (features: runtime-tokio, sqlite) | risk |
| thiserror | 2.x | 错误定义 | core |
| anyhow | 1.x | 应用级错误 | cli |
| chrono | 0.4.x | 时间处理 | core |
| uuid | 1.x | ID 生成 (features: v4) | core |
| hmac + sha2 | 0.12/0.10 | HMAC-SHA256 签名 | data, executor |
| base64 | 0.22.x | Base64 编解码 | data, executor |
| futures-util | 0.3.x | Stream 工具 | data |
| async-trait | 0.1.x | async trait | core |
| parking_lot | 0.12.x | 高性能锁 | data, risk |
| statrs | 0.17.x | 统计函数 (正态 CDF) | engine |

---

## 10. Agent 任务拆分

### Agent A: polyalpha-core + polyalpha-data (最高优先级)

**职责:** 基础类型定义 + 数据采集层

**任务:**
1. 创建 workspace root `Cargo.toml` 和所有 crate 骨架
2. 实现 `polyalpha-core` 全部类型 (**单位安全体系**: `UsdNotional` / `PolyShares` / `CexBaseQty` + `VenueQuantity`)
3. 实现 `Symbol` + `SymbolRegistry` + `MarketConfig` (**统一 ID + 正反向映射**)
4. 实现 traits、config、error、event、channel
5. 基于官方 SDK `polymarket-client-sdk` 封装 Poly WS 流 (stream.rs + normalizer.rs)
6. 实现 Binance Futures WebSocket 客户端 (深度 + 资金费率)
7. 实现 OKX WebSocket 客户端 (深度 + 资金费率)
8. 实现 L2OrderBook (BTreeMap, 增量更新, sequence 校验)
9. 实现 DataManager (启动所有 WS, 广播 MarketDataEvent)
10. 创建 `config/default.toml` (含 `cex_price_tick` / `cex_qty_step` / `cex_contract_multiplier` / `strategy.settlement`)
11. 单元测试: orderbook 更新、normalizer、SymbolRegistry

**关键:** core 必须先完成，其他 Agent 全依赖它。

### Agent B: polyalpha-engine

**职责:** 策略计算引擎 (内部全 f64 计算)

**任务:**
1. 实现 RollingWindow 泛型容器 (VecDeque + Welford 增量统计, f64)
2. 实现波动率估计 (从 CEX 价格序列, f64)
3. 实现 Poly 隐含概率提取 (加权 mid-price)
4. 实现 CEX 隐含概率推算 (Black-Scholes, f64 全程)
5. **实现 Binary Option Delta 计算** (delta_binary = pdf(d2) / (S * sigma * sqrt(T)))
6. **实现 Gamma 计算 + Rebalance 触发逻辑**
7. 实现 Basis Z-Score 计算
8. 实现 BasisSignal 生成 (输出 `poly_target_notional` + `poly_target_shares` + `cex_hedge_qty`，其中 `cex_hedge_qty = filled_poly_shares * delta_binary`)
9. 实现 Avellaneda-Stoikov DMM 定价 + 多层报价 (通过 `watch` latest-wins 下发 DMM state)
10. 实现 NegRisk Bundle 套利检测 (**V1 仅 buy-side complete-set**)
11. 实现 MarketPhase / 结算窗口守卫 (`Trading` / `PreSettlement` / `ForceReduce` / `CloseOnly`)
12. 实现 AlphaEngine 主循环 (**单线程顺序 for 循环**, DMM 用 `watch`, Arb 用 `try_send`)
13. 单元测试: 数值正确性 (已知 S/K/T/sigma -> 预期 delta/P_implied/z_score)
14. 单元测试: **预热期守卫** (window.count < MIN_SAMPLES 时返回空信号)
15. 单元测试: settlement/dispute / close-only phase 下禁止新信号

**关键设计决策: 单线程 Engine, 不搞并发**
- 资金 $10k-$200k, 同时监控 10-20 个市场
- Rust 单线程一秒算几百万次 BS 公式, 远超需求
- Engine 内部一个 `for market in &self.markets` 循环遍历所有 market 算信号
- 避免 Actor 模型的过度设计和调试复杂度

### Agent C: polyalpha-executor

**职责:** 交易执行层

**任务:**
1. 基于官方 SDK 封装 Polymarket 执行器 (poly_executor.rs, 调用 SDK 的 sign + post_order)
2. 实现 Binance Futures REST 下单 (HMAC-SHA256 签名)
3. 实现 OKX Futures REST 下单 (HMAC-SHA256 签名)
4. **实现 PrecisionNormalizer** (precision.rs): 从 SymbolRegistry 读取 tick_size/step_size, floor 对齐，并在 floor 后为 0 时拒单
5. **实现 N-Leg HedgeStateMachine** (支持 BasisHedge 2-leg + NegRiskArb N-leg + DeltaRebalance 1-leg)
6. 实现 LegManager (按**已成交 Poly shares**推导对冲量；N-leg partial fill 处理；超时撤单+反向平仓)
7. 实现 ExecutionManager 主循环
8. 实现启动时的 live open-order / position reconcile hook
9. 实现 DryRunExecutor (模拟成交，用于测试)
10. 单元测试: 精度对齐 (0.1234 -> 0.12)、状态机全路径覆盖 (2-leg/N-leg/rebalance)

### Agent D: polyalpha-risk + polyalpha-cli

**职责:** 风控 + 系统集成

**任务:**
1. 实现 PositionTracker (**纯内存 HashMap**, 按 `PositionKey` 聚合，加仓平仓、加权均价)
2. 实现 PnL 计算 (realized/unrealized, drawdown, **纯内存**, 先 mark-to-market 到 `UsdNotional`)
3. 实现 Pre-Trade 检查链 (**纯内存, 零 IO**, 含市场生命周期、持久化健康、精度对齐步骤)
4. 实现 CircuitBreaker FSM
5. **实现 AsyncPersistence** (单 writer task + mpsc channel + 批量 batch insert SQLite)
6. 实现 RiskManager 主循环 (内存决策 + 异步刷盘分离)
7. **实现启动恢复逻辑** (SQLite warm-start + live reconcile)
8. 实现 CLI (clap derive)
9. 实现 Orchestrator (创建 channels, 启动模块, 优雅关闭)
10. 实现 tracing 日志配置
11. 编写集成测试 (mock WebSocket server)
12. Dockerfile

---

## 11. 错误处理与恢复策略

| 错误类型 | 恢复策略 |
|----------|----------|
| WebSocket 断连 | 自动重连 (指数退避, 初始 1s, 最大 30s, +jitter) |
| REST 请求失败 | 重试 3 次 (exponential backoff) |
| 签名错误 | 不重试, 记录错误, 跳过 |
| 订单被拒 | 记录, 通知 risk manager |
| Orderbook sequence gap | 重新获取 snapshot |
| Channel closed | 触发优雅关闭 |
| SQLite 错误 | 进入 persistence-degraded；若超过 `max_persistence_lag_secs`，拒绝新开仓，只允许平仓/对冲 |
| 启动状态不一致 | 执行 live reconcile；冲突 symbol 进入 `CloseOnly` |
| 熔断触发 | 停止新单, cooldown 后 half-open |

**Graceful Shutdown (Ctrl+C):**
1. 广播 `SystemCommand::Shutdown`
2. Engine 停止生成信号
3. Executor 撤销所有挂单 (`cancel_all`)
4. 等待活跃 HedgeSession 达终态或超时 (30s)
5. Risk 保存最终 position snapshot
6. 关闭所有 WebSocket
7. 退出

---

## 12. 验证计划

### 12.1 编译验证

```bash
cargo build --release
cargo clippy --all-targets -- -D warnings
cargo test --all
```

### 12.2 单元测试覆盖

- core: 单位安全类型 (`UsdNotional` / `PolyShares` / `CexBaseQty`) 运算、config 加载、registry 正反向查找
- data: orderbook 增量更新、sequence gap 检测、crossed book 检测、`asset_id -> Symbol` 映射
- engine: 隐含概率计算、Z-score、A-S 报价、NegRisk 检测、结算/争议期守卫、DMM `watch` / Arb `try_send`
- executor: EIP-712 签名 (与 Python SDK 输出对比)、状态机全路径、已成交 shares 驱动对冲量
- risk: 仓位计算、PnL、熔断器状态转换、pre-trade 拒绝、persistence lag 降级

### 12.3 集成测试

- mock WebSocket server 推送价格数据 -> DataManager -> Engine -> 验证信号输出
- DryRun 模式端到端: 启动系统 -> 收行情 -> 产信号 -> 模拟执行 -> 验证仓位
- 启动恢复集成测试: SQLite warm-start + venue live state -> reconcile -> 验证 `CloseOnly` / 正常恢复路径

### 12.4 实盘验证

1. `--dry-run` 模式运行，观察日志中的信号和模拟执行
2. 小资金 ($100) 实际下单，验证签名和执行正确性
3. 逐步加码至目标资金规模

### 12.5 关键文件清单

实现优先级最高的 5 个文件:
- `crates/polyalpha-core/src/event.rs` - 系统通信契约
- `crates/polyalpha-core/src/traits/executor.rs` - 执行接口定义
- `crates/polyalpha-engine/src/basis/basis_signal.rs` - 核心策略 alpha
- `crates/polyalpha-executor/src/hedge/state_machine.rs` - 对冲 FSM
- `crates/polyalpha-cli/src/orchestrator.rs` - 系统编排
