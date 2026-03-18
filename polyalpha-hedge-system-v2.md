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
            | broadcast::Sender              | watch (DMM) +                  | watch::Sender
            | (MarketDataEvent)              | mpsc (Arb)                     | (RiskStateSnapshot)
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
|   |       |   +-- signal.rs     # AlphaSignal, SignalStrength
|   |       |   +-- quantity.rs   # PolyShares, UsdNotional, PerpQty
|   |       |   +-- price.rs      # Price newtype wrapper
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

### 3.1 双精度体系: Decimal (账务) vs f64 (计算)

**设计原则:** `rust_decimal::Decimal` 用于所有与"钱"相关的场景（下单价格、数量、仓位、PnL），
`f64` 用于 engine 内部所有数学运算（ln, exp, sqrt, normal_cdf, A-S 公式等）。
`rust_decimal` 不支持 `ln()`/`exp()`/`sqrt()`，强行使用会导致编译错误或极低效的近似。

### 3.1.1 数量类型显式拆分 (关键设计决策)

**问题:** Polymarket shares、USDC notional、CEX 永续合约数量是**完全不同的单位**，混用会导致对冲数量计算错误。

| 类型          | 含义                          | 示例               | 使用场景                      |
| ------------- | ----------------------------- | ------------------ | ----------------------------- |
| `PolyShares`  | Polymarket outcome token 数量 | 1000 shares of YES | Poly 下单、Poly 仓位          |
| `UsdNotional` | USDC 名义金额                 | $500 USDC          | 风控暴露计算、position sizing |
| `PerpQty`     | CEX 永续合约 base asset 数量  | 0.05 BTC           | CEX 下单、CEX 仓位            |

```rust
// types/quantity.rs

use rust_decimal::Decimal;

/// Polymarket outcome token 数量 (YES/NO shares)
/// 在 Polymarket 上，1 share 结算时值 $0 或 $1
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PolyShares(pub Decimal);

/// USDC 名义金额 (用于风控、position sizing)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UsdNotional(pub Decimal);

/// CEX 永续合约 base asset 数量 (如 BTC 数量)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PerpQty(pub Decimal);

impl PolyShares {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }
    
    /// shares * price = notional (用于计算 USDC 暴露)
    pub fn to_notional(&self, price: Price) -> UsdNotional {
        UsdNotional(self.0 * price.0)
    }
}

impl UsdNotional {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }
    
    /// notional / price = shares (用于 position sizing)
    pub fn to_shares(&self, price: Price) -> PolyShares {
        if price.0.is_zero() { return PolyShares::ZERO; }
        PolyShares(self.0 / price.0)
    }
}

impl PerpQty {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }
    
    /// 对齐到 CEX step_size
    pub fn floor_to_step(&self, step_size: Decimal) -> Self {
        PerpQty((self.0 / step_size).floor() * step_size)
    }
}
```

**单位转换规则 (关键!):**

```rust
// Delta 对冲量计算的正确公式:
// 
// 输入:
//   poly_shares: PolyShares    -- Poly 腿的 share 数量
//   poly_price: Price          -- 当前 YES token 价格 (如 0.35)
//   cex_price: Price           -- BTC 当前价格 (如 $90,000)
//   delta: f64                 -- Binary option delta (如 0.0001)
//
// 步骤:
//   1. notional = poly_shares.to_notional(poly_price)  -> UsdNotional
//   2. hedge_usd = notional.0 * delta                   -> Decimal (USD value to hedge)
//   3. perp_qty = PerpQty(hedge_usd / cex_price.0)     -> PerpQty (BTC 数量)
//   4. perp_qty = perp_qty.floor_to_step(step_size)    -> 对齐精度

fn calc_hedge_qty(
    poly_shares: PolyShares,
    poly_price: Price,
    cex_price: Price,
    delta: f64,
    step_size: Decimal,
) -> PerpQty {
    let notional = poly_shares.to_notional(poly_price);
    let hedge_usd = Decimal::from_f64(notional.to_f64() * delta).unwrap_or(Decimal::ZERO);
    let raw_qty = PerpQty(hedge_usd / cex_price.0);
    raw_qty.floor_to_step(step_size)
}
```

### 3.1.2 Price 类型

```rust
// types/price.rs

/// 价格类型 (用于 orderbook、下单、记账)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl Price {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);
    
    /// Engine 内部计算时调用: Decimal -> f64
    pub fn to_f64(&self) -> f64 { self.0.to_f64().unwrap_or(0.0) }
    
    /// Engine 输出信号时调用: f64 -> Decimal, 对齐到 tick_size
    pub fn from_f64_rounded(v: f64, tick_size: Decimal) -> Self {
        let d = Decimal::from_f64(v).unwrap_or(Decimal::ZERO);
        Price((d / tick_size).floor() * tick_size)
    }
}
```

**边界规则:**
- `data` 层: 归一化为 `Price`, `PolyShares` (Poly), `PerpQty` (CEX)
- `engine` 层: 入口处 `.to_f64()` -> 内部全部 f64 运算 -> 出口处转回强类型
- `executor` 层: 收到的信号已是强类型，**不同交易所使用不同数量类型**
- `risk` 层: 使用 `UsdNotional` 计算暴露，`PolyShares`/`PerpQty` 追踪仓位

### 3.2 OrderBook

```rust
// types/orderbook.rs

/// 通用 PriceLevel (数量类型由 Exchange 决定)
/// Polymarket: quantity 实际为 PolyShares
/// CEX: quantity 实际为 PerpQty
/// 为简化跨模块传递，使用 Decimal 内部值
pub struct PriceLevel {
    pub price: Price,
    pub size: Decimal,  // 原始数量, 下游按 exchange 解释为 PolyShares 或 PerpQty
}

pub enum Side { Bid, Ask }

pub enum Exchange { Polymarket, Binance, Okx }

pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: Symbol,             // <-- 统一使用 Symbol, 不再用 String
    pub bids: Vec<PriceLevel>,      // 价格降序
    pub asks: Vec<PriceLevel>,      // 价格升序
    pub timestamp: u64,             // exchange timestamp (ms)
    pub local_timestamp: Instant,
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

/// Polymarket 订单请求 (数量为 shares)
pub struct PolyOrderRequest {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub token_side: TokenSide,      // YES or NO
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub quantity: PolyShares,       // <-- 显式类型
    pub time_in_force: TimeInForce,
}

/// CEX 永续合约订单请求 (数量为 base asset)
pub struct PerpOrderRequest {
    pub client_order_id: ClientOrderId,
    pub exchange: Exchange,         // Binance or Okx
    pub symbol: Symbol,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub quantity: PerpQty,          // <-- 显式类型
    pub time_in_force: TimeInForce,
    pub reduce_only: bool,
}

/// 统一订单响应 (数量仍用 Decimal, 按 exchange 解释)
pub struct OrderResponse {
    pub client_order_id: ClientOrderId,
    pub exchange: Exchange,
    pub exchange_order_id: OrderId,
    pub status: OrderStatus,
    pub filled_size: Decimal,       // 按 exchange 解释为 PolyShares 或 PerpQty
    pub average_price: Option<Price>,
    pub timestamp: u64,
}
```

### 3.4 Fill / Position / Market / Signal

```rust
// types/fill.rs
pub struct Fill {
    pub fill_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,             // <-- 统一使用 Symbol
    pub order_id: OrderId,
    pub side: OrderSide,
    pub price: Price,
    pub size: Decimal,              // 按 exchange 解释
    pub fee: Decimal,
    pub is_maker: bool,
    pub timestamp: u64,
}

// types/position.rs
pub enum PositionSide { Long, Short, Flat }

/// Polymarket 仓位 (shares 计)
pub struct PolyPosition {
    pub symbol: Symbol,
    pub token_side: TokenSide,      // YES or NO
    pub side: PositionSide,
    pub shares: PolyShares,         // <-- 显式类型
    pub entry_price: Price,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}

/// CEX 永续仓位 (base asset 计)
pub struct PerpPosition {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub side: PositionSide,
    pub quantity: PerpQty,          // <-- 显式类型
    pub entry_price: Price,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
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

/// 完整的市场配置 (从 TOML 加载)
pub struct MarketConfig {
    pub symbol: Symbol,                    // 业务主键, engine/risk 只看这个
    pub poly_ids: PolymarketIds,           // Poly 交易所特有 ID
    pub cex_symbol: String,                // CEX 合约代码, e.g. "BTCUSDT"
    pub hedge_exchange: Exchange,          // 对冲用哪个 CEX
    pub strike_price: Option<Price>,       // 行权价 (如 BTC $100k)
    pub end_date: u64,                     // 结算时间 (unix timestamp)
    pub min_tick_size: Price,              // Poly 最小价格步长
    pub neg_risk: bool,                    // 是否属于 NegRisk bundle
    pub cex_price_tick: Decimal,           // CEX 价格精度 (如 0.1)
    pub cex_qty_step: Decimal,             // CEX 数量精度 (如 0.001)
}

/// ID 映射表 (data 和 executor 内部持有)
/// engine/risk 永远只用 Symbol, 不感知底层 token_id
pub struct SymbolRegistry {
    configs: HashMap<Symbol, MarketConfig>,
    // 反向映射 (data normalizer 和 executor 用于从交易所原始 ID 查 Symbol)
    token_to_symbol: HashMap<String, Symbol>,      // token_id -> Symbol
    condition_to_symbol: HashMap<String, Symbol>,  // condition_id -> Symbol
    cex_to_symbol: HashMap<String, Symbol>,        // cex_symbol -> Symbol
}

impl SymbolRegistry {
    /// 构建时自动生成反向映射
    pub fn new(configs: Vec<MarketConfig>) -> Self;
    
    // ---- 正向查询 (engine/risk 用) ----
    pub fn get_config(&self, symbol: &Symbol) -> Option<&MarketConfig>;
    pub fn get_poly_ids(&self, symbol: &Symbol) -> Option<&PolymarketIds>;
    pub fn get_cex_symbol(&self, symbol: &Symbol) -> Option<&str>;
    pub fn get_tick_sizes(&self, symbol: &Symbol) -> Option<(Decimal, Decimal)>;
    
    // ---- 反向查询 (data normalizer / executor 用) ----
    pub fn symbol_by_token_id(&self, token_id: &str) -> Option<&Symbol>;
    pub fn symbol_by_condition_id(&self, condition_id: &str) -> Option<&Symbol>;
    pub fn symbol_by_cex_symbol(&self, cex_symbol: &str) -> Option<&Symbol>;
}

// types/signal.rs
pub enum SignalStrength { Strong, Normal, Weak }
pub enum TokenSide { Yes, No }

pub struct ArbLeg {
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub shares: PolyShares,         // <-- 显式类型
}

pub enum SignalAction {
    BasisLong {
        token_side: TokenSide,      // YES or NO
        poly_side: OrderSide,
        poly_shares: PolyShares,    // <-- Poly 腿数量 (shares)
        cex_side: OrderSide,
        cex_qty: PerpQty,           // <-- CEX 对冲数量 (base asset)
        poly_price: Price,          // 用于计算 notional
        delta: f64,
    },
    BasisShort {
        token_side: TokenSide,
        poly_side: OrderSide,
        poly_shares: PolyShares,
        cex_side: OrderSide,
        cex_qty: PerpQty,
        poly_price: Price,
        delta: f64,
    },
    DeltaRebalance {
        cex_side: OrderSide,
        cex_qty_adjust: PerpQty,    // <-- CEX 腿增减量
        new_delta: f64,
    },
    DmmQuote { 
        token_side: TokenSide,
        bid: Price, 
        ask: Price, 
        bid_shares: PolyShares,     // <-- 做市数量
        ask_shares: PolyShares,
    },
    NegRiskArb { legs: Vec<ArbLeg> },
    ClosePosition { reason: String },
    DoNothing,
}

pub struct AlphaSignalEvent {
    pub signal_id: String,
    pub symbol: Symbol,            // 统一用 Symbol, 不再用 MarketId
    pub action: SignalAction,
    pub strength: SignalStrength,
    pub basis_value: Option<Decimal>,
    pub z_score: Option<Decimal>,
    pub expected_pnl: Decimal,
    pub timestamp: u64,            // <-- Unix timestamp (ms), 可序列化
}
```

### 3.5 跨模块事件

```rust
// event.rs — 全部使用 Symbol 作为业务键
pub enum MarketDataEvent {
    OrderBookUpdate { exchange: Exchange, symbol: Symbol, snapshot: OrderBookSnapshot },
    TradeUpdate { exchange: Exchange, symbol: Symbol, price: Price, size: Decimal, side: OrderSide, timestamp: u64 },
    FundingRate { exchange: Exchange, symbol: Symbol, rate: Decimal, next_funding_time: u64 },
    ConnectionEvent { exchange: Exchange, status: ConnectionStatus },
}

pub enum ExecutionEvent {
    OrderSubmitted(OrderResponse),
    OrderFilled(Fill),
    OrderCancelled { order_id: OrderId, exchange: Exchange },
    HedgeStateChanged { symbol: Symbol, session_id: Uuid, old_state: HedgeState, new_state: HedgeState },
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
pub const MARKET_DATA_CAPACITY: usize = 10240;  // broadcast: 高频行情, 宁大勿小
pub const EXECUTION_CAPACITY: usize = 256;       // mpsc: 执行事件 (fill 不能丢)
pub const COMMAND_CAPACITY: usize = 64;           // broadcast: 系统命令极少

// ---- 信号 Channel: Per-Symbol Latest-Wins ----
// 做市信号 (DmmQuote) 用 watch 实现 latest-wins 语义
// 套利信号 (Basis/NegRisk) 用有界 mpsc, 但不阻塞

/// 做市报价状态 (per-symbol, latest-wins)
pub type DmmQuoteTx = watch::Sender<Option<DmmQuoteState>>;
pub type DmmQuoteRx = watch::Receiver<Option<DmmQuoteState>>;

/// 套利信号 (跨 symbol 队列, 有界非阻塞)
pub type ArbSignalTx = mpsc::Sender<ArbSignalEvent>;
pub type ArbSignalRx = mpsc::Receiver<ArbSignalEvent>;

// ---- 其他 Channel 类型别名 ----
pub type MarketDataTx = broadcast::Sender<MarketDataEvent>;    // 1-to-many
pub type ExecutionTx  = mpsc::Sender<ExecutionEvent>;          // executor -> risk
pub type RiskStateTx  = watch::Sender<RiskStateSnapshot>;      // risk -> all (最新状态语义)
pub type CommandTx    = broadcast::Sender<SystemCommand>;       // cli -> all

// ---- 数据结构 ----

/// 做市报价状态 (用于 watch channel)
pub struct DmmQuoteState {
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub bid: Price,
    pub ask: Price,
    pub bid_shares: PolyShares,
    pub ask_shares: PolyShares,
    pub updated_at: u64,
}

/// 套利信号事件
pub enum ArbSignalEvent {
    BasisLong(BasisSignal),
    BasisShort(BasisSignal),
    DeltaRebalance(RebalanceSignal),
    NegRiskArb(NegRiskSignal),
    ClosePosition { symbol: Symbol, reason: String },
}

// ---- 工厂函数 ----
pub fn create_channels(symbols: &[Symbol]) -> ChannelBundle {
    let (market_data_tx, _) = broadcast::channel(MARKET_DATA_CAPACITY);
    
    // Per-symbol DMM quote channels (latest-wins)
    let dmm_channels: HashMap<Symbol, (DmmQuoteTx, DmmQuoteRx)> = symbols
        .iter()
        .map(|s| (s.clone(), watch::channel(None)))
        .collect();
    
    // 套利信号: try_send, 满了就 warn + 丢弃旧信号
    let (arb_tx, arb_rx) = mpsc::channel(64);
    
    let (execution_tx, execution_rx) = mpsc::channel(EXECUTION_CAPACITY);
    let (risk_state_tx, risk_state_rx) = watch::channel(RiskStateSnapshot::default());
    let (command_tx, _) = broadcast::channel(COMMAND_CAPACITY);
    
    ChannelBundle { 
        market_data_tx, 
        dmm_channels, 
        arb_tx, arb_rx,
        execution_tx, execution_rx,
        risk_state_tx, risk_state_rx,
        command_tx,
    }
}
```

**背压处理规则 (修订版):**

| Channel     | 类型               | 满时行为                      | 原因                   |
| ----------- | ------------------ | ----------------------------- | ---------------------- |
| market_data | broadcast(10240)   | 消费者 lag 时跳过旧数据       | 旧行情无价值           |
| dmm_quote   | watch (per-symbol) | **自动覆盖旧值**              | 做市报价只关心最新状态 |
| arb_signal  | mpsc(64)           | **try_send, 满了丢弃 + warn** | 防止 engine 阻塞       |
| execution   | mpsc(256)          | sender 阻塞等待               | risk 必须收到每个 fill |
| risk_state  | watch              | 自动保留最新值                | 无背压问题             |
| command     | broadcast(64)      | 基本不可能满                  | 命令极少               |

**关键设计变更:**

1. **DMM Quote 用 watch (latest-wins)**: 做市信号只关心"当前应该挂什么价"，过时的报价毫无意义
2. **Arb Signal 用 try_send**: 套利信号满了就丢弃并告警，不阻塞 engine
3. **Execution 保持阻塞**: fill 事件不能丢，必须被 risk 处理

```rust
// Engine 发送信号的模式:

// DMM Quote: 直接覆盖 (watch)
dmm_tx.send(Some(DmmQuoteState { ... })).ok();  // watch.send 永不失败

// Arb Signal: try_send, 满了就丢弃
match arb_tx.try_send(ArbSignalEvent::BasisLong(signal)) {
    Ok(()) => { /* 成功 */ }
    Err(mpsc::error::TrySendError::Full(_)) => {
        tracing::warn!("[ENGINE] Arb signal queue full, dropping signal");
        // 不阻塞, 继续处理下一个 market
    }
    Err(mpsc::error::TrySendError::Closed(_)) => {
        tracing::error!("[ENGINE] Arb signal channel closed!");
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
    pub poly_positions: HashMap<Symbol, PolyPosition>,
    pub perp_positions: HashMap<(Exchange, Symbol), PerpPosition>,
    pub daily_pnl: Decimal,
    pub max_drawdown: Decimal,
    pub timestamp: u64,             // <-- Unix timestamp, 可序列化
}
```

### 3.7 核心 Traits

```rust
// traits/data_source.rs
#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn subscribe_orderbook(&self, symbol: &str) -> Result<()>;
    fn connection_status(&self) -> ConnectionStatus;
    fn exchange(&self) -> Exchange;
}

// traits/alpha.rs
#[async_trait]
pub trait AlphaEngine: Send + Sync {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> Vec<AlphaSignalEvent>;
    fn update_params(&mut self, params: EngineParams);
}

// traits/executor.rs
#[async_trait]
pub trait PolyExecutor: Send + Sync {
    async fn submit_order(&self, request: PolyOrderRequest) -> Result<OrderResponse>;
    async fn cancel_order(&self, order_id: &OrderId) -> Result<()>;
    async fn cancel_all(&self, symbol: &Symbol) -> Result<u32>;
    async fn query_order(&self, order_id: &OrderId) -> Result<OrderResponse>;
}

#[async_trait]
pub trait PerpExecutor: Send + Sync {
    async fn submit_order(&self, request: PerpOrderRequest) -> Result<OrderResponse>;
    async fn cancel_order(&self, order_id: &OrderId) -> Result<()>;
    async fn cancel_all(&self, symbol: &Symbol) -> Result<u32>;
    async fn query_order(&self, order_id: &OrderId) -> Result<OrderResponse>;
}

// traits/risk.rs
#[async_trait]
pub trait RiskManager: Send + Sync {
    fn pre_trade_check_poly(&self, request: &PolyOrderRequest) -> Result<(), RiskRejection>;
    fn pre_trade_check_perp(&self, request: &PerpOrderRequest) -> Result<(), RiskRejection>;
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

// Binary option Delta = dP/dS (概率对底层价格的敏感度)
delta_binary = normal_pdf(d2) / (S * sigma * sqrt(T))

// 对冲数量计算
hedge_qty_cex = poly_position_notional * delta_binary
// 例: 持有 1000 USDC 的 YES token, delta=0.0001
//     -> 在 CEX 做空 0.1 BTC (而非 1000 USDC 等值的 BTC)
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
        poly_qty: calculated_from_position_sizing,
        cex_hedge_qty: poly_qty * delta_binary,  // <-- 关键: 不是 1:1
        current_delta: delta_binary,
    }
```

**信号结构定义:** 参见 3.6 节 `SignalAction` 枚举，已包含正确的数量类型 (`PolyShares`, `PerpQty`)。

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
    return vec![];  // 预热期不产出任何信号, 只收集数据
}
```

**预热期行为:**
- 系统启动后, engine 只接收行情、填充 RollingWindow, 不产出任何 AlphaSignalEvent
- 日志每 10 秒打印一次预热进度 (如 "Warming up: 342/600 samples")
- 预热完成后打印 "[ENGINE] Warm-up complete, starting signal generation"
- DMM 策略同样需要预热: sigma 未就绪时不挂单

### 4.3 结算前/争议期交易规则 (关键安全守卫)

**问题:** 二元期权在结算/争议阶段的风险是**跳跃风险 (Jump Risk)**，不是连续小步 gamma 风险。
永续合约无法对冲这种 jump——事件结果公布瞬间，YES token 从 0.5 跳到 1.0 或 0.0，但 BTC 价格可能只动了 0.1%。

**解决方案:** 在临近结算时逐步收紧策略，最终只允许平仓。

```rust
// config
[strategy.settlement_rules]
stop_new_position_hours = 24        // 结算前 24 小时停止开新仓
force_reduce_hours = 12             // 结算前 12 小时强制减仓 (减至 50%)
close_only_hours = 6                // 结算前 6 小时只允许平仓
emergency_close_hours = 1           // 结算前 1 小时触发紧急平仓
dispute_close_only = true           // UMA 争议期间只允许平仓
```

```rust
// engine 内部状态机
pub enum MarketPhase {
    /// 正常交易期 - 允许开平仓、做市
    Normal,
    /// 临近结算 - 停止开新仓, 允许平仓和 rebalance
    PreSettlement {
        hours_remaining: f64,
    },
    /// 强制减仓期 - 主动减少仓位至目标比例
    ForceReduce {
        target_ratio: f64,      // 如 0.5 = 减至一半
        hours_remaining: f64,
    },
    /// 只允许平仓 - 不允许任何开仓操作
    CloseOnly {
        hours_remaining: f64,
    },
    /// 争议中 - UMA 预言机争议, 结果未定
    Disputed,
    /// 已结算 - 等待最终结算, 禁止任何操作
    Settled,
}

impl MarketPhase {
    pub fn from_end_date(end_date: u64, now: u64, rules: &SettlementRules) -> Self {
        let hours_remaining = (end_date.saturating_sub(now) as f64) / 3600.0;
        
        if hours_remaining <= 0.0 {
            return MarketPhase::Settled;
        }
        if hours_remaining <= rules.emergency_close_hours {
            return MarketPhase::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.close_only_hours {
            return MarketPhase::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.force_reduce_hours {
            return MarketPhase::ForceReduce { 
                target_ratio: 0.5, 
                hours_remaining,
            };
        }
        if hours_remaining <= rules.stop_new_position_hours {
            return MarketPhase::PreSettlement { hours_remaining };
        }
        MarketPhase::Normal
    }
    
    /// 是否允许开新仓
    pub fn allows_new_position(&self) -> bool {
        matches!(self, MarketPhase::Normal)
    }
    
    /// 是否允许做市
    pub fn allows_dmm(&self) -> bool {
        matches!(self, MarketPhase::Normal | MarketPhase::PreSettlement { .. })
    }
    
    /// 是否需要强制减仓
    pub fn force_reduce_target(&self) -> Option<f64> {
        match self {
            MarketPhase::ForceReduce { target_ratio, .. } => Some(*target_ratio),
            _ => None,
        }
    }
}
```

**Engine 主循环中的阶段检查:**
```rust
// 每个 market 每次循环都检查阶段
let phase = MarketPhase::from_end_date(config.end_date, now_ts, &rules);

match phase {
    MarketPhase::Normal => {
        // 正常生成 Basis/DMM 信号
    }
    MarketPhase::PreSettlement { hours_remaining } => {
        tracing::info!("[{symbol}] Pre-settlement phase, {hours_remaining:.1}h to go, no new positions");
        // 只生成 DeltaRebalance 和平仓信号
    }
    MarketPhase::ForceReduce { target_ratio, .. } => {
        tracing::warn!("[{symbol}] Force reduce phase, reducing to {target_ratio:.0}%");
        // 生成 ClosePosition 信号, 减仓至目标
    }
    MarketPhase::CloseOnly { .. } => {
        tracing::warn!("[{symbol}] Close-only phase, closing all positions");
        // 只生成 ClosePosition 信号
    }
    MarketPhase::Disputed => {
        tracing::error!("[{symbol}] Market disputed! Close-only mode");
        // 只允许平仓, 触发告警
    }
    MarketPhase::Settled => {
        // 不生成任何信号, 等待结算
    }
}
```

**UMA 争议检测:**
```rust
// 通过 Polymarket API 或链上事件检测争议状态
// 一旦检测到争议:
// 1. 立即停止该市场的所有策略
// 2. 尝试平掉 CEX 对冲腿 (Poly 腿可能被锁定)
// 3. 发送告警通知交易员
```

### 4.4 动态做市 (Avellaneda-Stoikov)

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

### 4.5 NegRisk 组合套利

NegRisk 市场是 Polymarket 的多结果互斥事件 (如 "总统选举: A/B/C 三个候选人")。所有 YES token 的概率之和应该等于 1.0。当市场定价出现偏差时，存在套利机会。

**套利条件检测:**
```
sum_ask = sum(best_ask[i] for i in all_outcomes)
sum_bid = sum(best_bid[i] for i in all_outcomes)

if sum_ask < 1.0 - total_fees:  // Buy-side arb: 买入所有 YES 锁定利润
if sum_bid > 1.0 + total_fees:  // Sell-side arb: 卖出所有 YES 锁定利润
```

#### 4.5.1 Buy-Side 套利 (sum_ask < 1.0)

**逻辑:** 同时买入所有结果的 YES token，无论最终哪个结果获胜，都会有且仅有一个 YES 结算为 $1。

```
示例: 3 个结果的选举市场
  - 候选人 A YES: ask = 0.30
  - 候选人 B YES: ask = 0.35
  - 候选人 C YES: ask = 0.32
  - sum_ask = 0.97
  
操作: 买入每个结果 1000 shares
  - 成本: 1000 * 0.97 = $970
  - 结算收益: $1000 (无论谁赢)
  - 毛利: $30 (减去手续费和 gas)
```

**执行路径:** 直接在 CLOB 上对三个 outcome 分别下 Buy 单。

#### 4.5.2 Sell-Side 套利 (sum_bid > 1.0) — Complete-Set Redeem 机制

**问题:** 要卖出 YES token，首先需要持有它。从 flat 状态无法直接做空。

**解决方案:** 利用 Polymarket 的 **Complete-Set Mint/Redeem** 机制。

```
Complete-Set 机制:
  - Mint: 用 $1 USDC 铸造 1 份 complete set (所有结果各 1 share YES + 1 share NO)
  - Redeem: 将 1 份 complete set 兑换回 $1 USDC
  
NegRisk 特殊性:
  - NegRisk bundle 只有 YES tokens (NO 通过"不持有 YES"隐式表达)
  - Mint 1 份 complete set = 花 $1 得到所有 outcome 各 1 share YES
  - Redeem = 用所有 outcome 各 1 share YES 换回 $1
```

**Sell-Side 套利执行路径:**

```
示例: sum_bid = 1.05
  - 候选人 A YES: bid = 0.40
  - 候选人 B YES: bid = 0.38
  - 候选人 C YES: bid = 0.27
  - sum_bid = 1.05

步骤:
  1. Mint complete-set: 花 $1000 USDC -> 得到 1000 shares of A, B, C 各一份
  2. 卖出所有 YES tokens:
     - 卖 1000 A YES @ 0.40 -> $400
     - 卖 1000 B YES @ 0.38 -> $380
     - 卖 1000 C YES @ 0.27 -> $270
     - 总收入: $1050
  3. 毛利: $50 (减去 mint gas + 交易手续费)
```

**代码结构:**

```rust
pub enum NegRiskArbType {
    /// Buy-side: 买入所有 YES tokens
    BuySide {
        legs: Vec<ArbLeg>,      // 每个 outcome 一个 buy leg
        expected_profit: UsdNotional,
    },
    /// Sell-side: 先 mint complete-set, 再卖出所有 YES
    SellSide {
        mint_amount: UsdNotional,   // 需要 mint 的 USDC 数量
        legs: Vec<ArbLeg>,          // 每个 outcome 一个 sell leg
        expected_profit: UsdNotional,
    },
}

pub struct NegRiskSignal {
    pub bundle_id: String,          // NegRisk bundle 标识
    pub arb_type: NegRiskArbType,
    pub outcomes: Vec<Symbol>,      // 所有 outcome 的 Symbol
    pub sum_ask: Decimal,           // 当前 sum(ask)
    pub sum_bid: Decimal,           // 当前 sum(bid)
}
```

**执行状态机 (Sell-Side):**

```
IDLE --[sell-side signal]--> MINTING --[mint success]--> SELLING_LEGS --[all sold]--> COMPLETE
  ^                             |                              |                          |
  |                       [mint fail]                   [partial fill:             [profit locked]
  |                             |                        reduce other legs]               |
  +-----------------------------+------------------------------+                          |
                                                                                    --> IDLE
```

**风险控制:**

1. **Mint 前检查深度:** 确保所有 outcome 的 bid 深度足够消化 mint 数量
2. **原子性要求:** 尽量在 mint 后立即提交所有 sell 订单
3. **Partial Fill 处理:** 如果某个 leg 只 partial fill，减少其他 leg 的目标数量保持平衡
4. **Gas 成本:** Mint 需要链上交易，gas 成本需纳入利润计算

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
    pub legs: Vec<LegInfo>,          // N-Leg 通用, 不再硬编码 poly_leg/cex_leg
    pub created_at: u64,             // <-- Unix timestamp (ms), 可序列化, 可跨进程恢复
    pub updated_at: u64,             // <-- Unix timestamp (ms)
    pub timeout_ms: u64,             // 超时配置 (毫秒)
}

pub enum SessionType {
    BasisHedge,       // 2 legs: 1 poly + 1 cex
    DeltaRebalance,   // 1 leg: 仅 cex 调仓
    NegRiskArb,       // N legs: 全部 poly
}

/// Poly 腿信息
pub struct PolyLegInfo {
    pub leg_id: Uuid,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub target_shares: PolyShares,
    pub filled_shares: PolyShares,
    pub filled_price: Option<Price>,
    pub order_id: Option<OrderId>,
    pub status: LegStatus,
}

/// CEX 腿信息
pub struct PerpLegInfo {
    pub leg_id: Uuid,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub target_qty: PerpQty,
    pub filled_qty: PerpQty,
    pub filled_price: Option<Price>,
    pub order_id: Option<OrderId>,
    pub status: LegStatus,
}

/// 通用腿 (用于 HedgeSession.legs)
pub enum LegInfo {
    Poly(PolyLegInfo),
    Perp(PerpLegInfo),
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
```

### 5.4 状态转换规则

**BasisHedge (2-leg) — 含 Partial Fill 比例追单:**
- `Idle -> SubmittingLegs`: 收到 BasisLong/BasisShort 信号, 先提交 Poly leg
- `SubmittingLegs -> Hedging`: Poly leg filled (全部或部分), 立即按**实际成交比例**提交 CEX leg:
  ```
  // Poly 目标 1000 USDC, 实际成交 600 USDC (60%)
  // CEX hedge = signal.cex_hedge_qty * (600 / 1000) = 60% 的对冲量
  cex_actual_qty = signal.cex_hedge_qty * (poly_filled / poly_target)
  // 对齐 CEX step_size 后下单
  ```
- `Hedging -> Hedged`: CEX leg filled
- `Hedged -> Rebalancing`: DeltaRebalance 信号, 调整 CEX 腿数量
- `Hedged -> Closing`: ClosePosition 信号, 提交两腿反向平仓单
- **Poly 持续部分成交**: 如果 Poly leg 收到多次 partial fill, 每次都增量追加 CEX hedge:
  ```
  // 第一次 fill: 300/1000 -> CEX hedge 30%
  // 第二次 fill: 600/1000 -> CEX 追加 hedge 到 60%
  cex_increment = signal.cex_hedge_qty * (new_poly_filled - prev_poly_filled) / poly_target
  ```
- **超时处理**: SubmittingLegs 超时 -> 撤 Poly 单, 对已成交部分提交反向平仓 -> Idle;
  Hedging 超时 -> 市价平 Poly 腿 -> Idle

**NegRiskArb (N-leg) — 含 Partial Fill 处理:**
- `Idle -> SubmittingLegs`: 收到 NegRiskArb 信号, 按深度最浅的 leg 先下单
- `SubmittingLegs -> Hedged`: 所有 legs filled (locked)
- **部分成交处理**: NegRisk 套利要求所有腿**等量成交**才能锁定利润:
  - 取所有腿的 `min(filled_qty)` 作为有效锁定量
  - 超出 min 的部分视为多余敞口, 需要尝试撤单或反向平仓
  - 如果某个 leg 超时, 撤销未成交 legs, 对已成交 legs 尝试反向平仓

---

## 6. 风控系统

### 6.1 Pre-Trade 检查链 (按序, 纯内存, 零 IO)

所有风控检查在内存中完成，不涉及任何数据库读写:

1. 熔断器状态检查 (Closed 或 HalfOpen)
2. 单仓位上限: position + new_order <= max_single_position_usd
3. 总暴露上限: total_exposure + new_order <= max_total_exposure_usd
4. 日亏损限制: daily_loss < max_daily_loss_usd
5. 最大回撤: drawdown < max_drawdown_pct
6. 挂单数量: open_orders < max_open_orders
7. 频率限制: orders/sec < rate_limit
8. **精度对齐** (新增): 通过 PrecisionNormalizer 对 price/qty 做 tick_size/step_size floor

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

### 6.3 持久化策略: 纯内存风控 + 异步批量刷盘

**设计原则:** 交易关键路径 (fill -> position update -> pre_trade check) 绝不等待磁盘 IO。
SQLite 仅用于**事后复盘**和**辅助参考**，**不作为恢复的唯一依据**。

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
          tokio::spawn(async { batch_insert_to_sqlite(flush_buffer).await })
          // 后台 task, 不阻塞主循环
          // 失败只 log warn, 不影响交易
```

```rust
// persistence 模块
pub enum PersistenceCommand {
    SaveFill(Fill),
    SavePolyPosition(PolyPosition),
    SavePerpPosition(PerpPosition),
    SaveSignal(AlphaSignalEvent),
    SaveHedgeSession(HedgeSession),
}

pub struct AsyncPersistence {
    tx: mpsc::Sender<Vec<PersistenceCommand>>,  // 后台 writer task 接收端
}

impl AsyncPersistence {
    /// 启动后台 writer task (单独的 tokio::spawn)
    pub async fn start(db_path: &str) -> Result<Self>;

    /// 非阻塞: 把 commands 发到 channel, 立即返回
    pub fn flush(&self, commands: Vec<PersistenceCommand>);
}
```

### 6.4 启动恢复策略: Live Reconcile 优先 (关键设计变更)

**问题:** SQLite 异步写入可能丢失最近几秒的数据。如果 crash 后直接用 SQLite 恢复，可能导致"带着错误仓位继续交易"——这比停机更危险。

**解决方案:** 启动时必须向 Polymarket / Binance / OKX 做 **Live Reconcile**，以交易所返回的真实状态为准。

```
[启动恢复流程]

Step 1: 从交易所拉取真实状态 (Live Reconcile)
    |
    +---> Polymarket: 
    |       - 调用 SDK client.get_balances() 获取真实 token 余额
    |       - 调用 SDK client.get_open_orders() 获取未成交订单
    |
    +---> Binance: 
    |       - GET /fapi/v2/positionRisk 获取永续持仓
    |       - GET /fapi/v1/openOrders 获取挂单
    |       - GET /fapi/v2/balance 获取保证金余额
    |
    +---> OKX:
            - GET /api/v5/account/positions 获取持仓
            - GET /api/v5/trade/orders-pending 获取挂单
            - GET /api/v5/account/balance 获取余额

Step 2: 与 SQLite 快照对比 (可选, 用于告警)
    |
    +---> 如果 SQLite 有记录, 对比差异
    |       - 差异 > 阈值: 记录 WARN 日志, 以交易所为准
    |       - 差异 = 0: 记录 INFO, SQLite 和交易所一致
    |
    +---> SQLite 无记录或损坏: 不影响启动, 以交易所为准

Step 3: 初始化内存状态 (以 Live 数据为准)
    |
    +---> PositionTracker: 用交易所返回的持仓初始化
    +---> OpenOrdersTracker: 用交易所返回的挂单初始化
    +---> HedgeSessionManager: 重建未完成 session (见下)

Step 4: 处理未完成的 Hedge Sessions
    |
    +---> 检测"孤儿腿" (orphan legs):
    |       - Poly 有持仓但 CEX 无对冲: 危险! 需要人工干预或自动补 hedge
    |       - CEX 有持仓但 Poly 无: 同上
    |
    +---> 检测未完成订单:
            - 挂单中的订单: 标记为 Submitted, 继续跟踪
            - 已取消/已成交: 更新状态

Step 5: 决定是否自动恢复交易
    |
    +---> 无未对冲敞口 + 熔断器未触发: 自动开始交易
    +---> 有孤儿腿: 启动为 "SAFE MODE"
            - 只允许平仓/补 hedge
            - 需要人工确认后才能正常交易
            - CLI 提示: "Detected unhedged position, run `polyalpha reconcile` to resolve"
```

```rust
// reconcile 模块

/// 从单个交易所拉取的实时状态
pub struct ExchangeLiveState {
    pub exchange: Exchange,
    pub positions: Vec<LivePosition>,     // 真实持仓
    pub open_orders: Vec<LiveOpenOrder>,  // 真实挂单
    pub balances: HashMap<String, Decimal>, // 余额 (USDC, BTC, etc.)
    pub timestamp: u64,
}

pub struct LivePosition {
    pub symbol: String,           // 交易所原始符号
    pub side: PositionSide,
    pub size: Decimal,            // 原始数量
    pub entry_price: Price,
    pub unrealized_pnl: Decimal,
}

pub struct LiveOpenOrder {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub price: Price,
    pub size: Decimal,
    pub filled_size: Decimal,
}

/// Reconcile 结果
pub enum ReconcileResult {
    /// 状态干净, 可以自动开始交易
    Clean {
        poly_positions: Vec<PolyPosition>,
        perp_positions: Vec<PerpPosition>,
    },
    /// 检测到未对冲敞口, 需要人工干预
    OrphanLegsDetected {
        unhedged_poly: Vec<PolyPosition>,   // Poly 有仓位但 CEX 无对冲
        unhedged_perp: Vec<PerpPosition>,   // CEX 有仓位但 Poly 无
        recommended_action: String,          // 建议操作
    },
    /// 交易所 API 调用失败, 无法启动
    ApiError { exchange: Exchange, error: String },
}

#[async_trait]
pub trait Reconciler: Send + Sync {
    /// 从所有交易所拉取实时状态并对比
    async fn reconcile(&self, registry: &SymbolRegistry) -> Result<ReconcileResult>;
    
    /// 获取单个交易所的实时状态
    async fn fetch_live_state(&self, exchange: Exchange) -> Result<ExchangeLiveState>;
}

/// CLI 命令: 手动触发 reconcile 并显示差异
/// `polyalpha reconcile --dry-run`  显示差异但不修改
/// `polyalpha reconcile --fix`      自动补 hedge (危险, 需确认)
```

**关键规则:**

1. **SQLite 写入失败不阻塞交易** - 但启动时不依赖 SQLite
2. **Live 状态是唯一真相** - 交易所返回什么就是什么
3. **孤儿腿必须处理** - 不允许带着未对冲敞口自动开始交易
4. **提供手动 reconcile 命令** - 让交易员可以主动检查和修复

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
market_id = "will-btc-reach-100k-by-march-2026"
condition_id = "0x..."
yes_token_id = "0x..."
no_token_id = "0x..."
end_date = 1774915200              # 2026-03-31 00:00:00 UTC
min_tick_size = 0.01
neg_risk = false
cex_symbol = "BTCUSDT"
hedge_exchange = "Binance"
strike_price = 100000
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

[risk]
max_total_exposure_usd = 50000.0
max_single_position_usd = 10000.0
max_daily_loss_usd = 2000.0
max_drawdown_pct = 5.0
max_open_orders = 50
circuit_breaker_cooldown_secs = 300
rate_limit_orders_per_sec = 5
```

---

## 8. Polymarket 官方 Rust SDK 集成

**使用官方 SDK `polymarket-client-sdk` v0.4.3** (https://github.com/Polymarket/rs-clob-client)

这是 Polymarket 官方维护的 Rust SDK，覆盖了 REST API、WebSocket、EIP-712 签名、认证的全部功能。
**无需手写 Polymarket 的 REST client、WS client、签名逻辑、API 认证**。

### 8.1 SDK 功能清单

| Feature Flag | 功能                                                         |
| ------------ | ------------------------------------------------------------ |
| `clob`       | CLOB REST API (订单、交易、市场、余额)                       |
| `ws`         | WebSocket 实时流 (orderbook, prices, midpoints, user events) |
| `data`       | Data API                                                     |
| `gamma`      | Gamma API                                                    |
| `bridge`     | Bridge API                                                   |
| `rfq`        | RFQ API                                                      |
| `tracing`    | 结构化日志集成                                               |
| `heartbeats` | 连接保活                                                     |
| `ctf`        | CTF 合约交互                                                 |

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

| Crate                        | 版本      | 用途                                            | 使用者         |
| ---------------------------- | --------- | ----------------------------------------------- | -------------- |
| tokio                        | 1.x       | 异步运行时 (features: full)                     | all            |
| polymarket-client-sdk        | 0.4.x     | Polymarket 官方 SDK (features: clob,ws,tracing) | data, executor |
| tokio-tungstenite            | 0.24.x    | WebSocket 客户端 (Binance/OKX)                  | data           |
| serde + serde_json           | 1.x       | 序列化                                          | all            |
| rust_decimal                 | 1.x       | 精确小数 (features: serde)                      | core           |
| alloy                        | 0.9.x     | EIP-712 签名 + Polygon                          | executor       |
| reqwest                      | 0.12.x    | HTTP (features: json, rustls-tls)               | data, executor |
| tracing + tracing-subscriber | 0.1/0.3   | 结构化日志                                      | all            |
| clap                         | 4.x       | CLI (features: derive)                          | cli            |
| config                       | 0.14.x    | 配置管理                                        | core, cli      |
| sqlx                         | 0.8.x     | SQLite (features: runtime-tokio, sqlite)        | risk           |
| thiserror                    | 2.x       | 错误定义                                        | core           |
| anyhow                       | 1.x       | 应用级错误                                      | cli            |
| chrono                       | 0.4.x     | 时间处理                                        | core           |
| uuid                         | 1.x       | ID 生成 (features: v4)                          | core           |
| hmac + sha2                  | 0.12/0.10 | HMAC-SHA256 签名                                | data, executor |
| base64                       | 0.22.x    | Base64 编解码                                   | data, executor |
| futures-util                 | 0.3.x     | Stream 工具                                     | data           |
| async-trait                  | 0.1.x     | async trait                                     | core           |
| parking_lot                  | 0.12.x    | 高性能锁                                        | data, risk     |
| statrs                       | 0.17.x    | 统计函数 (正态 CDF)                             | engine         |

---

## 10. Agent 任务拆分

### Agent A: polyalpha-core + polyalpha-data (最高优先级)

**职责:** 基础类型定义 + 数据采集层

**任务:**
1. 创建 workspace root `Cargo.toml` 和所有 crate 骨架
2. 实现 `polyalpha-core` 全部类型:
   - 数量类型三件套: `PolyShares`, `UsdNotional`, `PerpQty` (含转换方法)
   - `Price` 类型 (含 `to_f64`, `from_f64_rounded`)
   - `Symbol`, `SymbolRegistry` (含正向+反向映射), `MarketConfig`
3. 实现 traits (`PolyExecutor`, `PerpExecutor`, `RiskManager`)
4. 实现 config、error、event、channel (DMM watch + Arb mpsc)
5. 基于官方 SDK `polymarket-client-sdk` 封装 Poly WS 流 (stream.rs + normalizer.rs)
6. 实现 Binance Futures WebSocket 客户端 (深度 + 资金费率)
7. 实现 OKX WebSocket 客户端 (深度 + 资金费率)
8. 实现 L2OrderBook (BTreeMap, 增量更新, sequence 校验)
9. 实现 DataManager (启动所有 WS, 广播 MarketDataEvent)
10. 创建 `config/default.toml` (含 settlement_rules 配置节)
11. 单元测试: orderbook 更新、normalizer、SymbolRegistry 双向映射

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
8. 实现 BasisSignal 生成 (使用 `calc_hedge_qty()` 正确计算对冲量)
9. **实现 MarketPhase 状态机** (Normal/PreSettlement/ForceReduce/CloseOnly/Disputed)
10. 实现 Avellaneda-Stoikov DMM 定价 + 多层报价 (输出 `DmmQuoteState`)
11. 实现 NegRisk Bundle 套利检测 (Buy-side + Sell-side/Mint)
12. 实现 AlphaEngine 主循环 (**单线程顺序 for 循环**, DMM 用 watch, Arb 用 try_send)
13. 单元测试: 数值正确性、预热期守卫、结算前阶段切换

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
4. **实现 PrecisionNormalizer** (precision.rs): 从 SymbolRegistry 读取 tick_size/step_size, floor 对齐
5. **实现 N-Leg HedgeStateMachine** (支持 BasisHedge 2-leg + NegRiskArb N-leg + DeltaRebalance 1-leg)
6. 实现 LegManager (N-leg partial fill 处理, 超时撤单+反向平仓)
7. 实现 ExecutionManager 主循环
8. 实现 DryRunExecutor (模拟成交，用于测试)
9. 单元测试: 精度对齐 (0.1234 -> 0.12)、状态机全路径覆盖 (2-leg/N-leg/rebalance)

### Agent D: polyalpha-risk + polyalpha-cli

**职责:** 风控 + 系统集成

**任务:**
1. 实现 PositionTracker (**纯内存 HashMap**, 分 `PolyPosition` / `PerpPosition`)
2. 实现 PnL 计算 (realized/unrealized, drawdown, **纯内存**)
3. 实现 Pre-Trade 检查链 (**纯内存, 零 IO**, 分 Poly/Perp 两个方法)
4. 实现 CircuitBreaker FSM
5. **实现 AsyncPersistence** (后台 tokio::spawn, mpsc channel, 批量 batch insert SQLite)
6. 实现 RiskManager 主循环 (内存决策 + 异步刷盘分离)
7. **实现 Live Reconcile 启动恢复** (从三交易所拉取真实状态, 检测孤儿腿, SAFE MODE)
8. 实现 Reconciler trait 及 Poly/Binance/OKX 实现
9. 实现 CLI (clap derive, 含 `reconcile --dry-run/--fix` 子命令)
10. 实现 Orchestrator (创建 channels, 启动模块, 优雅关闭)
11. 实现 tracing 日志配置
12. 编写集成测试 (mock WebSocket server)
13. Dockerfile

---

## 11. 错误处理与恢复策略

| 错误类型               | 恢复策略                                        |
| ---------------------- | ----------------------------------------------- |
| WebSocket 断连         | 自动重连 (指数退避, 初始 1s, 最大 30s, +jitter) |
| REST 请求失败          | 重试 3 次 (exponential backoff)                 |
| 签名错误               | 不重试, 记录错误, 跳过                          |
| 订单被拒               | 记录, 通知 risk manager                         |
| Orderbook sequence gap | 重新获取 snapshot                               |
| Channel closed         | 触发优雅关闭                                    |
| SQLite 错误            | 日志告警, 不阻塞交易                            |
| 熔断触发               | 停止新单, cooldown 后 half-open                 |

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

- core: Price/Quantity 运算、config 加载
- data: orderbook 增量更新、sequence gap 检测、crossed book 检测
- engine: 隐含概率计算、Z-score、A-S 报价、NegRisk 检测 (全部用已知输入/预期输出)
- executor: EIP-712 签名 (与 Python SDK 输出对比)、状态机全路径
- risk: 仓位计算、PnL、熔断器状态转换、pre-trade 拒绝

### 12.3 集成测试

- mock WebSocket server 推送价格数据 -> DataManager -> Engine -> 验证信号输出
- DryRun 模式端到端: 启动系统 -> 收行情 -> 产信号 -> 模拟执行 -> 验证仓位

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