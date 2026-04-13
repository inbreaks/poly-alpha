# PolyAlpha Current Status

更新日期：`2026-04-12`

## 一句话现状

项目已经进入“可运行、可验、但还没完全 live-ready”的阶段。

当前可信主线是：

- `engine -> planner -> execution -> artifact/audit`
- `sim / paper / live mock` 共享执行栈
- `backtest rust-replay` 作为默认历史事实源

## Pulse Arb Runtime 状态

`pulse_arb` 独立 runtime 的 MVP 主线已经接通，并且有基础验收覆盖。

当前范围：

- 资产：`BTC / ETH`
- anchor：`Deribit options`
- hedge：`Binance USD-M perp`
- 模式：`paper / live-mock`
- 观测：pulse audit summary、warehouse summary row、monitor pulse snapshot
- operator surface：`paper pulse run`、`live pulse run --executor-mode mock`、`paper pulse inspect`、`audit pulse-sessions`、`audit pulse-session-detail`
- 运行形态：已接真实 `Deribit / Polymarket / Binance` feed-driven pulse runtime
- 审计回放：`pulse-session-detail` 可回放 lifecycle timeline；active session 期间的 raw book tape 已保留在 audit events，可继续升级到 queue reconstruction

当前明确非目标：

- 不做真钱 live trading
- 不做 `SOL / XRP` 实盘接入
- 不做完整 queue reconstruction
- 不做全局 volatility surface calibration

## 已完成并验证

### 1. 交易真相已经硬切到 planner

- `OpenCandidate` 只表达 alpha 机会
- `PlanningIntent` 表达系统要做的动作
- `TradePlan` 是唯一计划真相
- `ExecutionResult` 是唯一成交真相

这条主线的事实源：

- [`docs/superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md`](superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md)

### 2. live / paper / sim 已接到共享执行链

- `live run`
- `live run-multi`
- `paper`
- `sim`

都不再各自维护一套独立的执行语义。

### 3. 多市场 live-mock 能完整跑完并回看

已验证：

- 能启动
- 能接 WS
- 能产候选
- 能生成计划
- 能成交
- 能写 artifact
- 能写 audit summary
- 能通过 `live inspect` 回看

### 4. artifact / audit 的关键收口问题已修复

之前多市场 runtime 存在“中途运行了，但结果文件没落盘”或“已完成 plan 仍卡住后续 plan”的问题。

当前状态：

- 多市场 artifact 已持久化
- `Frozen` plan 不再错误占据 in-flight active set
- live-mock 不再因为 `higher_priority_plan_active` 共享崩点中途退出

### 5. 市场分层和 retention 语义已进入 monitor

当前 monitor 已能表达：

- `交易池`
- `重点池`
- `观察池`

以及 focus 市场为什么被保留：

- 持仓
- residual recovery
- close
- delta rebalance
- force exit

事实源：

- [`docs/superpowers/specs/2026-04-02-monitor-market-tier-retention-design.md`](superpowers/specs/2026-04-02-monitor-market-tier-retention-design.md)

### 6. 市场池不再只是展示语义

当前 open flow 已经增加真实门禁：

- 市场处于 open cooldown 时，candidate 不再进入 planner
- 有持仓 / close / rebalance / residual / force-exit 责任的市场，不再被当作可开新仓市场
- `zero_cex_hedge_qty`、`residual_delta_too_large`、`non_positive_planned_edge`、订单簿漂移类 planner 拒绝，会把市场打入分层冷却
- 结构性坏市场现在不是“一分钟后再试”，而是会被临时隔离更久，只有真正恢复可交易后才回池
- `paper/live` 与 `sim` 共用同一套市场冷却语义

意义：

- `交易池 / 重点池 / 观察池` 不再只是 monitor 标签
- 一部分“明知大概率做不了还反复送进 planner”的噪声已被前置挡掉
- 市场池开始具备真正的“自动踢出坏市场”能力，而不是只在末端重复报错

### 7. 两个会污染判断的时间语义问题已收口

- engine 之前会在 `build_open_candidate` 时回头从 `self.states` 取 `last_update_ms`
- 但 `generate_output` 正在临时把当前 symbol 的 state 从 map 里拿出来计算
- 结果就是 candidate 可能稳定带着 `timestamp_ms = 0`

这个问题现在已经修掉：

- `OpenCandidate.timestamp_ms` 直接绑定本次计算上下文里的 `state.last_update_ms`
- 已有回归测试锁住 “candidate 时间必须等于触发它的那笔 CEX 事件时间”

同时，多市场 WS 启动补齐阶段的拒绝语义也做了收口：

- 在启动补齐尚未完成、市场当前还没进入可交易状态时
- 系统不再把这类拒绝误记成 `cex_stale`
- 而是明确记为 `bootstrap_in_progress`

意义：

- 候选时间、事件时间、市场池时间不再混成一团
- 启动期“还没补齐”和运行中“真实 stale”终于被拆成两个不同原因码

### 8. WS 连接真相链路已重新收敛

之前多市场 WS runtime 还存在一个关键工程 bug：

- 某个交易所只要已有活跃 WS stream
- 后续新 stream 启动时，`mark_connecting()` 仍会把全局状态重新打回 `Connecting`
- 结果就是“数据已经在流动，但 runtime / engine 仍判定连接异常”

这个问题现在已经锁住：

- `ExchangeConnectionTracker` 在已有活跃 stream 时不再降级全局状态
- 活跃 WS stream 在持续收消息时，会低频重申一次 `Connected`
- 所以即使启动洪峰下初始 `Connected` 事件被消费者错过，runtime 也会重新收敛
- 已补失败测试，验证 `Connected -> Connecting` 的错误回退不再发生
- 最新 live-mock 也已确认最终 monitor 连接状态稳定收敛到
  `Binance=已连接, Polymarket=已连接`

意义：

- WS 数据、runtime 连接状态、engine 连接判断重新回到同一套事实
- “明明已经连上并持续收行情，却被卡在 `connection_impaired`” 这条 blocker 已关闭

## 最新验证证据

### 全量 Rust 验证

历史已跑：

```bash
cargo test -p polyalpha-core -p polyalpha-data -p polyalpha-risk -p polyalpha-executor -p polyalpha-cli -- --nocapture
```

最新结果：

- `polyalpha-cli`: `141 passed, 0 failed, 4 ignored`
- `polyalpha-core`: `39 passed, 0 failed`
- `polyalpha-data`: `28 passed, 0 failed`
- `polyalpha-executor`: `59 passed, 0 failed`
- `polyalpha-risk`: `8 passed, 0 failed`

针对 2026-04-02 这轮 WS 连接状态修复，最新补跑：

```bash
cargo test -p polyalpha-cli -- --nocapture
```

最新结果：

- `polyalpha-cli`: `148 passed, 0 failed, 4 ignored`

针对 `pulse_arb` MVP，最新补跑：

```bash
cargo test -p polyalpha-audit -- --nocapture
cargo test -p polyalpha-pulse -- --nocapture
cargo test -p polyalpha-cli pulse_session_timeline_collects_lifecycle_and_tape_events -- --nocapture
cargo build -p polyalpha-cli
./target/debug/polyalpha-cli paper pulse inspect --env multi-market-active.fresh
./target/debug/polyalpha-cli audit pulse-sessions --env multi-market-active.fresh --session-id 270da19c-3aba-41b5-9cfa-51a6882b2694 --format json
./target/debug/polyalpha-cli audit pulse-session-detail --env multi-market-active.fresh --session-id 270da19c-3aba-41b5-9cfa-51a6882b2694 --pulse-session-id pulse-btc-4 --format json
```

最新结果：

- `polyalpha-audit`: `6 passed, 0 failed`
- `polyalpha-pulse`: `37 passed, 0 failed`
- `polyalpha-cli`: pulse timeline targeted regression pass
- `cargo build -p polyalpha-cli`: success
- `paper pulse inspect`: `BTC / ETH` 都确认 routing 到 `Deribit` anchor + `Binance USD-M perp`
- `audit pulse-sessions`: 已确认真实会话 `270da19c-3aba-41b5-9cfa-51a6882b2694` 下 `pulse_session_summaries` 非空
- `audit pulse-session-detail`: 已确认 `pulse-btc-4` timeline 可回放 `poly_opening -> closed`，并保留 session 级审计字段

这轮 `polyalpha-pulse` 测试里，已经直接覆盖：

- partial fill / zero fill 按 `actual_filled_qty` 缩放
- pin risk `delta_clamp / protective_only / freeze`
- `GlobalHedgeAggregator` 多 session 净额仲裁
- external actual position sync
- `maker exit`、`pegging`、`rehedging`、`timeout`、`emergency flatten`
- audit / monitor snapshot 保持

### live-mock 运行证据

历史长跑样本：

```bash
./target/debug/polyalpha-cli live run-multi \
  --env multi-market-active.fresh \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

对应产物：

- artifact: `data/live/multi-market-active.fresh-last-run.json`
- audit summary: `data/audit/sessions/e69e0c60-7bd9-4133-bb38-889809252ec4/summary.json`

最新一轮结果：

- `ticks_processed = 60`
- `signals_seen = 228`
- `signals_rejected = 348`
- `execution_rejected = 125`
- `order_submitted = 4`
- `fills = 4`
- `status = completed`

和紧邻上一轮同口径 60 tick 对比：

- `zero_cex_hedge_qty`: `179 -> 69`
- `execution_rejected`: `237 -> 125`
- `signals_rejected`: `559 -> 348`
- `order_submitted / fills`: 仍然保持 `4 / 4`

这说明：

- 结构性坏市场的重复 planner 命中已经明显下降
- 新的市场池冷却没有把真实成交机会一起打掉
- 当前主问题开始从“同一批坏市场反复撞 planner”转向“实时数据质量和剩余坏市场治理”

意义：

- 不是“入口能启动但不会交易”
- 也不是“有成交就会中途崩”
- 当前真正的问题已经变成“机会质量和实时可交易性”

针对 2026-04-02 这轮 WS 连接状态和实时准入修复，最新补跑：

```bash
cargo run -p polyalpha-cli -- live run-multi \
  --env multi-market-active.fresh \
  --poll-interval-ms 1000 \
  --max-ticks 60 \
  --print-every 60 \
  --executor-mode mock
```

针对 `pulse_arb` 的最新真实验收（`2026-04-12`）：

```bash
./target/debug/polyalpha-cli paper pulse run --env multi-market-active.fresh --assets btc,eth
./target/debug/polyalpha-cli live pulse run --env multi-market-active.fresh --assets btc,eth --executor-mode mock
```

最新结果：

- `paper` 会话 `9c066758-b37e-429c-ab8a-c575740ef073`
  - `status = completed`
  - `ticks_processed = 133`
  - `market_events = 13168`
  - `signals_seen = 2362`
  - `evaluation_attempts = 13076`
  - `trades_closed = 226`
  - `pulse_session_summaries` 非空
- `live-mock` 会话 `38ff2765-5075-4883-a9b6-d025227d8896`
  - `status = completed`
  - `ticks_processed = 144`
  - `market_events = 15614`
  - `signals_seen = 2449`
  - `evaluation_attempts = 13936`
  - `trades_closed = 244`
  - `pulse_session_summaries` 非空
- `paper` smoke 会话 `270da19c-3aba-41b5-9cfa-51a6882b2694`
  - `status = completed`
  - `ticks_processed = 174`
  - `evaluation_attempts = 17556`
  - `signals_seen = 2389`
  - `order_submitted = 0`
  - `fills = 0`
  - `pulse_session_summaries` 非空
  - `audit pulse-session-detail` 已确认示例会话 `pulse-btc-4` 为 `poly_opening -> closed`

这说明：

- `paper / live-mock` 对 `BTC / ETH` 的 pulse runtime 已能稳定起真实行情链路
- `Deribit` 作为 options anchor、`Binance USD-M perp` 作为 hedge venue 的主装配已经接通
- session 级审计已经能回放 timeline，并保留 raw tape、`anchor_latency_delta_ms`、`distance_to_mid_bps`、`relative_order_age_ms`
- 真实短跑里已经能观测到 `PulseSession` 创建并落盘，不再是“只起 runtime 但完全没有 session”

同时也必须明确：

- 这轮真实样本里，大部分 session 仍然是 `zero-fill`，表现为 `poly_opening -> closed`
- 所以真实短跑当前证明的是“行情链、session 链、audit 链打通”，不是“已经在 live window 里稳定观察到 maker/rehedge/flatten 全生命周期”
- `maker exit`、`pegging`、`rehedging`、`timeout`、`emergency flatten` 的完整链路，目前主要由 deterministic runtime tests 保证
- 启动早期仍有少量 `connection_lost`，但最后一次出现在启动后约 4 秒内，已经不再是持续性 blocker

## 当前 live-ready 阻塞

### 1. 交易池治理已经起效，但还没过线

大量市场虽然能产候选，但本质上不是真正可交易机会。

在最近一轮 `WS + mock` 中，主要拒绝原因是：

- `zero_cex_hedge_qty`
- `residual_delta_too_large`
- `above_or_equal_max_poly_price`
- `non_positive_planned_edge`

相比紧邻上一轮，`zero_cex_hedge_qty` 已经显著下降，但还没有下降到“坏市场不再主导漏斗”的程度。

### 2. 实时可交易性质量仍然不够稳定

最近一轮历史证据里：

- 启动早期 `cex_stale` 有一部分其实是启动补齐阶段的误分类
- 这部分现在已被单独归因为 `bootstrap_in_progress`
- 高价带市场仍然频繁卡在价格过滤
- `poly_stale` 虽然下降，但还不能说明实时质量问题已经彻底解决

这说明问题不是系统不会交易，而是“进入漏斗的市场质量”和“实时双腿数据质量”还没有同时过线。

补充说明：

- 2026-04-02 本地已经重跑到新的 live-mock session，确认 WS 连接状态最终能收敛到 `已连接`
- 当前残留的连接异常只出现在启动早期几秒内，不再是主要工程 blocker
- 现在更需要处理的是“哪些市场应该进交易池”和“哪些报价其实不该进 planner”

### 3. planner 末端拒绝仍然偏多

`execution_planner` 侧的 revalidation 失败说明：

- 有些候选直到最后一步才发现不成立
- 这些问题已经开始前置为“市场池准入 / 冷却 / 降级”逻辑，但还需要继续扩展和调参，而不是只在最后一步拒绝

### 4. 还没做长时间 soak

当前我们有 5 分钟级别的 live-mock 验证，但还没完成真正的长时间 soak 验收。

工程上要标记为 live-ready，至少需要：

- 更长时间的连续运行
- 无中途致命退出
- artifact / audit 稳定收口
- 拒绝结构明显收敛

## 下一阶段只做什么

不要再开大摊子。当前下一阶段只做三件事：

### 1. 市场池治理

目标：

- 让进入交易池的市场，大多数时间都是真正可交易的

结果判断标准：

- `zero_cex_hedge_qty` 继续下降
- 高价带 / 低质量市场更少进入开仓漏斗

### 2. 实时可交易性治理

目标：

- 把 `cex_stale`
- 高价带误入场
- 剩余结构性 planner 拒绝

这些问题变成自动准入 / 踢出 / 冷却规则

结果判断标准：

- planner 末端无效拒绝继续下降
- 交易池里的市场更稳定

### 3. 长时间 live-mock soak

目标：

- 用更长运行时间验证系统稳定性

结果判断标准：

- 运行不中断
- artifact / audit 稳定落盘
- 能按成交逐笔回看
- 主要拒绝原因收敛到少数、可解释类别

## 工程上什么算 live-ready

当前项目的工程性 live-ready 口径，不看“短期盈亏是不是正的”，而看下面 4 件事：

1. 运行稳定
   - 长时间 `WS + mock` 不致命退出
2. 真相闭环完整
   - plan、fill、result、artifact、audit 都能复盘
3. 市场池质量过线
   - 交易池不再被明显垃圾市场主导
4. 拒绝结构健康
   - 主要是少量、合理、可解释的拒绝，而不是到处都是假机会

## 当前不建议做的事

- 不要再回头怀疑 hard-cut 主方向本身
- 不要为了提高交易频次去删护栏
- 不要把“能连上 WS”误当成“已经可以交易”
- 不要再让不同 runtime 偷偷分叉执行语义
