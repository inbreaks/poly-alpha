# Pulse Signal Realignment 设计

## 目标

把 `pulse_arb` 的信号主线从“报价偏离 + 静态摩擦墙”重写为“脉冲 -> 真空 -> 回补 -> 会话 EV”的机制模型。

这次改造保真的对象不是“更多 feature”或者“更厚的 detector”，而是策略本身的第一性原理：

- `Poly` 是情绪扫盘和流动性真空发生地
- `Deribit options` 是机构未确认的公允概率锚
- `Binance perp` 是 delta 对冲腿
- 真正的 alpha 不是 `fair_prob - best_ask`，而是 Poly 被扫穿后留下的短时回补空间

## 背景与现状偏差

当前实现已经具备完整 runtime、session、hedge、audit 和 monitor 骨架，但 signal 语义仍然偏薄，主要问题有四个：

- 入场价格仍然用 `best_ask` 近似，而不是“按本次计划开仓名义额真实可成交的 VWAP”
- admission 仍然以 `instant basis - 静态 friction` 为核心，没有把 timeout 出场、回补空间和可成交性统一进同一个会话 EV
- pulse 主要依赖短窗报价位移，缺少对“扫盘后真空”的表达
- market selection 以 `pulse_score` 为主，不是以“哪一个 market 的完整会话最值得开”来排序

因此现在的 runtime 更像“在跑步机上跑得很快的发动机”，而不是那台真正能识别地形、判断可达路径、再决定是否踩油门的越野车。

## 策略第一性原理

这条策略不是泛化的 mispricing engine，也不是普通的 value gap 套利。

它要捕捉的是下面这个短时结构：

1. `Impulse`
   Poly 某一侧被情绪资金扫穿，claim 价格在极短窗口内发生剧烈跳变。
2. `Divergence`
   同一窗口里，`Deribit` 推导出来的 fair probability 没有同步重定价，`Binance` 现货/永续锚也没有确认同方向的大漂移。
3. `Vacuum`
   被扫过的 Poly 盘口在当前价位和历史价位之间出现薄层或真空，后续回补时容易一口气走完几档。
4. `Session EV`
   在给定开仓名义额、对冲腿、maker exit、timeout chase 和摩擦假设下，这一整笔会话仍然是正期望，才允许开仓。

只有这四层同时成立，才是这条策略要的 setup。

## 核心定义

### 1. 可成交入场价不是 top-of-book，而是 executable entry

runtime 不再把 `best_ask` 当成入场成本，而是基于 `opening_request_notional_usd` 扫描 Poly 对应 claim side 的卖盘深度，求出：

- `entry_executable_vwap`
- `entry_executable_qty`
- `entry_depth_shortfall`
- `entry_slippage_bps`

如果市场深度不足以支撑这次请求，或者成交均价把利润吃掉，这就是无效机会。

### 2. 真空不是 abstract idea，而是当前 book 上可观察的 pocket

第一阶段不做完整 queue reconstruction，但必须先把“真空”落成一组最小可用指标：

- `reversion_reference_price`
  脉冲前短窗内的 claim ask 参考价
- `reversion_pocket_ticks`
  从当前可成交入场价到参考价之间，理论上存在多少个 tick 的回补空间
- `reversion_pocket_notional_usd`
  如果在当前簿面上向上回补到目标价，能吃到多少对手深度
- `vacuum_ratio`
  当前入场附近的卖盘厚度，相对于回补区间内厚度的稀薄程度

第一阶段不要求这些指标足够完美，但它们必须进入 admission，而不是只留在 audit 里旁观。

### 3. Deribit 不是 alpha 本体，而是“未确认”锚

`Deribit` 继续负责：

- 提供 `fair_prob_yes/no`
- 提供 `event_delta_yes`
- 提供 `gamma_estimate`
- 提供 `pricing_quality`

但 `Deribit` 在本模块里的角色只是：

- 排除“其实机构已经确认了”的行情
- 提供会话 EV 里的 fair reference
- 给后续的 pin risk / gamma suppression 提供边界信息

它不是主要成交场，也不是单独就能触发信号的充分条件。

### 4. 会话 EV 是 admission 的最终语言

每个 market 的入场判断都必须被统一映射成一份 `SessionEdgeEstimate`，至少包含：

- `entry_executable_price`
- `entry_executable_notional_usd`
- `instant_anchor_gap_bps`
- `reversion_pocket_ticks`
- `maker_target_price`
- `maker_gross_edge_bps`
- `timeout_exit_haircut_bps`
- `hedge_cost_bps`
- `fees_bps`
- `reserve_bps`
- `expected_net_edge_bps`
- `expected_net_pnl_usd`

Admission 语义改成：

- `Pulse` 没有成立，不开
- `Vacuum / pocket` 太浅，不开
- `Session EV <= 0` 或低于阈值，不开

也就是说，最终决定权交给“完整会话经济性”，不是交给某个单一指标。

## 第一阶段实现范围

本轮只改 signal 主线，不改执行骨架语义。

### 这轮要做

- 新增独立 `signal` 模块，承接 executable price、vacuum/pocket、session EV
- 把 Poly 入场从 `best_ask` 改成 `按请求名义额扫描盘口后的 executable VWAP`
- 把 detector 输入从“静态 friction 若干标量”升级为“结构化 session edge 输入”
- 把 `best_entry_candidate` 的排序主键从 `pulse_score` 改成 `expected_net_pnl_usd / expected_net_edge_bps`
- 保留并继续使用现有 `Deribit -> EventPricer`、`PulseSession`、`GlobalHedgeAggregator`、`HedgeExecutor`
- 在 audit / monitor 中补出新 signal 字段，保证后续能复盘“为什么这个 market 被选中”

### 这轮先不做

- 不重写 `PulseSession` 状态机
- 不重写 `GlobalHedgeAggregator`
- 不改 maker exit / rehedge / timeout / emergency flatten 的执行骨架
- 不做完整 trade tape 驱动 detector
- 不做 queue reconstruction
- 不做 reachability-based target 的二阶段版本

## 模块设计

建议新增 `crates/polyalpha-pulse/src/signal.rs`，把当前散落在 `runtime.rs` 的 admission 经济学抽离出去。

### `SignalBookView`

负责把 `OrderBookSnapshot` 解释成当前这次入场所需的最小信号视图：

- claim side executable VWAP
- timeout/chase 的近似可成交价
- 当前价位到参考价之间的 pocket 深度

### `SessionEdgeEstimate`

负责表达“这一笔会话值不值得开”，而不是只表达瞬时价差。

### `SignalEvaluator`

输入：

- `AnchorSnapshot`
- `EventPriceOutput`
- Poly/cex books
- signal tape history
- runtime config

输出：

- `PulseSignalEstimate`
  至少包含 `pulse_strength`、`vacuum_quality`、`expected_net_edge_bps`、`expected_net_pnl_usd`、`target_exit_price`

runtime 只负责：

- 遍历 market
- 调 evaluator
- 选出 EV 最好的 candidate
- 交给现有开仓流程

## Admission 与排序语义

### Admission

一个 market 只有在以下条件同时满足时才进入候选池：

- anchor quality OK
- data freshness OK
- pricing quality OK
- 有足够的 pulse history
- claim move 明显大于 fair/cex move
- 可成交入场后仍存在足够 pocket
- `expected_net_edge_bps >= min_net_session_edge_bps`
- `expected_net_pnl_usd >= min_expected_net_pnl_usd`

### 排序

第一排序键：

- `expected_net_pnl_usd`

第二排序键：

- `expected_net_edge_bps`

第三排序键：

- `pulse_strength`

含义很明确：先找“最值得开”的会话，再用 pulse 强度打破同价差候选之间的并列，而不是反过来。

## 审计与监控要求

这次 signal 改造必须同步把原始 tape 留下来，避免后面再次掉回“只剩 summary、无法追责”的状态。

至少补充：

- executable entry VWAP 与对应深度
- reference price / pocket ticks / pocket notional
- timeout exit executable proxy
- expected net edge / expected net pnl
- candidate rank reason

这不是为了好看，而是为了后面能回答两个硬问题：

- 为什么它当时看起来能赚钱？
- 为什么真正成交后没有按预期回补？

## 第二阶段预留位

第一阶段完成后，第二阶段才进入：

- trade-confirmed pulse
- flow toxicity / adverse selection proxy
- reachability-based dynamic target
- queue position reconstruction

这几个都必须建立在第一阶段已经把“完整会话 EV”立住的基础上，不能再次回到 feature soup。

## 成功标准

这轮完成后，`pulse_arb` 的信号主线应该满足下面几条：

- 代码里不再用 `best_ask` 直接代表本次开仓成本
- market selection 不再由 `pulse_score` 主导
- audit 能解释一笔候选为什么有正 EV
- 浅盘口假机会会因为 executable EV 为负而被拒绝
- 同样的 pulse，在深盘口和浅盘口上的 admission 结果会不同

## 一句话钢印

这次不是做 feature soup，而是把信号链重写成一套真正反映策略本体的机制模型：

`脉冲 -> 真空 -> 回补 -> 会话 EV`
