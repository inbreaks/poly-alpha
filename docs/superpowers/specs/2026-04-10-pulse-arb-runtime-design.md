# Pulse 套利 Runtime 设计

## 目标

新增一条独立的短周期策略 runtime，用来收割 Polymarket 上短时情绪脉冲带来的定价超调。

这条 runtime 的核心机制是：

- 用期权市场推导出来的公允概率作为定价锚
- 用 Binance USD-M 永续合约做快速 Delta 对冲
- 用完整的 15 分钟会话生命周期管理开仓、挂单止盈、动态 peg、补对冲、超时追价和平仓

MVP 只交易：

- `BTC`
- `ETH`

但架构必须从第一天就保留下面这些扩展位，后面接进去时不能大拆：

- `SOL`
- `XRP`
- 新的期权锚 provider
- 新的应急对冲 venue

## 策略 Thesis

这条 runtime 不是现有 `price-only basis` 的一个变体。

它属于另一类策略，核心 thesis 是：

- `Polymarket` 是零售情绪溢价发生地
- `Options anchor` 是更冷静、更接近机构定价的公允概率锚
- `Binance perp` 是对冲执行腿，用来快速把方向暴露压平

所以这条 runtime 必须是：

- 以会话为中心，而不是以 signal 为中心
- 以 options anchor 为中心，而不是以 realized vol 近似为中心
- 以完整执行生命周期为中心，而不是只做“入场-回归-离场”的轻量逻辑

## 已确认范围

### MVP 范围

- 运行模式：`paper` 和 `live-mock`
- 资产范围：`BTC`、`ETH`
- 期权定价锚：`Deribit`
- 对冲 venue：`Binance USD-M perpetual`
- Poly 执行：由新 runtime 自己管理完整订单生命周期
- 单笔最长持仓：`15 分钟`
- maker 止盈：必须支持
- pegging：必须支持
- 会话期内 Delta rehedge：必须支持
- 紧急 flatten：必须支持
- 结构化 audit 和 monitor：必须支持

### 明确保留的未来扩展能力

MVP 虽然只做 `BTC/ETH`，但设计上必须明确支持以后扩到：

- `SOL`、`XRP`
- `Binance Options` 作为新的 anchor provider
- 按资产分配不同 routing 策略
- 增加 `OKX` 等次级应急 hedge venue

未来扩展时，允许发生的变化只有：

- 新增 provider 实现
- 新增或修改 routing 配置
- 新增资产级策略参数

未来扩展时，不允许必须重做的东西包括：

- 重写会话状态机
- 重写 `EventPricer` 的接口
- 重做 audit schema
- 把这条 runtime 强行揉回旧的 basis 策略链

## 非目标

- 不把这条策略 retrofit 进现有 `basis` engine
- 不改变现有 `TradePlan` 驱动 basis runtime 的语义
- MVP 不做真钱 live trading
- MVP 不承诺 `SOL/XRP` 已经可交易
- MVP 不做完整 queue reconstruction
- MVP 不要求上来就做完整 volatility surface calibration

## 总体架构

建议新增一条独立的策略栈，暂定名 `pulse_arb`，拥有自己的 runtime 和 session 模型。

推荐的 crate / 模块拆分：

- `polyalpha-data`
  继续负责 Polymarket、Binance perp、options anchor feeds 的行情接入和归一化。
- `polyalpha-pulse` 新 crate
  负责这条策略的 runtime、定价、会话状态机、执行逻辑和审计模型。
- `polyalpha-cli`
  新增独立入口，比如 `paper pulse` 和 `live-mock pulse`。
- `polyalpha-executor`
  只在能复用底层 adapter 的地方复用，不再承担这条策略的上层生命周期管理。

### 核心模块职责

- `AnchorProvider`
  抽象 options anchor 的统一接口。
- `AnchorRouter`
  根据资产把请求路由到对应 provider。
- `EventPricer`
  把归一化后的 anchor 数据和 market rule 映射成公允概率与事件 Greeks。
- `PulseDetector`
  判断一次 Polymarket 偏离在扣掉完整会话摩擦后是否还值得做。
- `SessionFSM`
  管理一笔交易从创建到收尾的完整生命周期。
- `GlobalHedgeAggregator`
  汇总同一账户下所有 session 上报的目标 Delta 暴露，统一计算净 hedge 偏移，避免多 session 在同一资产上互相打架。
- `HedgeExecutor`
  接收 `GlobalHedgeAggregator` 产出的增量 hedge 指令，负责真正向 Binance perp 提交订单。
- `ExitManager`
  管理 Poly maker 止盈、peg、超时追价和平仓。
- `PulseAuditSink`
  写出 session 级别的审计产物和统计摘要。

这个拆法是后面避免大规模重构的核心。

## Provider 和 Routing 模型

这条 runtime 从第一天就必须把 `anchor provider` 和 `asset routing` 做成一等对象。

### Provider Registry

runtime 内部维护一个 provider registry，以逻辑 provider id 为键。

MVP 实例：

- `deribit_primary`

未来实例：

- `binance_options_primary`
- 其他 provider

所有 provider 必须输出同一套归一化结构，这样：

- routing 变化不会影响会话状态机
- 会话状态机不需要关心 anchor 来自 Deribit 还是 Binance Options

### Asset Routing

每个资产都通过 routing 配置明确：

- `anchor_provider`
- `hedge_venue`
- `enabled`
- 可选资产级 override

MVP routing：

- `BTC -> Deribit anchor + Binance perp`
- `ETH -> Deribit anchor + Binance perp`

未来 routing：

- `SOL -> Binance Options anchor + Binance perp`
- `XRP -> Binance Options anchor + Binance perp`

runtime 必须允许某个资产被单独 disable，而不影响其他资产。

## 归一化 Anchor 输出

`AnchorProvider` 不能直接把交易所原始 IV 字段向上抛，而应该统一输出 `AnchorSnapshot`。

`AnchorSnapshot` 至少包含：

- `asset`
- `provider_id`
- `ts_ms`
- `index_price`
- `expiry_ts_ms`
- `atm_iv`
- `local_surface_points`
- `quality_metrics`

每个 `local_surface_point` 至少包含：

- `strike`
- `bid_iv`
- `ask_iv`
- `mark_iv`
- `delta`
- `gamma`
- `best_bid`
- `best_ask`

`quality_metrics` 必须足够支撑“是否暂停交易”的判断。

最低质量维度：

- anchor age
- bid/ask spread quality
- strike coverage
- local liquidity quality
- expiry mismatch
- required Greeks availability

## 定价模型

这条 runtime 定价的是二元事件 claim，不是 vanilla option。

### 支持的 market rule

- `Above K`
- `Below K`
- `Between [K1, K2)`

### 公允概率定义

对任意市场：

- `fair_prob_yes` 表示事件 payout 为 `1` 的风险中性概率
- `fair_prob_no = 1 - fair_prob_yes`

anchor provider 不直接给事件概率。

正确链路应该是：

- provider 提供局部 options 链面
- `EventPricer` 恢复该 expiry 的局部风险中性视图
- market rule 再把这个视图映射成事件概率

### Expiry Mismatch 修正

Deribit 期权到期时刻与 Polymarket 事件结算时刻几乎不会天然完全重合。

因此 `EventPricer` 不能只依赖一个硬性的 `max_expiry_mismatch_minutes` 做全量剔除。

MVP 应采用“两层控制”：

- `soft adjustment`
  对仍在可接受窗口内的 mismatch，做简单的一阶方差桥接修正
- `hard cap`
  当 mismatch 超过硬上限时，直接禁做

推荐的 MVP 口径：

- 用局部 ATM IV 或局部链面估计 `total_variance`
- 当 option expiry 早于事件结算时，用一阶方差外推补上真空期
- 当 option expiry 晚于事件结算时，用一阶方差插值回事件时点

MVP 不要求在这一步做复杂的 local vol / stochastic vol 修正，但必须明确加入 `expiry_gap_adjustment`，否则临近到期时会系统性放大时间错配带来的概率偏差。

### MVP 的数学复杂度控制

MVP 不要求上来就做全局 surface calibration。

MVP 推荐做法：

- 选取与 Polymarket 结算日最接近且满足质量门槛的 expiry
- 取目标 strike 附近若干个 strikes
- 做局部插值，而不是全局拟合
- 对 `Between` 命题，用两个边界 strike 计算区间概率

这样接口和未来更复杂的 surface 模型仍然兼容。

## Delta 和 Rehedge 模型

这条 runtime 不能用 vanilla option 的 `N(d1)` 直接当 binary claim 的 hedge qty。

正确口径是：

- 先把事件价值定义成 `fair_prob_yes`
- 再对事件价值做数值微分

具体做法：

- 计算 `fair_prob_yes(S + dS)`
- 计算 `fair_prob_yes(S - dS)`
- 用中心差分得到：
  - `event_delta_yes`
- `event_delta_no = - event_delta_yes`

目标 hedge 数量定义为：

- `target_hedge_qty = poly_shares * event_delta`

### Rehedge 触发规则

rehedge 不能只看现货价格变了多少，而应该看目标 hedge 漂移了多少。

最少要参考：

- 当前目标 hedge qty
- 当前实际 hedge qty
- delta drift threshold
- 靠近关键 strike 时的 gamma-sensitive zone
- throttle interval

当价格进入 gamma 敏感区时，不能简单理解成“只要更频繁 rehedge 就更安全”。

对于 binary claim，靠近关键 strike 且临近结算时，事件 Delta / Gamma 可能进入极端敏感区。此时如果只是机械地提高 rehedge 频率，会把 runtime 推进 `hedging storm`，导致：

- 频繁在 Binance perp 上来回开平
- 手续费和滑点迅速吞噬 `net_session_edge`
- 账户净暴露并没有得到稳定改善

因此 rehedge 逻辑必须加入显式的 `pin risk suppression`。

MVP 至少应支持：

- `gamma_cap_mode`
  可选模式例如 `delta_clamp`、`protective_only`、`freeze`
- `max_abs_event_delta`
  对目标 Delta 做上限钳制
- `pin_risk_zone_bps`
  定义距离关键 strike 多近才进入 pin risk 区
- `pin_risk_time_window_secs`
  定义离事件结算多近才触发 pin risk 抑制

推荐的 MVP 口径：

- 默认先做 `delta_clamp`
- 当进入 pin risk zone 且估计 Gamma 超过阈值时，不再追求严格 delta-neutral
- 仅允许最小保护性补对冲，或在更极端情况下冻结新增 hedge 调整

如果 pin risk 已经让会话的剩余可实现边际明显恶化，还应允许会话直接进入 `EmergencyFlatten`，而不是继续在关键价位附近做高频无效对冲。

## 入场经济学

入场不能只看一个粗糙的概率差。

runtime 应该计算一个完整会话级的准入量，例如：

- `net_session_edge`

它至少应包含：

- Poly 侧按 top `N` 档算出来的真实入场 VWAP
- Binance hedge 侧的预估可执行成本
- fees
- slippage
- perp/index basis penalty
- 会话期内预估 rehedge reserve
- timeout exit reserve

只有在下面这些条件同时成立时才允许开会话：

- anchor quality 通过
- Poly 和 hedge 数据新鲜度通过
- 必要深度通过
- `net_session_edge > configured_entry_threshold`

## 以会话为中心的 Runtime

这条 runtime 的主对象应该是 `PulseSession`，而不是 basis 风格的 open candidate。

每个 `PulseSession` 至少包含：

- `session_id`
- market / asset 身份
- token side
- entry snapshot
- deadline
- target take-profit state
- current hedge state
- current lifecycle state

### 推荐状态机

- `Idle`
- `PreTradeAudit`
- `PolyOpening`
- `HedgeOpening`
- `MakerExitWorking`
- `Pegging`
- `Rehedging`
- `EmergencyHedge`
- `EmergencyFlatten`
- `ChasingExit`
- `Closed`

### 生命周期摘要

1. `PreTradeAudit`
   冻结本次入场依赖的所有关键 snapshot，并计算最终会话经济学。
2. `PolyOpening`
   用抢脉冲语义去开 Poly 腿。
3. `HedgeOpening`
   Poly 成交确认后，立刻在 Binance perp 补上 hedge。
4. `MakerExitWorking`
   双腿建立后立刻挂 Poly 的 post-only 止盈单。
5. `Pegging`
   当 anchor 和市场波动让原挂单明显失真时，撤旧单、重算、重挂。
6. `Rehedging`
   会话期内当事件 Delta 漂移过大时，微调 Binance hedge。
7. `Emergency*`
   残腿、补腿失败、信息性跳变等危险情况进入应急处理。
8. `ChasingExit`
   到了 deadline 还没 maker 成交，就撤单并 aggressive 平掉。
9. `Closed`
   计算最终经济学并完成审计落盘。

这套状态机必须独立存在，不能借现有 basis runtime 的 close-band 语义来凑。

### Poly Partial Fill 处理

虽然这条策略偏向“全成优先”，但在真实 Polymarket 脉冲里，Poly 开仓仍可能出现部分成交。

因此 `PolyOpening` 不能把“未全成”简单视为边角异常，而必须有明确策略：

- 会话参数里必须定义：
  - `min_opening_notional_usd`
  - 可选 `min_open_fill_ratio`
- `PolyOpening` 必须读取真实 `actual_filled_qty`
- 后续 `HedgeOpening` 必须以 `actual_filled_qty` 为输入，而不是继续沿用 `planned_qty`

推荐口径：

- 如果实际成交量低于 `min_opening_notional_usd`，立即反向平掉已成交 Poly，放弃本次会话
- 如果实际成交量高于最低门槛但未全成：
  - 放弃剩余未成交部分
  - 仅对已成交部分建立 hedge
  - 以真实成交规模更新整个 session 的预算、TP 和后续风控参数

这条规则必须明确写入 `SessionFSM`，不能靠运行时临场猜测。

## 执行语义

### Poly 开仓

Poly 开仓应当采用“全成优先、失败优先”的抢脉冲语义，并显式约束：

- 最大平均成交价
- 最大 shares / budget
- 如果吃不到干净仓位就快速失败

### Hedge 开仓

Binance hedge 腿应采用有界 aggressive 执行。

MVP 推荐：

- aggressive limit + `IOC`

原因：

- 这类短周期 edge 更怕无界滑点，而不是怕少一点成交确定性
- 也避免把不存在的 `MARKET + IOC` 误写成一种独立 API 语义

### Maker Exit

默认第一退出路径始终是：

- 在 Poly 挂 post-only maker 止盈单

### Timeout Exit

到了 deadline：

- 撤销工作中的 maker 单
- 进入 aggressive exit
- 优先释放风险和资金，不再等更好成交

### 账户级 Hedge 冲突仲裁

单笔 `PulseSession` 可以拥有自己的目标 Delta 暴露，但不能各自直接向 Binance perp 下 hedge 单。

原因：

- 同一资产可能同时存在多个 session
- 不同 session 的目标 Delta 方向可能相反
- 如果每个 session 各自下单，账户会出现“内部对冲打架”，白送手续费和滑点

因此必须引入账户级的：

- `GlobalHedgeAggregator`

它的职责是：

- 接收每个 session 上报的 `target_delta_exposure`
- 在账户层汇总出同一资产的 `net_target_delta`
- 与账户当前实际 hedge 仓位比较
- 只向 `HedgeExecutor` 发净增量订单

每个 session 看到的是：

- 自己的目标暴露
- 自己被分摊到的 hedge attribution

真正向交易所发单的动作，必须统一由 `GlobalHedgeAggregator -> HedgeExecutor` 路径完成。

这条设计从 MVP 就要建立，不然后面一旦允许同资产多 session，就会被迫做架构级返工。

## 配置模型

这条 runtime 必须有独立配置域，例如：

- `[strategy.pulse_arb]`

建议配置组：

- `runtime`
- `session`
- `entry`
- `rehedge`
- `pin_risk`
- `providers`
- `routing`
- `asset_policy`

### 配置必须具备的能力

- 策略级 enable/disable
- 资产级 enable/disable
- provider 级质量门槛
- 按资产 routing
- 按资产的会话和经济学 override
- 显式最大持仓时间
- 显式 timeout exit mode
- 显式 rehedge 阈值和 throttle
- 显式 pin risk suppression 模式和阈值
- 显式 expiry gap adjustment 模式、soft mismatch window 和 hard cap
- 显式全局 hedge 聚合的净额阈值、最小下单单位和 reconcile 节流
- 显式 partial fill 最低成交门槛

MVP 的实际行为可以很窄，但配置 schema 一开始就必须足够宽。

## 可观测性和审计

这条 runtime 必须是 session-centric observability，而不是只记录订单事件。

### 主审计对象

建议引入独立的会话级审计对象，例如：

- `PulseSessionRecord`

它至少包含：

- `session_header`
- `entry_snapshot`
- `lifecycle_events`
- `final_economics`

### 入场时延指标

审计层必须记录原始 timing sample，而不是只记录汇总分数。

必记字段：

- `anchor_ts_ms`
- `anchor_recv_ts_ms`
- `anchor_latency_ms`
- `poly_book_ts_ms`
- `poly_book_recv_ts_ms`
- `poly_latency_ms`
- `hedge_book_ts_ms`
- `hedge_book_recv_ts_ms`
- `hedge_latency_ms`
- `decision_ts_ms`
- `decision_skew_ms`
- `submit_poly_order_ts_ms`
- `submit_delay_ms`
- `poly_fill_latency_ms`
- `hedge_fill_latency_ms`

离线报表再基于这些原始字段去算：

- latency percentiles
- 分桶胜率
- information lag entropy

`information lag entropy` 更适合作为离线聚合指标名，而不是单笔字段名。

### Maker 质量指标

MVP 应当先上稳定可落地的代理指标，而不是一开始就宣称能精确重建 queue rank。

必记代理字段：

- `tp_post_price`
- `tp_post_ts_ms`
- `distance_to_mid_bps_at_post`
- `distance_to_touch_ticks_at_post`
- `relative_order_age_ms`
- `peg_count`
- `cancel_replace_count`
- `maker_fill_wait_ms`
- `deadline_hit`
- `post_tp_adverse_move_bps_1s`
- `post_tp_adverse_move_bps_5s`
- `fill_then_reversal_flag`

### 为未来 Queue Reconstruction 预留原始数据

虽然 MVP 只上代理指标，但 audit 层必须保留足够的原始 tape，保证未来要做 queue reconstruction 时不需要重做采集链路。

至少保留：

- post / cancel / replace / fill 边界时刻的 Poly top-`N` book snapshot
- maker 挂单存活期间的相关 Poly book delta
- maker 挂单存活期间的相关 trade summary
- 每次 peg 前后的局部盘口上下文

这是必须写进设计里的 future-proofing 约束。

### Partial Fill 与 Hedge Attribution 审计

由于会话允许 Poly partial fill、账户又采用全局 hedge 聚合，因此 audit 必须明确记录：

- `planned_poly_qty`
- `actual_poly_filled_qty`
- `actual_poly_fill_ratio`
- `session_target_delta_exposure`
- `session_allocated_hedge_qty`
- `account_net_target_delta_before_order`
- `account_net_target_delta_after_order`

否则后面无法判断：

- 是 Poly 部分成交把会话缩小了
- 还是全局 hedge 聚合把 session 的理论 hedge 稀释了

### 失败原因码

runtime 必须输出机器可读的 failure reason code。

最少包含：

- `anchor_stale`
- `anchor_surface_insufficient`
- `anchor_expiry_mismatch`
- `poly_depth_insufficient`
- `hedge_depth_insufficient`
- `poly_open_rejected`
- `hedge_open_rejected`
- `maker_post_rejected`
- `peg_rate_limited`
- `rehedge_blocked`
- `deadline_exit_triggered`
- `emergency_flatten_triggered`
- `asset_disabled`
- `hedge_disconnect`
- `poly_disconnect`

## Monitor 模型

这条策略应当有自己的 monitor 视图，不应该只是 basis 面板改几个字段名。

建议至少有三块：

- active session list
- single-session detail
- asset health panel

### Active Session List

每一行至少展示：

- symbol
- side
- state
- 剩余持仓时间
- 当前 delta drift
- 当前 session net pnl
- 当前 maker 工作状态

### Session Detail

至少展示：

- entry snapshot
- anchor provider 和质量
- 当前 TP 价格
- peg 历史
- rehedge 历史
- pin risk 抑制状态
- timeout countdown
- emergency 状态历史

### Asset Health

至少展示：

- enabled / degraded / disabled
- 当前 anchor provider
- 当前 anchor quality summary
- 当前 disable reason
- 最近 session failure rate
- timeout ratio
- 平均 rehedge count

## 健康、降级和禁用语义

每个资产必须支持独立的运行状态：

- `Enabled`
- `Degraded`
- `Disabled`

### Degraded

表示：

- 继续观察
- 但以更保守的参数交易

触发例子：

- anchor 质量弱但没坏透
- timeout exit 比例过高
- rehedge drag 持续过高

### Disabled

表示：

- 该资产停止新开会话

触发例子：

- anchor 断开或严重 stale
- strike coverage 不足
- hedge venue 不可用
- 严重残腿事件过多

没有 routing 明确允许的情况下，runtime 不允许 silent fallback 到弱锚或替代 provider。

## 与现有基础设施的复用边界

这条 runtime 可以复用：

- 行情归一化 feeds
- 共享 market metadata 和 market-rule parsing
- 底层 order adapter
- 共享 audit / artifact 框架
- 共享 monitor socket 基础设施

这条 runtime 不应继承现有 basis 的：

- signal 语义
- exit-band 语义
- planner truth model
- monitor 词汇体系

## MVP 完成标准

满足下面这些条件时，MVP 才算完成：

- `BTC` 和 `ETH` 能在 `paper` 和 `live-mock` 中稳定跑起来
- runtime 对这两个资产统一使用 `Deribit` 作为 options anchor
- runtime 统一使用 `Binance USD-M perp` 做 hedge
- 每一笔交易都以完整 `PulseSession` 表达
- Poly partial fill 能按 `actual_filled_qty` 正确缩放后续会话逻辑
- pin risk / gamma storm 有显式抑制模式，而不是只会提高 rehedge 频率
- 同资产多 session 的 hedge 冲突由 `GlobalHedgeAggregator` 统一仲裁
- maker exit、pegging、rehedge、timeout、emergency flatten 都能在 audit 中回放
- monitor 能展示 active sessions 和 asset health
- audit schema 已经保留足够原始 tape，未来可升级到 queue reconstruction
- config schema 已经具备 future provider / routing 扩展位

## 未来扩展路径

MVP 后推荐按下面顺序扩：

1. 新增 `BinanceOptionsAnchorProvider`
2. 为未来资产如 `SOL`、`XRP` 增加 routing
3. 增加资产级质量和经济学 override
4. 视需要增加 secondary emergency hedge venue
5. 视需要从 maker 代理指标升级到 queue reconstruction analytics

扩展时不应改变：

- `PulseSession`
- `SessionFSM`
- `GlobalHedgeAggregator`
- `EventPricer` 接口
- `AnchorProvider` 接口
- audit schema 总体形状

## 涉及区域

- 新 crate：`crates/polyalpha-pulse`
  负责 pulse runtime、定价、状态机、执行和 audit 模型。
- `crates/polyalpha-data`
  补齐 Deribit options 和 Binance perp 所需的行情归一化支持。
- `crates/polyalpha-core`
  只放真正跨 runtime 共享的配置和类型。
- `crates/polyalpha-cli`
  增加策略 runtime 入口、monitor 集成和配置加载。
- 存储 / monitor plumbing
  复用共享框架，但新增 pulse 专属 schema 和视图。
