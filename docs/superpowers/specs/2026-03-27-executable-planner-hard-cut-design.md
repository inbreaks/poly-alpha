# 可执行 Planner 硬切重构设计

本文档定义 `poly-alpha` 交易链从“mid-based 候选直接驱动执行”迁移到“planner 产出的 `TradePlan` 作为唯一交易真相”的硬切方案。该文档是本次重构的事实源，后续实现、测试、审计和回放不得偏离本文口径。

## 1. 背景与问题定义

当前链路存在三套彼此不一致的交易语义：

- 信号层：
  [`crates/polyalpha-engine/src/lib.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-engine/src/lib.rs) 按 `mid` 计算 `poly_target_shares`、`cex_hedge_qty` 和 `expected_pnl`。
- 预览层：
  [`crates/polyalpha-cli/src/paper.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-cli/src/paper.rs) 会在开平仓前做 `preview_requests_for_signal(...)`，并按实时订单簿逐档估算 executable price。
- 执行层：
  [`crates/polyalpha-executor/src/dry_run.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-executor/src/dry_run.rs) 和 [`crates/polyalpha-executor/src/manager.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-executor/src/manager.rs) 按真实订单簿吃单，并叠加额外滑点。

这导致当前系统存在以下系统性问题：

1. `expected_pnl` 是 mid-based 理论值，不是 executable-based 交易真相。
2. Poly 开仓语义仍然是“固定 shares 的市价单”，不是“预算受限的 capped order”。
3. CEX hedge 数量来自候选信号，不是来自最终可执行的 Poly 计划或实际成交。
4. 预览层只是事后否决器，不是交易计划生成器。
5. `paper`、`sim`、`backtest`、`live` 没有被同一个 planner 统一，容易继续产生口径漂移。

## 2. 目标与非目标

目标：

- 将“看到的机会”和“能成交的机会”统一到一套数据模型。
- 让 planner 成为唯一的执行定价器，而不是事后 rejection gate。
- 用预算受限的 Poly 计划取代固定 shares 市价单。
- 让 CEX hedge、delta rebalance、residual recovery 都基于计划或实际成交重算。
- 让 `paper`、`sim`、`backtest`、`live` 共享同一 planner 逻辑。
- 让审计流同时展示理论边际、计划边际、实际边际和摩擦拆分。

非目标：

- 本次不重构 DMM 普通挂单逻辑。
- 本次不引入跨 venue 的伪原子撮合承诺。
- 本次不保留旧 `ArbSignalEvent -> executor` 兼容入口。

## 3. 核心决策

### 3.1 唯一真相边界

本次硬切后，链路拆成四层：

- `OpenCandidate`
  - 只表达 alpha 机会。
  - 不允许携带任何可执行字段。
- `PlanningIntent`
  - 统一表达“系统现在想做什么”。
  - 覆盖开仓、平仓、rebalance 和 residual recovery。
- `TradePlan`
  - 唯一交易计划真相。
  - 只有 planner 可以产出。
- `ExecutionResult`
  - 唯一成交真相。
  - 只记录实际 fills、实际成本和实际 residual。

原则：

- 只有 `TradePlan` 能定义“准备怎么交易”。
- 只有 `ExecutionResult` 能定义“实际上发生了什么”。
- executor 不再接受 `OpenCandidate`、旧 signal 或任何中间推导字段作为执行入口。

### 3.2 执行原子性语义

跨 venue 不存在真正原子成交，因此采用以下默认执行语义：

- Polymarket 是主腿。
- CEX 是跟随腿。
- 真实风险以实际成交为准。
- 任何超阈值 residual 都必须进入 planner 驱动的 recovery。

该语义适用于：

- `OpenPosition`
- `ClosePosition`
- `DeltaRebalance`
- `ResidualRecovery`

### 3.3 迁移策略

本次采用直接硬切：

- 删除旧的 `ArbSignalEvent` 可执行语义。
- 不保留 `candidate -> legacy signal -> executor` 兼容路径。
- 所有开平仓、rebalance、recovery 都统一进入 `PlanningIntent -> TradePlan -> ExecutionResult` 链路。

## 4. 新数据流

新链路固定为：

- WS 行情
- 对齐后的订单簿快照
- `OpenCandidate`
- `PlanningIntent`
- `ExecutionPlanner`
- `TradePlan`
- `ExecutionCoordinator`
- `ExecutionResult`

其中：

- `engine` 只负责产出 `OpenCandidate`。
- 持仓/风控/到期规则负责产出非 alpha 类 `PlanningIntent`。
- planner 基于实时订单簿和风险约束生成 `TradePlan`。
- executor 只负责执行和状态推进。
- 任何 fills 偏离计划时，必须显式回到 planner。

## 5. 模块边界与落点

### 5.1 `crates/polyalpha-engine`

保留职责：

- fair value
- raw mispricing
- token delta 估计
- z-score 或其他机会评分
- 机会发现和候选排序

删除职责：

- `poly_target_shares`
- `poly_target_notional`
- `cex_hedge_qty`
- `expected_pnl`

产出对象：

- `OpenCandidate`

建议落点：

- 将现有 signal 构造逻辑从 [`crates/polyalpha-engine/src/lib.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-engine/src/lib.rs) 中剥离。
- 保留 basis snapshot 能力，但不再把 snapshot 直接解释成可执行订单参数。

### 5.2 新增 `ExecutionPlanner`

建议新建模块：

- [`crates/polyalpha-executor/src/planner.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-executor/src/planner.rs)

原因：

- planner 与执行预览、订单簿吃单、实际恢复路径强相关。
- 放在 executor crate 内更方便共享 orderbook 和执行约束，但必须保持逻辑纯函数化，避免依赖运行时副作用。

planner 的外部接口建议为：

- `plan(intent, context) -> Result<TradePlan, PlanRejection>`
- `revalidate(plan, context) -> RevalidationResult`
- `replan_from_execution(plan, actual_fills, context) -> Result<TradePlan, PlanRejection>`

### 5.3 `crates/polyalpha-executor`

执行侧改造目标：

- `ExecutionManager` 不再从旧 signal 生成 preview orders。
- `ExecutionManager` 只接收 `TradePlan`。
- `TradePlan` 的 Poly 主腿先执行。
- 依据 Poly 的实际 fill 生成或更新 CEX 跟随计划。
- `DeltaRebalance` 和 `ResidualRecovery` 也通过 planner 输出标准 plan。

### 5.4 `crates/polyalpha-cli`

`paper`、`sim`、monitor、audit 统一围绕新对象工作：

- 展示 `OpenCandidate`
- 展示 `PlanningIntent`
- 展示 `TradePlan`
- 展示 `ExecutionResult`

不得再把某个单一字段继续叫 `expected_pnl` 并暗示其为真相。

## 6. 新核心类型设计

### 6.0 共享版本规则

所有新的顶层对象都必须携带 `schema_version`，至少包括：

- `OpenCandidate`
- `PlanningIntent`
- `TradePlan`
- `ExecutionResult`
- socket message 中对应的新事件载体
- runtime residual snapshot
- 持久化 audit / recovery / execution session 记录

不兼容迁移策略固定为：

- 读取到兼容版本时，按正常恢复流程恢复。
- 读取到不兼容版本时，不做“猜测式兼容恢复”。
- 不兼容对象只允许归档、跳过并记录结构化告警。
- residual state、旧 audit session、旧 socket consumer 都不得因为不兼容 schema 被强行映射回新对象。

### 6.1 `OpenCandidate`

字段：

- `schema_version`
- `candidate_id`
- `correlation_id`
- `symbol`
- `token_side`
- `direction`
- `fair_value`
- `raw_mispricing`
- `delta_estimate`
- `risk_budget_usd`
- `strength`
- `z_score`
- `timestamp_ms`

约束：

- 不包含 shares sizing。
- 不包含 CEX hedge qty。
- 不包含 executable pnl。

### 6.2 `PlanningIntent`

统一枚举建议为：

- `OpenPosition`
- `ClosePosition`
- `DeltaRebalance`
- `ResidualRecovery`
- `ForceExit`

共享字段：

- `schema_version`
- `intent_id`
- `correlation_id`
- `symbol`

各变体最小字段：

- `OpenPosition`
  - `candidate`
  - `max_budget_usd`
  - `max_residual_delta`
  - `max_shock_loss_usd`
- `ClosePosition`
  - `symbol`
  - `close_reason`
  - `target_close_ratio`
- `DeltaRebalance`
  - `symbol`
  - `target_residual_delta_max`
  - `target_shock_loss_max`
- `ResidualRecovery`
  - `symbol`
  - `residual_snapshot`
  - `recovery_reason`
- `ForceExit`
  - `symbol`
  - `force_reason`
  - `allow_negative_edge`

### 6.3 `PlanningContext`

planner 必须显式接收上下文，禁止隐式读取全局状态。上下文至少包括：

- canonical Poly yes/no 订单簿快照
- canonical CEX 订单簿快照
- 当前持仓快照
- 当前未终态订单
- residual session 快照
- 市场规则与 settlement phase
- 风险参数
- 当前时间
- 数据 freshness 与 cross-leg skew 检查结果

确定性与 canonicalization 规则固定为：

- planner 只接受 canonicalized context，不直接接受各端自己的原始 book 表示。
- canonical orderbook 的构造规则固定为：
  - 删除零数量和负数量档位。
  - 同价位数量先聚合。
  - `bids` 按价格从高到低排序，`asks` 按价格从低到高排序。
  - 每边只保留固定深度 `planner_depth_levels`，该深度值必须显式进入 context。
- 经济时序以 `exchange_timestamp_ms` 为主，`received_at_ms` 只用于 freshness 判断和并列 tie-break。
- sequence 裁决规则固定为：
  - 同一 instrument 只接受最大 `sequence` 的快照。
  - 若同一 `sequence` 下 book 内容不同，直接拒绝，返回 `book_conflict_same_sequence`。
  - 若新快照 `sequence` 倒退，直接拒绝，返回 `stale_book_sequence`。
- one-sided / missing book 不允许进入 planner 正常路径：
  - Poly 缺失返回 `missing_poly_book`
  - CEX 缺失返回 `missing_cex_book`
  - Poly 单边返回 `one_sided_poly_book`
  - CEX 单边返回 `one_sided_cex_book`
- v1 中 `fair_value` 和 `delta_estimate` 由 `OpenCandidate` 提供，并被视为 planner 的不可变输入；planner 不在执行快照上重算它们。
- 若未来需要在 planner 内重算 `fair_value` 或 `delta_estimate`，必须作为新的 schema 版本显式引入，而不是在现有 `PlanningContext` 下偷偷改变语义。

### 6.4 `TradePlan`

`TradePlan` 是唯一计划真相，字段至少包括：

- 标识
  - `schema_version`
  - `plan_id`
  - `parent_plan_id`
  - `supersedes_plan_id`
  - `idempotency_key`
  - `correlation_id`
  - `symbol`
  - `intent_type`
  - `priority`
  - `created_at_ms`
- 快照绑定
  - `poly_exchange_timestamp_ms`
  - `poly_received_at_ms`
  - `poly_sequence`
  - `cex_exchange_timestamp_ms`
  - `cex_received_at_ms`
  - `cex_sequence`
  - `plan_hash`
- Poly 计划
  - `poly_side`
  - `poly_token_side`
  - `poly_sizing_mode`
  - `poly_requested_shares`
  - `poly_planned_shares`
  - `poly_max_cost_usd`
  - `poly_max_avg_price`
  - `poly_max_shares`
  - `poly_min_avg_price`
  - `poly_min_proceeds_usd`
  - `poly_book_avg_price`
  - `poly_executable_price`
  - `poly_friction_cost_usd`
  - `poly_fee_usd`
- CEX 计划
  - `cex_side`
  - `cex_planned_qty`
  - `cex_book_avg_price`
  - `cex_executable_price`
  - `cex_friction_cost_usd`
  - `cex_fee_usd`
- 经济性
  - `raw_edge_usd`
  - `planned_edge_usd`
  - `expected_funding_cost_usd`
  - `residual_risk_penalty_usd`
- 风险
  - `post_rounding_residual_delta`
  - `shock_loss_up_1pct`
  - `shock_loss_down_1pct`
  - `shock_loss_up_2pct`
  - `shock_loss_down_2pct`
- 有效性
  - `plan_ttl_ms`
  - `max_poly_sequence_drift`
  - `max_cex_sequence_drift`
  - `max_poly_price_move`
  - `max_cex_price_move`
  - `min_planned_edge_usd`
  - `max_residual_delta`
  - `max_shock_loss_usd`
  - `max_plan_vs_fill_deviation_usd`

### 6.5 `ExecutionResult`

字段：

- `schema_version`
- `plan_id`
- `correlation_id`
- `symbol`
- `status`
- `poly_order_ledger`
- `cex_order_ledger`
- `actual_poly_cost_usd`
- `actual_cex_cost_usd`
- `actual_poly_fee_usd`
- `actual_cex_fee_usd`
- `actual_funding_cost_usd`
- `realized_edge_usd`
- `plan_vs_fill_deviation_usd`
- `actual_residual_delta`
- `actual_shock_loss_up_1pct`
- `actual_shock_loss_down_1pct`
- `actual_shock_loss_up_2pct`
- `actual_shock_loss_down_2pct`
- `recovery_required`
- `timestamp_ms`

每腿执行账本至少记录：

- `order_id`
- `client_order_id`
- `requested_qty`
- `filled_qty`
- `cancelled_qty`
- `avg_price`
- `fee_usd`
- `exchange_timestamp_ms`
- `received_at_ms`
- `status`

## 7. Planner 语义

### 7.1 Poly 从固定 shares 改为受约束计划

planner 必须将 Poly 开平仓改为受约束 order 语义，而不是固定 shares 市价语义。

规则：

- 买单 / 开仓必须使用：
  - `BuyBudgetCap { max_cost_usd, max_avg_price, max_shares }`
- 卖单 / 平仓必须使用以下两种约束语义之一：
  - `SellExactShares { shares, min_avg_price }`
  - `SellMinProceeds { shares, min_proceeds_usd }`
- planner 必须根据 intent 选择具体语义：
  - `OpenPosition` 默认走 `BuyBudgetCap`
  - `ClosePosition`、`ForceExit` 和大多数 `ResidualRecovery` 默认走 `SellExactShares` 或 `SellMinProceeds`
- 买单路径中，planner 先按订单簿逐档吃出预算内最大可成交量，再叠加额外滑点，得到最终 executable price。
- 卖单路径中，planner 必须以明确的 `shares` 为输入，同时验证：
  - 平均成交价不低于 `min_avg_price`；或
  - 总成交收入不低于 `min_proceeds_usd`
- `poly_planned_shares` 必须同时满足：
  - 不超过约束语义允许的成本、收入或 shares 边界
  - 不超过当前可成交深度
  - 不超过市场 phase 允许的风险暴露

结论：

- 买单路径中 shares 是 planner 的输出。
- 卖单 / 平仓路径中 shares 是 planner 的输入约束之一，但成交价格和收入仍由 planner 校验。
- `PolyOrderRequest` 需要支持互斥 sizing 语义，不能继续容忍“固定 shares + quote_notional 同时存在”的模糊模式。

### 7.2 CEX qty 必须基于计划 shares 或实际成交重算

规则：

- 开仓 plan 中的 `cex_planned_qty` 基于 `poly_planned_shares`、delta 估计和 CEX 步长重算。
- 执行中，CEX 跟随量必须基于 Poly 实际 fill 重算。
- 执行后 residual delta 必须按“步长舍入后的真实持仓”计算。

禁止：

- 禁止继承候选信号中的旧 hedge 数字。
- 禁止使用 Poly 理论目标 shares 直接驱动实际 CEX 补腿。

### 7.3 经济性必须 executable-based

planner 需要同时产出：

- `raw_edge_usd`
  - 给候选解释用。
- `planned_edge_usd`
  - 作为是否允许交易的唯一经济性闸门。
- `realized_edge_usd`
  - 留给执行结果和审计。

`planned_edge_usd` 必须纳入：

- Poly 吃簿成本
- Poly 额外滑点
- Poly fee
- CEX 吃簿成本
- CEX 额外滑点
- CEX fee
- 预期 funding carry
- 舍入后的 residual 风险惩罚

`realized_edge_usd` 必须纳入：

- 实际 Poly 吃簿与滑点偏差
- 实际 CEX 吃簿与滑点偏差
- 实际 Poly fee
- 实际 CEX fee
- 实际 funding 成本
- `plan_vs_fill_deviation_usd`

原则：

- 若 `planned_edge_usd <= 0`，planner 必须拒绝生成计划。
- `ForceExit` 是唯一允许绕过正边际闸门的路径，但仍不得绕过 residual 风险约束。

### 7.4 风险检查进入 planner，而不是事后指标

所有计划都必须在 planner 内完成以下检查：

- `post_rounding_residual_delta <= threshold`
- 预设冲击场景损失 `<= threshold`

若不满足：

- planner 先尝试缩小 `poly_planned_shares`
- 若缩单后仍不满足，则拒绝生成计划

## 8. 执行与状态机

### 8.1 intent 优先级、抢占与幂等规则

同一 `symbol` 的固定 intent 优先级为：

- `ForceExit`
- `ResidualRecovery`
- `ClosePosition`
- `DeltaRebalance`
- `OpenPosition`

硬规则：

- 同一 `symbol` 同时只允许一个 in-flight `TradePlan`。
- 更高优先级 intent 到达时，必须抢占低优先级 plan，并通过 `supersedes_plan_id` 显式标记。
- 低优先级 intent 在高优先级 plan 活跃时必须排队或直接拒绝，不能并行穿透。
- 相同 `idempotency_key` 的重复请求必须返回同一 plan 或同一终态，不得重复建计划。
- `parent_plan_id` 用于表达由实际 fill、revalidate 或 recovery 触发的子计划关系。

### 8.2 统一状态

每个 `symbol` 至少维护以下状态：

- `Idle`
- `PlanReady`
- `SubmittingPoly`
- `HedgingCex`
- `VerifyingResidual`
- `Recovering`
- `Frozen`
- `CloseOnly`

### 8.3 状态迁移

1. `Idle -> PlanReady`
   - 收到 `PlanningIntent`
   - planner 成功生成 `TradePlan`
2. `PlanReady -> SubmittingPoly`
   - 执行前 `revalidate(plan, context)` 通过
3. `SubmittingPoly -> HedgingCex`
   - Poly 有实际 fill
   - 基于实际 fill 生成或更新 CEX 跟随计划
4. `HedgingCex -> VerifyingResidual`
   - CEX 跟随完成、部分完成或超时
5. `VerifyingResidual -> Idle`
   - residual delta 和 shock loss 回到阈值内
6. `VerifyingResidual -> Recovering`
   - residual 超阈值，或跟随失败
7. `Recovering -> Idle`
   - recovery 收敛
8. `Recovering -> Frozen`
   - recovery 超时、超重试、风险持续超阈值或 market phase 阻止继续恢复

### 8.4 计划重验

executor 执行前必须做轻量重验，检查：

- plan ttl
- orderbook freshness
- sequence drift
- 顶档或均价变化
- `planned_edge_usd`
- residual 风险上限

若任意项失败：

- 当前 plan 作废
- 回到 planner 重新生成 plan
- 禁止使用旧 plan 硬打

## 9. Recovery 语义

residual recovery 不再是 executor 内部的特殊分支，而是标准 `PlanningIntent::ResidualRecovery -> TradePlan` 流程。

planner 必须在两种恢复路线之间做选择：

- `补 CEX`
- `反向 flatten Poly`

选择标准固定为：

- 哪条路线 executable 成本更低
- 哪条路线更快回到 residual 阈值内
- 当前 market phase 是否允许执行
- 当前未终态订单是否已满足 ghost-order guard

任何 unresolved residual 都必须：

- 冻结该 symbol 的新开仓
- 保持 `reconcile-only`
- 只允许 recovery、reduce-only 或 force-exit 路径

## 10. 事件、审计与监控字段

### 10.1 事件流

建议引入新的事件体系：

- `CandidateEvent`
- `IntentEvent`
- `TradePlanEvent`
- `ExecutionResultEvent`
- `RecoveryEvent`

### 10.2 审计字段

审计必须能同时展示：

- `schema_version`
- `fair_value`
- `raw_mispricing`
- `raw_edge_usd`
- `planned_edge_usd`
- `realized_edge_usd`
- `poly_sizing_mode`
- `poly_max_cost_usd`
- `poly_max_avg_price`
- `poly_min_avg_price`
- `poly_min_proceeds_usd`
- `poly_planned_shares`
- `cex_planned_qty`
- `poly_friction_cost_usd`
- `cex_friction_cost_usd`
- `poly_fee_usd`
- `cex_fee_usd`
- `expected_funding_cost_usd`
- `actual_funding_cost_usd`
- `plan_vs_fill_deviation_usd`
- `post_rounding_residual_delta`
- `shock_loss_*`
- `poly_exchange_timestamp_ms`
- `poly_received_at_ms`
- `poly_sequence`
- `cex_exchange_timestamp_ms`
- `cex_received_at_ms`
- `cex_sequence`
- `plan_hash`
- `parent_plan_id`
- `supersedes_plan_id`
- `idempotency_key`
- `plan_rejection_reason`
- `revalidation_failure_reason`
- `recovery_decision_reason`

### 10.3 monitor 展示

monitor 不再只显示一个很大的 `expected_pnl`。至少需要同时展示：

- 理论边际
- 计划边际
- 实际边际
- Poly 吃簿成本
- CEX 摩擦成本
- Poly/CEX fee
- funding 成本
- 计划与实际偏差
- 计划 residual delta
- 实际 residual delta
- 当前 plan 是否失效
- 当前 session 状态和 recovery deadline

### 10.4 机器可读 reason taxonomy

必须定义稳定的机器可读枚举，并将“稳定 code”和“中文展示文案”分离：

- `PlanRejectionReason`
- `RevalidationFailureReason`
- `RecoveryDecisionReason`

最低要求：

- code 在审计、事件流、monitor 和回放中稳定可比。
- 中文文案只用于展示层，不得作为业务判断依据。
- 所有 `rejection / revalidation / recovery` 分支都必须产出对应 reason code。

v1 至少覆盖以下 code：

- `PlanRejectionReason`
  - `missing_poly_book`
  - `missing_cex_book`
  - `one_sided_poly_book`
  - `one_sided_cex_book`
  - `stale_book_sequence`
  - `book_conflict_same_sequence`
  - `non_positive_planned_edge`
  - `insufficient_poly_depth`
  - `insufficient_cex_depth`
  - `poly_max_price_exceeded`
  - `poly_min_proceeds_not_met`
  - `residual_delta_too_large`
  - `shock_loss_too_large`
  - `market_phase_disallows_intent`
  - `higher_priority_plan_active`
  - `adapter_cannot_preserve_constraints`
- `RevalidationFailureReason`
  - `plan_ttl_expired`
  - `poly_sequence_drift_exceeded`
  - `cex_sequence_drift_exceeded`
  - `poly_price_move_exceeded`
  - `cex_price_move_exceeded`
  - `planned_edge_deteriorated`
  - `residual_risk_deteriorated`
  - `plan_superseded`
  - `book_missing_on_revalidate`
  - `book_conflict_on_revalidate`
- `RecoveryDecisionReason`
  - `cex_top_up_cheaper`
  - `poly_flatten_cheaper`
  - `cex_top_up_faster`
  - `poly_flatten_only_safe_route`
  - `ghost_order_guard_blocks_flatten`
  - `market_phase_blocks_poly`
  - `cex_depth_insufficient`
  - `poly_depth_insufficient`
  - `force_exit_required`

## 11. 对现有类型与接口的硬切要求

### 11.1 删除旧可执行 signal 语义

需要重构或移除：

- [`crates/polyalpha-core/src/types/signal.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-core/src/types/signal.rs) 中旧的 `BasisLong` / `BasisShort` 可执行字段
- `expected_pnl`
- `poly_target_shares`
- `poly_target_notional`
- `cex_hedge_qty`

### 11.2 重构 `OrderRequest`

Poly 下单必须从模糊的 `shares + quote_notional + optional limit_price` 组合，升级成显式的互斥 sizing 语义，例如：

- `BuyBudgetCap`
- `SellExactShares`
- `SellMinProceeds`

要求：

- dry-run、paper、sim、backtest、live 的 Poly 下单都走同一 sizing 语义
- 预算受限单必须在逐档吃簿时真正受 cap 约束

### 11.3 live adapter 降级约束

若交易所 API 不原生支持 planner 语义，adapter 只能做“保持约束不变”的降级，不允许做“静默放宽约束”的降级。

固定规则：

- 若 venue 不原生支持 `BuyBudgetCap`，adapter 只能降级成能继续满足 `max_cost_usd`、`max_avg_price`、`max_shares` 的 `IOC/FOK` 限价组合。
- 若 venue 不原生支持 `SellExactShares` 或 `SellMinProceeds`，adapter 只能降级成能继续满足 `shares`、`min_avg_price` 或 `min_proceeds_usd` 的受约束 `IOC/FOK` 限价组合。
- 若 adapter 无法严格保持这些边界，必须返回 `adapter_cannot_preserve_constraints`。
- 任何 adapter 都不得静默退回 share-driven market order。

### 11.4 schema version 与持久化迁移

必须对以下对象显式维护 `schema_version`：

- `OpenCandidate`
- `PlanningIntent`
- `TradePlan`
- `ExecutionResult`
- socket message
- runtime residual snapshot
- persisted audit session
- persisted recovery state

固定迁移规则：

- 同版本或明确兼容版本允许恢复。
- 不兼容版本只归档，不恢复。
- 旧 residual state 不得被强制映射成新 planner state。
- 旧 socket consumer 收到不兼容版本时，必须显式拒绝或降级为只读观察，不能假装理解。

### 11.5 重构 `ExecutionManager`

当前 `preview_requests_for_signal(...)` 这类接口需要被替换为围绕 `TradePlan` 的接口，例如：

- `preview_plan(...)`
- `submit_plan(...)`
- `advance_plan_state(...)`
- `recover_plan(...)`

## 12. 一致性验证要求

### 12.1 单一 planner

必须满足：

- 相同 `PlanningIntent + PlanningContext` 在 `paper`、`sim`、`backtest`、`live` 下产出相同 `TradePlan`
- 不允许各端复制自己的 executable pricing
- 相同 canonicalized context 必须产出相同 `plan_hash`

### 12.2 dry-run 语义修正

[`crates/polyalpha-executor/src/dry_run.rs`](/Users/le/Documents/source-code/poly-alpha/crates/polyalpha-executor/src/dry_run.rs) 当前对 `quote_notional` 的处理不足以表达真正的 capped order 语义，必须重构为：

- 能按预算 cap 反推出最大可成交 shares
- 能在预算不足时自然截断
- 能在部分成交场景下正确产出实际 fill 和 residual
- 能覆盖 `SellExactShares` 和 `SellMinProceeds` 的收入边界校验

### 12.3 必测场景

至少覆盖：

- 预算 cap 不被突破
- 卖单 `min_avg_price` / `min_proceeds_usd` 被严格满足
- `planned_edge_usd <= 0` 时拒单
- 步长舍入后的 residual delta 正确
- shock risk 超阈值时自动缩单或拒单
- Poly 部分成交后 CEX qty 按实际 fill 重算
- revalidate 失败时丢弃旧 plan 并重算
- residual recovery 在“补 CEX / flatten Poly”之间做正确选择
- 相同 canonical context 产出相同 `TradePlan`
- one-sided / missing book 返回稳定 rejection code
- adapter 无法保持约束时显式 reject，禁止静默降级
- 不兼容 schema 版本对象被归档而非恢复
- `Open -> partial fill -> hedge fail -> recovery -> resolved`
- `Close -> partial exit -> replan -> resolved`
- `DeltaRebalance -> rounded residual -> accepted/rejected`
- `ForceExit -> negative edge allowed but risk bounded`

## 13. 实施顺序

硬切实施顺序固定为：

1. 在 `polyalpha-core` 引入新类型：
   - `OpenCandidate`
   - `PlanningIntent`
   - `TradePlan`
   - `ExecutionResult`
2. 为新对象、socket message 和 residual snapshot 加入 `schema_version`。
3. 删除旧 `ArbSignalEvent` 的可执行字段和下游依赖。
4. 在 `polyalpha-engine` 改为产出 `OpenCandidate`。
5. 在 `polyalpha-executor` 新增 `ExecutionPlanner`，并把 `ExecutionManager` 改成只接 `TradePlan`。
6. 重构 Poly `OrderRequest` sizing 语义和 live adapter 降级约束，打通 dry-run、paper、sim、backtest、live。
7. 将开仓、平仓、rebalance、recovery 全部迁移到 `PlanningIntent -> TradePlan -> ExecutionResult`。
8. 重构 audit、monitor、socket 消息和回测输出字段。
9. 用 replay、dry-run 和端到端验收验证四端口径一致。

## 14. 完成标准

只有以下全部成立，才算本次硬切完成：

1. engine 不再产出任何可执行参数。
2. executor 不再接受旧 signal 作为执行入口。
3. Poly 买单/开仓使用 `BuyBudgetCap`，卖单/平仓使用 `SellExactShares` 或 `SellMinProceeds`，不再退回无约束 market order。
4. CEX hedge、rebalance 和 recovery 均基于计划或实际成交重算。
5. `paper`、`sim`、`backtest`、`live` 共用同一个 planner。
6. monitor/audit 能同时展示理论、计划、实际三层边际、fee、funding 和偏差拆分。
7. residual recovery 不再是特殊补丁路径，而是 planner 驱动的正式 plan。
8. 新对象、socket 和持久化状态都带 `schema_version`，不兼容版本不会被错误恢复。
9. 所有必测场景均通过。
