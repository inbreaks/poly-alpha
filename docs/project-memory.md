# PolyAlpha Project Memory

这份文档用来记录团队已经达成共识、后续不该反复争论的关键口径。

它不是设计草稿，也不是任务列表，而是“进入项目后必须先同步的大脑缓存”。

## 1. 交易链的唯一真相

当前项目已经明确不接受旧的“mid-based 信号直接驱动执行”口径。

统一口径是：

- `OpenCandidate` 只发现机会
- `PlanningIntent` 只表达系统动作
- `TradePlan` 是唯一交易计划真相
- `ExecutionResult` 是唯一成交真相

这意味着：

- engine 里的 `expected_pnl` 不能再被当成真实可执行收益
- mid-based sizing 不能再偷偷回到执行链
- executor 不该再接受旧 signal 直接下单

## 2. planner 是定价器，不是事后否决器

项目已经明确拒绝这种旧思路：

- 先按理论信号算 shares / pnl
- 再在最后一步用订单簿做个 preview
- preview 不通过就拒绝

新口径是：

- planner 必须直接用实时订单簿生成交易计划
- 如果计划做不出来，就说明这不是当前可交易机会

## 3. 预算和下单语义已经明确

买单不是“固定 shares 的市价单”。

买单语义应理解为：

- `BudgetCap { max_cost_usd, max_avg_price, max_shares }`

卖单 / 平仓语义应理解为：

- `ExactShares { shares, min_avg_price }`
  或
- `MinProceeds { shares, min_proceeds_usd }`

后续任何 adapter、paper、dry-run、live 都不能静默退回 share-driven market order。

## 4. CEX 对冲看 residual risk，不看名义金额对不对齐

这个项目的 CEX 腿是 delta hedge，不是 notional hedge。

因此团队内部默认判断标准不是：

- “CEX 名义金额有没有和 Poly 差不多”

而是：

- 开仓后 residual delta 是否过线
- 预设冲击场景下 residual risk 是否过线

如果这两项过线，就应该缩单或不做；
如果不过线，就不能简单说“对冲不够”。

## 5. `zero_cex_hedge_qty` 是护栏，不是 bug

开仓前拦住 `zero_cex_hedge_qty` 是正确行为。

它表达的是：

- 这笔机会当前已经退化成“纯 Poly 方向仓”
- 它不再是我们要做的 hedge trade

后续如果某个市场频繁触发这个原因，正确做法是：

- 降级市场池
- 限制其进入交易池
- 对结构性坏市场施加更长的临时隔离，而不是分钟级反复重试

而不是删掉这个检查。

## 6. WS 质量问题不能靠 silent fallback 掩盖

团队已经明确达成的原则：

- 如果 WS 一直有问题，就不具备交易条件
- 不能通过静默 fallback 到另一套交易语义来“看起来还能跑”

允许的只有：

- 明确的数据恢复 / 重连 / 快照补齐
- 明确的 reject / cooldown / 降级

不允许的是：

- 不告诉操作者，悄悄换成另一套决策或执行口径

## 7. sim / paper / live 必须共用一条执行真相

这是本项目的重要边界。

我们允许：

- 运行模式不同
- executor mode 不同
- 数据源密度不同

但不允许：

- 交易语义不同
- planner 逻辑不同
- 风险边界不同
- live 悄悄退回旧执行语义

## 8. schema compatibility 不是可选项

以下对象已经要求带 `schema_version`：

- `OpenCandidate`
- `PlanningIntent`
- `TradePlan`
- `ExecutionResult`
- 相关 socket message
- runtime residual snapshot

团队默认原则：

- 不兼容版本只归档、不强行恢复
- 不允许“猜测式兼容”

## 9. 市场分层已经是产品语义，不只是 UI 装饰

当前项目已经引入：

- `交易池`
- `重点池`
- `观察池`

以及 focus retention reason。

这个语义不是为了 monitor 好看，而是为了真实 market pool governance。

当前默认口径已经是：

- `paper/live` 的 open candidate 不能跳过市场池门禁
- 市场进入 open cooldown 时，candidate 不得继续进入 planner
- `sim` 也必须沿用同一套冷却语义，不能自己放宽

当前进一步明确的执行口径是：

- 结构性 planner 拒绝要用更长冷却做“临时隔离”
- 边际恶化类拒绝用中等冷却
- 订单簿漂移 / 单边 / 缺簿这类更偏实时波动的问题才适合短冷却

后续治理都应该围绕：

- 市场什么时候进入重点池
- 什么时候进入交易池
- 什么时候自动踢出
- 为什么被保留

## 10. backtest 口径和 live 主线的关系

当前默认研究事实源是：

- Rust `backtest rust-replay`

Python 层主要承担：

- 中文报表
- JSON / CSV 导出
- 对照和分析入口

这意味着：

- 不要再把旧 Python 回测当成默认事实源
- `run-legacy` / `walk-forward` 主要是历史对照，不是默认执行真相

## 11. 当前最重要的问题不是“有没有机会”

最近的 live-mock 已经证明：

- 系统会产候选
- 系统会生成 plan
- 系统会成交
- 系统会落 artifact / audit

所以当前主问题不是“系统不会交易”，而是：

- 交易池质量不够高
- 实时可交易性不够稳定
- 末端拒绝太多

## 12. 当前最该推进的方向

团队后续优先级已经明确，不要每次重新发散：

1. 市场池治理
2. 实时可交易性治理
3. 长时间 live-mock soak

在这些完成之前，不要把项目标记成 production-ready。
