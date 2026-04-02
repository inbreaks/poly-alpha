# PolyAlpha Current Status

更新日期：`2026-04-02`

## 一句话现状

项目已经进入“可运行、可验、但还没完全 live-ready”的阶段。

当前可信主线是：

- `engine -> planner -> execution -> artifact/audit`
- `sim / paper / live mock` 共享执行栈
- `backtest rust-replay` 作为默认历史事实源

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

## 最新验证证据

### 全量 Rust 验证

已跑：

```bash
cargo test -p polyalpha-core -p polyalpha-data -p polyalpha-risk -p polyalpha-executor -p polyalpha-cli -- --nocapture
```

最新结果：

- `polyalpha-cli`: `134 passed, 0 failed, 4 ignored`
- `polyalpha-core`: `39 passed, 0 failed`
- `polyalpha-data`: `28 passed, 0 failed`
- `polyalpha-executor`: `59 passed, 0 failed`
- `polyalpha-risk`: `8 passed, 0 failed`

### live-mock 运行证据

已跑：

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
- audit summary: `data/audit/sessions/1302ed9d-d9e3-4d53-a067-5454fd85c961/summary.json`

这轮结果的高层结论：

- `ticks_processed = 60`
- `signals_seen = 242`
- `signals_rejected = 419`
- `order_submitted = 4`
- `fills = 4`
- `status = completed`

意义：

- 不是“入口能启动但不会交易”
- 也不是“有成交就会中途崩”
- 当前真正的问题已经变成“机会质量和实时可交易性”

## 当前 live-ready 阻塞

### 1. 交易池太脏

大量市场虽然能产候选，但本质上不是真正可交易机会。

在最近一轮 `WS + mock` 中，主要拒绝原因是：

- `zero_cex_hedge_qty`
- `residual_delta_too_large`
- `above_or_equal_max_poly_price`
- `non_positive_planned_edge`

这说明系统当前还在看太多“视觉上像机会、实际上不该做”的市场。

### 2. Polymarket 实时可交易性质量仍然不够

最近一轮里：

- `poly_quote_stale` 仍然占大头
- `no_fresh_poly_sample` 仍然很多

这说明问题不是系统不看机会，而是很多时刻拿不到足够可信、足够新鲜的 Poly 报价。

### 3. 末端 revalidation 拒绝还偏多

`execution_planner` 侧的 revalidation 失败说明：

- 有些候选直到最后一步才发现不成立
- 这些问题应该更多地前置为“市场池准入 / 冷却 / 降级”逻辑，而不是只在最后一步拒绝

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

- `zero_cex_hedge_qty` 比例明显下降
- 高价带 / 低质量市场更少进入开仓漏斗

### 2. 实时可交易性治理

目标：

- 把 `poly_quote_stale`
- `zero_cex_hedge_qty`
- `poly_price_move_exceeded`

这些问题变成自动准入 / 踢出 / 冷却规则

结果判断标准：

- planner 末端无效拒绝下降
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
