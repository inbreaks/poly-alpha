# Pulse Dislocation Strategy Matrix 设计

## 目标

把 `data/reports/pulse_research/dislocation_signal_iteration_20260420.md` 里已经明确的下一步，压缩成一轮 **一次性可复跑的研究矩阵**，并且明确对齐 [2026-04-16-pulse-dual-mode-reachability-design.md](/Users/le/Documents/source-code/poly-alpha/docs/superpowers/specs/2026-04-16-pulse-dual-mode-reachability-design.md) 里的主线：

- 不再继续扫开放式静态阈值网格
- 不把 `exit` 继续当成搜索维度
- 只在固定 `15m` 短时退出口径下，验证“确认层”能否让当前 `dislocation` thesis 发展成真正能赚钱的退出信号

这轮真正要回答的问题只有一个：

`在固定 15m 退出读数下，post-pulse confirmation 能不能把当前结构性脱锚候选，从“看起来合理但不稳定”推进到“15m 内可达退出且扣完成本后仍然赚钱”的 signal。`

## 非目标

这轮明确不做下面这些事：

- 不做通用 `strategy-matrix` 基建
- 不做任意多天、任意 exit、任意策略 DSL
- 不改 Rust runtime
- 不继续扩静态结构阈值搜索空间
- 不产出通用 markdown 报告框架
- 不把 `1s / 3s` 细粒度恢复特征直接升级成 live admission 规则

如果这轮结果依然失败，结论应该是“确认层假设仍不够”，而不是“研究基建还不够完整”。

## 固定研究口径

### 固定 exit

这轮矩阵统一固定为：

- horizon: `15m`
- touch mode: `any_touch`
- target mode: `raw_tick`
- target ticks: `1`

原因：

- `2026-04-16` 设计文档里，原始假设本来就是“脉冲后是否能在 15m 内出现可达回补”
- `60m` 更像把信号偷换成了“长时间深回归”，这和用户这轮目标不一致
- `15m / any_touch / raw_tick / 1 tick` 不是最终 target 生成器，只是这轮最保守、最接近“第一口 recoil” 的研究读数

这轮结论的解释边界也随之固定：

- 如果矩阵结果改善，说明这些确认层特征有助于识别 **15m 内可达退出且更可能赚钱** 的浅回补
- 不等于已经证明它们适合更长持仓或更深回归

### 固定样本

这轮至少支持下面两份 outcome 进入同一次矩阵评估：

- `data/reports/pulse_research/features/pulse-aggressive-outcomes-overnight_aggressive-20260418T165101Z.jsonl.zst`
- `data/reports/pulse_research/features/pulse-aggressive-outcomes-20260420_partial_snapshot_clean-20260420T041401Z.jsonl.zst`

进入矩阵前，仍然先通过 `enrich_frame_with_market_context(...)` 回填结构字段，确保旧产物也能消费新结构列。

## 基线 thesis

这轮矩阵不再把“静态结构过滤”拆成大网格，而是固定一个 baseline 行，作为所有确认层策略的对照。

### `structure_core`

`structure_core` 定义为：

- `setup=directional_dislocation`
- `disp>=1500`
- `refill<=0.35`

原因：

- `setup=directional_dislocation` 代表当前结构性脱锚主线
- `disp>=1500` 与 `refill<=0.35` 是当前文档里已经反复出现的主过滤条件
- 这条线已经被真实样本证明“方向可能对，但稳定性不够”，适合作为确认层增量的基线

这轮要比较的不是“有没有一条神奇新规则”，而是：

- 加入确认层以后，是否能让 `structure_core` 的跨天结果更稳
- 哪类确认层有帮助
- 哪类确认层只是把样本切得更少，但没有改善质量

这里的“改善”统一解释为：

- 更像 `2026-04-16` 文档里的 `Elastic Snapback`
- 而且在固定 `15m` 退出读数下，表现出更接近盈利信号的 realized pnl
- 而不是“是否支持 60m 深回归”

## 确认层特征

这轮只实现文档里已经明确写出的四类确认信号，不额外扩展新特征。

但它们在本轮中的定位必须明确：

- 这是 **research-only** 的细粒度确认特征
- 目的是验证它们是否能帮助识别短时 recoil
- 即使有效，也不直接意味着它们可以原样进入 live admission

这点与 `2026-04-16` 设计保持一致：

- live admission 的 MVP 应优先使用更稳的恢复特征和 observation-quality gating
- `1s / 3s refill`、更细粒度 depth / spread 指标，先用于研究验证，再决定是否抽象成更稳的 admission 特征

### 1. `post_pulse_refill_ratio_1s`

定义：

- 以本次 pulse 的低点 `window_low_ts_ms` 为锚点
- 取 `low_ts + 1s` 时的可观测价格
- 计算相对 pulse 区间 `[window_high_price, window_low_price]` 的回补比例

解释：

- 越高表示在 pulse 极值后的 `1s` 内，价格已经出现更快的回补
- 这是一种“坏单继续塌 vs 好单开始修复”的最早期区分

### 2. `post_pulse_refill_ratio_3s`

定义与上面一致，只是观测点改为 `low_ts + 3s`。

解释：

- `1s` 更偏极短反应
- `3s` 更偏确认“不是偶发抖动，而是真的开始回补”

### 3. `spread_snapback_bps`

定义：

- 记录低点时刻的盘口 spread
- 再记录 `low_ts + 3s` 的盘口 spread
- 输出 `spread_at_low - spread_at_low_plus_3s`

解释：

- 正值越大，说明脉冲过后盘口开始重新收敛
- 如果 spread 继续恶化或几乎不恢复，这类单更接近“继续衰减/塌陷”

### 4. `bid_depth_rebuild_ratio`

定义：

- 记录低点时刻买盘前 `3` 档总名义深度
- 记录 `low_ts + 3s` 时同口径买盘前 `3` 档总名义深度
- 输出 `depth_at_low_plus_3s / max(depth_at_low, eps)`

解释：

- 比例越高，说明极值后买盘开始回补
- 这比只看价格更接近“市场有没有开始重新接单”

### 确认窗口完整性

新增布尔字段：

- `confirm_window_complete_1s`
- `confirm_window_complete_3s`

约束：

- 若当前样本时间还没有走到 `low_ts + 1s` 或 `low_ts + 3s`
- 对应确认窗口视为不完整
- 任何依赖该窗口的策略都不得消费该字段

这轮必须保证：

`确认层特征只能用样本当时已经发生的数据，不能在研究链里偷读未来。`

## 数据流与实现位置

### 文件范围

只改：

- `scripts/pulse_research_sweep.py`
- `scripts/tests/test_pulse_research_sweep.py`

### 特征生成位置

确认层字段放在 `feature-extract` 阶段生成，原因如下：

- 这些字段本质上是“样本时刻可知的微观结构确认”
- 它们应该与 `flow_refill_ratio`、`flow_residual_gap_bps` 一样，属于研究样本主表的一部分
- `aggressive-outcomes` 只负责未来价格结果，不应该承担确认层字段的后验拼接

### 最小实现方式

不做通用 history framework，只补够这轮研究所需的最小能力：

- 在现有事件扫描过程中，保留 `(symbol, side)` 的轻量历史观测
- 支持按时间查询：
  - 价格
  - spread
  - top3 bid depth

需要的只是：

- 低点时刻观测
- `low_ts + 1s`
- `low_ts + 3s`

不需要构建可泛化的 full-feature microstructure layer。

这一节也明确服从 `2026-04-16` 的边界：

- 这轮只是在离线研究里验证细粒度确认信号
- 不在这轮里同时实现 mode classification、candidate target generation、observation-quality score 全框架

## 策略矩阵

这轮不做笛卡尔积搜索，而是固定一组 **硬编码策略行**。

### 必跑策略行

1. `structure_core`
2. `structure_core + refill1s_fast`
3. `structure_core + refill3s_fast`
4. `structure_core + spread_snapback`
5. `structure_core + depth_rebuild`
6. `structure_core + refill3s_fast + spread_snapback`
7. `structure_core + refill3s_fast + depth_rebuild`
8. `structure_core + spread_snapback + depth_rebuild`
9. `structure_core + refill3s_fast + spread_snapback + depth_rebuild`

### 首版阈值

这轮先用单档阈值，避免再次陷入参数空间膨胀：

- `refill1s_fast`: `post_pulse_refill_ratio_1s >= 0.20`
- `refill3s_fast`: `post_pulse_refill_ratio_3s >= 0.35`
- `spread_snapback`: `spread_snapback_bps >= 50`
- `depth_rebuild`: `bid_depth_rebuild_ratio >= 1.25`

如果首版结果说明某个确认层方向有价值，但阈值偏紧或偏松，再单独做第二轮阈值细化。

## 评估逻辑

矩阵评估统一基于：

- 固定 exit 生成的 `realized_net_pnl_usd`
- 每天最少成交数门槛
- “每天都不能为负”的跨天过滤

这里的 `realized_net_pnl_usd` 只表示：

- 在固定 `15m / any_touch / raw_tick / 1 tick` 读数下
- 这些确认层是否更接近“短时可达、能退出、且扣完成本仍然赚钱”

它不再承担下面这些解释：

- 是否支持 `60m` 深回归
- 是否已经得到最终 live target policy
- 是否已经覆盖 `Elastic Snapback` 与 `Deep Reversion` 两模态

### 结果列

每一条策略行至少输出：

- `strategy_name`
- `filters`
- `trade_count_by_day`
- `pnl_by_day`
- `hit_rate_by_day`
- `min_day_pnl_usd`
- `total_realized_pnl_usd`
- `avg_realized_pnl_usd`
- `delta_vs_structure_core`

### 排序逻辑

优先级固定为：

1. `min_day_pnl_usd`
2. `total_realized_pnl_usd`
3. `avg_realized_pnl_usd`

这样可以避免“某一天爆赚、另一天失效”的组合被错误顶到前面。

## 输出形式

这轮只需要两个输出：

### 1. 机器结果

写一个 checkpoint JSON，记录整张矩阵结果表。

### 2. 研究摘要

写一份短 markdown，总结：

- 哪些确认层完全无增益
- 哪些确认层改善了 `min_day_pnl_usd`
- 是否存在任何一条策略能通过跨天正收益门槛

这份摘要的职责不是做漂亮报告，而是给下一轮研究方向一个明确结论。

## 测试要求

### 1. 确认特征计算测试

验证：

- `post_pulse_refill_ratio_1s`
- `post_pulse_refill_ratio_3s`
- `spread_snapback_bps`
- `bid_depth_rebuild_ratio`

都按设计口径计算。

### 2. 无未来信息测试

验证：

- 当 `ts_ms < low_ts + 1s` 时，`1s` 确认窗口无效
- 当 `ts_ms < low_ts + 3s` 时，`3s` 确认窗口无效
- 策略过滤不会错误消费不完整确认窗口

### 3. 矩阵结果测试

构造两天小样本，验证：

- `structure_core` 一定存在
- 加确认层后如果某天 `PnL <= 0`，会被淘汰
- `delta_vs_structure_core` 正确计算

## 成功标准

这轮完成后，用户应当能一次性跑出一张固定口径的策略矩阵，并据此回答：

1. `post-pulse confirmation` 是否真的比 `structure_core` 更能识别 `15m` 内可达退出且赚钱的信号
2. 哪类确认层值得进入下一轮，并转译成更稳的 recovery / observation-quality admission 特征
3. 是否已经出现“值得继续推进”的短时 recoil 候选，还是应当继续回到信号定义本身

## 风险与接受方式

这轮最大风险不是“代码复杂度不够”，而是：

- 确认窗口定义过松，导致好坏单仍然混在一起
- 样本数进一步下降，结论变成“没有足够交易”

这两个风险都接受，因为它们本身就是研究结论的一部分。

如果矩阵结果显示：

- 所有确认层都无增益
- 或者增益只来自极小样本、不可复现组合

那么这轮应明确给出负结论，而不是再继续在同一组特征上打补丁。
