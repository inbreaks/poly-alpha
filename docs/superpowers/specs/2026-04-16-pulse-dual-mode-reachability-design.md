# Pulse Dual-Mode Reachability 设计

## 目标

把 `pulse_arb` 的信号主线从“看到价差就尝试开仓”重写成“先证明短时间内大概率能卖掉，再决定进不进”的两模态框架。

这次改造的目标不是继续给旧 detector 堆 feature，而是修正一条已经被服务器实盘样本证明错误的假设：

- 旧假设：高脉冲偏离通常会在 `15m` 内回到足够深的位置，maker 目标价有较高命中概率
- 新假设：高脉冲通常会出现“浅回落”与“深回归”两类不同结构，必须先判断回落可达性，再决定 target、timeout 和 admission

## 现状与病灶

服务器 `multi-market-active.fresh` 历史样本已经把病灶定性清楚：

- `effective_open = 121`
- `maker_proxy_hit = 14`
- `timeout_chase = 107`
- maker 实际命中率 `= 11.57%`
- 开仓时纸面 `expected_open_net_pnl_usd` 合计 `= +468.12 USD`
- 已实现 `realized_pnl_usd` 合计 `= -2325.20 USD`

更关键的一组对照：

- admission 隐含要求的平均 `required_hit_rate ≈ 64.56%`
- 真实 maker hit 率只有 `11.57%`

这说明问题已经不是“执行层不会挂 maker”，而是“信号层系统性高估了 target 的短期可达性”。

### 最有辨识度的真实指标

按 `distance_to_mid_bps` 观察 effective-open 样本：

| `distance_to_mid_bps` | 样本数 | maker hit rate | 平均实际 PnL |
| --- | ---: | ---: | ---: |
| `[0,100)` | 14 | `42.86%` | `-1.69 USD` |
| `[100,200)` | 12 | `41.67%` | `-1.28 USD` |
| `[200,300)` | 2 | `0%` | `-3.36 USD` |
| `[300,500)` | 19 | `15.79%` | `-5.90 USD` |
| `[500,1000)` | 14 | `0%` | `-11.80 USD` |
| `[1000,2000)` | 31 | `0%` | `-31.45 USD` |
| `[2000,+)` | 29 | `0%` | `-35.42 USD` |

结论非常直接：

- 一旦 maker target 离当前市场中枢超过约 `500bps`，历史上几乎就是“物理不可达”
- 旧系统仍把这部分远目标利润提前记入 EV
- 因此“高预期利润”经常只是“挂得更远”，并不是“更容易赚钱”

## 第一性原理修正

这条策略真正要捕捉的不是抽象的 `fair_value_gap`，而是下面两种短时结构之一：

### 模式 A：Elastic Snapback

用户视角：

- 市场被情绪单快速扫上去
- 短时间内经常会先回一小口
- 但方向未必反转，后面仍可能继续沿脉冲方向走

例子：

- 原价 `1.00`
- 脉冲扫到 `1.05`
- 很容易先回到 `1.04`
- 随后继续往 `1.06` 走

这类 setup 的 alpha 不是“深回归到 1.00”，而是“吃第一口 recoil”。

### 模式 B：Deep Reversion

用户视角：

- Poly 这边被情绪资金独立扫穿
- `Deribit` 和 `Binance` 没有同步确认同等幅度的重定价
- 因此后续更可能向 fair/reference 一侧进行更深回归

这类 setup 才适合保留更长持仓与更深 target。

### 核心纠偏

旧系统把两类结构混成一种：

- 看见高 pulse
- 默认按 `15m` 深回归去挂单

新系统必须改成：

- 先识别它是 `Elastic Snapback` 还是 `Deep Reversion`
- 再按对应模式生成 target、timeout 和 admission

一句话钢印：

`以后系统不再问“这单会不会回归”，而是先问“这单会回几口、多久能回、我的单子会不会被撞到”。`

## 新框架总览

新框架按六层运行，顺序固定：

1. 基础健康与 anchorability 过滤
2. Reachability hard gate
3. Pulse mode classification
4. Candidate target generation
5. Observation-quality-gated recovery features
6. Realizable EV admission

### 1. 基础健康与 Anchorability 过滤

沿用现有 runtime 已有能力：

- Poly transport 健康
- Binance perp transport 健康
- Deribit anchor 健康
- data freshness 合格
- expiry 能桥接，不是结构性错配市场

这一层负责回答：

- “数据能不能信？”
- “这个市场有没有资格进入信号层？”

### 2. Reachability Envelope Hard Gate

这是新框架的第一主闸门。

系统在这一步不先生成最终 target，而是先看：

- 基于可执行开仓成本推导出的 `min_profitable_target_distance_bps`
- 当前 market 是否存在“至少一个够得着、且经济上仍为正”的退出带宽

这里的 `min_profitable_target_distance_bps` 定义为：

```text
在给定 entry VWAP、双边费用、对冲成本与 timeout 兜底损失假设下，
要让 maker exit 仍具备正的 maker_net_pnl，最少需要离当前中枢多远。
```

它回答的不是“我想挂多远”，而是“我至少得挂多远才不亏”。

只有当这个最小盈利退出距离本身仍处于现实可达区，setup 才有资格继续往下走。

MVP 默认阈值：

- `<= 150bps`：正常候选区
- `150~200bps`：灰区，只在强证据下保留
- `> 200bps`：MVP 默认拒绝
- `> 500bps`：明确死区，直接拒绝

### 这一步会带来的行为变化

按当前历史样本，`effective_open = 121` 中，实际 maker target 的 `distance_to_mid_bps < 200` 的只剩约 `26` 笔，约占 `21.5%`。

这意味着：

- 交易频率下降 `70%-80%` 是预期内行为
- 这不是副作用，而是目标

系统会变得非常挑食，但它是在拒绝那些历史上几乎不可能成交的幻想 target。

### 3. Pulse Mode Classification

Reachability 过关后，再判断 setup 属于哪种脉冲类型。

分类输入至少包括：

- `pulse_score_bps`
- `claim_price_move_bps`
- `fair_claim_move_bps`
- `cex_mid_move_bps`
- `swept_notional_usd`
- `swept_levels_count`
- `post_pulse_depth_gap_bps`

#### 判别逻辑

判成 `Elastic Snapback` 的典型特征：

- pulse 很强
- 扫盘很猛
- `fair_claim_move_bps` 或 `cex_mid_move_bps` 也不是很小
- continuation 风险高

判成 `Deep Reversion` 的典型特征：

- Poly 的 claim move 明显大于 anchor / CEX move
- 更像 Poly 独立情绪超调
- 后续更可能向 fair/reference 深回归

### 4. Candidate Target Generation

这一步是整个系统行为质变的关键。

旧系统：

- 进场后按固定 `1/2/3 tick ladder` 直接挂单

新系统：

- 先生成一组候选 target
- 对每个 target 分别计算 `target_distance_to_mid_bps`、命中率和 EV
- 选 `realizable_ev` 最大者
- 若所有 candidate 都不值得做，则不交易

Candidate generator 必须服从上一层的 reachability envelope：

- 任何 candidate 的 `target_distance_to_mid_bps` 都不能小于 `min_profitable_target_distance_bps`
- MVP 下任何 candidate 也不能突破 `200bps` 的 reachability 上限
- 若没有 candidate 同时满足“高于 break-even 下限”与“低于 reachability 上限”，则直接不交易

#### 4.1 Elastic Snapback Mode 的 target

这类单子只吃第一口回落，不赌深回归。

初始建议：

- `target_distance_to_mid_bps <= 100~150`
- target 用“脉冲幅度的 `20%~35%` 回撤”生成
- timeout 缩短到 `30s ~ 120s`

例子：

- claim 从 `0.40` 脉冲到 `0.45`
- 旧系统可能赌回 `0.43`
- 新系统只允许挂在 `0.442 ~ 0.444` 一带

如果短时间内不回，说明 continuation 更强，应尽快退出，而不是继续挂着幻想深回归。

#### 4.2 Deep Reversion Mode 的 target

这类单子允许更深一些的回归，但仍要受可达性约束。

初始建议：

- `target_distance_to_mid_bps <= 150~200`
- timeout `5m ~ 15m`
- target 可以部分参考 fair/reference，但不能无视 reachability

### 5. Observation-Quality-Gated Recovery Features

这一步专门解决 Polymarket WS 的观测限制问题。

#### 现状限制

当前代码虽然在 Poly WS 解析时保留了两套时间：

- `exchange_timestamp_ms`
- `received_at_ms`

但 runtime 的短窗 signal tape 采样主要还是按 `received_at_ms` 记。

这意味着：

- freshness 用 `received_at_ms` 没问题
- `1s / 3s refill_ratio` 这种微结构特征，如果直接按接收时间做硬门槛，会引入“后视镜偏差”

#### 设计原则

`refill_ratio_1s / 3s` 不能在 MVP 中直接做硬 admission 规则。

MVP 正确做法：

- 用更稳的恢复特征替代“伪精确时点”
- 给 recovery 特征加 observation quality
- 质量不够时，只记 audit，不进入 admission

#### 推荐的恢复特征

- `max_recovery_ratio_within_2s`
- `max_recovery_ratio_within_5s`
- `time_to_first_50pct_refill_ms`
- `time_to_first_80pct_refill_ms`
- `post_sweep_update_count_5s`
- `max_interarrival_gap_ms_5s`
- `used_exchange_ts`
- `native_sequence_present`
- `observation_quality_score`

#### MVP 的 observation quality 门槛

- `used_exchange_ts = true`
- `native_sequence_present = true`
- `post_sweep_update_count_5s >= 3`
- `max_interarrival_gap_ms_5s <= 700`

只有当 observation quality 达标时，recovery 特征才允许影响 admission。

#### Admission 与 Audit 的边界

为了避免 live admission 再次被“看起来很聪明、其实很脆弱”的微结构特征带偏，MVP 明确分成两类：

进入 live admission 的，只能是：

- `min_profitable_target_distance_bps`
- mode classification 结果
- `max_recovery_ratio_within_2s`
- `max_recovery_ratio_within_5s`
- `time_to_first_50pct_refill_ms`
- `time_to_first_80pct_refill_ms`
- `post_sweep_update_count_5s`
- `max_interarrival_gap_ms_5s`
- `observation_quality_score`

只进入 audit / replay，不进入 live admission 的，包括：

- `refill_ratio_1s`
- `refill_ratio_3s`
- 更细粒度的 queue reconstruction 指标
- 更细粒度的 toxicity / adverse selection 指标

这样做的目的不是放弃细节，而是先把 MVP 建在足够稳的证据上，再用 overnight recorder 反推第二阶段特征。

### 6. Realizable EV Admission

最终 admission 语言不再是旧的“纸面开仓利润”，而是考虑 maker 命中率后的真实会话 EV。

新公式：

```text
realizable_ev =
  predicted_hit_rate * maker_net_pnl
  + (1 - predicted_hit_rate) * timeout_net_pnl
```

并要求：

```text
predicted_hit_rate >= required_hit_rate + safety_margin
realizable_ev >= min_realizable_ev_usd
```

MVP 参数建议：

- `safety_margin = 10pp`
- `min_realizable_ev_usd = 1.0`
- `max_timeout_loss_usd = 10~12 USD`

## `predicted_hit_rate` 的 MVP 方案

第一版不做复杂模型，先上可解释、保守的规则模型。

### Base Prior：按 `target_distance_to_mid_bps` 给先验

| `target_distance_to_mid_bps` | 初始先验 `p_hit` |
| --- | ---: |
| `<=100` | `0.40` |
| `100~200` | `0.35` |
| `200~300` | `0.10` |
| `300~500` | `0.05` |
| `>500` | `0.00` |

### 再叠加修正项

- 强 `swept_notional_usd`：`+0.10 ~ +0.15`
- 强 `post_pulse_depth_gap_bps`：`+0.05`
- recovery 慢且 observation quality 高：`+0.05 ~ +0.10`
- 质量低：不给 bonus，甚至 `-0.05`
- fill ratio 太差：`-0.05`

最后 clamp 到 `[0.0, 0.90]`。

## 三个例子

### 例子 1：旧系统会做，新系统必须拒绝

真实样本：`pulse-btc-1`

- entry `= 0.2412`
- target `= 0.2512`
- `required_hit_rate = 65.0%`
- `distance_to_mid_bps = 921.7`
- 纸面 open EV `= +9.12 USD`
- 实际 PnL `= -27.53 USD`
- 退出路径 `= timeout_chase`

旧系统为什么会做：

- pulse 强
- edge 厚
- paper EV 是正的

新系统为什么必须拒绝：

- 第一层 reachability 就失败
- `target_distance_to_mid_bps = 921.7`
- 该区间历史 maker hit 率为 `0`

这类单子不该进入 mode classification，更不该进入 target 选择。

### 例子 2：高脉冲但只应按浅回落处理

假设某条 claim：

- 脉冲前 `0.40`
- 脉冲后 `0.45`
- anchor 只从 `0.405` 到 `0.408`

真实市场路径可能是：

- `0.45 -> 0.442 ~ 0.444`
- 随后继续往 `0.46` 走

旧系统错误：

- 把它当“会深回归到 `0.43` 甚至更低”

新系统正确做法：

- 识别成 `Elastic Snapback`
- target 只挂 `0.442 ~ 0.444`
- timeout 设为 `30s ~ 120s`

### 例子 3：什么样的 setup 才值得保留

假设一个候选：

- `opening_request_notional_usd = 250`
- `target_distance_to_mid_bps = 95`
- `swept_notional_usd = 420`
- `swept_levels_count = 3`
- `max_recovery_ratio_within_2s = 0.35`
- `max_recovery_ratio_within_5s = 0.65`
- `post_sweep_update_count_5s = 7`
- `max_interarrival_gap_ms_5s = 280`
- `used_exchange_ts = true`
- `native_sequence_present = true`

保守估算：

- base `p_hit = 0.40`
- 强 sweep `+0.15`
- 慢 recovery `+0.10`
- 高质量观测 `+0.05`

得到：

- `predicted_hit_rate ≈ 0.70`

若：

- `maker_net_pnl = +5.5 USD`
- `timeout_net_pnl = -3.5 USD`

则：

- `realizable_ev = 0.70 * 5.5 + 0.30 * (-3.5) = +2.80 USD`

这类 setup 才是新系统真正要留下来的高质量机会。

## 需要新增的核心字段

### Reachability / Target

- `target_distance_to_mid_bps`
- `target_regime` (`elastic_snapback` / `deep_reversion`)
- `candidate_targets`
- `maker_net_pnl`
- `timeout_net_pnl`
- `predicted_hit_rate`
- `realizable_ev`

### Flow / Pulse

- `swept_notional_usd`
- `swept_levels_count`
- `post_pulse_depth_gap_bps`
- `entry_side_depth_fill_ratio`

### Recovery / Observation Quality

- `max_recovery_ratio_within_2s`
- `max_recovery_ratio_within_5s`
- `time_to_first_50pct_refill_ms`
- `time_to_first_80pct_refill_ms`
- `post_sweep_update_count_5s`
- `max_interarrival_gap_ms_5s`
- `used_exchange_ts`
- `native_sequence_present`
- `observation_quality_score`

## Overnight Tape Recorder 与完整回放

### 为什么现有 audit 不够

当前 `PulseBookTape` 只会在已有 active session 时，记录对应 symbol 的 book tape。

这足以解释：

- “为什么这笔已开仓单亏了”

但不够解释：

- “哪些候选本来就不该开”
- “哪些没开出来的 setup 其实是高质量的浅回落”
- “如果换一套 signal，当时会不会做出不同决策”

### 推荐方案：Global Pulse Tape Recorder

服务器过夜运行时，记录：

- Poly：BTC / ETH 全候选市场 `YES/NO` book
- Binance：对应 perp book
- Deribit：anchor snapshot 与 surface 摘要
- detector：每次接近可交易的候选信号快照
- session lifecycle：所有 pulse 会话状态变化
- executor feedback：mock fill / hedge attribution

### 存储格式

MVP 推荐：

- 按小时滚动的 `zstd` 压缩 `JSONL`
- 第二天再导入 `DuckDB` 做复盘与统计

### 原始 tape 每条至少包含

- `event_kind`
- `asset`
- `symbol`
- `instrument`
- `exchange`
- `exchange_timestamp_ms`
- `received_at_ms`
- `sequence`
- `best_bid`
- `best_ask`
- `mid`
- `top_5_bids`
- `top_5_asks`
- `last_trade_price`（若有）

### Detector snapshot 每条至少包含

- `mode_candidate`
- `pulse_score_bps`
- `claim_price_move_bps`
- `fair_claim_move_bps`
- `cex_mid_move_bps`
- `swept_notional_usd`
- `swept_levels_count`
- `post_pulse_depth_gap_bps`
- `target_distance_to_mid_bps`
- `predicted_hit_rate`
- `maker_net_pnl`
- `timeout_net_pnl`
- `realizable_ev`
- `admission_result`
- `rejection_reason`

### Replay 系统

建议新增：

- `polyalpha-cli pulse replay --input <dir> --asset btc,eth --speed 1x`
- `polyalpha-cli pulse replay --input <dir> --market <symbol> --from ... --to ...`

回放要做三件事：

1. 按原始事件时间重建 book
2. 用同一套 detector / target generator / FSM 重跑
3. 输出“当时市场发生了什么”与“模型当时如何判断”的对照

## 过夜数据收集后的固定复盘报表

次日固定先看五张表：

1. `maker hit rate vs target_distance_to_mid_bps`
2. `mode distribution`（snapback / deep reversion）
3. `realizable_ev` 分桶表现
4. `timeout_chase attribution`
5. `BTC vs ETH` 的分布差异

## 成功标准

这轮改造完成后，系统应该表现出下面这些质变：

- 开仓频率显著下降，但留下的 setup 更高质量
- `distance_to_mid_bps > 200` 的幻想 target 不再成为主流
- `timeout_chase` 比例显著下降
- maker hit 率显著高于当前 `11.57%`
- paper EV 与 realized EV 的偏差显著收敛
- audit 能回答“为什么这单是 snapback，而不是 deep reversion”
- 服务器过夜 recorder 能支撑次日完整复盘与 replay

## 一句话钢印

这次不是“再优化一次 pulse detector”。

这次是把系统从“看到价差就尝试开仓”的冲动型行为，改成一套真正围绕短期可达性、模式识别和可实现 EV 的交易框架：

`先证明能卖掉，再决定值不值得买。`
