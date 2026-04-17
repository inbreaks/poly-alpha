# Pulse Research Tape 设计

## 目标

为 `pulse_arb` 增加一套面向研究回放的原始数据采集层，让本地离线分析能够稳定回答两个主问题：

1. 当前信号定义本身是否有问题
2. 当前退出机制是否符合预期、最终能不能赚钱

这里的目标不是做“交易所级逐笔完美仿真”，而是做一套 **research-grade replay** 数据底座：

- 同一份真实市场数据上，可以反复重跑不同的 signal / admission 定义
- 对同一笔理论入场，可以离线比较不同退出策略的结果
- 可以做参数 sweep，而不是只看解释日志

## 当前病灶

现有 `pulse` 运行链已经能稳定产出：

- runtime session summary
- checkpoint monitor state
- asset health 时间线

但这套数据仍然不足以做本地参数反推，核心缺口有三个：

1. **没有全量 market tape**
   - 当前 raw 导出里主要只有 `pulse_asset_health`
   - 缺少可重放的 Poly / Binance / Deribit 原始输入流

2. **没有全量 evaluation 特征**
   - 当前 `ready` 市场在 checkpoint 中仍然可能全是 `fair_prob_yes = null`
   - 本地无法区分：
     - transport / freshness 没过
     - 还是 pricer / detector 没有真正生成 candidate

3. **没有退出回放所需的后续盘口路径**
   - 即使某次理论上能入场，如果没有后续 L2 路径，就无法离线比较：
     - maker fixed target
     - maker + pegging
     - timeout chase
     - 不同持有时长

一句话总结：

`现在的数据足够证明“没开仓”，但还不足以回答“怎样才会赚钱”。`

## 设计原则

### 1. 以“回答研究问题”为目标，不追求过度抽象

这次设计只服务两个问题：

- 信号定义对不对
- 退出机制赚不赚钱

不在本轮引入和这两个问题无关的泛化层。

### 2. 记录真实输入，而不是只记录解释结果

本地想要反推参数，必须能回到“当时市场真实发生了什么”，因此必须记录原始输入而不是只记录高层 summary。

### 3. 全量记录当前运行池的一天数据，但采用事件流而不是大快照

用户目标是“先完整录 1 天，再在本地做分析”。在这个目标下，默认采用：

- 固定市场池
- 1 天全量数据
- 事件驱动增量 tape

而不是只录最终 candidate 窗口。

### 4. 市场数据和评估特征必须同时存在

只有 L2 tape，不足以高效扫参数；
只有 evaluation rows，不足以验证退出机制。

两者必须同时存在。

## 研究问题与数据映射

### 问题 1：信号定义是否有问题

本地离线要能回答：

- 当前 detector / pricer 放出来的机会，事后看是否真的存在可实现的回归利润
- 哪类 rejection 本来应该放宽，哪类 rejection 绝对不能放
- 哪类 market type 天然不适合这套信号

因此必须具备：

- 全量评估时刻的 detector / pricer 输入
- 每一次 `evaluation_attempt` 的特征和拒绝原因
- 可重建当时市场状态的多腿 tape

### 问题 2：退出机制是否符合预期、能不能赚钱

本地离线要能回答：

- 同一笔理论 entry，在真实后续盘口路径下，哪套 exit policy 的 realized EV 更好
- maker target 是否真的能被 hit
- timeout chase 的损失分布到底多大

因此必须具备：

- 入场后多腿盘口的完整后续路径
- 当时使用的 fee / tick / qty_step / timeout / latency 假设
- 与退出判断相关的价格带和 sequence / timestamp

## 数据产品总览

本轮设计固定为四层数据。

### 1. `market_tape`

记录当前运行市场池的一天全量事件流。

#### Poly

对当前 `pulse` 运行市场池内的每个 market：

- `yes` book 增量事件
- `no` book 增量事件

要求能离线重建 top-N L2。

#### Binance

只记录当前 hedge 需要的 perp：

- `BTCUSDT`
- `ETHUSDT`

要求能离线重建：

- best bid / ask / mid
- top-N L2

#### Deribit

不记录整个 Deribit 全期权链的全量 L2。

只记录当前 anchor 计算真正使用到的 instrument universe 输入：

- ticker
- best bid / ask
- mark iv
- greeks
- index price
- 其他 fair prob 映射实际用到的字段

原因很明确：

- Deribit 在这里是“概率锚”，不是执行 venue
- 本地研究的关键是能离线重建 `fair_prob_yes`
- 不是重建整个期权市场微观结构

### 2. `evaluation_rows`

每一次 `evaluation_attempt` 都落一行，不管最后有没有 candidate、有没有 admission 通过。

这是本地参数 sweep 的主表。

它必须能回答：

- 当时有没有算出 `fair_prob`
- 算出了多少 `edge`
- 是在哪道 gate 被拒绝
- 如果进场，理论 target / timeout economics 是什么

### 3. `execution_context`

把离线仿真退出所需的执行约束单独固化，作为当前 session 的静态上下文。

这层不是 market event，而是一份当前运行参数快照。

内容至少包括：

- fee 假设
- tick / qty_step / contract multiplier
- timeout / notional / qty 相关参数
- maker proxy 版本
- latency 假设
- 当前运行资产与市场池

### 4. `checkpoint`

低频 checkpoint 继续保留，用于：

- 快速定位时间段
- 生成监控快照
- 支撑轻量排障

checkpoint 不承担研究 replay 的主数据职责。

## 数据契约

### `market_tape` 通用字段

所有 event 至少包含：

- `session_id`
- `ts_exchange_ms`
- `ts_recv_ms`
- `sequence`
- `asset`
- `symbol`
- `venue`
- `instrument`
- `update_kind`
- `top_n_depth`

其中：

- `ts_exchange_ms` 用于重建市场事件顺序
- `ts_recv_ms` 用于分析链路延迟和本地 freshness
- `sequence` 用于流内重放与对齐

### Poly tape 字段

在通用字段之外至少包含：

- `token_side`：`yes` / `no`
- `delta_bids`
- `delta_asks`
- `snapshot_bids`（仅 resync / periodic snapshot）
- `snapshot_asks`（仅 resync / periodic snapshot）
- `best_bid`
- `best_ask`
- `mid`
- `last_trade_price`（若有）

### Binance tape 字段

在通用字段之外至少包含：

- `best_bid`
- `best_ask`
- `mid`
- `delta_bids`
- `delta_asks`
- `snapshot_bids`（仅 resync / periodic snapshot）
- `snapshot_asks`（仅 resync / periodic snapshot）

### Deribit anchor tape 字段

在通用字段之外至少包含：

- `instrument_name`
- `expiry_ts_ms`
- `strike`
- `option_type`
- `best_bid`
- `best_ask`
- `mark_price`
- `mark_iv`
- `bid_iv`
- `ask_iv`
- `delta`
- `gamma`
- `vega`
- `theta`
- `underlying_index_price`

## `evaluation_rows` 字段

每一条 evaluation row 至少包含：

- `ts_eval_ms`
- `asset`
- `symbol`
- `mode_candidate`
- `admission_result`
- `rejection_reason`

### 价格与概率

- `fair_prob_yes`
- `entry_price`
- `target_exit_price`
- `timeout_exit_price`
- `final_claim_side`

### 经济性

- `net_edge_bps`
- `expected_net_pnl_usd`
- `timeout_loss_estimate_usd`
- `required_hit_rate`
- `predicted_hit_rate`
- `realizable_ev_usd`

### signal / detector primitives

- `pulse_score_bps`
- `claim_price_move_bps`
- `fair_claim_move_bps`
- `cex_mid_move_bps`
- `swept_notional_usd`
- `swept_levels_count`
- `post_pulse_depth_gap_bps`
- `min_profitable_target_distance_bps`
- `reachability_cap_bps`
- `target_distance_to_mid_bps`
- `reversion_pocket_ticks`
- `reversion_pocket_notional_usd`
- `vacuum_ratio`
- `observation_quality_score`

### freshness / transport / expiry

- `anchor_age_ms`
- `anchor_latency_delta_ms`
- `poly_quote_age_ms`
- `cex_quote_age_ms`
- `cross_leg_skew_ms`
- `minutes_to_expiry`
- `poly_transport_ok`
- `cex_transport_ok`
- `anchor_transport_ok`

### tape join 键

每条 evaluation row 还必须能关联回当时 tape 的精确位置，至少包括：

- `poly_yes_sequence_ref`
- `poly_no_sequence_ref`
- `cex_sequence_ref`
- `anchor_sequence_ref`

如果某条腿没有原生 sequence，则必须有稳定的替代字段，例如：

- `anchor_exchange_ts_ref`

## `execution_context` 字段

建议作为 session 级 sidecar 文件 `execution_context.json` 写入。

至少包括：

- `env`
- `session_id`
- `assets`
- `market_symbols`
- `poly_fee_bps`
- `cex_fee_bps`
- `poly_tick`
- `cex_tick`
- `cex_qty_step`
- `cex_contract_multiplier`
- `opening_request_notional_usd`
- `min_opening_notional_usd`
- `timeout_secs`
- `max_holding_secs`
- `maker_proxy_rule_version`
- `latency_assumption_ms`
- `anchor_provider`
- `hedge_venue`

## 存储与分段

### 原始 tape

按 `session / venue / instrument-family / segment` 分段。

建议形态：

- `raw/poly-market-tape-00001.jsonl.zst`
- `raw/binance-market-tape-00001.jsonl.zst`
- `raw/deribit-anchor-tape-00001.jsonl.zst`
- `raw/evaluation-rows-00001.jsonl.zst`

说明：

- 继续沿用现有 raw segment 思路
- 但把研究数据和传统 audit summary 明确区分
- 默认使用压缩，避免再出现“204M 原始 JSONL -> 7.5M tar.gz”这种二次打包才压下来的情况

### Checkpoint

继续使用现有：

- `checkpoint.json`
- `summary.json`
- `manifest.json`

### Execution Context

新增：

- `execution_context.json`

## 为什么不是“只录 candidate 窗口”

因为目标不是解释单笔样本，而是要在本地 **重定义 signal**。

如果只录最终通过 admission 的 candidate：

- 一旦候选器本身有问题
- 你连“本来应该关注但没进候选”的时刻都没有数据

这会形成自证循环。

因此本轮必须记录：

- 固定市场池的全量 tape
- 每次 evaluation 的全量特征

这样本地才能回答：

- 现在的信号定义是不是错了
- 如果换一套定义，会不会更赚钱

## 本地研究能力边界

这套数据设计能支持：

- research-grade replay
- signal / admission 参数 sweep
- target / timeout / maker proxy 的离线比较
- 不同 market type 的收益与 rejection 归因

这套数据设计 **不承诺**：

- 交易所级逐笔完美重放
- 真实队列位置与隐藏流动性的精确还原

一句话边界：

`目标是“找到可能赚钱的参数区间”，不是“把线上逐笔成交一模一样复刻出来”。`

## 成功标准

本轮改造完成后，一次 `paper` 或 `live-mock` 的 `pulse` session 下载到本地，必须能做到：

1. 离线读取全量 Poly / Binance / Deribit tape
2. 离线读取每一次 `evaluation_attempt` 的完整特征行
3. 明确算出：
   - 哪类 rejection 占比最高
   - 哪类市场最常处于 `ready`
   - 哪些 `ready` 市场仍然没有定价结果
4. 对同一时刻的理论 entry，离线比较不同退出参数
5. 不依赖服务器在线查询，就能完成首轮参数归因分析

## 非目标

本轮不做：

- 全 Deribit 期权链无差别 L2 存储
- 所有历史市场的长期归档治理
- 完整 queue reconstruction
- 研究 UI / dashboard

这些都可以建立在本轮数据契约之上，后续再扩。
