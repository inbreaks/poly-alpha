# Pulse Dislocation Continuation Matrix 设计

## 目标

在已经证伪“`structure_core` 脉冲后会在 `15m` 内回补到可赚钱退出”的前提下，最小化地验证另一个更贴近样本路径的假设：

`这些 pulse 更像 post-pulse continuation / failed rebound，若把原低价 side 视为触发器、改为同刻买入对手盘 claim，固定 15m 退出后是否能形成跨天正收益。`

这轮只回答这个问题，不重开新的大框架。

## 非目标

- 不做通用多空引擎
- 不实现理论 short 原 claim 的 PnL 口径
- 不补新的 outcome 宇宙字段
- 不扩任意阈值搜索 DSL
- 不改 Rust runtime

## 固定研究口径

### 触发器

延用上一轮已经证实有代表性的结构触发：

- `setup=directional_dislocation`
- `disp>=1500`
- `refill<=0.35`

也就是仍以 `structure_core` 作为 baseline trigger。

### 执行侧

不再买入触发侧 claim，改成：

- 在同一 `(symbol, ts_ms)` 上
- 选择对手盘 side 的 feature row 作为执行侧
- 用执行侧自己的 `entry_price`、`filled_notional_usd`、`min_tick_size`
- 用执行侧自己的 `15m` future outcome 算 realized pnl

这轮明确把它定义为：

- `complementary-claim continuation long`

而不是：

- 理论 short 原 claim

原因：

- 当前 outcome 只保存了单边 `future_max_* / deadline_bid`
- 没有 future min / deadline ask，不能严肃算 short 平仓
- 但 feature 主表在同刻双边覆盖接近完整，足够支撑“同刻买对手盘”这条可执行近似

### 固定 exit

继续固定为：

- `15m / any_touch / raw_tick / 1 tick`

原因：

- 用户目标仍然是 `15m` 内能退出的赚钱信号
- 这轮只是把 thesis 从 `rebound` 改成 `continuation`
- 不重新把 exit 搜索拉回讨论里

## 数据来源与最小实现

这轮不读取上一轮的 outcome 文件作为主输入，而是直接消费 full feature 文件。

原因：

- outcome 文件只保留 `flow_shape_valid` 行
- continuation 执行侧需要同刻对手盘 row
- 对手盘 row 往往不满足原来的 `flow_shape_valid`
- 但 full feature 文件里同刻双边配对覆盖接近完整，足够做 trigger/execution pairing

最小数据流：

1. 流式读取 full feature 文件
2. 以 `(symbol, ts_ms)` 分组
3. 从组内找出满足 `structure_core` 的 trigger row
4. 给每个 trigger row 绑定一条对手盘 execution row
5. 用 raw tape 重建 execution side 的 `15m` outcome
6. 在小样本 paired frame 上跑固定 continuation 策略矩阵

## continuation 确认层

这轮不再测试“回补强”，而是测试“修复弱 / 恶化继续”的固定确认层。

### baseline

- `structure_core`

### 单层确认

- `structure_core + refill1s_weak`
- `structure_core + refill3s_weak`
- `structure_core + spread_worse`
- `structure_core + depth_stalled`

### 组合确认

- `structure_core + refill3s_weak + spread_worse`
- `structure_core + refill3s_weak + depth_stalled`
- `structure_core + spread_worse + depth_stalled`
- `structure_core + refill3s_weak + spread_worse + depth_stalled`

### 固定阈值

最小硬编码阈值：

- `refill1s_weak`: `confirm_window_complete_1s && post_pulse_refill_ratio_1s <= 0.10`
- `refill3s_weak`: `confirm_window_complete_3s && post_pulse_refill_ratio_3s <= 0.15`
- `spread_worse`: `confirm_window_complete_3s && spread_snapback_bps <= 0.0`
- `depth_stalled`: `confirm_window_complete_3s && bid_depth_rebuild_ratio <= 1.00`

这些阈值的含义不是“最佳参数”，而是固定出一组与上一轮对称、且能表达 failed rebound 的 continuation matrix。

## 结果解释边界

若 continuation matrix 也失败，结论才更接近：

`当前 pulse/dislocation 既不是 15m mean reversion alpha，也不是简单 complementary-claim continuation alpha。`

若 continuation matrix 成功，结论也只能写成：

`当前样本更支持 complementary-claim continuation，而不是原 thesis 的 rebound exit。`

不允许把这轮结果直接吹成“已经证明可实盘 short 原 claim”。
