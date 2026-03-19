# BTC Price-Only Basis MVP 策略事实源

这份文档只定义当前 `BTC price-only basis MVP` 的统一口径。

目标很简单：

- Python 回测按它实现
- Rust paper trading 按它对齐
- 后续 parity 验收按它比

不在这份文档里的内容，默认都不算正式口径。

## 1. 适用范围

当前只覆盖这一条主线：

- 标的：BTC 价格型 Polymarket 二元市场
- 组合：`Poly token + BTC CEX hedge`
- 模式：研究回测 + paper trading

当前**明确不包含**：

- DMM 做市
- NegRisk 套利
- Polymarket 真下单
- 更复杂的 Greeks / 批量执行优化

## 2. 样本范围

只保留 `price-only` BTC 市场。

入样规则：

- 问题文本必须明显是 BTC 价格题
- 允许的题面结构目前只支持：
  - `above $X`
  - `below $X`
  - `between $X and $Y`
- 像 `MicroStrategy announces ...`、`reserve act`、ETF 新闻、公司买币新闻这类事件，默认剔除

说明：

- 市场过滤先靠规则与误杀词表做第一层约束
- 后续如果发现漏网样本，再补白名单 / 回归测试，不靠人工记忆

## 3. 输入数据

回测和 paper 路径都围绕以下事实输入：

- Polymarket 历史 / 实时价格
- Binance BTC/USDT 1m K 线或对应实时价格
- 市场题面文本
- 市场到期时间
- Polymarket 官方 resolution 信息

默认 CEX 历史基准源：

- `binance_usdm futures`

原则：

- 默认不允许 silently 回退到现货镜像
- 如果为了调试强行允许 fallback，必须在元数据里明确留下痕迹

## 4. 时间对齐

这是当前最关键的因果约束。

### 4.1 决策时间

对任意一个 Polymarket 分钟点 `t`：

- 只能看到 `t-1` 那根**已经收完**的 CEX 1m close
- 不能使用同一分钟还在形成中的 CEX bar

等价写法：

```text
cex_ref_price(t) = close_cex(t - 1 minute)
```

### 4.2 构库口径

数据库里的 `basis_1m` 只是一个原始对齐表：

```text
raw_basis = poly_price - prev_completed_cex_close
```

它不是正式交易信号，只是调试 / 检查用。

### 4.3 回测信号口径

正式信号不直接用 `basis_1m.basis`。

正式信号定义：

```text
signal_basis = poly_price - causal_terminal_probability
```

其中 `causal_terminal_probability` 来自上一根已完成 CEX bar、滚动波动率、剩余到期时间、题面结构。

## 5. 题面解析

当前只支持三类规则：

### 5.1 Above

例子：

- `Will the price of Bitcoin be above $100,000 on March 31?`

YES payout:

```text
1, if terminal_price > strike
0, otherwise
```

### 5.2 Below

例子：

- `Will BTC be below $80,000 on April 1?`

YES payout:

```text
1, if terminal_price < strike
0, otherwise
```

### 5.3 Between

例子：

- `Will Bitcoin be between $90,000 and $95,000 on April 5?`

YES payout:

```text
1, if lower <= terminal_price < upper
0, otherwise
```

## 6. Fair Value

当前研究版 fair value 用结构化概率近似：

输入：

- `spot_price = prev_completed_cex_close`
- `sigma = trailing realized volatility`
- `minutes_to_expiry`
- `market_rule`

近似模型：

- 使用对数正态终值分布
- 用题面规则把终值分布映射到 YES 概率
- NO 概率直接用 `1 - YES`

注意：

- 这是研究版概率模型，不是实盘成交模型
- 但它比“整段样本 min/max 归一化”更接近因果口径

## 7. Delta

当前对冲腿使用数值 delta：

```text
delta = d(token_fair_value) / d(spot_price)
```

做法：

- 对 spot 做一个很小的上下 bump
- 分别重算 fair value
- 用中心差分估计 delta

方向约定：

- `hedge_qty_btc = - poly_shares * delta * hedge_ratio`

这意味着：

- 买入 YES token 后，如果 token 对 BTC 是正 delta，CEX 会偏空
- 买入 NO token 后，delta 可能为负，CEX 方向会自动翻转

## 8. 入场 / 出场

### 8.1 滚动 history

每个 token 各自维护一条 `signal_basis` 历史序列。

### 8.2 z-score

```text
z = (latest_signal_basis - rolling_mean) / rolling_std
```

### 8.3 规则

当前 MVP 只做一侧：

- `z <= -entry_z` 时开仓
- `abs(z) <= exit_z` 时平仓
- 如果持仓后 `z >= entry_z`，也允许触发离场

保守约束：

- 剩余到期时间必须大于 1 分钟
- Poly 与 CEX 价格必须都有效

## 9. 资金与记账

### 9.1 Poly 腿

- 开仓时按真实 premium 扣减现金
- 平仓时按真实卖出价值回收现金

### 9.2 CEX 腿

- 使用线性合约近似记账
- PnL 用 `qty * (exit - entry)` 计算
- 同时单独冻结初始保证金

### 9.3 资金占用

当前资金占用定义：

```text
capital_in_use = poly_entry_notional + cex_reserved_margin
```

### 9.4 上限

总资金池之上再加一个使用率约束：

```text
capital_in_use <= initial_capital * max_capital_usage
```

## 10. 费用

当前仍是研究级近似：

- Poly / CEX 暂时共用统一的 fee / slippage 参数
- 但报表会分腿拆开显示

这块后续允许继续细化，但不能再回到“完全不记成本”的口径。

## 11. 到期结算

到期结算优先级：

1. 先用 Polymarket 官方 `outcomePrices`
2. 只有官方 resolution 缺失时，才回退到本地 CEX 终值近似 payout

这个顺序不能反过来。

## 12. Walk-Forward 口径

当前 walk-forward 目标不是自动找“最优参数”，而是检查固定保守参数在样本外是否稳定。

规则：

- 每个测试窗都单独统计
- 测试窗允许使用之前一小段 warmup 数据填充滚动 history
- warmup 只更新指标，不允许提前开仓

这样做的目的：

- 保证测试窗开头不会因为 history 为空而失真
- 同时避免把训练窗持仓直接带进测试窗

## 13. Rust 对齐要求

Rust paper / sim 后续必须按下面这些点对齐：

- 相同的题面解析
- 相同的上一根已完成 CEX bar 对齐规则
- 相同的 rolling volatility
- 相同的 fair value / delta 定义
- 相同的 entry / exit 逻辑
- 相同的资金占用和结算顺序

如果 Rust 为了工程实现做了近似，必须显式写出来，不能静默偏离。

## 14. 当前已知边界

这份事实源承认下面这些现实：

- CEX 腿仍是研究版 delta 对冲近似
- 费用模型还不够细
- 执行层仍不是可成交收益模拟器
- 真正的 Polymarket live trading 还没接完

但这些边界必须被写清楚，不能再和未来函数、样本内偷看、口径漂移混在一起。
