# PolyAlpha — Polymarket + CEX 对冲原型

这是一个拿 Polymarket 和 CEX 一起做联动交易的原型项目。

说人话就是：

- Polymarket 上是二元期权，价格看起来像“概率”
- Binance / OKX 上是 BTC 永续，价格是正常的币价
- 我们想做的是：盯住两边的价差、订单簿、市场阶段，找到能做对冲、能做回归、能做模拟盘和回测的那部分

现在这个仓库已经不是“只有设计稿”，也不是“全都做完了”的状态。

它现在更准确的定位是：

- Rust 侧：把实时数据、信号、风控、dry-run 执行、CLI 这些骨架先跑通
- Python 侧：把 BTC 样本构库、中文 CLI 报表和历史对照入口先做出来
- 真正的 Polymarket 实盘下单：还没接完，先别拿它当能直接上线的系统

这里有个很重要的默认口径，先说死：

- `scripts/btc_basis_backtest_report.py run` 现在默认不再自己算一套策略
- 它会调用 Rust 的 `backtest rust-replay` 当事实源
- Python 这一层主要负责中文报表、JSON / CSV 导出和历史对照入口
- 旧版 Python 回测保留在 `run-legacy` / `walk-forward`，仅供历史对照，不再是默认事实源

如果你想看当前这条 `BTC price-only basis MVP` 到底以什么口径为准，直接看这份事实源：

- [`docs/btc-price-only-basis-spec.md`](/Users/le/Documents/source-code/poly-alpha/docs/btc-price-only-basis-spec.md)

现在 Rust 纸面盘和 Python 回测已经共用同一套核心口径：

- 都按“上一根已完成的 CEX 1m close”做决策
- 都按 `market_rule` 算 terminal probability，而不是旧的线性 proxy
- 都支持显式区分 `Yes / No` 两条 token 腿

如果你自己写市场配置，建议把这两个字段带上：

- `market_question = "..."`
- `market_rule = { kind = "above|below|between", ... }`

没写 `market_rule` 时，Rust 现在只会把 `strike_price` 当成一个保底回退，并默认按 `above` 理解。

## 这项目现在能干什么

当前能直接跑的主线有 4 条：

### 1. 模拟盘

纯本地 mock 行情，配合 dry-run 执行器，把这几样东西串起来：

- 行情输入
- 信号生成
- 订单提交
- 撮合成交
- 风控状态
- 持仓变化

这个模式很适合先看“系统是不是活的”，而不是一上来就接真交易所。

### 2. 回测

我们有一套 Python 脚本，会：

- 从 Polymarket 抓历史价格
- 从 Binance 抓 BTC 1 分钟 K 线
- 对齐成分钟级数据
- 写进 DuckDB
- 跑一个最小可交付版本的基差/均值回归策略
- 用中文 CLI 报表把结果打出来

这套回测现在默认走的是更保守的口径：

- 样本尽量收紧到 BTC 价格型市场
- 收益按 Poly + CEX 组合口径来记
- 报表会告诉你钱到底用了多少，不再只给一个“初始资金 10 万、收益几块钱”的误导性数字
- 到期结算优先使用 Polymarket 官方 outcomePrices，缺失时才回退到本地 CEX 终值近似
- 默认 `run` 已经降成“Rust replay 的中文报表层”
- `run-legacy` / `walk-forward` 还在，但它们只是旧 Python 回测的历史对照入口

### 3. 实时行情检查

可以接：

- Polymarket
- Binance
- OKX

用来做两件事：

- 检查配置和 token id 对不对
- 检查实时 orderbook / funding / mark price 能不能拿到

这个模式不下真单，适合先验线路。

### 4. 市场发现

项目里有个命令能去 Polymarket Gamma 公共接口找活跃 BTC 市场，然后直接吐出可用的配置片段。

这个很好用，因为 Polymarket 的 token id 经常是第一道门槛。

## 策略这块，现在到底做到哪儿了

为了避免 README 写得太虚，这里直接分成“已经能跑”和“还在路上”。

### 1. Basis / 对冲回归

这是当前最完整的一块。

你可以把它理解成：

- 先看 Polymarket 的价格
- 再看 CEX 的 BTC 价格
- 用题面里的价位区间、剩余到期时间和过去一段 BTC 波动率，做一个因果一致的终值概率映射
- 如果 Poly 相对 CEX 看起来偏离太多，就开一组 Poly + CEX 的组合
- 等偏差回归，再平掉

当前实现状态：

- Rust 实时链路里，已经有 basis 信号和 dry-run 执行流程
- Python 回测里，已经会用上一根已完成的 CEX 1m bar、滚动实现波动率和市场题面结构来做因果回测
- 但这里的 CEX 对冲腿仍然是“数值 delta 近似”，不是完整的实盘级 Greeks / 成交引擎

### 2. DMM / 做市

这块已经有框架，也能在模拟链路里看到挂单状态更新，但还不是一个成熟的实盘做市系统。

你可以把它理解成：

- 系统会根据当前状态给出一组买卖报价
- dry-run 下可以看到挂单和撤单的变化
- 但还没做到那种可以直接上真实 Polymarket 做稳定做市的程度

当前实现状态：

- 报价状态流、订单状态和模拟执行是有的
- 更像“DMM 骨架 + 联调通路”，不是“已完成的实盘做市模块”

### 3. NegRisk / 完整集合套利

这块目前主要还是类型定义、信号结构和框架预留。

说直白点：

- 想法和接口有了
- 真正的检测和执行还没落完

所以现在不要把它当成“已经可用的第三条成熟策略”。

## 项目结构

仓库主结构是这样：

```text
poly-alpha/
+-- crates/
|   +-- polyalpha-core       # 公共类型、配置、事件、trait
|   +-- polyalpha-data       # Polymarket / Binance / OKX 数据接入
|   +-- polyalpha-engine     # 信号引擎、basis / DMM 骨架
|   +-- polyalpha-executor   # dry-run 执行、CEX 预览、执行状态机
|   +-- polyalpha-risk       # 风控、持仓跟踪、熔断
|   +-- polyalpha-cli        # 命令行入口
+-- config/                  # 默认配置和本地 overlay
+-- scripts/                 # DuckDB 构库、回测、报表脚本
+-- data/                    # 本地产物目录
+-- polyalpha-hedge-system.md
+-- polyalpha-hedge-system-v2.md
```

### 每个 crate 大概管什么

- `polyalpha-core`
  放公共类型，别的 crate 都靠它对齐说话方式。

- `polyalpha-data`
  接交易所数据，把不同来源的数据归一化成统一事件。

- `polyalpha-engine`
  收到归一化行情以后，决定要不要发 basis 信号、要不要更新 DMM 报价。

- `polyalpha-executor`
  负责把信号变成订单动作。
  现在 dry-run 是能跑的，Binance / OKX 的签名预览也能看，Polymarket 真执行还没接完。

- `polyalpha-risk`
  做仓位、暴露、日内亏损、熔断这类约束。

- `polyalpha-cli`
  把上面这些东西串起来，给你命令行入口。

## 技术栈

- 核心实时链路：Rust
- 异步运行时：Tokio
- 回测 / 构库 / 报表：Python
- 本地回测数据库：DuckDB
- 历史行情来源：Polymarket CLOB + Binance 1m K 线

这里有个很重要的现实情况：

- 这个项目现在不是“Rust 和 Python 一套完全共用的单一策略代码源”
- 而是 Rust 先把实时原型跑通，Python 先把回测和数据链路补上

所以你可以把它理解成：

- Rust 管实时原型
- Python 管历史验证

后面如果要继续进化，再考虑把两边进一步统一。

## 快速开始

### 1. 跑测试

```bash
cargo test --workspace
```

### 2. 看模拟盘

跑一小段本地 mock 行情：

```bash
cargo run -p polyalpha-cli -- sim run \
  --env default \
  --market-index 0 \
  --scenario basis-entry \
  --tick-interval-ms 0 \
  --print-every 1 \
  --max-ticks 6
```

看上一轮模拟盘产物：

```bash
cargo run -p polyalpha-cli -- sim inspect --env default --format table
```

### 3. 构建回测数据库

默认会抓近 90 天、已结算、BTC 价格型市场的历史样本。

这里有个故意收紧的变化：

- 默认只接受 Binance futures 历史源
- futures 拉不到时，不再悄悄回退 spot 来混样本

命令还是原来的：

```bash
cargo run -p polyalpha-cli -- backtest prepare-db \
  --output data/btc_basis_backtest_price_only_ready.duckdb
```

如果你想自己改时间范围：

```bash
cargo run -p polyalpha-cli -- backtest prepare-db \
  --start-date 2025-12-01 \
  --end-date 2026-03-17 \
  --output data/btc_basis_backtest_price_only_ready.duckdb
```

### 4. 检查回测数据库

```bash
cargo run -p polyalpha-cli -- backtest inspect-db \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  --show-failures
```

### 5. 运行回测

这里现在有两层入口，但默认口径只有一个：

- Rust `backtest rust-replay` 是事实源，负责真正的回放和记账
- Python `btc_basis_backtest_report.py run` 是默认报表层，负责把 Rust 结果转成中文 CLI / JSON / CSV
- `cargo run -p polyalpha-cli -- backtest run` 只是一个顺手入口，本质上还是会走上面这个 Python 报表层
- 旧版 Python 回测还在，但已经挪到 `run-legacy` / `walk-forward`

如果你想直接走默认报表层：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run
```

如果你习惯从 Rust CLI 进，也可以：

```bash
cargo run -p polyalpha-cli -- backtest run \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb
```

如果你想直接调用底层事实源，不经过 Python 报表层：

```bash
cargo run -p polyalpha-cli -- backtest rust-replay \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb
```

如果你想改组合仓位，可以直接传：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run \
  --position-notional-usd 500 \
  --max-capital-usage 0.20 \
  --cex-hedge-ratio 1.0 \
  --cex-margin-ratio 0.10
```

导出 JSON / CSV：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run \
  --report-json data/reports/btc-basis-report.json \
  --equity-csv data/reports/btc-basis-equity.csv
```

如果你要看旧 Python 口径，明确走历史对照入口：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run-legacy
```

如果你要看旧版样本外滚动评估，也明确走历史对照入口：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  walk-forward \
  --train-days 30 \
  --test-days 7 \
  --step-days 7 \
  --report-json data/reports/btc-basis-walk-forward.json
```

### 6. 发现活跃 BTC 市场

先找候选：

```bash
cargo run -p polyalpha-cli -- markets discover-btc --match-text 100k
```

选一个并生成 overlay：

```bash
cargo run -p polyalpha-cli -- markets discover-btc \
  --match-text 100k \
  --pick 0 \
  --output config/live.auto.toml
```

### 7. 做实时行情检查

```bash
cargo run -p polyalpha-cli -- live-data-check --env live.auto --market-index 0
```

### 8. 看纸面盘

持续轮询真实行情，但执行仍走 dry-run：

```bash
cargo run -p polyalpha-cli -- paper run \
  --env live.auto \
  --market-index 0 \
  --poll-interval-ms 2000 \
  --print-every 1
```

回看上一轮纸面盘 artifact：

```bash
cargo run -p polyalpha-cli -- paper inspect --env live.auto --format table
```

### 9. 看 CEX 下单预览

这个命令不会真的下单，主要用来看签名和请求参数是不是像你预期的那样：

```bash
cargo run -p polyalpha-cli -- live-exec-preview \
  --env live.auto \
  --market-index 0 \
  --exchange binance \
  --side buy \
  --order-type market \
  --qty 0.001
```

## 回测这块，怎么理解结果

这里非常值得单独说一下，因为这是最容易误读的地方。

现在的回测结果不要直接理解成：

“我拿 10 万美元满仓去跑，最后就赚这么多。”

更准确的理解方式是：

- 回测会告诉你组合净收益
- 同时也会告诉你峰值资金占用、峰值保证金占用、峰值资金使用率
- 所以你能看出来这套策略到底用了多少资本

这比只给一个 `initial_capital` 更靠谱。

当前这版回测已经比最开始诚实很多，但仍然要记住：

- 当前已经去掉了明显的未来函数，信号会用上一根已完成的 CEX bar，不再偷看整段样本极值
- 当前样本外检查已经可以用 `walk-forward` 跑，不再只剩单段全样本 replay
- 但 CEX 腿仍然是研究版数值 delta 近似，不是最终版严格 Delta 对冲回测
- 可以拿来筛样本、看方向、看参数
- 不能直接当实盘收益承诺

## 当前默认参数，别怎么理解错

回测里现在最重要的不是“初始资金”，而是这几个参数：

- `position-notional-usd`
  每次信号在 Poly 上打多大的名义金额

- `cex-hedge-ratio`
  CEX 对冲腿打多大

- `cex-margin-ratio`
  假设 CEX 这条腿要占多少保证金

- `max-capital-usage`
  整体最多允许用掉多少资金

换句话说：

- `initial-capital` 是总资金池
- 真正决定“每次到底出多大手”的，是仓位参数和资金使用率约束

### 费率口径，别怎么理解错

当前 Python 回测脚本默认是：

- `--fee-bps 2`
- `--slippage-bps 10`

这两个数字是研究参数，不是官方标准费率。

更具体地说：

- 当前脚本会把 Poly / CEX 两条腿都压成统一的 `fee_bps` 与 `slippage_bps` 来记账，这样做的好处是实验简单、参数扫描方便，但它不是执行级成本模型。
- 截至 2026-03-19，Polymarket 官方文档说明，多数市场免交易费，但 `crypto markets` 使用动态 taker fee，不是固定 `2 bps`。官方 fee table 在 `50c` 左右的有效费率峰值约为 `1.56%`。所以如果把当前 `--fee-bps 2` 直接理解成真实 Poly crypto fee，通常会偏乐观。
- Binance / OKX 的永续手续费按 `venue + maker/taker + VIP tier` 分层，也不是一个统一常数。当前 README 和脚本里的统一 `fee_bps` 更适合看成研究近似，而不是实盘会自然拿到的真实费率。以 OKX 官方 FAQ 的 BTCUSDT 永续示例为例，页面展示的是 `maker 0.02% / taker 0.05%`。
- 这也是为什么当前回测结果更适合拿来比较样本、方向和参数敏感性，而不适合直接拿来承诺实盘净收益。
- 官方参考：Polymarket https://docs.polymarket.com/trading/fees
- 官方参考：Binance https://www.binance.com/en/fee/trading
- 官方参考：OKX https://www.okx.com/en-us/help/trading-fee-rules-faq

## 当前边界

这部分我写得直接一点：

### 已经能用来干活的

- 模拟盘联调
- 回测构库
- 中文 CLI 报表
- 实时行情接线检查
- Binance / OKX 请求预览

### 还没做完的

- Polymarket 真下单执行
- 更完整的 DMM 实盘细节
- NegRisk 真正的检测和执行
- 严格版 CEX 对冲建模

### 所以最合理的使用方式是

先用它做这三件事：

1. 找市场
2. 跑模拟盘
3. 跑回测和样本清洗

然后再逐步推进到：

1. 实时行情长期运行
2. CEX 真下单
3. Polymarket 真下单

## 一句话总结

如果你想找一句最不误导人的描述，那就是：

这是一个“Polymarket + CEX 对冲交易系统”的可运行原型。

它已经能让我们：

- 看实时行情
- 跑模拟盘
- 构建本地历史数据库
- 做保守版 BTC 样本回测

但它还不是一个“Polymarket 真金白银自动跑全链路”的成品系统。

## License

MIT
