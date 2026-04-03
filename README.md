# PolyAlpha

PolyAlpha 是一个把 Polymarket 二元市场和 CEX 永续对冲交易串成同一条执行链的项目。

当前这条主线的目标不是“多策略大而全”，而是先把一条可信的交易事实链做扎实：

- 看到机会
- 生成计划
- 执行成交
- 复盘审计

现在仓库已经不是纯原型，也还不是 production-ready。

更准确地说：

- `sim / paper / live mock` 已经接到统一的 planner / executor 链上
- `backtest rust-replay` 已经是默认历史事实源
- 多市场 `WS + mock` 可以完整运行、落盘、回看
- 真正的 live-ready 还差“交易池治理 + 实时可交易性治理 + 长时间 soak 验收”

## 当前主线

当前主线是 `executable planner hard-cut`。

它解决的是一个核心问题：系统不能再让“mid-based 理论信号”直接驱动交易，必须让 `TradePlan` 成为唯一交易真相。

当前统一数据流是：

- WS 行情 / 对齐后的订单簿快照
- `OpenCandidate`
- `PlanningIntent`
- `ExecutionPlanner`
- `TradePlan`
- `ExecutionResult`
- artifact / audit / inspect

如果你是第一次进入项目，先记住 4 个默认口径：

1. `TradePlan` 是唯一执行真相，不是 engine 里的 mid-based 字段。
2. `ExecutionResult` 是唯一成交真相，不是理论 `expected_pnl`。
3. `paper / sim / live mock` 走共享执行栈，不允许再各自维护一套执行语义。
4. WS 是交易前提，不允许把“WS 有问题”静默降级成另一套看起来还能跑的交易语义。

## 当前状态

工程上，已经完成并验证的部分：

- executable planner 硬切主链已经落地
- `live run` / `live run-multi` 已接到共享 runtime
- 多市场 live-mock 结果会稳定写出 artifact，可通过 `live inspect` 回看
- 关键 planning 对象和 socket message 已带 `schema_version`
- market tier / retention reason 已进入 monitor 语义
- Binance / OKX / Polymarket 的若干 WS 解析防呆已经补上

还没完成到 production-ready 的部分：

- 交易池还不够干净，假机会仍然很多
- Polymarket 实时可交易性质量仍然不稳定
- 需要把拒绝原因进一步前置成“自动准入 / 踢出 / 冷却”规则
- 还没有完成长时间 live-mock soak 和逐笔成交抽查闭环

更完整的现状和未完成项见：

- [`docs/current-status.md`](docs/current-status.md)
- [`docs/project-memory.md`](docs/project-memory.md)

## 事实源文档

不要靠聊天记录理解项目。当前事实源如下：

- [`docs/superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md`](docs/superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md)
  executable planner 硬切重构事实源
- [`docs/superpowers/specs/2026-04-02-monitor-market-tier-retention-design.md`](docs/superpowers/specs/2026-04-02-monitor-market-tier-retention-design.md)
  市场分层与 retention UI 语义
- [`docs/btc-price-only-basis-spec.md`](docs/btc-price-only-basis-spec.md)
  BTC price-only basis 研究口径事实源
- [`docs/current-status.md`](docs/current-status.md)
  当前 done / doing / next 与 live-ready 阻塞
- [`docs/project-memory.md`](docs/project-memory.md)
  关键决策、不要回退的口径、项目记忆
- [`CONTRIBUTING.md`](CONTRIBUTING.md)
  团队协作和验证流程

## 仓库结构

```text
poly-alpha/
+-- crates/
|   +-- polyalpha-core       # 公共类型、配置、schema、socket message
|   +-- polyalpha-data       # Polymarket / Binance / OKX 数据接入与归一化
|   +-- polyalpha-engine     # alpha candidate 发现
|   +-- polyalpha-executor   # execution planner、order adapter、execution manager
|   +-- polyalpha-risk       # 风控、仓位与熔断
|   +-- polyalpha-cli        # CLI、runtime、monitor、paper/live/backtest 入口
+-- config/                  # 默认配置与环境配置
+-- docs/                    # 事实源、状态、团队协作文档
+-- scripts/                 # Python 报表、对账、诊断脚本
+-- data/                    # 本地 artifact / audit / 数据库产物
```

## 快速开始

### 1. 跑完整测试

```bash
cargo test -p polyalpha-core -p polyalpha-data -p polyalpha-risk -p polyalpha-executor -p polyalpha-cli -- --nocapture
```

### 2. 检查环境配置

```bash
./target/debug/polyalpha-cli check-config --env live-ready-smoke
./target/debug/polyalpha-cli check-config --env multi-market-active.fresh
```

### 3. 跑一轮最小 live-mock 烟雾验证

```bash
./target/debug/polyalpha-cli live run-multi \
  --env live-ready-smoke \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

回看上一轮结果：

```bash
./target/debug/polyalpha-cli live inspect --env live-ready-smoke --format table
```

### 4. 跑更接近真实机会分布的多市场 live-mock

```bash
./target/debug/polyalpha-cli live run-multi \
  --env multi-market-active.fresh \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

结果回看：

```bash
./target/debug/polyalpha-cli live inspect --env multi-market-active.fresh --format table
```

按审计日志复盘已平仓交易：

```bash
./target/debug/polyalpha-cli audit trade-timeline \
  --env multi-market-active.fresh
```

查看单笔完整平仓时间线：

```bash
./target/debug/polyalpha-cli audit trade-timeline \
  --env multi-market-active.fresh \
  --trade 1
```

### 5. 准备和运行回测

准备数据库：

```bash
cargo run -p polyalpha-cli -- backtest prepare-db \
  --output data/btc_basis_backtest_price_only_ready.duckdb
```

默认中文报表层：

```bash
.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run
```

底层 Rust 事实源：

```bash
cargo run -p polyalpha-cli -- backtest rust-replay \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb
```

## 当前优先级

如果你准备继续推进项目，不要同时开很多方向。当前最重要的是：

1. 清交易池
   让进入交易池的市场尽量都是真正可对冲、可成交、值得做的市场。
2. 做实时准入 / 踢出 / 冷却
   把 `poly_quote_stale`、`zero_cex_hedge_qty`、`poly_price_move_exceeded` 这类问题变成自动治理规则，而不是只留到最后一步拒绝。
3. 做长时间 live-mock soak
   验证运行稳定性、结果落盘、审计闭环和拒绝结构是否收敛。

## 当前边界

已经能用来做工程推进的：

- executable planner 主链
- 多市场 live mock 运行与 inspect
- 模拟盘 / 干跑执行栈
- 回测构库、Rust replay、中文报表
- 市场发现和配置生成

还不能误认为已经完成的：

- Polymarket 真下单上线
- production-grade DMM
- production-grade NegRisk
- 已经达成 live-ready

## 不要做的事

以下回退在这个项目里视为倒退，而不是修复：

- 重新把 engine 的 mid-based `expected_pnl` 当成可执行真相
- 重新让 executor 直接吃旧 signal，而不是只吃 `TradePlan`
- 为了“先跑起来”加 silent fallback，把 WS 交易语义偷偷改成别的口径
- 让 `paper / sim / live` 再次分叉成不同执行语义
- 删除 `zero_cex_hedge_qty` 这类护栏，只为了提高表面交易频次

## 团队协作入口

第一次参与项目，建议顺序：

1. 读本 README
2. 读 [`docs/current-status.md`](docs/current-status.md)
3. 读 [`docs/project-memory.md`](docs/project-memory.md)
4. 读 [`CONTRIBUTING.md`](CONTRIBUTING.md)
5. 需要深入交易链时，再读 hard-cut spec

## License

MIT
