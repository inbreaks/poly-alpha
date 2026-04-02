# Contributing to PolyAlpha

这份文档面向第一次加入项目的工程成员。

目标只有三个：

- 快速知道先看什么
- 快速知道本地怎么跑
- 快速知道哪些口径不能回退

## 第一次进入项目先做什么

建议顺序：

1. 读 [`README.md`](README.md)
2. 读 [`docs/current-status.md`](docs/current-status.md)
3. 读 [`docs/project-memory.md`](docs/project-memory.md)
4. 如果要改交易链，再读：
   [`docs/superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md`](docs/superpowers/specs/2026-03-27-executable-planner-hard-cut-design.md)

## 开发原则

### 1. 不要重新引入多套交易真相

后续任何改动，都默认服从这条主线：

- `OpenCandidate`
- `PlanningIntent`
- `TradePlan`
- `ExecutionResult`

如果你的改动试图重新让旧 signal、mid-based shares、旧 `expected_pnl` 回到执行路径，这就是回退。

### 2. 不要为“先跑起来”牺牲真实语义

不允许：

- silent WS fallback
- live / paper / sim 分叉执行口径
- 删除 `zero_cex_hedge_qty` 之类的护栏来换交易频次

### 3. 工程优先级高于“看起来有更多机会”

当前团队目标是 `live ready`，不是“更多表面信号”。

优先做：

- 交易池质量
- 实时可交易性
- 长时间 soak

## 常用命令

### 完整测试

```bash
cargo test -p polyalpha-core -p polyalpha-data -p polyalpha-risk -p polyalpha-executor -p polyalpha-cli -- --nocapture
```

### 检查配置

```bash
./target/debug/polyalpha-cli check-config --env live-ready-smoke
./target/debug/polyalpha-cli check-config --env multi-market-active.fresh
```

### 运行最小 live-mock 烟雾验证

```bash
./target/debug/polyalpha-cli live run-multi \
  --env live-ready-smoke \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

### 回看 live 结果

```bash
./target/debug/polyalpha-cli live inspect --env live-ready-smoke --format table
./target/debug/polyalpha-cli live inspect --env multi-market-active.fresh --format table
```

### 运行更真实的多市场 live-mock

```bash
./target/debug/polyalpha-cli live run-multi \
  --env multi-market-active.fresh \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

### 准备和运行回测

```bash
cargo run -p polyalpha-cli -- backtest prepare-db \
  --output data/btc_basis_backtest_price_only_ready.duckdb

.venv/bin/python scripts/btc_basis_backtest_report.py \
  --db-path data/btc_basis_backtest_price_only_ready.duckdb \
  run
```

## 改代码前先想清楚的问题

### 如果你改的是 engine

要确认：

- 它是否只产出 `OpenCandidate`
- 有没有把可执行字段偷偷塞回 engine

### 如果你改的是 planner / executor

要确认：

- `TradePlan` 仍然是唯一计划真相
- execution revalidation 是否还能解释清楚
- live / paper / sim 是否继续共享同一套语义

### 如果你改的是市场池 / monitor

要确认：

- `交易池 / 重点池 / 观察池` 语义是否仍然清晰
- retention reason 是否仍然可解释
- 这些变化是否真的帮助治理，而不是只改 UI

## 提交前最低验证

### 文档改动

至少检查：

- README 的命令是否真能运行
- 链接是否有效

### planner / executor / runtime 改动

至少运行：

```bash
cargo test -p polyalpha-executor -p polyalpha-cli -- --nocapture
```

如果涉及 live runtime，再跑一轮：

```bash
./target/debug/polyalpha-cli live run-multi \
  --env live-ready-smoke \
  --executor-mode mock \
  --max-ticks 60 \
  --poll-interval-ms 5000 \
  --print-every 12 \
  --warmup-klines 600
```

## 当前团队主线

如果你要继续推进项目，不要凭感觉开新方向。

当前默认优先级：

1. 市场池治理
2. 实时可交易性治理
3. 长时间 live-mock soak

## 不要提交什么

以下内容默认不应该提交：

- `tmp/`
- 本地诊断导出产物
- 私有 key / secret / `.env`
- 未经确认的大体量 `data/` 产物

## 分支建议

建议每条主题工作独立分支，避免把：

- 回测诊断
- live-ready 工程
- 市场清洗
- 文档整理

混到一笔难以审查的提交里。
